use aether_contracts::ExecutionResult;
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use serde_json::json;
use std::collections::BTreeMap;

use super::status as provider_status;

const OAUTH_ACCOUNT_BLOCK_PREFIX: &str = "[ACCOUNT_BLOCK] ";
const OAUTH_REFRESH_FAILED_PREFIX: &str = "[REFRESH_FAILED] ";
const OAUTH_EXPIRED_PREFIX: &str = "[OAUTH_EXPIRED] ";
const OAUTH_REQUEST_FAILED_PREFIX: &str = "[REQUEST_FAILED] ";
const CODEX_SPARK_LIMIT_NAME: &str = "GPT-5.3-Codex-Spark";
const CODEX_ADDITIONAL_QUOTA_WINDOWS_KEY: &str = "additional_quota_windows";
const CODEX_ACTIVE_LIMIT_HEADER: &str = "x-codex-active-limit";
const CODEX_HEADER_PREFIX: &str = "x-codex-";
const CODEX_LIMIT_NAME_HEADER_SUFFIX: &str = "-limit-name";

pub fn provider_auto_remove_banned_keys(config: Option<&serde_json::Value>) -> bool {
    config
        .and_then(|value| value.get("pool_advanced"))
        .and_then(serde_json::Value::as_object)
        .and_then(|object| object.get("auto_remove_banned_keys"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

pub fn provider_auto_remove_quota_exhausted_keys(config: Option<&serde_json::Value>) -> bool {
    config
        .and_then(|value| value.get("pool_advanced"))
        .and_then(serde_json::Value::as_object)
        .and_then(|object| object.get("auto_remove_quota_exhausted_keys"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

pub fn should_auto_remove_structured_reason(reason: Option<&str>) -> bool {
    provider_status::should_auto_remove_account_state(&provider_status::resolve_pool_account_state(
        None, None, reason,
    ))
}

fn oauth_reason_has_tag(reason: Option<&str>, tag: &str) -> bool {
    reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|reason| {
            reason
                .lines()
                .map(str::trim)
                .any(|line| line.starts_with(tag))
        })
}

fn oauth_refresh_failure_is_terminal(reason: Option<&str>) -> bool {
    reason
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|reason| {
            reason
                .lines()
                .map(str::trim)
                .filter(|line| line.starts_with(OAUTH_REFRESH_FAILED_PREFIX))
                .any(|line| {
                    let lowered = line.to_ascii_lowercase();
                    lowered.contains("invalid_grant")
                        || lowered.contains("invalid_refresh_token")
                        || lowered.contains("refresh_token_expired")
                        || lowered.contains("could not validate your refresh token")
                        || lowered.contains("refresh_token 无效")
                        || lowered.contains("已过期或已撤销")
                        || lowered.contains("已被使用并轮换")
                        || (lowered.contains("refresh token")
                            && ["expired", "revoked", "invalid", "reused"]
                                .iter()
                                .any(|keyword| lowered.contains(keyword)))
                })
        })
}

fn oauth_access_token_expired(key: &StoredProviderCatalogKey, now_unix_secs: u64) -> bool {
    let now_unix_secs = if now_unix_secs == 0 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .map(|duration| duration.as_secs())
            .unwrap_or(0)
    } else {
        now_unix_secs
    };
    key.expires_at_unix_secs
        .is_none_or(|expires_at| expires_at == 0 || expires_at <= now_unix_secs)
}

pub fn should_auto_remove_oauth_invalid_key(
    key: &StoredProviderCatalogKey,
    candidate_reason: Option<&str>,
    access_token_invalid_proven: bool,
    now_unix_secs: u64,
) -> bool {
    if should_auto_remove_structured_reason(candidate_reason)
        || should_auto_remove_structured_reason(key.oauth_invalid_reason.as_deref())
    {
        return true;
    }

    let refresh_token_failed = oauth_reason_has_tag(candidate_reason, OAUTH_REFRESH_FAILED_PREFIX)
        || oauth_reason_has_tag(
            key.oauth_invalid_reason.as_deref(),
            OAUTH_REFRESH_FAILED_PREFIX,
        );
    if !refresh_token_failed {
        return false;
    }
    if !oauth_refresh_failure_is_terminal(candidate_reason)
        && !oauth_refresh_failure_is_terminal(key.oauth_invalid_reason.as_deref())
    {
        return false;
    }

    access_token_invalid_proven
        || oauth_reason_has_tag(key.oauth_invalid_reason.as_deref(), OAUTH_EXPIRED_PREFIX)
        || oauth_access_token_expired(key, now_unix_secs)
}

pub fn normalize_string_id_list(values: Option<Vec<String>>) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for value in values.into_iter().flatten() {
        let trimmed = value.trim();
        if trimmed.is_empty() || !seen.insert(trimmed.to_string()) {
            continue;
        }
        out.push(trimmed.to_string());
    }
    (!out.is_empty()).then_some(out)
}

pub fn coerce_json_u64(value: &serde_json::Value) -> Option<u64> {
    match value {
        serde_json::Value::Number(number) => number.as_u64(),
        serde_json::Value::String(text) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
}

pub fn coerce_json_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

pub fn coerce_json_bool(value: &serde_json::Value) -> Option<bool> {
    match value {
        serde_json::Value::Bool(value) => Some(*value),
        serde_json::Value::String(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

pub fn coerce_json_string(value: Option<&serde_json::Value>) -> Option<String> {
    value
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub fn extract_execution_error_message(result: &ExecutionResult) -> Option<String> {
    if let Some(body_json) = result
        .body
        .as_ref()
        .and_then(|body| body.json_body.as_ref())
        .and_then(serde_json::Value::as_object)
    {
        if let Some(error) = body_json
            .get("error")
            .and_then(serde_json::Value::as_object)
        {
            if let Some(message) = error.get("message").and_then(serde_json::Value::as_str) {
                let trimmed = message.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
        }
        if let Some(message) = body_json.get("message").and_then(serde_json::Value::as_str) {
            let trimmed = message.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }

    result
        .error
        .as_ref()
        .map(|error| error.message.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Keeps the structured upstream error intact for classifiers that depend on fields such as
/// `error.code`, while retaining the execution-error fallback used by transport failures.
pub fn extract_execution_error_detail(result: &ExecutionResult) -> Option<String> {
    result
        .body
        .as_ref()
        .and_then(|body| body.json_body.as_ref())
        .and_then(|body| serde_json::to_string(body).ok())
        .filter(|value| !value.is_empty())
        .or_else(|| extract_execution_error_message(result))
}

pub fn quota_refresh_success_invalid_state(
    key: &StoredProviderCatalogKey,
) -> (Option<u64>, Option<String>) {
    let current_reason = key
        .oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    if current_reason.starts_with(OAUTH_ACCOUNT_BLOCK_PREFIX) {
        return (
            key.oauth_invalid_at_unix_secs,
            (!current_reason.is_empty()).then_some(current_reason.to_string()),
        );
    }
    (None, None)
}

pub fn parse_antigravity_usage_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let models = value.get("models")?.as_object()?;
    let mut quota_by_model = serde_json::Map::new();
    let mut opaque_key_display_index = 1usize;

    for (model_id, model_value) in models {
        let mut payload = serde_json::Map::new();
        let upstream_display_name = coerce_json_string(
            model_value
                .get("displayName")
                .or_else(|| model_value.get("display_name")),
        );
        if let Some(display_name) = friendly_quota_display_name(
            upstream_display_name,
            model_id,
            &mut opaque_key_display_index,
        ) {
            payload.insert("display_name".to_string(), json!(display_name));
        }

        let quota_info = model_value
            .get("quotaInfo")
            .and_then(serde_json::Value::as_object);
        let remaining_fraction = quota_info
            .and_then(|object| object.get("remainingFraction"))
            .and_then(coerce_json_f64);
        if let Some(remaining_fraction) = remaining_fraction {
            let used_percent = ((1.0 - remaining_fraction).max(0.0) * 100.0).min(100.0);
            payload.insert("remaining_fraction".to_string(), json!(remaining_fraction));
            payload.insert("used_percent".to_string(), json!(used_percent));
        }
        if let Some(reset_time) = quota_info
            .and_then(|object| object.get("resetTime"))
            .cloned()
            .filter(|value| !value.is_null())
        {
            payload.insert("reset_time".to_string(), reset_time);
        }
        quota_by_model.insert(model_id.clone(), serde_json::Value::Object(payload));
    }

    Some(json!({
        "updated_at": updated_at_unix_secs,
        "is_forbidden": false,
        "forbidden_reason": serde_json::Value::Null,
        "forbidden_at": serde_json::Value::Null,
        "quota_by_model": quota_by_model,
    }))
}

pub fn parse_antigravity_quota_summary_response(
    value: &serde_json::Value,
) -> Option<serde_json::Value> {
    let groups = value.get("groups")?.as_array()?;
    let mut parsed_groups = Vec::new();

    for (group_index, group) in groups.iter().enumerate() {
        let Some(group) = group.as_object() else {
            continue;
        };
        let group_id = coerce_json_string(
            group
                .get("groupId")
                .or_else(|| group.get("group_id"))
                .or_else(|| group.get("id")),
        );
        let display_name = coerce_json_string(
            group
                .get("displayName")
                .or_else(|| group.get("display_name")),
        )
        .unwrap_or_else(|| format!("Quota group {}", group_index + 1));
        let description = coerce_json_string(group.get("description"));
        let mut parsed_buckets = Vec::new();

        for (bucket_index, bucket) in group
            .get("buckets")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .enumerate()
        {
            let Some(bucket) = bucket.as_object() else {
                continue;
            };
            let bucket_id = coerce_json_string(
                bucket
                    .get("bucketId")
                    .or_else(|| bucket.get("bucket_id"))
                    .or_else(|| bucket.get("id")),
            )
            .unwrap_or_else(|| format!("bucket-{}", bucket_index + 1));
            let window = coerce_json_string(bucket.get("window"));
            let remaining_fraction = bucket
                .get("remainingFraction")
                .or_else(|| bucket.get("remaining_fraction"))
                .and_then(coerce_json_f64)
                .map(|value| value.clamp(0.0, 1.0));
            let reset_time = bucket
                .get("resetTime")
                .or_else(|| bucket.get("reset_time"))
                .cloned()
                .filter(|value| !value.is_null());
            let bucket_display_name = coerce_json_string(
                bucket
                    .get("displayName")
                    .or_else(|| bucket.get("display_name")),
            );
            let bucket_description = coerce_json_string(bucket.get("description"));

            if window.is_none()
                && remaining_fraction.is_none()
                && reset_time.is_none()
                && bucket_display_name.is_none()
                && bucket_description.is_none()
            {
                continue;
            }

            let mut parsed_bucket = serde_json::Map::new();
            parsed_bucket.insert("bucket_id".to_string(), json!(bucket_id));
            if let Some(window) = window {
                parsed_bucket.insert("window".to_string(), json!(window));
            }
            if let Some(remaining_fraction) = remaining_fraction {
                parsed_bucket.insert("remaining_fraction".to_string(), json!(remaining_fraction));
                parsed_bucket.insert(
                    "used_percent".to_string(),
                    json!((1.0 - remaining_fraction) * 100.0),
                );
                parsed_bucket.insert(
                    "is_exhausted".to_string(),
                    json!(remaining_fraction <= 1e-6),
                );
            }
            if let Some(reset_time) = reset_time {
                parsed_bucket.insert("reset_time".to_string(), reset_time);
            }
            if let Some(display_name) = bucket_display_name {
                parsed_bucket.insert("display_name".to_string(), json!(display_name));
            }
            if let Some(description) = bucket_description {
                parsed_bucket.insert("description".to_string(), json!(description));
            }
            parsed_buckets.push(serde_json::Value::Object(parsed_bucket));
        }

        if parsed_buckets.is_empty() {
            continue;
        }
        let mut parsed_group = serde_json::Map::new();
        if let Some(group_id) = group_id {
            parsed_group.insert("group_id".to_string(), json!(group_id));
        }
        parsed_group.insert("display_name".to_string(), json!(display_name));
        if let Some(description) = description {
            parsed_group.insert("description".to_string(), json!(description));
        }
        parsed_group.insert(
            "buckets".to_string(),
            serde_json::Value::Array(parsed_buckets),
        );
        parsed_groups.push(serde_json::Value::Object(parsed_group));
    }

    (!parsed_groups.is_empty()).then_some(serde_json::Value::Array(parsed_groups))
}

/// Windows reported by `GET /api/oauth/usage`, as `(response key, metadata prefix)`.
pub const CLAUDE_CODE_USAGE_WINDOWS: [(&str, &str); 4] = [
    ("five_hour", "five_hour"),
    ("seven_day", "seven_day"),
    ("seven_day_sonnet", "seven_day_sonnet"),
    ("seven_day_overage_included", "seven_day_fable"),
];

/// Parses the Anthropic OAuth usage response into the `claude_code` metadata bucket.
///
/// Each window carries `utilization` (percent, 0-100) and `resets_at` (RFC 3339).
/// Windows that are absent or `null` (e.g. plans without a Sonnet/Fable window) are
/// skipped; `None` is returned when no window is present at all.
pub fn parse_claude_code_oauth_usage_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    let mut bucket = serde_json::Map::new();
    for (response_key, prefix) in CLAUDE_CODE_USAGE_WINDOWS {
        let Some(window) = root
            .get(response_key)
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };
        let utilization = window
            .get("utilization")
            .and_then(|value| match value {
                serde_json::Value::Number(number) => number.as_f64(),
                serde_json::Value::String(text) => text.trim().parse::<f64>().ok(),
                _ => None,
            })
            .filter(|value| value.is_finite());
        let reset_at = window
            .get("resets_at")
            .and_then(serde_json::Value::as_str)
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(text.trim()).ok())
            .and_then(|time| u64::try_from(time.timestamp()).ok());
        if utilization.is_none() && reset_at.is_none() {
            continue;
        }
        if let Some(utilization) = utilization {
            bucket.insert(
                format!("{prefix}_used_percent"),
                serde_json::json!(utilization.clamp(0.0, 100.0)),
            );
        }
        if let Some(reset_at) = reset_at {
            bucket.insert(format!("{prefix}_reset_at"), serde_json::json!(reset_at));
        }
    }
    if bucket.is_empty() {
        return None;
    }
    // Explicit null so a refresh overwrites (rather than keeps) credits that were used up.
    bucket.insert(
        "reset_credits".to_string(),
        parse_claude_code_reset_credits(root, updated_at_unix_secs)
            .unwrap_or(serde_json::Value::Null),
    );
    bucket.insert(
        "updated_at".to_string(),
        serde_json::json!(updated_at_unix_secs),
    );
    Some(serde_json::Value::Object(bucket))
}

/// Projects the `cedar_ember` block (returned with `?cedar_ember=1`) into the same
/// `reset_credits` shape codex uses. Upstream grant/organization ids are never copied; each
/// usable grant becomes one entry, and `available_count` sums their remaining resets.
fn parse_claude_code_reset_credits(
    root: &serde_json::Map<String, serde_json::Value>,
    now_unix_secs: u64,
) -> Option<serde_json::Value> {
    let grants = root
        .get("cedar_ember")
        .and_then(serde_json::Value::as_object)?
        .get("grants")
        .and_then(serde_json::Value::as_array)?;
    let parse_time = |grant: &serde_json::Map<String, serde_json::Value>, field: &str| {
        grant
            .get(field)
            .and_then(serde_json::Value::as_str)
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(text.trim()).ok())
            .and_then(|time| u64::try_from(time.timestamp()).ok())
    };
    let mut available_count = 0u64;
    let mut credits = Vec::new();
    for grant in grants.iter().filter_map(serde_json::Value::as_object) {
        let resets_left = grant
            .get("resets_left")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let has_clears = grant
            .get("clears")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|clears| !clears.is_empty());
        let paused = grant
            .get("paused")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false);
        let starts_at = parse_time(grant, "starts_at");
        let expires_at = parse_time(grant, "ends_at");
        if resets_left == 0
            || !has_clears
            || paused
            || starts_at.is_some_and(|value| value > now_unix_secs)
            || expires_at.is_some_and(|value| value <= now_unix_secs)
        {
            continue;
        }
        available_count += resets_left;
        let Some(expires_at) = expires_at else {
            continue;
        };
        credits.push(serde_json::json!({
            "display_key": format!("Key-{}", credits.len() + 1),
            "status": "available",
            "expires_at": expires_at,
            "remaining_seconds": expires_at - now_unix_secs,
        }));
    }
    if available_count == 0 {
        return None;
    }
    Some(serde_json::json!({
        "available_count": available_count,
        "updated_at": now_unix_secs,
        "detail_source": "claude_oauth_usage",
        "detail_status": "ok",
        "credits": credits,
    }))
}

/// Parses the `anthropic-ratelimit-unified-*` response headers into a partial `claude_code`
/// metadata bucket (same field names as [`parse_claude_code_oauth_usage_response`]).
///
/// Utilization headers are 0-1 fractions and are stored as percent; reset headers are Unix
/// seconds (millisecond values are normalized). Returns `None` when no window is reported.
pub fn parse_claude_code_usage_headers(
    headers: &BTreeMap<String, String>,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let normalized = headers
        .iter()
        .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect::<BTreeMap<_, _>>();
    let mut bucket = serde_json::Map::new();
    for (header_window, prefix) in [
        ("5h", "five_hour"),
        ("7d", "seven_day"),
        ("7d_oi", "seven_day_fable"),
    ] {
        let header = |suffix: &str| {
            normalized
                .get(&format!(
                    "anthropic-ratelimit-unified-{header_window}-{suffix}"
                ))
                .map(String::as_str)
        };
        if let Some(utilization) = header("utilization")
            .and_then(|value| value.parse::<f64>().ok())
            .filter(|value| value.is_finite())
        {
            bucket.insert(
                format!("{prefix}_used_percent"),
                serde_json::json!((utilization * 100.0).clamp(0.0, 100.0)),
            );
        }
        if let Some(reset_at) = header("reset")
            .and_then(|value| value.parse::<u64>().ok())
            .map(|value| {
                if value > 100_000_000_000 {
                    value / 1_000
                } else {
                    value
                }
            })
            .filter(|value| *value > 0)
        {
            bucket.insert(format!("{prefix}_reset_at"), serde_json::json!(reset_at));
        }
    }
    if bucket.is_empty() {
        return None;
    }
    bucket.insert(
        "updated_at".to_string(),
        serde_json::json!(updated_at_unix_secs),
    );
    Some(serde_json::Value::Object(bucket))
}

fn is_opaque_reset_credit_quota_identifier(value: &str) -> bool {
    value.trim().starts_with("RateLimitResetCredit_")
}

fn friendly_quota_display_name(
    candidate: Option<String>,
    quota_key: &str,
    opaque_key_display_index: &mut usize,
) -> Option<String> {
    let candidate = candidate
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    if let Some(candidate) = candidate.as_deref() {
        if !is_opaque_reset_credit_quota_identifier(candidate) {
            return Some(candidate.to_string());
        }
    }

    if is_opaque_reset_credit_quota_identifier(quota_key)
        || candidate
            .as_deref()
            .is_some_and(is_opaque_reset_credit_quota_identifier)
    {
        let label = format!("Key-{}", *opaque_key_display_index);
        *opaque_key_display_index += 1;
        return Some(label);
    }

    candidate
}

fn first_json_value_by_paths<'a>(
    value: &'a serde_json::Value,
    paths: &[&[&str]],
) -> Option<&'a serde_json::Value> {
    for path in paths {
        let mut current = value;
        let mut matched = true;
        for segment in *path {
            let Some(next) = current.get(*segment) else {
                matched = false;
                break;
            };
            current = next;
        }
        if matched {
            return Some(current);
        }
    }
    None
}

fn first_json_string_by_paths(value: &serde_json::Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| coerce_json_string(first_json_value_by_paths(value, &[*path])))
}

pub fn normalize_codex_plan_type(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
}

pub fn build_codex_quota_exhausted_fallback_metadata(
    plan_type: Option<&str>,
    updated_at_unix_secs: u64,
) -> serde_json::Value {
    let mut object = serde_json::Map::new();
    if let Some(plan_type) = normalize_codex_plan_type(plan_type) {
        object.insert(
            "plan_type".to_string(),
            serde_json::Value::String(plan_type),
        );
    }
    object.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    object.insert("primary_used_percent".to_string(), json!(100.0));
    if normalize_codex_plan_type(plan_type) != Some("free".to_string()) {
        object.insert("secondary_used_percent".to_string(), json!(100.0));
    }
    serde_json::Value::Object(object)
}

const CODEX_QUOTA_WINDOW_SUFFIXES: &[&str] = &[
    "used_percent",
    "reset_seconds",
    "reset_after_seconds",
    "reset_at",
    "next_reset_at",
    "window_minutes",
];
const CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS: u64 = 30;
// Header and WHAM observations for the same active limit can be a few requests apart. Keep the
// historical-contamination fingerprint tolerant of that small movement, but require a close
// usage match so coincidentally aligned account and Spark reset deadlines are not enough.
const CODEX_QUOTA_CONTAMINATION_USAGE_TOLERANCE_PERCENT: f64 = 5.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexQuotaWindowCoverage {
    Patch,
    AccountSnapshot,
    FullSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexQuotaMergeContext<'a> {
    pub observed_at_unix_secs: u64,
    pub request_started_at_unix_ms: Option<u64>,
    pub request_order_id: Option<&'a str>,
    /// Reset generation captured before the upstream request was sent.
    /// Once a key has entered generation 1, an absent or different value is
    /// treated as a pre-reset observation for account quota and metadata.
    pub observed_reset_generation: Option<u64>,
    /// Generation owned by an explicit reset reconciliation request. Normal
    /// quota observations leave this unset, but a complete account snapshot
    /// started after the reset fence may still reconcile a delayed reset.
    pub authoritative_reset_generation: Option<u64>,
    /// Non-secret credential generation captured with the transport snapshot.
    pub observed_credential_generation: Option<&'a str>,
    /// Identifies the reset-credit fence that authorized this observation.
    /// Retained for generation-0 rolling-upgrade compatibility.
    pub account_reset_fence_id: Option<&'a str>,
    pub coverage: CodexQuotaWindowCoverage,
}

impl<'a> CodexQuotaMergeContext<'a> {
    fn request_order(self) -> Option<CodexQuotaRequestOrder<'a>> {
        codex_quota_request_order(self.request_started_at_unix_ms, self.request_order_id)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodexQuotaMergeOutcome {
    pub metadata: serde_json::Value,
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodexQuotaWindowFamily {
    Account,
    Spark,
}

impl CodexQuotaWindowFamily {
    fn watermark_key(self) -> &'static str {
        match self {
            Self::Account => CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_KEY,
            Self::Spark => CODEX_QUOTA_SPARK_REQUEST_WATERMARK_KEY,
        }
    }

    fn watermark_id_key(self) -> &'static str {
        match self {
            Self::Account => CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_ID_KEY,
            Self::Spark => CODEX_QUOTA_SPARK_REQUEST_WATERMARK_ID_KEY,
        }
    }
}

pub const CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_KEY: &str =
    "account_quota_request_started_at_unix_ms";
pub const CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_ID_KEY: &str = "account_quota_request_id";
pub const CODEX_QUOTA_SPARK_REQUEST_WATERMARK_KEY: &str = "spark_quota_request_started_at_unix_ms";
pub const CODEX_QUOTA_SPARK_REQUEST_WATERMARK_ID_KEY: &str = "spark_quota_request_id";
pub const CODEX_QUOTA_METADATA_REQUEST_WATERMARK_KEY: &str =
    "quota_metadata_request_started_at_unix_ms";
pub const CODEX_QUOTA_METADATA_REQUEST_WATERMARK_ID_KEY: &str = "quota_metadata_request_id";
pub const CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY: &str = "oauth_state_request_started_at_unix_ms";
pub const CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY: &str = "oauth_state_request_id";
pub const CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY: &str = "account_quota_reset_fence_unix_ms";
pub const CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY: &str = "account_quota_reset_fence_id";
pub const CODEX_QUOTA_ACCOUNT_RESET_PROCESSED_IDS_KEY: &str = "account_quota_reset_processed_ids";
pub const CODEX_QUOTA_ACCOUNT_RESET_PENDING_KEY: &str = "account_quota_reset_pending";
pub const CODEX_QUOTA_ACCOUNT_RESET_SEQUENCE_KEY: &str = "account_quota_reset_sequence";
pub const CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY: &str = "account_quota_reset_generation";
pub const CODEX_QUOTA_ACCOUNT_RESET_PENDING_GENERATION_KEY: &str =
    "account_quota_reset_pending_generation";
pub const CODEX_QUOTA_ACCOUNT_RESET_RESERVATION_KEY: &str = "account_quota_reset_reservation";
pub const CODEX_QUOTA_ACCOUNT_RESET_HISTORY_KEY: &str = "account_quota_reset_history";
pub const CODEX_CREDENTIAL_GENERATION_KEY: &str = "credential_generation";

pub fn codex_quota_account_reset_generation(codex: Option<&serde_json::Value>) -> u64 {
    codex
        .and_then(serde_json::Value::as_object)
        .and_then(|codex| codex.get(CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY))
        .and_then(coerce_json_u64)
        .unwrap_or(0)
}

pub fn codex_credential_generation(codex: Option<&serde_json::Value>) -> Option<&str> {
    codex
        .and_then(serde_json::Value::as_object)
        .and_then(|codex| codex.get(CODEX_CREDENTIAL_GENERATION_KEY))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

pub fn codex_credential_generation_matches(
    codex: Option<&serde_json::Value>,
    observed: Option<&str>,
) -> bool {
    let observed = observed.map(str::trim).filter(|value| !value.is_empty());
    codex_credential_generation(codex) == observed
}

fn codex_quota_observation_matches_reset_generation(
    object: &serde_json::Map<String, serde_json::Value>,
    context: CodexQuotaMergeContext<'_>,
) -> bool {
    let active_generation = object
        .get(CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY)
        .and_then(coerce_json_u64)
        .unwrap_or(0);
    if active_generation == 0 {
        context.observed_reset_generation.unwrap_or(0) == 0
    } else {
        context.observed_reset_generation == Some(active_generation)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CodexQuotaRequestOrder<'a> {
    started_at_unix_ms: u64,
    request_id: Option<&'a str>,
}

fn codex_quota_request_order<'a>(
    started_at_unix_ms: Option<u64>,
    request_id: Option<&'a str>,
) -> Option<CodexQuotaRequestOrder<'a>> {
    started_at_unix_ms.map(|started_at_unix_ms| CodexQuotaRequestOrder {
        started_at_unix_ms,
        request_id: request_id
            .map(str::trim)
            .filter(|request_id| !request_id.is_empty()),
    })
}

fn codex_quota_read_request_order<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    watermark_key: &str,
    watermark_id_key: &str,
) -> Option<CodexQuotaRequestOrder<'a>> {
    object
        .get(watermark_key)
        .and_then(coerce_json_u64)
        .map(|started_at_unix_ms| CodexQuotaRequestOrder {
            started_at_unix_ms,
            request_id: object
                .get(watermark_id_key)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|request_id| !request_id.is_empty()),
        })
}

fn codex_quota_request_order_is_stale(
    incoming: Option<CodexQuotaRequestOrder<'_>>,
    current: Option<CodexQuotaRequestOrder<'_>>,
) -> bool {
    match (incoming, current) {
        (Some(incoming), Some(current)) => incoming <= current,
        (None, Some(_)) => true,
        _ => false,
    }
}

/// Returns whether an incoming Codex observation is older than or identical
/// to the stored request order. Request ids break ties within one millisecond;
/// a legacy watermark without an id sorts before one that has an id.
pub fn codex_request_order_is_stale(
    incoming_started_at_unix_ms: Option<u64>,
    incoming_request_id: Option<&str>,
    stored_started_at_unix_ms: Option<u64>,
    stored_request_id: Option<&str>,
) -> bool {
    codex_quota_request_order_is_stale(
        codex_quota_request_order(incoming_started_at_unix_ms, incoming_request_id),
        codex_quota_request_order(stored_started_at_unix_ms, stored_request_id),
    )
}

/// Compares an OAuth-state observation against every persisted Codex response
/// watermark. Quota-only responses also prove request ordering, so an older
/// runtime authentication failure cannot override them.
pub fn codex_oauth_state_request_order_is_stale(
    codex: Option<&serde_json::Map<String, serde_json::Value>>,
    incoming_started_at_unix_ms: Option<u64>,
    incoming_request_id: Option<&str>,
) -> bool {
    let stored = codex.and_then(|codex| {
        [
            (
                CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY,
                CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY,
            ),
            (
                CODEX_QUOTA_METADATA_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_METADATA_REQUEST_WATERMARK_ID_KEY,
            ),
            (
                CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_ID_KEY,
            ),
            (
                CODEX_QUOTA_SPARK_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_SPARK_REQUEST_WATERMARK_ID_KEY,
            ),
        ]
        .into_iter()
        .filter_map(|(watermark_key, watermark_id_key)| {
            codex_quota_read_request_order(codex, watermark_key, watermark_id_key)
        })
        .max()
    });
    codex_quota_request_order_is_stale(
        codex_quota_request_order(incoming_started_at_unix_ms, incoming_request_id),
        stored,
    )
}

/// Compares a successful OAuth observation against every persisted Codex
/// response watermark. Equality is allowed because quota persistence and the
/// success effect may independently process the same upstream response.
pub fn codex_oauth_success_request_order_is_stale(
    codex: Option<&serde_json::Map<String, serde_json::Value>>,
    incoming_started_at_unix_ms: Option<u64>,
    incoming_request_id: Option<&str>,
) -> bool {
    let stored = codex.and_then(|codex| {
        [
            (
                CODEX_OAUTH_STATE_REQUEST_WATERMARK_KEY,
                CODEX_OAUTH_STATE_REQUEST_WATERMARK_ID_KEY,
            ),
            (
                CODEX_QUOTA_METADATA_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_METADATA_REQUEST_WATERMARK_ID_KEY,
            ),
            (
                CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_ACCOUNT_REQUEST_WATERMARK_ID_KEY,
            ),
            (
                CODEX_QUOTA_SPARK_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_SPARK_REQUEST_WATERMARK_ID_KEY,
            ),
        ]
        .into_iter()
        .filter_map(|(watermark_key, watermark_id_key)| {
            codex_quota_read_request_order(codex, watermark_key, watermark_id_key)
        })
        .max()
    });
    match (
        codex_quota_request_order(incoming_started_at_unix_ms, incoming_request_id),
        stored,
    ) {
        (Some(incoming), Some(stored)) => incoming < stored,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn codex_quota_request_order_is_newer(
    incoming: CodexQuotaRequestOrder<'_>,
    current: Option<CodexQuotaRequestOrder<'_>>,
) -> bool {
    current.is_none_or(|current| incoming > current)
}

fn codex_quota_write_request_order(
    object: &mut serde_json::Map<String, serde_json::Value>,
    watermark_key: &str,
    watermark_id_key: &str,
    order: CodexQuotaRequestOrder<'_>,
) {
    object.insert(watermark_key.to_string(), json!(order.started_at_unix_ms));
    if let Some(request_id) = order.request_id {
        object.insert(watermark_id_key.to_string(), json!(request_id));
    } else {
        object.remove(watermark_id_key);
    }
}

fn codex_quota_is_request_order_key(key: &str) -> bool {
    key == CODEX_QUOTA_METADATA_REQUEST_WATERMARK_KEY
        || key == CODEX_QUOTA_METADATA_REQUEST_WATERMARK_ID_KEY
        || [
            CodexQuotaWindowFamily::Account,
            CodexQuotaWindowFamily::Spark,
        ]
        .into_iter()
        .any(|family| key == family.watermark_key() || key == family.watermark_id_key())
}

fn codex_quota_is_reset_fence_key(key: &str) -> bool {
    matches!(
        key,
        CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_PROCESSED_IDS_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_PENDING_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_SEQUENCE_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_PENDING_GENERATION_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_RESERVATION_KEY
            | CODEX_QUOTA_ACCOUNT_RESET_HISTORY_KEY
            | CODEX_CREDENTIAL_GENERATION_KEY
    )
}

#[derive(Debug, Clone, Copy)]
struct CodexQuotaAccountResetFence<'a> {
    unix_ms: u64,
    id: &'a str,
    pending: bool,
}

fn codex_quota_account_reset_fence(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<CodexQuotaAccountResetFence<'_>> {
    let unix_ms = object
        .get(CODEX_QUOTA_ACCOUNT_RESET_FENCE_UNIX_MS_KEY)
        .and_then(coerce_json_u64)
        .filter(|value| *value > 0)?;
    let id = object
        .get(CODEX_QUOTA_ACCOUNT_RESET_FENCE_ID_KEY)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let pending = object
        .get(CODEX_QUOTA_ACCOUNT_RESET_PENDING_KEY)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some(CodexQuotaAccountResetFence {
        unix_ms,
        id,
        pending,
    })
}

fn codex_quota_reset_fence_authorizes(
    fence: CodexQuotaAccountResetFence<'_>,
    context: CodexQuotaMergeContext<'_>,
) -> bool {
    context
        .account_reset_fence_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        == Some(fence.id)
}

fn codex_quota_request_started_after_reset_fence(
    fence: CodexQuotaAccountResetFence<'_>,
    context: CodexQuotaMergeContext<'_>,
) -> bool {
    context
        .request_started_at_unix_ms
        .is_some_and(|started_at| started_at > fence.unix_ms)
}

fn codex_quota_reset_fence_blocks(
    fence: CodexQuotaAccountResetFence<'_>,
    context: CodexQuotaMergeContext<'_>,
) -> bool {
    !codex_quota_reset_fence_authorizes(fence, context)
        && !codex_quota_request_started_after_reset_fence(fence, context)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CodexQuotaWindowSlot {
    Primary,
    Secondary,
    SparkPrimary,
    SparkSecondary,
}

impl CodexQuotaWindowSlot {
    const ALL: [Self; 4] = [
        Self::Primary,
        Self::Secondary,
        Self::SparkPrimary,
        Self::SparkSecondary,
    ];

    fn prefix(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Secondary => "secondary",
            Self::SparkPrimary => "spark_primary",
            Self::SparkSecondary => "spark_secondary",
        }
    }

    fn family(self) -> CodexQuotaWindowFamily {
        match self {
            Self::Primary | Self::Secondary => CodexQuotaWindowFamily::Account,
            Self::SparkPrimary | Self::SparkSecondary => CodexQuotaWindowFamily::Spark,
        }
    }
}

#[derive(Debug, Clone)]
struct CodexQuotaWindowObservation {
    slot: CodexQuotaWindowSlot,
    fields: serde_json::Map<String, serde_json::Value>,
    window_minutes: Option<u64>,
    deadline: Option<u64>,
    disabled: bool,
}

impl CodexQuotaWindowObservation {
    fn active(&self) -> bool {
        !self.disabled && !self.fields.is_empty()
    }

    fn used_percent(&self) -> Option<f64> {
        self.fields
            .get("used_percent")
            .and_then(coerce_json_f64)
            .filter(|value| value.is_finite())
    }

    fn persist_deadline(&mut self) {
        if let Some(deadline) = self.deadline {
            self.fields.remove("next_reset_at");
            self.fields.insert("reset_at".to_string(), json!(deadline));
        }
    }
}

fn codex_quota_window_key(slot: CodexQuotaWindowSlot, suffix: &str) -> String {
    format!("{}_{suffix}", slot.prefix())
}

fn codex_quota_is_window_key(key: &str) -> bool {
    CodexQuotaWindowSlot::ALL.iter().any(|slot| {
        CODEX_QUOTA_WINDOW_SUFFIXES
            .iter()
            .any(|suffix| key == codex_quota_window_key(*slot, suffix))
    })
}

fn codex_quota_read_window(
    object: &serde_json::Map<String, serde_json::Value>,
    slot: CodexQuotaWindowSlot,
    observed_at_unix_secs: Option<u64>,
) -> Option<CodexQuotaWindowObservation> {
    let mut fields = serde_json::Map::new();
    for suffix in CODEX_QUOTA_WINDOW_SUFFIXES {
        let key = codex_quota_window_key(slot, suffix);
        let Some(raw) = object.get(&key) else {
            continue;
        };
        let normalized = match *suffix {
            "used_percent" => coerce_json_f64(raw)
                .filter(|value| value.is_finite())
                .map(|value| json!(value)),
            _ => coerce_json_u64(raw).map(|value| json!(value)),
        };
        if let Some(value) = normalized {
            fields.insert((*suffix).to_string(), value);
        }
    }
    if fields.is_empty() {
        return None;
    }

    let window_minutes = fields.get("window_minutes").and_then(coerce_json_u64);
    let disabled = window_minutes == Some(0);
    let explicit_deadline = fields
        .get("reset_at")
        .and_then(coerce_json_u64)
        .filter(|value| *value > 0)
        .or_else(|| {
            fields
                .get("next_reset_at")
                .and_then(coerce_json_u64)
                .filter(|value| *value > 0)
        });
    let reset_after_seconds = fields
        .get("reset_after_seconds")
        .and_then(coerce_json_u64)
        .or_else(|| fields.get("reset_seconds").and_then(coerce_json_u64));
    let deadline = explicit_deadline.or_else(|| {
        observed_at_unix_secs
            .zip(reset_after_seconds)
            .map(|(observed_at, reset_after)| observed_at.saturating_add(reset_after))
    });

    Some(CodexQuotaWindowObservation {
        slot,
        fields,
        window_minutes: window_minutes.filter(|value| *value > 0),
        deadline,
        disabled,
    })
}

fn codex_quota_read_family_windows(
    object: &serde_json::Map<String, serde_json::Value>,
    family: CodexQuotaWindowFamily,
    observed_at_unix_secs: Option<u64>,
) -> Vec<CodexQuotaWindowObservation> {
    CodexQuotaWindowSlot::ALL
        .iter()
        .copied()
        .filter(|slot| slot.family() == family)
        .filter_map(|slot| codex_quota_read_window(object, slot, observed_at_unix_secs))
        .collect()
}

pub fn codex_quota_metadata_has_account_windows(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|object| {
        !codex_quota_read_family_windows(object, CodexQuotaWindowFamily::Account, None).is_empty()
    })
}

pub fn codex_quota_metadata_has_spark_windows(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|object| {
        !codex_quota_read_family_windows(object, CodexQuotaWindowFamily::Spark, None).is_empty()
    })
}

fn codex_quota_same_window_identity(
    current: &CodexQuotaWindowObservation,
    incoming: &CodexQuotaWindowObservation,
) -> bool {
    match (current.window_minutes, incoming.window_minutes) {
        (Some(current), Some(incoming)) => current == incoming,
        // Old metadata did not always carry a duration. Only fall back to its
        // storage slot when at least one side has that legacy shape.
        _ => current.slot == incoming.slot,
    }
}

fn codex_quota_same_window_generation(
    left: &CodexQuotaWindowObservation,
    right: &CodexQuotaWindowObservation,
) -> bool {
    codex_quota_same_window_duration(left, right)
        && left
            .deadline
            .zip(right.deadline)
            .is_some_and(|(left, right)| {
                left.abs_diff(right) <= CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS
            })
}

fn codex_quota_same_contamination_fingerprint(
    account: &CodexQuotaWindowObservation,
    spark: &CodexQuotaWindowObservation,
) -> bool {
    codex_quota_same_window_generation(account, spark)
        && account
            .used_percent()
            .zip(spark.used_percent())
            .is_some_and(|(account, spark)| {
                (account - spark).abs() <= CODEX_QUOTA_CONTAMINATION_USAGE_TOLERANCE_PERCENT
            })
}

fn codex_quota_same_window_duration(
    left: &CodexQuotaWindowObservation,
    right: &CodexQuotaWindowObservation,
) -> bool {
    left.window_minutes
        .zip(right.window_minutes)
        .is_some_and(|(left, right)| left > 0 && left == right)
}

fn codex_quota_merge_same_window(
    current: &CodexQuotaWindowObservation,
    incoming: &CodexQuotaWindowObservation,
) -> CodexQuotaWindowObservation {
    // A deadline must be present on both sides to prove a natural generation
    // change. Legacy metadata without one stays monotonic until a later pair of
    // observations establishes the deadline.
    if current
        .deadline
        .zip(incoming.deadline)
        .is_some_and(|(current, incoming)| {
            incoming.saturating_add(CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS) < current
        })
    {
        return current.clone();
    }

    if current
        .deadline
        .zip(incoming.deadline)
        .is_some_and(|(current, incoming)| {
            incoming > current.saturating_add(CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS)
        })
    {
        let mut next = incoming.clone();
        next.persist_deadline();
        return next;
    }

    let mut merged = current.clone();
    for (suffix, value) in &incoming.fields {
        if suffix != "used_percent" {
            merged.fields.insert(suffix.clone(), value.clone());
        }
    }
    merged.window_minutes = incoming.window_minutes.or(current.window_minutes);
    // Keep the established deadline when observations differ only by normal
    // countdown/clock jitter so repeated responses cannot inch it forward.
    merged.deadline = current.deadline.or(incoming.deadline);
    merged.persist_deadline();

    let used_percent = match (current.used_percent(), incoming.used_percent()) {
        (Some(current), Some(incoming)) => Some(current.max(incoming)),
        (Some(current), None) => Some(current),
        (None, Some(incoming)) => Some(incoming),
        (None, None) => None,
    };
    if let Some(used_percent) = used_percent {
        merged
            .fields
            .insert("used_percent".to_string(), json!(used_percent));
    } else {
        merged.fields.remove("used_percent");
    }
    merged
}

fn codex_quota_merge_stale_same_window_usage(
    current: &CodexQuotaWindowObservation,
    incoming: &CodexQuotaWindowObservation,
) -> CodexQuotaWindowObservation {
    let same_generation = match (current.deadline, incoming.deadline) {
        (Some(current), Some(incoming)) => {
            current.abs_diff(incoming) <= CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS
        }
        (None, None) => true,
        _ => false,
    };
    if !same_generation {
        return current.clone();
    }

    let Some(incoming_used_percent) = incoming.used_percent() else {
        return current.clone();
    };
    if current
        .used_percent()
        .is_some_and(|current_used_percent| current_used_percent >= incoming_used_percent)
    {
        return current.clone();
    }

    let mut merged = current.clone();
    merged
        .fields
        .insert("used_percent".to_string(), json!(incoming_used_percent));
    merged
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct CodexQuotaWindowAssignmentScore {
    unmatched: usize,
    worst_deadline_rank: u8,
    deadline_rank_sum: usize,
    deadline_distance_sum: u128,
    slot_mismatches: usize,
    assignment_key: Vec<usize>,
}

fn codex_quota_window_deadline_match_score(
    current: &CodexQuotaWindowObservation,
    incoming: &CodexQuotaWindowObservation,
) -> (u8, u64) {
    match (current.deadline, incoming.deadline) {
        (Some(current), Some(incoming)) => {
            let distance = current.abs_diff(incoming);
            (
                u8::from(distance > CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS),
                distance,
            )
        }
        (None, None) => (0, 0),
        _ => (2, 0),
    }
}

fn codex_quota_window_assignment_score(
    current: &[CodexQuotaWindowObservation],
    incoming: &[CodexQuotaWindowObservation],
    assignment: &[Option<usize>],
) -> CodexQuotaWindowAssignmentScore {
    let mut unmatched = 0;
    let mut worst_deadline_rank = 0;
    let mut deadline_rank_sum = 0;
    let mut deadline_distance_sum = 0u128;
    let mut slot_mismatches = 0;

    for (incoming_index, current_index) in assignment.iter().copied().enumerate() {
        if !incoming[incoming_index].active() {
            continue;
        }
        let Some(current_index) = current_index else {
            unmatched += 1;
            continue;
        };
        let current_window = &current[current_index];
        let incoming_window = &incoming[incoming_index];
        let (deadline_rank, deadline_distance) =
            codex_quota_window_deadline_match_score(current_window, incoming_window);
        worst_deadline_rank = worst_deadline_rank.max(deadline_rank);
        deadline_rank_sum += usize::from(deadline_rank);
        deadline_distance_sum += u128::from(deadline_distance);
        slot_mismatches += usize::from(current_window.slot != incoming_window.slot);
    }

    CodexQuotaWindowAssignmentScore {
        unmatched,
        worst_deadline_rank,
        deadline_rank_sum,
        deadline_distance_sum,
        slot_mismatches,
        assignment_key: assignment
            .iter()
            .map(|index| index.unwrap_or(usize::MAX))
            .collect(),
    }
}

fn codex_quota_search_window_assignments(
    current: &[CodexQuotaWindowObservation],
    incoming: &[CodexQuotaWindowObservation],
    incoming_index: usize,
    used_current: &mut [bool],
    assignment: &mut [Option<usize>],
    best: &mut Option<(CodexQuotaWindowAssignmentScore, Vec<Option<usize>>)>,
) {
    if incoming_index == incoming.len() {
        let score = codex_quota_window_assignment_score(current, incoming, assignment);
        if best
            .as_ref()
            .is_none_or(|(best_score, _)| score < *best_score)
        {
            *best = Some((score, assignment.to_vec()));
        }
        return;
    }

    assignment[incoming_index] = None;
    codex_quota_search_window_assignments(
        current,
        incoming,
        incoming_index + 1,
        used_current,
        assignment,
        best,
    );
    if !incoming[incoming_index].active() {
        return;
    }

    for (current_index, current_window) in current.iter().enumerate() {
        if used_current[current_index]
            || !current_window.active()
            || !codex_quota_same_window_identity(current_window, &incoming[incoming_index])
        {
            continue;
        }
        used_current[current_index] = true;
        assignment[incoming_index] = Some(current_index);
        codex_quota_search_window_assignments(
            current,
            incoming,
            incoming_index + 1,
            used_current,
            assignment,
            best,
        );
        used_current[current_index] = false;
    }
    assignment[incoming_index] = None;
}

fn codex_quota_match_windows(
    current: &[CodexQuotaWindowObservation],
    incoming: &[CodexQuotaWindowObservation],
) -> Vec<Option<usize>> {
    let mut best = None;
    codex_quota_search_window_assignments(
        current,
        incoming,
        0,
        &mut vec![false; current.len()],
        &mut vec![None; incoming.len()],
        &mut best,
    );
    best.map(|(_, assignment)| assignment)
        .unwrap_or_else(|| vec![None; incoming.len()])
}

fn codex_quota_reset_observation_proves_new_baseline(
    current: &[CodexQuotaWindowObservation],
    incoming: &[CodexQuotaWindowObservation],
) -> bool {
    let current = current
        .iter()
        .filter(|window| window.active())
        .cloned()
        .collect::<Vec<_>>();
    let incoming = incoming
        .iter()
        .filter(|window| window.active())
        .cloned()
        .collect::<Vec<_>>();
    if current.is_empty() {
        return !incoming.is_empty();
    }

    let incoming_is_confirmed_zero_baseline = !incoming.is_empty()
        && incoming
            .iter()
            .all(|window| window.used_percent() == Some(0.0));
    if incoming_is_confirmed_zero_baseline
        && current
            .iter()
            .all(|window| window.used_percent() == Some(0.0))
    {
        return true;
    }

    let window_matches = codex_quota_match_windows(&current, &incoming);
    incoming
        .iter()
        .enumerate()
        .any(|(incoming_index, incoming_window)| {
            let Some(current_index) = window_matches[incoming_index] else {
                return true;
            };
            let current_window = &current[current_index];
            if current_window
                .deadline
                .zip(incoming_window.deadline)
                .is_some_and(|(current, incoming)| {
                    incoming > current.saturating_add(CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS)
                })
            {
                return true;
            }
            current_window
                .used_percent()
                .zip(incoming_window.used_percent())
                .is_some_and(|(current, incoming)| incoming < current)
        })
}

fn codex_quota_write_family_windows(
    object: &mut serde_json::Map<String, serde_json::Value>,
    family: CodexQuotaWindowFamily,
    windows: &[CodexQuotaWindowObservation],
) {
    for slot in CodexQuotaWindowSlot::ALL
        .iter()
        .copied()
        .filter(|slot| slot.family() == family)
    {
        for suffix in CODEX_QUOTA_WINDOW_SUFFIXES {
            object.remove(&codex_quota_window_key(slot, suffix));
        }
    }
    for window in windows.iter().filter(|window| window.active()) {
        for (suffix, value) in &window.fields {
            object.insert(codex_quota_window_key(window.slot, suffix), value.clone());
        }
    }
}

fn codex_quota_stabilize_legacy_deadlines(
    current: &serde_json::Map<String, serde_json::Value>,
    merged: &mut serde_json::Map<String, serde_json::Value>,
) {
    let observed_at = current.get("updated_at").and_then(coerce_json_u64);
    for family in [
        CodexQuotaWindowFamily::Account,
        CodexQuotaWindowFamily::Spark,
    ] {
        let mut windows = codex_quota_read_family_windows(current, family, observed_at);
        if windows.iter().any(|window| window.deadline.is_some()) {
            for window in &mut windows {
                window.persist_deadline();
            }
            codex_quota_write_family_windows(merged, family, &windows);
        }
    }
}

fn codex_quota_family_authoritative(
    coverage: CodexQuotaWindowCoverage,
    family: CodexQuotaWindowFamily,
) -> bool {
    matches!(coverage, CodexQuotaWindowCoverage::FullSnapshot)
        || matches!(
            (coverage, family),
            (
                CodexQuotaWindowCoverage::AccountSnapshot,
                CodexQuotaWindowFamily::Account
            )
        )
}

fn codex_quota_processes_family(
    coverage: CodexQuotaWindowCoverage,
    family: CodexQuotaWindowFamily,
    incoming: &[CodexQuotaWindowObservation],
) -> bool {
    match coverage {
        CodexQuotaWindowCoverage::Patch => !incoming.is_empty(),
        CodexQuotaWindowCoverage::AccountSnapshot => family == CodexQuotaWindowFamily::Account,
        CodexQuotaWindowCoverage::FullSnapshot => true,
    }
}

fn codex_quota_apply_family(
    current_object: &serde_json::Map<String, serde_json::Value>,
    incoming_object: &serde_json::Map<String, serde_json::Value>,
    merged: &mut serde_json::Map<String, serde_json::Value>,
    family: CodexQuotaWindowFamily,
    context: CodexQuotaMergeContext<'_>,
) {
    let active_reset_generation = current_object
        .get(CODEX_QUOTA_ACCOUNT_RESET_GENERATION_KEY)
        .and_then(coerce_json_u64)
        .unwrap_or(0);
    let generation_matches =
        codex_quota_observation_matches_reset_generation(current_object, context);
    if family == CodexQuotaWindowFamily::Account && !generation_matches {
        return;
    }
    let current_observed_at = current_object.get("updated_at").and_then(coerce_json_u64);
    let current = codex_quota_read_family_windows(current_object, family, current_observed_at);
    let mut incoming = codex_quota_read_family_windows(
        incoming_object,
        family,
        Some(context.observed_at_unix_secs),
    );
    if context.coverage == CodexQuotaWindowCoverage::Patch
        && current.iter().filter(|window| window.active()).count() > 1
    {
        // A partial header reports upstream's primary/secondary name, while
        // paid accounts may store those windows in the opposite slots. Without
        // a duration there is no stable identity, so leave both windows alone.
        incoming.retain(|window| !window.active() || window.window_minutes.is_some());
    }
    if !codex_quota_processes_family(context.coverage, family, &incoming) {
        return;
    }

    let account_reset_fence = (family == CodexQuotaWindowFamily::Account)
        .then(|| codex_quota_account_reset_fence(current_object))
        .flatten();
    if active_reset_generation == 0
        && account_reset_fence.is_some_and(|fence| codex_quota_reset_fence_blocks(fence, context))
    {
        return;
    }
    let authoritative = codex_quota_family_authoritative(context.coverage, family);
    let pending_generation_matches = active_reset_generation > 0
        && current_object
            .get(CODEX_QUOTA_ACCOUNT_RESET_PENDING_GENERATION_KEY)
            .and_then(coerce_json_u64)
            == Some(active_reset_generation);
    let generation_authorizes_reset = pending_generation_matches
        && (context.authoritative_reset_generation == Some(active_reset_generation)
            || account_reset_fence.is_some_and(|fence| {
                codex_quota_request_started_after_reset_fence(fence, context)
            }));
    let legacy_fence_authorizes_reset = active_reset_generation == 0
        && account_reset_fence.is_some_and(|fence| {
            codex_quota_reset_fence_authorizes(fence, context)
                || codex_quota_request_started_after_reset_fence(fence, context)
        });
    let reset_baseline = account_reset_fence.is_some_and(|fence| {
        fence.pending
            && authoritative
            && incoming.iter().any(CodexQuotaWindowObservation::active)
            && codex_quota_reset_observation_proves_new_baseline(&current, &incoming)
            && (generation_authorizes_reset || legacy_fence_authorizes_reset)
    });
    if account_reset_fence.is_some_and(|fence| fence.pending) && !reset_baseline {
        return;
    }
    let stored_watermark = codex_quota_read_request_order(
        current_object,
        family.watermark_key(),
        family.watermark_id_key(),
    );
    let stale_family = !reset_baseline
        && codex_quota_request_order_is_stale(context.request_order(), stored_watermark);
    let window_matches = (!reset_baseline).then(|| codex_quota_match_windows(&current, &incoming));
    // A full wham snapshot reports the account and named model families together. Older
    // releases could copy the active Spark window into the account slot, after which its later
    // deadline made the normal monotonic merge reject the real account window forever. Keep the
    // conservative deadline rule unless both the stored Spark family and this authoritative
    // response prove that the stored account generation is actually a Spark generation.
    let incoming_spark = if family == CodexQuotaWindowFamily::Account
        && context.coverage == CodexQuotaWindowCoverage::FullSnapshot
    {
        codex_quota_read_family_windows(
            incoming_object,
            CodexQuotaWindowFamily::Spark,
            Some(context.observed_at_unix_secs),
        )
    } else {
        Vec::new()
    };
    let current_spark = if !incoming_spark.is_empty() {
        codex_quota_read_family_windows(
            current_object,
            CodexQuotaWindowFamily::Spark,
            current_observed_at,
        )
    } else {
        Vec::new()
    };
    let mut next = if reset_baseline || (authoritative && !stale_family) {
        Vec::new()
    } else {
        current
            .iter()
            .filter(|window| window.active())
            .cloned()
            .collect::<Vec<_>>()
    };

    for (incoming_index, incoming_window) in incoming
        .iter()
        .enumerate()
        .filter(|(_, window)| window.active())
    {
        let current_index = window_matches
            .as_ref()
            .and_then(|matches| matches[incoming_index]);
        let Some(current_index) = current_index else {
            if !stale_family {
                next.retain(|window| window.slot != incoming_window.slot);
                let mut accepted = incoming_window.clone();
                accepted.persist_deadline();
                next.push(accepted);
            }
            continue;
        };
        let repairs_model_scoped_account_window = !stale_family
            && codex_quota_same_window_duration(&current[current_index], incoming_window)
            && current[current_index]
                .deadline
                .zip(incoming_window.deadline)
                .is_some_and(|(current, incoming)| {
                    incoming.saturating_add(CODEX_QUOTA_RESET_DEADLINE_TOLERANCE_SECONDS) < current
                })
            && current_spark.iter().any(|spark_window| {
                codex_quota_same_contamination_fingerprint(&current[current_index], spark_window)
            })
            && incoming_spark.iter().any(|spark_window| {
                codex_quota_same_contamination_fingerprint(&current[current_index], spark_window)
            });
        let mut merged_window = if repairs_model_scoped_account_window {
            let mut accepted = incoming_window.clone();
            accepted.persist_deadline();
            accepted
        } else if stale_family {
            codex_quota_merge_stale_same_window_usage(&current[current_index], incoming_window)
        } else {
            codex_quota_merge_same_window(&current[current_index], incoming_window)
        };
        let target_slot = if authoritative && !stale_family {
            incoming_window.slot
        } else {
            current[current_index].slot
        };
        merged_window.slot = target_slot;
        next.retain(|window| window.slot != target_slot);
        next.push(merged_window);
    }

    if !authoritative && !stale_family {
        for incoming_window in incoming.iter().filter(|window| window.disabled) {
            next.retain(|window| window.slot != incoming_window.slot);
        }
    }
    next.sort_by_key(|window| window.slot);

    codex_quota_write_family_windows(merged, family, &next);
    if reset_baseline {
        merged.insert(
            CODEX_QUOTA_ACCOUNT_RESET_PENDING_KEY.to_string(),
            json!(false),
        );
    }
    if !stale_family {
        if let Some(incoming_order) = context
            .request_order()
            .filter(|incoming| codex_quota_request_order_is_newer(*incoming, stored_watermark))
        {
            codex_quota_write_request_order(
                merged,
                family.watermark_key(),
                family.watermark_id_key(),
                incoming_order,
            );
        }
    }
}

fn codex_quota_semantic_metadata(
    object: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    object
        .iter()
        .filter(|(key, _)| {
            key.as_str() != "updated_at"
                && !key.ends_with("_reset_seconds")
                && !key.ends_with("_reset_after_seconds")
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn codex_quota_is_account_status_key(key: &str) -> bool {
    matches!(key, "allowed" | "limit_reached")
}

fn codex_quota_merge_reset_credits(
    current_object: &serde_json::Map<String, serde_json::Value>,
    incoming: &serde_json::Value,
) -> serde_json::Value {
    let Some(incoming_object) = incoming.as_object() else {
        return incoming.clone();
    };
    let mut merged = current_object
        .get("reset_credits")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let failed_detail = incoming_object
        .get("detail_status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|status| status.trim().eq_ignore_ascii_case("failed"));
    if !failed_detail {
        return incoming.clone();
    }

    for (key, value) in incoming_object {
        // A failed readonly-detail request contributes diagnostics, not an
        // authoritative empty list. Keep the last known items and count so a
        // transient 429 cannot make reset credits disappear from the UI.
        if failed_detail
            && key == "credits"
            && value.as_array().is_some_and(|credits| credits.is_empty())
            && merged
                .get("credits")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|credits| !credits.is_empty())
        {
            continue;
        }
        merged.insert(key.clone(), value.clone());
    }

    serde_json::Value::Object(merged)
}

/// Merge a parsed Codex quota observation into the stored flat metadata.
///
/// Positive `window_minutes` values identify windows independently of the
/// primary/secondary storage slot. Within one reset deadline usage is
/// monotonic; advancing the deadline starts a new generation. Snapshot modes
/// replace the covered window families, while patch mode leaves absent windows
/// alone. A request-start/id watermark prevents a delayed request from
/// restoring a superseded window shape.
pub fn merge_codex_quota_metadata_snapshot(
    current: Option<&serde_json::Value>,
    incoming: &serde_json::Value,
    context: CodexQuotaMergeContext<'_>,
) -> Option<CodexQuotaMergeOutcome> {
    let incoming_object = incoming.as_object()?;
    let current_object = current
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if !codex_credential_generation_matches(current, context.observed_credential_generation) {
        return Some(CodexQuotaMergeOutcome {
            metadata: serde_json::Value::Object(current_object),
            changed: false,
        });
    }
    let mut merged = current_object.clone();
    codex_quota_stabilize_legacy_deadlines(&current_object, &mut merged);

    let stored_metadata_watermark = std::iter::once(codex_quota_read_request_order(
        &current_object,
        CODEX_QUOTA_METADATA_REQUEST_WATERMARK_KEY,
        CODEX_QUOTA_METADATA_REQUEST_WATERMARK_ID_KEY,
    ))
    .chain(
        [
            CodexQuotaWindowFamily::Account,
            CodexQuotaWindowFamily::Spark,
        ]
        .into_iter()
        .map(|family| {
            codex_quota_read_request_order(
                &current_object,
                family.watermark_key(),
                family.watermark_id_key(),
            )
        }),
    )
    .flatten()
    .max();
    let has_incoming_metadata = incoming_object.keys().any(|key| {
        key != "updated_at"
            && !codex_quota_is_account_status_key(key)
            && !codex_quota_is_request_order_key(key)
            && !codex_quota_is_reset_fence_key(key)
            && !codex_quota_is_window_key(key)
    });
    let stale_metadata = has_incoming_metadata
        && (codex_quota_request_order_is_stale(context.request_order(), stored_metadata_watermark)
            || !codex_quota_observation_matches_reset_generation(&current_object, context)
            || (codex_quota_account_reset_generation(Some(&serde_json::Value::Object(
                current_object.clone(),
            ))) == 0
                && codex_quota_account_reset_fence(&current_object)
                    .is_some_and(|fence| codex_quota_reset_fence_blocks(fence, context))));

    if has_incoming_metadata && !stale_metadata {
        for (key, value) in incoming_object {
            if key == "updated_at"
                || codex_quota_is_account_status_key(key)
                || codex_quota_is_request_order_key(key)
                || codex_quota_is_reset_fence_key(key)
                || codex_quota_is_window_key(key)
            {
                continue;
            }
            let value = if key == "reset_credits" {
                codex_quota_merge_reset_credits(&current_object, value)
            } else {
                value.clone()
            };
            merged.insert(key.clone(), value);
        }
        if let Some(incoming_order) = context.request_order().filter(|incoming| {
            codex_quota_request_order_is_newer(*incoming, stored_metadata_watermark)
        }) {
            codex_quota_write_request_order(
                &mut merged,
                CODEX_QUOTA_METADATA_REQUEST_WATERMARK_KEY,
                CODEX_QUOTA_METADATA_REQUEST_WATERMARK_ID_KEY,
                incoming_order,
            );
        }
    }

    // `allowed` and `limit_reached` describe the account family in WHAM and account-scoped
    // WebSocket observations. A newer Spark-only patch must not make those account signals look
    // stale, so order them against the account watermark rather than the newest watermark from
    // any quota family.
    let has_incoming_account_status = incoming_object
        .keys()
        .any(|key| codex_quota_is_account_status_key(key));
    let stored_account_watermark = codex_quota_read_request_order(
        &current_object,
        CodexQuotaWindowFamily::Account.watermark_key(),
        CodexQuotaWindowFamily::Account.watermark_id_key(),
    );
    let stale_account_status = has_incoming_account_status
        && (codex_quota_request_order_is_stale(context.request_order(), stored_account_watermark)
            || !codex_quota_observation_matches_reset_generation(&current_object, context)
            || (codex_quota_account_reset_generation(Some(&serde_json::Value::Object(
                current_object.clone(),
            ))) == 0
                && codex_quota_account_reset_fence(&current_object)
                    .is_some_and(|fence| codex_quota_reset_fence_blocks(fence, context))));
    if has_incoming_account_status && !stale_account_status {
        for key in ["allowed", "limit_reached"] {
            if let Some(value) = incoming_object.get(key) {
                merged.insert(key.to_string(), value.clone());
            }
        }
        if let Some(incoming_order) = context.request_order().filter(|incoming| {
            codex_quota_request_order_is_newer(*incoming, stored_account_watermark)
        }) {
            codex_quota_write_request_order(
                &mut merged,
                CodexQuotaWindowFamily::Account.watermark_key(),
                CodexQuotaWindowFamily::Account.watermark_id_key(),
                incoming_order,
            );
        }
    }

    codex_quota_apply_family(
        &current_object,
        incoming_object,
        &mut merged,
        CodexQuotaWindowFamily::Account,
        context,
    );
    codex_quota_apply_family(
        &current_object,
        incoming_object,
        &mut merged,
        CodexQuotaWindowFamily::Spark,
        context,
    );

    let changed =
        codex_quota_semantic_metadata(&current_object) != codex_quota_semantic_metadata(&merged);
    if changed {
        let updated_at_unix_secs = current_object
            .get("updated_at")
            .and_then(coerce_json_u64)
            .unwrap_or_default()
            .max(context.observed_at_unix_secs);
        merged.insert("updated_at".to_string(), json!(updated_at_unix_secs));
        Some(CodexQuotaMergeOutcome {
            metadata: serde_json::Value::Object(merged),
            changed: true,
        })
    } else {
        Some(CodexQuotaMergeOutcome {
            metadata: serde_json::Value::Object(current_object),
            changed: false,
        })
    }
}

fn codex_write_window(
    target: &mut serde_json::Map<String, serde_json::Value>,
    source: &serde_json::Map<String, serde_json::Value>,
    target_prefix: &str,
) {
    if let Some(value) = source.get("used_percent").and_then(coerce_json_f64) {
        target.insert(format!("{target_prefix}_used_percent"), json!(value));
    }
    if let Some(value) = source.get("reset_after_seconds").and_then(coerce_json_u64) {
        target.insert(format!("{target_prefix}_reset_after_seconds"), json!(value));
    }
    if let Some(value) = source.get("reset_at").and_then(coerce_json_u64) {
        target.insert(format!("{target_prefix}_reset_at"), json!(value));
    }
    if let Some(value) = source.get("window_minutes").and_then(coerce_json_u64) {
        target.insert(format!("{target_prefix}_window_minutes"), json!(value));
    }
    if let Some(value) = source
        .get("limit_window_seconds")
        .and_then(coerce_json_u64)
        .map(|seconds| seconds / 60)
    {
        target.insert(format!("{target_prefix}_window_minutes"), json!(value));
    }
}

fn codex_window_has_active_limit(source: &serde_json::Map<String, serde_json::Value>) -> bool {
    [
        "window_minutes",
        "limit_window_seconds",
        "reset_after_seconds",
        "reset_at",
    ]
    .iter()
    .any(|key| {
        source
            .get(*key)
            .and_then(coerce_json_u64)
            .is_some_and(|value| value > 0)
    }) || source
        .get("used_percent")
        .and_then(coerce_json_f64)
        .is_some_and(|value| value > 0.0)
}

fn codex_window_is_explicitly_disabled(
    source: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let used_percent_is_zero = source
        .get("used_percent")
        .and_then(coerce_json_f64)
        .is_some_and(|value| value == 0.0);
    let reset_after_is_zero = source
        .get("reset_after_seconds")
        .and_then(coerce_json_u64)
        .is_some_and(|value| value == 0);
    let reset_at_is_empty = source.get("reset_at").is_some_and(|value| {
        value.is_null()
            || value.as_str().is_some_and(|value| value.trim().is_empty())
            || coerce_json_u64(value).is_some_and(|value| value == 0)
    });
    let duration_is_zero = ["window_minutes", "limit_window_seconds"]
        .iter()
        .find_map(|key| source.get(*key).and_then(coerce_json_u64))
        .is_some_and(|value| value == 0);

    used_percent_is_zero && reset_after_is_zero && reset_at_is_empty && duration_is_zero
}

fn codex_write_disabled_window(
    target: &mut serde_json::Map<String, serde_json::Value>,
    target_prefix: &str,
) {
    target.insert(format!("{target_prefix}_window_minutes"), json!(0u64));
}

fn codex_find_spark_rate_limit(
    root: &serde_json::Map<String, serde_json::Value>,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    root.get("additional_rate_limits")
        .and_then(serde_json::Value::as_array)?
        .iter()
        .filter_map(serde_json::Value::as_object)
        .find(|item| {
            item.get("limit_name")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|name| name.trim() == CODEX_SPARK_LIMIT_NAME)
        })?
        .get("rate_limit")
        .and_then(serde_json::Value::as_object)
}

/// Preserve every additional Codex rate-limit bucket in a model-addressable
/// representation.  The upstream can add independent limits over time; do
/// not discard them merely because they are not one of the legacy flat
/// primary/secondary slots.
fn codex_additional_quota_windows(
    root: &serde_json::Map<String, serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut windows = Vec::new();
    let Some(items) = root
        .get("additional_rate_limits")
        .and_then(serde_json::Value::as_array)
    else {
        return windows;
    };

    for (index, item) in items.iter().enumerate() {
        let Some(item_object) = item.as_object() else {
            continue;
        };
        if item_object
            .get("limit_name")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|name| name.trim() == CODEX_SPARK_LIMIT_NAME)
        {
            continue;
        }
        let Some(limit_name) = ["limit_name", "metered_feature", "name", "id"]
            .iter()
            .find_map(|key| item_object.get(*key).and_then(serde_json::Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let model_identity = item_object
            .get("model")
            .or_else(|| item_object.get("model_name"))
            .or_else(|| item_object.get("model_id"))
            .cloned()
            .unwrap_or_else(|| json!(limit_name));
        let model_identities = item_object
            .get("models")
            .or_else(|| item_object.get("model_ids"))
            .cloned();
        let Some(rate_limit) = item_object
            .get("rate_limit")
            .and_then(serde_json::Value::as_object)
        else {
            continue;
        };

        for (slot, slot_name) in [
            ("primary_window", "primary"),
            ("secondary_window", "secondary"),
        ] {
            let Some(source) = rate_limit.get(slot).and_then(serde_json::Value::as_object) else {
                continue;
            };
            let used_percent = source.get("used_percent").and_then(coerce_json_f64);
            let reset_after_seconds = source
                .get("reset_after_seconds")
                .or_else(|| source.get("reset_seconds"))
                .and_then(coerce_json_u64);
            let reset_at = source.get("reset_at").and_then(coerce_json_u64);
            let window_minutes = source
                .get("limit_window_seconds")
                .or_else(|| source.get("window_seconds"))
                .and_then(coerce_json_u64)
                .map(|seconds| seconds.saturating_add(59) / 60)
                .filter(|minutes| *minutes > 0);
            if used_percent.is_none()
                && reset_after_seconds.is_none()
                && reset_at.is_none()
                && window_minutes.is_none()
            {
                continue;
            }

            let used_ratio = used_percent.map(|value| (value / 100.0).clamp(0.0, 1.0));
            let mut window = serde_json::Map::new();
            window.insert(
                "code".to_string(),
                json!(format!("additional_{index}_{slot_name}")),
            );
            window.insert("label".to_string(), json!(limit_name));
            window.insert("scope".to_string(), json!("model"));
            // The upstream limit name is the only stable identity available
            // for an additional bucket.  Keeping it in both fields allows
            // exact model matching and generic family matching downstream.
            window.insert("model".to_string(), model_identity.clone());
            if let Some(model_identities) = model_identities.clone() {
                window.insert("models".to_string(), model_identities);
            }
            window.insert("quota_group".to_string(), json!(limit_name));
            window.insert("limit_name".to_string(), json!(limit_name));
            window.insert("used_ratio".to_string(), json!(used_ratio));
            window.insert(
                "remaining_ratio".to_string(),
                json!(used_ratio.map(|value| (1.0 - value).max(0.0))),
            );
            window.insert(
                "is_exhausted".to_string(),
                json!(used_ratio.is_some_and(|value| value >= 1.0 - 1e-6)),
            );
            window.insert("reset_at".to_string(), json!(reset_at));
            window.insert("reset_seconds".to_string(), json!(reset_after_seconds));
            window.insert("window_minutes".to_string(), json!(window_minutes));
            windows.push(serde_json::Value::Object(window));
        }
    }
    windows
}

fn codex_reset_credits_container(
    root: &serde_json::Map<String, serde_json::Value>,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    [
        "rate_limit_reset_credits",
        "rateLimitResetCredits",
        "reset_credits",
        "resetCredits",
    ]
    .iter()
    .find_map(|key| root.get(*key).and_then(serde_json::Value::as_object))
}

fn codex_reset_credits_available_count(
    root: &serde_json::Map<String, serde_json::Value>,
) -> Option<u64> {
    let container = codex_reset_credits_container(root)?;
    [
        "available_count",
        "availableCount",
        "available",
        "remaining",
        "count",
    ]
    .iter()
    .find_map(|key| container.get(*key).and_then(coerce_json_u64))
}

fn codex_reset_credits_count_snapshot(
    available_count: u64,
    updated_at_unix_secs: u64,
) -> serde_json::Value {
    json!({
        "available_count": available_count,
        "updated_at": updated_at_unix_secs,
        "detail_source": "wham_usage",
        "detail_status": "not_requested",
        "credits": [],
    })
}

pub fn parse_codex_wham_usage_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    if root.is_empty() {
        return None;
    }

    let mut result = serde_json::Map::new();
    let plan_type =
        normalize_codex_plan_type(root.get("plan_type").and_then(serde_json::Value::as_str));
    if let Some(plan_type) = plan_type.as_ref() {
        result.insert("plan_type".to_string(), json!(plan_type));
    }

    let rate_limit = root
        .get("rate_limit")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    if let Some(allowed) = rate_limit.get("allowed").and_then(coerce_json_bool) {
        result.insert("allowed".to_string(), json!(allowed));
    }
    if let Some(limit_reached) = rate_limit.get("limit_reached").and_then(coerce_json_bool) {
        result.insert("limit_reached".to_string(), json!(limit_reached));
    }
    let primary_window = rate_limit
        .get("primary_window")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();
    let secondary_window = rate_limit
        .get("secondary_window")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();

    let use_paid_windows =
        codex_window_has_active_limit(&secondary_window) && plan_type.as_deref() != Some("free");
    if use_paid_windows {
        codex_write_window(&mut result, &secondary_window, "primary");
        codex_write_window(&mut result, &primary_window, "secondary");
    } else {
        codex_write_window(&mut result, &primary_window, "primary");
        if codex_window_is_explicitly_disabled(&secondary_window) {
            codex_write_disabled_window(&mut result, "secondary");
        }
    }

    if let Some(spark_rate_limit) = codex_find_spark_rate_limit(root) {
        if let Some(primary_window) = spark_rate_limit
            .get("primary_window")
            .and_then(serde_json::Value::as_object)
        {
            codex_write_window(&mut result, primary_window, "spark_primary");
        }
        if let Some(secondary_window) = spark_rate_limit
            .get("secondary_window")
            .and_then(serde_json::Value::as_object)
        {
            codex_write_window(&mut result, secondary_window, "spark_secondary");
        }
    }

    // Keep all additional limits, including future ones with names unknown to
    // this version of the gateway, so scheduling can select their bucket by
    // metadata rather than by a hard-coded model name.
    result.insert(
        CODEX_ADDITIONAL_QUOTA_WINDOWS_KEY.to_string(),
        serde_json::Value::Array(codex_additional_quota_windows(root)),
    );

    if let Some(credits) = root.get("credits").and_then(serde_json::Value::as_object) {
        if let Some(value) = credits.get("has_credits").and_then(coerce_json_bool) {
            result.insert("has_credits".to_string(), json!(value));
        }
        if let Some(value) = credits.get("balance").and_then(coerce_json_f64) {
            result.insert("credits_balance".to_string(), json!(value));
        }
        if let Some(value) = credits.get("unlimited").and_then(coerce_json_bool) {
            result.insert("credits_unlimited".to_string(), json!(value));
        }
    }

    if let Some(available_count) = codex_reset_credits_available_count(root) {
        result.insert(
            "reset_credits".to_string(),
            codex_reset_credits_count_snapshot(available_count, updated_at_unix_secs),
        );
    }

    if result.is_empty() {
        return None;
    }
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    Some(serde_json::Value::Object(result))
}

/// Normalizes quota metadata emitted by the Codex Responses WebSocket.
///
/// The upstream normally sends a `codex.rate_limits` item inside a `chunks`
/// envelope. When the account is already exhausted it can instead send a
/// terminal `usage_limit_reached` error whose embedded `X-Codex-*` headers
/// contain the authoritative final quota snapshot.
pub fn parse_codex_websocket_rate_limits_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let mut latest = parse_codex_websocket_quota_event(value, updated_at_unix_secs);
    for chunk in value
        .get("chunks")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        if let Some(parsed) = parse_codex_websocket_quota_event(chunk, updated_at_unix_secs) {
            latest = Some(parsed);
        }
    }
    latest
}

fn codex_websocket_usage_limit_error(
    value: &serde_json::Value,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    let root = value.as_object()?;
    if root.get("type").and_then(serde_json::Value::as_str) != Some("error") {
        return None;
    }
    let status_code = root
        .get("status_code")
        .or_else(|| root.get("status"))
        .and_then(coerce_json_u64);
    if status_code != Some(429) {
        return None;
    }
    let error = root.get("error").and_then(serde_json::Value::as_object)?;
    (error.get("type").and_then(serde_json::Value::as_str) == Some("usage_limit_reached"))
        .then_some(error)
}

/// Returns whether a Codex WebSocket response contains the terminal quota error for the limit
/// used by this request. This is intentionally request-scoped: callers must not interpret it as
/// proof that the account-wide Codex quota is exhausted.
pub fn codex_websocket_response_has_usage_limit_error(value: &serde_json::Value) -> bool {
    codex_websocket_usage_limit_error(value).is_some()
        || value
            .get("chunks")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|chunks| {
                chunks
                    .iter()
                    .any(|chunk| codex_websocket_usage_limit_error(chunk).is_some())
            })
}

/// Reads the reset deadline attached to the latest terminal quota error in a Codex WebSocket
/// response. The error body follows the active limit, so this is suitable for a current-turn
/// retry exclusion even when that limit is model-scoped.
pub fn codex_websocket_usage_limit_reset_at(
    value: &serde_json::Value,
    observed_at_unix_secs: u64,
) -> Option<u64> {
    let read_reset = |event: &serde_json::Value| {
        let error = codex_websocket_usage_limit_error(event)?;
        error
            .get("resets_at")
            .and_then(coerce_json_u64)
            .or_else(|| {
                error
                    .get("resets_in_seconds")
                    .and_then(coerce_json_u64)
                    .map(|seconds| observed_at_unix_secs.saturating_add(seconds))
            })
    };

    let mut latest = read_reset(value);
    for chunk in value
        .get("chunks")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
    {
        if codex_websocket_usage_limit_error(chunk).is_some() {
            latest = read_reset(chunk);
        }
    }
    latest
}

fn parse_codex_websocket_quota_event(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    parse_codex_websocket_rate_limits_chunk(value, updated_at_unix_secs)
        .or_else(|| parse_codex_websocket_usage_limit_error(value, updated_at_unix_secs))
}

/// Returns whether normalized Codex rate-limit metadata says that the account
/// cannot accept another request. Explicit upstream flags take precedence, and
/// the percentage fallback keeps older payloads working when those flags are
/// absent.
pub fn codex_rate_limit_metadata_exhausted(value: &serde_json::Value) -> bool {
    let allowed = value.get("allowed").and_then(coerce_json_bool);
    let limit_reached = value.get("limit_reached").and_then(coerce_json_bool);
    if allowed == Some(false) || limit_reached == Some(true) {
        return true;
    }
    if allowed == Some(true) || limit_reached == Some(false) {
        return false;
    }
    ["primary_used_percent", "secondary_used_percent"]
        .into_iter()
        .filter_map(|key| value.get(key))
        .filter_map(coerce_json_f64)
        .any(|used_percent| used_percent >= 100.0 - 1e-6)
}

fn parse_codex_websocket_rate_limits_chunk(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    if root.get("type").and_then(serde_json::Value::as_str) != Some("codex.rate_limits") {
        return None;
    }
    let rate_limits = root
        .get("rate_limits")
        .and_then(serde_json::Value::as_object)?;

    let mut result = serde_json::Map::new();
    let plan_type = root
        .get("plan_type")
        .or_else(|| rate_limits.get("plan_type"))
        .and_then(serde_json::Value::as_str)
        .and_then(|value| normalize_codex_plan_type(Some(value)));
    if let Some(plan_type) = plan_type {
        result.insert("plan_type".to_string(), json!(plan_type));
    }
    if let Some(allowed) = rate_limits.get("allowed").and_then(coerce_json_bool) {
        result.insert("allowed".to_string(), json!(allowed));
    }
    if let Some(limit_reached) = rate_limits.get("limit_reached").and_then(coerce_json_bool) {
        result.insert("limit_reached".to_string(), json!(limit_reached));
    }
    if let Some(primary) = rate_limits
        .get("primary")
        .and_then(serde_json::Value::as_object)
    {
        codex_write_window(&mut result, primary, "primary");
    }
    if let Some(secondary) = rate_limits
        .get("secondary")
        .and_then(serde_json::Value::as_object)
    {
        codex_write_window(&mut result, secondary, "secondary");
    }
    if result.is_empty() {
        return None;
    }
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    Some(serde_json::Value::Object(result))
}

fn parse_codex_websocket_usage_limit_error(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    let error = codex_websocket_usage_limit_error(value)?;

    let headers = root
        .get("headers")
        .and_then(serde_json::Value::as_object)
        .map(|headers| {
            headers
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .as_str()
                        .map(|value| (name.clone(), value.to_string()))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut result = parse_codex_usage_headers(&headers, updated_at_unix_secs)
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let account_scoped = codex_headers_carry_account_windows(&headers);

    if !result.contains_key("plan_type") {
        if let Some(plan_type) = error
            .get("plan_type")
            .and_then(serde_json::Value::as_str)
            .and_then(|value| normalize_codex_plan_type(Some(value)))
        {
            result.insert("plan_type".to_string(), json!(plan_type));
        }
    }
    // `resets_at` describes whichever limit rejected the request. When a named per-model limit
    // owned the window headers it owns this timing too, so backfilling it into the account slot
    // would put a model window's reset on the account's own quota.
    if account_scoped {
        if !result.contains_key("primary_reset_at") {
            if let Some(reset_at) = error.get("resets_at").and_then(coerce_json_u64) {
                result.insert("primary_reset_at".to_string(), json!(reset_at));
            }
        }
        if !result.contains_key("primary_reset_after_seconds") {
            if let Some(reset_after_seconds) =
                error.get("resets_in_seconds").and_then(coerce_json_u64)
            {
                result.insert(
                    "primary_reset_after_seconds".to_string(),
                    json!(reset_after_seconds),
                );
            }
        }
    }

    // `usage_limit_reached` is terminal for the active limit. Only project it onto the account
    // when the unprefixed headers belong to the account; a named model limit (for example Spark)
    // must not make every Codex model on the credential look exhausted. With no ownership headers
    // the legacy account-wide fallback remains intact.
    if account_scoped {
        result.insert("allowed".to_string(), json!(false));
        result.insert("limit_reached".to_string(), json!(true));
    }
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    Some(serde_json::Value::Object(result))
}

fn parse_codex_reset_credit_timestamp(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    if let Some(timestamp) = coerce_json_u64(value) {
        return Some(if timestamp > 1_000_000_000_000 {
            timestamp / 1000
        } else {
            timestamp
        });
    }
    let raw = value.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .and_then(|timestamp| u64::try_from(timestamp.timestamp()).ok())
}

fn codex_reset_credit_detail_items(value: &serde_json::Value) -> Option<&Vec<serde_json::Value>> {
    if let Some(items) = value.as_array() {
        return Some(items);
    }

    first_json_value_by_paths(
        value,
        &[
            &["credits"],
            &["data"],
            &["items"],
            &["rate_limit_reset_credits", "credits"],
            &["rate_limit_reset_credits", "data"],
            &["rateLimitResetCredits", "credits"],
            &["rateLimitResetCredits", "data"],
            &["reset_credits", "credits"],
            &["resetCredits", "credits"],
            &["rate_limit_reset_credits"],
            &["rateLimitResetCredits"],
            &["reset_credits"],
            &["resetCredits"],
        ],
    )
    .and_then(serde_json::Value::as_array)
}

fn codex_reset_credit_id(object: &serde_json::Map<String, serde_json::Value>) -> Option<String> {
    [
        "id",
        "credit_id",
        "creditId",
        "key",
        "idempotency_key",
        "idempotencyKey",
    ]
    .iter()
    .find_map(|key| coerce_json_string(object.get(*key)))
}

fn codex_reset_credit_display_key(id: &str) -> Option<String> {
    id.split('-')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn codex_reset_credit_status(
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    ["status", "state"]
        .iter()
        .find_map(|key| coerce_json_string(object.get(*key)))
}

fn codex_reset_credit_is_available(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    let reset_type = ["reset_type", "resetType"]
        .iter()
        .find_map(|key| coerce_json_string(object.get(*key)));
    if reset_type
        .as_deref()
        .is_some_and(|value| !value.trim().eq_ignore_ascii_case("codex_rate_limits"))
    {
        return false;
    }

    codex_reset_credit_status(object).is_none_or(|status| {
        let status = status.trim();
        status.eq_ignore_ascii_case("available") || status.eq_ignore_ascii_case("active")
    })
}

fn parse_codex_reset_credit_detail_item(item: &serde_json::Value) -> Option<serde_json::Value> {
    let object = item.as_object()?;
    if !codex_reset_credit_is_available(object) {
        return None;
    }
    let expires_at = parse_codex_reset_credit_timestamp(
        object
            .get("expires_at")
            .or_else(|| object.get("expiresAt"))
            .or_else(|| object.get("expiration_time"))
            .or_else(|| object.get("expirationTime")),
    )?;
    let granted_at = parse_codex_reset_credit_timestamp(
        object
            .get("granted_at")
            .or_else(|| object.get("grantedAt"))
            .or_else(|| object.get("created_at"))
            .or_else(|| object.get("createdAt")),
    );

    let mut out = serde_json::Map::new();
    if let Some(id) = codex_reset_credit_id(object) {
        if let Some(display_key) = codex_reset_credit_display_key(&id) {
            out.insert("display_key".to_string(), json!(display_key));
        }
        out.insert("id".to_string(), json!(id));
    }
    if let Some(status) = codex_reset_credit_status(object) {
        out.insert("status".to_string(), json!(status));
    }
    if let Some(granted_at) = granted_at {
        out.insert("granted_at".to_string(), json!(granted_at));
    }
    out.insert("expires_at".to_string(), json!(expires_at));
    Some(serde_json::Value::Object(out))
}

pub fn parse_codex_wham_reset_credits_detail_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object();
    let detail_items = codex_reset_credit_detail_items(value);
    if root.is_none() && detail_items.is_none() {
        return None;
    }

    let available_item_count = detail_items
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_object)
        .filter(|item| codex_reset_credit_is_available(item))
        .count();
    let available_count = root
        .and_then(codex_reset_credits_available_count)
        .or_else(|| {
            root.and_then(|root| {
                [
                    "available_count",
                    "availableCount",
                    "available",
                    "remaining",
                    "count",
                ]
                .iter()
                .find_map(|key| root.get(*key).and_then(coerce_json_u64))
            })
        })
        .or_else(|| detail_items.and_then(|_| u64::try_from(available_item_count).ok()));
    let mut credits = detail_items
        .into_iter()
        .flatten()
        .filter_map(parse_codex_reset_credit_detail_item)
        .collect::<Vec<_>>();
    credits.sort_by_key(|item| {
        item.get("expires_at")
            .and_then(coerce_json_u64)
            .unwrap_or(u64::MAX)
    });

    let detail_status = if available_count.is_some_and(|count| count > 0) {
        "available"
    } else {
        "empty"
    };
    let mut reset_credits = serde_json::Map::new();
    reset_credits.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    reset_credits.insert("detail_source".to_string(), json!("wham_readonly"));
    reset_credits.insert("detail_status".to_string(), json!(detail_status));
    reset_credits.insert("credits".to_string(), serde_json::Value::Array(credits));

    if let Some(available_count) = available_count {
        reset_credits.insert("available_count".to_string(), json!(available_count));
    }

    Some(json!({ "reset_credits": reset_credits }))
}

pub fn normalize_codex_reset_credit_consume_outcome(
    value: Option<&serde_json::Value>,
) -> Option<String> {
    let object = value.and_then(serde_json::Value::as_object)?;
    let raw = ["outcome", "status", "result", "code"]
        .iter()
        .find_map(|key| coerce_json_string(object.get(*key)));
    if let Some(raw) = raw {
        let normalized = raw.trim().replace(['-', ' '], "_").to_ascii_lowercase();
        return match normalized.as_str() {
            "reset" | "success" | "redeemed" => Some("reset".to_string()),
            "alreadyredeemed" | "already_redeemed" => Some("already_redeemed".to_string()),
            "nothingtoreset" | "nothing_to_reset" => Some("nothing_to_reset".to_string()),
            "nocredit" | "no_credit" => Some("no_credit".to_string()),
            "error" | "failed" => Some("error".to_string()),
            _ => None,
        };
    }

    for (field, outcome) in [
        ("reset", "reset"),
        ("alreadyRedeemed", "already_redeemed"),
        ("already_redeemed", "already_redeemed"),
        ("nothingToReset", "nothing_to_reset"),
        ("nothing_to_reset", "nothing_to_reset"),
        ("noCredit", "no_credit"),
        ("no_credit", "no_credit"),
    ] {
        if object.get(field).and_then(coerce_json_bool) == Some(true) {
            return Some(outcome.to_string());
        }
    }

    None
}

fn codex_json_object<'a>(
    root: &'a serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    keys.iter()
        .find_map(|key| root.get(*key).and_then(serde_json::Value::as_object))
}

fn codex_json_string_from_object(
    object: Option<&serde_json::Map<String, serde_json::Value>>,
    keys: &[&str],
) -> Option<String> {
    let object = object?;
    keys.iter()
        .find_map(|key| coerce_json_string(object.get(*key)))
}

fn codex_json_string_from_root(
    root: &serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<String> {
    keys.iter()
        .find_map(|key| coerce_json_string(root.get(*key)))
}

fn codex_backend_me_account_object(
    root: &serde_json::Map<String, serde_json::Value>,
) -> Option<&serde_json::Map<String, serde_json::Value>> {
    codex_json_object(root, &["account", "current_account", "selected_account"])
        .or_else(|| {
            root.get("accounts")
                .and_then(serde_json::Value::as_array)?
                .iter()
                .filter_map(serde_json::Value::as_object)
                .find(|account| {
                    account
                        .get("is_default")
                        .or_else(|| account.get("selected"))
                        .or_else(|| account.get("current"))
                        .and_then(coerce_json_bool)
                        .unwrap_or(false)
                })
        })
        .or_else(|| {
            root.get("accounts")
                .and_then(serde_json::Value::as_array)?
                .iter()
                .find_map(serde_json::Value::as_object)
        })
}

fn codex_backend_me_plan_object<'a>(
    root: &'a serde_json::Map<String, serde_json::Value>,
    account: Option<&'a serde_json::Map<String, serde_json::Value>>,
) -> Option<&'a serde_json::Map<String, serde_json::Value>> {
    codex_json_object(root, &["plan", "subscription", "workspace_plan"]).or_else(|| {
        account
            .and_then(|account| account.get("plan"))
            .and_then(serde_json::Value::as_object)
    })
}

pub fn parse_codex_backend_me_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    if root.is_empty() {
        return None;
    }

    let user = codex_json_object(root, &["user", "auth_user", "profile"]);
    let account = codex_backend_me_account_object(root);
    let plan = codex_backend_me_plan_object(root, account);
    let mut result = serde_json::Map::new();

    if let Some(user_id) = codex_json_string_from_object(user, &["id", "user_id"])
        .or_else(|| codex_json_string_from_root(root, &["user_id"]))
    {
        result.insert("user_id".to_string(), json!(user_id));
    }
    if let Some(email) = codex_json_string_from_object(user, &["email"])
        .or_else(|| codex_json_string_from_root(root, &["email"]))
    {
        result.insert("email".to_string(), json!(email));
    }
    if let Some(name) = codex_json_string_from_object(user, &["name", "display_name", "full_name"])
        .or_else(|| codex_json_string_from_root(root, &["name", "display_name", "full_name"]))
    {
        result.insert("user_name".to_string(), json!(name));
    }
    if let Some(account_id) =
        codex_json_string_from_object(account, &["id", "account_id", "accountId", "workspace_id"])
            .or_else(|| {
                codex_json_string_from_root(root, &["account_id", "accountId", "workspace_id"])
            })
    {
        result.insert("account_id".to_string(), json!(account_id));
    }
    if let Some(account_name) =
        codex_json_string_from_object(account, &["name", "title", "display_name"])
    {
        result.insert("account_name".to_string(), json!(account_name));
    }

    let plan_type = codex_json_string_from_object(
        account,
        &["plan_type", "planType", "subscription_plan", "tier"],
    )
    .or_else(|| codex_json_string_from_object(plan, &["type", "plan_type", "name", "tier"]))
    .or_else(|| codex_json_string_from_root(root, &["plan_type", "planType"]));
    if let Some(plan_type) = normalize_codex_plan_type(plan_type.as_deref()) {
        result.insert("plan_type".to_string(), json!(plan_type));
    }
    if let Some(plan_title) =
        codex_json_string_from_object(plan, &["title", "display_name", "label"])
    {
        result.insert("plan_title".to_string(), json!(plan_title));
    }

    if result.is_empty() {
        return None;
    }
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    Some(serde_json::Value::Object(result))
}

fn codex_normalized_headers(headers: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    headers
        .iter()
        .map(|(key, value)| (key.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect()
}

/// Whether the account's own quota windows can be read from these headers' unprefixed fields.
///
/// False when a named per-model limit governed the request: the unprefixed fields — and any reset
/// timing reported alongside them — then describe that limit instead.
fn codex_headers_carry_account_windows(headers: &BTreeMap<String, String>) -> bool {
    let normalized = codex_normalized_headers(headers);
    let named_limits = codex_named_limit_headers(&normalized);
    !codex_named_limit_owns_unprefixed_windows(&normalized, &named_limits)
}

/// Lists the named per-model limits the response headers announce, as `(feature, limit_name)`.
///
/// `feature` is the header segment that carries the limit's windows
/// (`x-codex-bengalfox-primary-used-percent`), and matches the `metered_feature` reported by
/// `wham/usage` minus its `codex_` prefix.
fn codex_named_limit_headers(normalized: &BTreeMap<String, String>) -> Vec<(String, String)> {
    normalized
        .iter()
        .filter_map(|(key, value)| {
            let feature = key
                .strip_prefix(CODEX_HEADER_PREFIX)?
                .strip_suffix(CODEX_LIMIT_NAME_HEADER_SUFFIX)?;
            let limit_name = value.trim();
            (!feature.is_empty() && !limit_name.is_empty())
                .then(|| (feature.to_string(), limit_name.to_string()))
        })
        .collect()
}

/// Whether one of the announced named limits owns the unprefixed `x-codex-primary/secondary-*`
/// windows.
///
/// `x-codex-active-limit` names the metered feature this request was billed against, and reports
/// the plan's own limit as `premium`, which matches no announced feature. The account therefore
/// keeps the unprefixed windows unless the active limit is one of the named ones.
fn codex_named_limit_owns_unprefixed_windows(
    normalized: &BTreeMap<String, String>,
    named_limits: &[(String, String)],
) -> bool {
    let Some(active_limit) = normalized
        .get(CODEX_ACTIVE_LIMIT_HEADER)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return false;
    };
    named_limits.iter().any(|(feature, _)| {
        active_limit.eq_ignore_ascii_case(feature)
            || active_limit.eq_ignore_ascii_case(&format!("codex_{feature}"))
    })
}

pub fn parse_codex_usage_headers(
    headers: &BTreeMap<String, String>,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let mut result = serde_json::Map::new();
    let normalized = codex_normalized_headers(headers);
    if !normalized
        .keys()
        .any(|key| key.starts_with(CODEX_HEADER_PREFIX))
    {
        return None;
    }

    let plan_type =
        normalize_codex_plan_type(normalized.get("x-codex-plan-type").map(String::as_str));
    if let Some(plan_type) = plan_type.as_ref() {
        result.insert("plan_type".to_string(), json!(plan_type));
    }

    let read_window = |prefix: &str| -> serde_json::Map<String, serde_json::Value> {
        let mut object = serde_json::Map::new();
        let used_key = format!("x-codex-{prefix}-used-percent");
        let reset_after_key = format!("x-codex-{prefix}-reset-after-seconds");
        let reset_at_key = format!("x-codex-{prefix}-reset-at");
        let window_minutes_key = format!("x-codex-{prefix}-window-minutes");
        if let Some(value) = normalized
            .get(&used_key)
            .and_then(|value| value.parse::<f64>().ok())
        {
            object.insert("used_percent".to_string(), json!(value));
        }
        if let Some(value) = normalized
            .get(&reset_after_key)
            .and_then(|value| value.parse::<u64>().ok())
        {
            object.insert("reset_after_seconds".to_string(), json!(value));
        }
        if let Some(value) = normalized
            .get(&reset_at_key)
            .and_then(|value| value.parse::<u64>().ok())
        {
            object.insert("reset_at".to_string(), json!(value));
        } else if normalized
            .get(&reset_at_key)
            .is_some_and(|value| value.is_empty())
        {
            // Preserve an explicitly empty reset-at long enough to recognize
            // the complete all-zero secondary-window disabled marker.
            object.insert("reset_at".to_string(), serde_json::Value::Null);
        }
        if let Some(value) = normalized
            .get(&window_minutes_key)
            .and_then(|value| value.parse::<u64>().ok())
        {
            object.insert("window_minutes".to_string(), json!(value));
        }
        object
    };

    // Every named limit is announced by its own `x-codex-<feature>-limit-name` header and carries
    // its windows under the same `<feature>` prefix, so the header set describes itself.
    let named_limits = codex_named_limit_headers(&normalized);
    for (feature, limit_name) in &named_limits {
        if limit_name != CODEX_SPARK_LIMIT_NAME {
            continue;
        }
        let spark_primary_window = read_window(&format!("{feature}-primary"));
        if !spark_primary_window.is_empty() {
            codex_write_window(&mut result, &spark_primary_window, "spark_primary");
        }
        let spark_secondary_window = read_window(&format!("{feature}-secondary"));
        if !spark_secondary_window.is_empty() {
            codex_write_window(&mut result, &spark_secondary_window, "spark_secondary");
        }
    }

    // The unprefixed windows describe whichever limit governed this request, not the account:
    // on a request metered against a named limit they repeat that limit's windows verbatim.
    // `x-codex-active-limit` is the only thing that tells the two apart, so an account window is
    // only claimed when no named limit owns the unprefixed set. Upstreams that do not send the
    // header keep the previous behaviour.
    if !codex_named_limit_owns_unprefixed_windows(&normalized, &named_limits) {
        let primary_window = read_window("primary");
        let secondary_window = read_window("secondary");
        let use_paid_windows = codex_window_has_active_limit(&secondary_window)
            && plan_type.as_deref() != Some("free");
        if use_paid_windows {
            codex_write_window(&mut result, &secondary_window, "primary");
            codex_write_window(&mut result, &primary_window, "secondary");
        } else {
            codex_write_window(&mut result, &primary_window, "primary");
            if codex_window_is_explicitly_disabled(&secondary_window) {
                codex_write_disabled_window(&mut result, "secondary");
            }
        }

        if let Some(value) = normalized
            .get("x-codex-primary-over-secondary-limit-percent")
            .and_then(|value| value.parse::<f64>().ok())
        {
            result.insert(
                "primary_over_secondary_limit_percent".to_string(),
                json!(value),
            );
        }
    }
    if let Some(value) = normalized
        .get("x-codex-credits-has-credits")
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        })
    {
        result.insert("has_credits".to_string(), json!(value));
    }
    if let Some(value) = normalized
        .get("x-codex-credits-balance")
        .and_then(|value| value.parse::<f64>().ok())
    {
        result.insert("credits_balance".to_string(), json!(value));
    }
    if let Some(value) = normalized
        .get("x-codex-credits-unlimited")
        .and_then(|value| match value.to_ascii_lowercase().as_str() {
            "true" | "1" => Some(true),
            "false" | "0" => Some(false),
            _ => None,
        })
    {
        result.insert("credits_unlimited".to_string(), json!(value));
    }

    if result.is_empty() {
        return None;
    }
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    Some(serde_json::Value::Object(result))
}

fn codex_current_invalid_reason(key: &StoredProviderCatalogKey) -> String {
    key.oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string()
}

fn codex_merge_invalid_reason(current: &str, candidate_reason: &str) -> String {
    if current.is_empty() {
        return candidate_reason.to_string();
    }
    if current.starts_with(OAUTH_ACCOUNT_BLOCK_PREFIX) {
        return current.to_string();
    }
    if current.starts_with(OAUTH_EXPIRED_PREFIX)
        && candidate_reason.starts_with(OAUTH_REFRESH_FAILED_PREFIX)
    {
        if current
            .lines()
            .map(str::trim)
            .any(|line| line.starts_with(OAUTH_REFRESH_FAILED_PREFIX))
        {
            return current.to_string();
        }
        return format!("{current}\n{candidate_reason}");
    }
    if candidate_reason.starts_with(OAUTH_EXPIRED_PREFIX) {
        let candidate_lines = candidate_reason
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>();
        let missing_refresh_failures = current
            .lines()
            .map(str::trim)
            .filter(|line| line.starts_with(OAUTH_REFRESH_FAILED_PREFIX))
            .filter(|line| !candidate_lines.contains(line))
            .collect::<Vec<_>>();
        if missing_refresh_failures.is_empty() {
            return candidate_reason.to_string();
        }
        return format!(
            "{candidate_reason}\n{}",
            missing_refresh_failures.join("\n")
        );
    }
    if candidate_reason.starts_with(OAUTH_REQUEST_FAILED_PREFIX)
        && current.lines().map(str::trim).any(|line| {
            line.starts_with(OAUTH_EXPIRED_PREFIX) || line.starts_with(OAUTH_REFRESH_FAILED_PREFIX)
        })
    {
        return current.to_string();
    }
    candidate_reason.to_string()
}

pub fn codex_build_invalid_state(
    key: &StoredProviderCatalogKey,
    candidate_reason: String,
    now_unix_secs: u64,
) -> (Option<u64>, Option<String>) {
    let current_reason = codex_current_invalid_reason(key);
    let merged_reason = codex_merge_invalid_reason(&current_reason, &candidate_reason);
    if merged_reason == current_reason {
        return (key.oauth_invalid_at_unix_secs, Some(merged_reason));
    }
    (Some(now_unix_secs), Some(merged_reason))
}

pub fn codex_looks_like_token_invalidated(message: Option<&str>) -> bool {
    let lowered = message.unwrap_or_default().trim().to_ascii_lowercase();
    lowered.contains("token_invalidated")
        || lowered.contains("authentication token has been invalidated")
        || lowered.contains("token has been invalidated")
        || lowered.contains("token invalidated")
        || lowered.contains("agent runtime has been deleted")
        || lowered.contains("personal access token owner is inactive")
        || lowered.contains("biscuit_baker_service_auth_credential_error_status")
        || lowered.contains("auth_credential")
        || lowered.contains("invalidated")
        || lowered.contains("revoked")
        || lowered.contains("已撤销")
        || lowered.contains("被撤销")
        || lowered.contains("撤销")
        || lowered.contains("作废")
}

pub fn codex_looks_like_token_expired(message: Option<&str>) -> bool {
    let lowered = message.unwrap_or_default().trim().to_ascii_lowercase();
    lowered.contains("session has expired")
        || lowered.contains("session expired")
        || lowered.contains("access token expired")
        || lowered.contains("expired access token")
        || lowered.contains("token has expired")
        || lowered.contains("token expired")
        || lowered.contains("security token included in the request is expired")
        || lowered.contains("已过期")
        || lowered.contains("过期")
}

fn codex_looks_like_account_deactivated(message: Option<&str>) -> bool {
    let lowered = message.unwrap_or_default().trim().to_ascii_lowercase();
    lowered.contains("account has been deactivated") || lowered.contains("account deactivated")
}

pub fn codex_looks_like_workspace_deactivated(message: Option<&str>) -> bool {
    let lowered = message.unwrap_or_default().trim().to_ascii_lowercase();
    lowered.contains("deactivated_workspace")
        || (lowered.contains("workspace") && lowered.contains("deactivated"))
}

pub fn codex_structured_invalid_reason(status_code: u16, upstream_message: Option<&str>) -> String {
    let message = upstream_message.unwrap_or_default().trim();
    if status_code == 402 && codex_looks_like_workspace_deactivated(Some(message)) {
        return format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}工作区已停用 (deactivated_workspace)");
    }
    if codex_looks_like_account_deactivated(Some(message)) {
        return format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}OpenAI 账号已停用");
    }
    if codex_looks_like_token_invalidated(Some(message)) {
        return format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已失效");
    }
    if codex_looks_like_token_expired(Some(message)) {
        return format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已过期");
    }
    if status_code == 401 {
        return format!("{OAUTH_EXPIRED_PREFIX}Codex Token 已过期 (401)");
    }
    if status_code == 403 {
        return format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}Codex 账户访问受限 (403)");
    }
    if status_code == 402 {
        return format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}Codex 账户需要付款 (402)");
    }
    format!("Codex 请求失败 ({status_code})")
}

pub fn codex_runtime_invalid_reason(
    status_code: u16,
    upstream_message: Option<&str>,
) -> Option<String> {
    match status_code {
        401 => Some(codex_structured_invalid_reason(401, upstream_message)),
        402 => Some(codex_structured_invalid_reason(402, upstream_message)),
        403 if codex_looks_like_token_invalidated(upstream_message)
            || codex_looks_like_token_expired(upstream_message)
            || codex_looks_like_account_deactivated(upstream_message) =>
        {
            Some(codex_structured_invalid_reason(403, upstream_message))
        }
        403 => Some(codex_generic_forbidden_runtime_invalid_reason(
            upstream_message,
        )),
        _ => None,
    }
}

fn codex_generic_forbidden_runtime_invalid_reason(upstream_message: Option<&str>) -> String {
    let _ = upstream_message;
    let detail = "Codex Token 已失效 (403)";
    format!("{OAUTH_EXPIRED_PREFIX}{detail}")
}

pub fn codex_soft_request_failure_reason(
    status_code: u16,
    upstream_message: Option<&str>,
) -> String {
    let _ = upstream_message;
    let detail = format!("Codex 请求失败 ({status_code})");
    format!("{OAUTH_REQUEST_FAILED_PREFIX}{detail}")
}

fn compute_kiro_total_usage_limit(breakdown: &serde_json::Value) -> f64 {
    let mut total = breakdown
        .get("usageLimitWithPrecision")
        .and_then(coerce_json_f64)
        .unwrap_or(0.0);

    if breakdown
        .get("freeTrialInfo")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|free_trial| {
            free_trial
                .get("freeTrialStatus")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .is_some_and(|value| value.eq_ignore_ascii_case("ACTIVE"))
        })
    {
        total += breakdown
            .get("freeTrialInfo")
            .and_then(|value| value.get("usageLimitWithPrecision"))
            .and_then(coerce_json_f64)
            .unwrap_or(0.0);
    }

    if let Some(bonuses) = breakdown
        .get("bonuses")
        .and_then(serde_json::Value::as_array)
    {
        for bonus in bonuses {
            let is_active = bonus
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .is_some_and(|value| value.eq_ignore_ascii_case("ACTIVE"));
            if is_active {
                total += bonus
                    .get("usageLimit")
                    .and_then(coerce_json_f64)
                    .unwrap_or(0.0);
            }
        }
    }

    total
}

fn compute_kiro_current_usage(breakdown: &serde_json::Value) -> f64 {
    let mut total = breakdown
        .get("currentUsageWithPrecision")
        .and_then(coerce_json_f64)
        .unwrap_or(0.0);

    if breakdown
        .get("freeTrialInfo")
        .and_then(serde_json::Value::as_object)
        .is_some_and(|free_trial| {
            free_trial
                .get("freeTrialStatus")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .is_some_and(|value| value.eq_ignore_ascii_case("ACTIVE"))
        })
    {
        total += breakdown
            .get("freeTrialInfo")
            .and_then(|value| value.get("currentUsageWithPrecision"))
            .and_then(coerce_json_f64)
            .unwrap_or(0.0);
    }

    if let Some(bonuses) = breakdown
        .get("bonuses")
        .and_then(serde_json::Value::as_array)
    {
        for bonus in bonuses {
            let is_active = bonus
                .get("status")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .is_some_and(|value| value.eq_ignore_ascii_case("ACTIVE"));
            if is_active {
                total += bonus
                    .get("currentUsage")
                    .and_then(coerce_json_f64)
                    .unwrap_or(0.0);
            }
        }
    }

    total
}

pub fn parse_kiro_usage_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    let breakdown = root
        .get("usageBreakdownList")
        .and_then(serde_json::Value::as_array)
        .and_then(|items| items.first())?;

    let usage_limit = compute_kiro_total_usage_limit(breakdown);
    let current_usage = compute_kiro_current_usage(breakdown);
    let remaining = (usage_limit - current_usage).max(0.0);
    let usage_percentage = if usage_limit > 0.0 {
        ((current_usage / usage_limit) * 100.0).min(100.0)
    } else {
        0.0
    };

    let mut result = serde_json::Map::new();
    result.insert("current_usage".to_string(), json!(current_usage));
    result.insert("usage_limit".to_string(), json!(usage_limit));
    result.insert("remaining".to_string(), json!(remaining));
    result.insert("usage_percentage".to_string(), json!(usage_percentage));
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));

    if let Some(subscription_title) = root
        .get("subscriptionInfo")
        .and_then(|value| value.get("subscriptionTitle"))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        result.insert("subscription_title".to_string(), json!(subscription_title));
    }

    if let Some(next_reset_at) = root
        .get("nextDateReset")
        .and_then(coerce_json_f64)
        .or_else(|| breakdown.get("nextDateReset").and_then(coerce_json_f64))
    {
        result.insert("next_reset_at".to_string(), json!(next_reset_at));
    }

    let email = root
        .get("desktopUserInfo")
        .and_then(|value| value.get("email"))
        .or_else(|| root.get("userInfo").and_then(|value| value.get("email")))
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if let Some(email) = email {
        result.insert("email".to_string(), json!(email));
    }

    Some(serde_json::Value::Object(result))
}

pub fn parse_xai_billing_response(
    value: &serde_json::Value,
    updated_at_unix_secs: u64,
) -> Option<serde_json::Value> {
    let root = value.as_object()?;
    let config = root
        .get("config")
        .and_then(serde_json::Value::as_object)
        .unwrap_or(root);

    let usage_percentage = coerce_json_f64_from_map(config, "creditUsagePercent")
        .or_else(|| extract_xai_product_usage_percent(config));
    let period = config.get("currentPeriod");
    let period_type = period
        .and_then(|value| value.get("type").or_else(|| value.get("periodType")))
        .and_then(normalize_xai_period_type);
    let next_reset_at = period
        .and_then(|value| value.get("end"))
        .and_then(parse_xai_timestamp)
        .or_else(|| config.get("billingPeriodEnd").and_then(parse_xai_timestamp));
    let monthly_limit =
        coerce_xai_cents_dollars(config.get("monthlyLimit")).filter(|value| *value > 0.0);
    let current_usage = if monthly_limit.is_some() {
        coerce_xai_cents_dollars(config.get("used"))
    } else {
        None
    };
    let remaining = monthly_limit
        .zip(current_usage)
        .map(|(limit, used)| (limit - used).max(0.0));
    let usage_percentage = usage_percentage.or_else(|| {
        monthly_limit
            .zip(current_usage)
            .map(|(limit, used)| ((used / limit) * 100.0).clamp(0.0, 100.0))
    });
    let usage_percentage = match usage_percentage {
        Some(value) => Some(value.clamp(0.0, 100.0)),
        None if period_type.is_some() || next_reset_at.is_some() => Some(0.0),
        None => None,
    };
    let prepaid_balance = coerce_xai_cents_dollars(config.get("prepaidBalance"));
    let on_demand_cap = coerce_xai_cents_dollars(config.get("onDemandCap"));
    let on_demand_used = coerce_xai_cents_dollars(config.get("onDemandUsed"));
    let on_demand_enabled = coerce_json_bool_from_map(root, "onDemandEnabled")
        .or_else(|| coerce_json_bool_from_map(config, "onDemandEnabled"));
    let subscription_title = first_json_string_by_paths(
        value,
        &[
            &["subscriptionTier"],
            &["subscription_tier"],
            &["config", "subscriptionTier"],
            &["config", "subscription_title"],
        ],
    );

    if usage_percentage.is_none()
        && monthly_limit.is_none()
        && current_usage.is_none()
        && prepaid_balance.is_none()
        && on_demand_cap.is_none()
        && next_reset_at.is_none()
        && subscription_title.is_none()
    {
        return None;
    }

    let mut result = serde_json::Map::new();
    result.insert("updated_at".to_string(), json!(updated_at_unix_secs));
    if let Some(value) = usage_percentage {
        result.insert("usage_percentage".to_string(), json!(value));
    }
    if let Some(value) = monthly_limit {
        result.insert("usage_limit".to_string(), json!(value));
    }
    if let Some(value) = current_usage {
        result.insert("current_usage".to_string(), json!(value));
    }
    if let Some(value) = remaining {
        result.insert("remaining".to_string(), json!(value));
    }
    if let Some(value) = next_reset_at {
        result.insert("next_reset_at".to_string(), json!(value));
    }
    if let Some(value) = period_type {
        result.insert("period_type".to_string(), json!(value));
    }
    if let Some(value) = prepaid_balance {
        result.insert("prepaid_balance".to_string(), json!(value));
    }
    if let Some(value) = on_demand_cap {
        result.insert("on_demand_cap".to_string(), json!(value));
    }
    if let Some(value) = on_demand_used {
        result.insert("on_demand_used".to_string(), json!(value));
    }
    if let Some(value) = on_demand_enabled {
        result.insert("on_demand_enabled".to_string(), json!(value));
    }
    if let Some(value) = subscription_title {
        result.insert("subscription_title".to_string(), json!(value));
    }
    Some(serde_json::Value::Object(result))
}

fn coerce_json_f64_from_map(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<f64> {
    object.get(key).and_then(coerce_json_f64)
}

fn coerce_json_bool_from_map(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Option<bool> {
    object.get(key).and_then(coerce_json_bool)
}

fn extract_xai_product_usage_percent(
    config: &serde_json::Map<String, serde_json::Value>,
) -> Option<f64> {
    let items = config.get("productUsage")?.as_array()?;
    let grok_build = items.iter().find(|item| {
        item.get("product")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|product| product.eq_ignore_ascii_case("GrokBuild"))
    });
    grok_build
        .or(items.first())
        .and_then(|item| item.get("usagePercent").and_then(coerce_json_f64))
}

fn coerce_xai_cents_dollars(value: Option<&serde_json::Value>) -> Option<f64> {
    let value = value?;
    let cents = match value {
        serde_json::Value::Object(object) => object.get("val").and_then(coerce_json_f64)?,
        other => coerce_json_f64(other)?,
    };
    Some(cents / 100.0)
}

fn normalize_xai_period_type(value: &serde_json::Value) -> Option<String> {
    let raw = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let lowered = raw.to_ascii_lowercase();
    if lowered.contains("week") {
        Some("weekly".to_string())
    } else if lowered.contains("month") {
        Some("monthly".to_string())
    } else {
        Some(raw.to_string())
    }
}

fn parse_xai_timestamp(value: &serde_json::Value) -> Option<u64> {
    if let Some(value) = coerce_json_u64(value) {
        return Some(if value > 1_000_000_000_000 {
            value / 1000
        } else {
            value
        });
    }
    let raw = value.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    chrono::DateTime::parse_from_rfc3339(raw)
        .ok()
        .and_then(|timestamp| u64::try_from(timestamp.timestamp()).ok())
}

#[cfg(test)]
mod xai_quota_tests {
    use super::parse_xai_billing_response;
    use serde_json::json;

    #[test]
    fn parse_xai_credits_percent_and_weekly_period() {
        let metadata = parse_xai_billing_response(
            &json!({
                "config": {
                    "currentPeriod": {
                        "type": "USAGE_PERIOD_TYPE_WEEKLY",
                        "start": "2026-08-08T01:53:09.930537+00:00",
                        "end": "2026-08-15T01:53:09.930537+00:00"
                    },
                    "creditUsagePercent": 46.0,
                    "productUsage": [
                        {"product": "GrokBuild", "usagePercent": 41.0},
                        {"product": "GrokChat"}
                    ],
                    "onDemandCap": {"val": 0},
                    "onDemandUsed": {"val": 0},
                    "prepaidBalance": {"val": 0}
                },
                "subscriptionTier": "SuperGrok"
            }),
            1_775_000_000,
        )
        .expect("credits payload should parse");

        assert_eq!(metadata["usage_percentage"], json!(46.0));
        assert_eq!(metadata["period_type"], json!("weekly"));
        assert_eq!(metadata["next_reset_at"], json!(1_786_758_789u64));
        assert_eq!(metadata["prepaid_balance"], json!(0.0));
        assert_eq!(metadata["on_demand_cap"], json!(0.0));
        assert_eq!(metadata["subscription_title"], json!("SuperGrok"));
    }

    #[test]
    fn parse_xai_omitted_percent_as_fresh_weekly_zero() {
        let metadata = parse_xai_billing_response(
            &json!({
                "config": {
                    "currentPeriod": {
                        "type": "USAGE_PERIOD_TYPE_WEEKLY",
                        "end": "2026-08-15T01:53:09.930537+00:00"
                    },
                    "isUnifiedBillingUser": true
                }
            }),
            1_775_000_000,
        )
        .expect("fresh weekly period should parse");

        assert_eq!(metadata["usage_percentage"], json!(0.0));
        assert_eq!(metadata["period_type"], json!("weekly"));
    }

    #[test]
    fn parse_xai_legacy_monthly_cents() {
        let metadata = parse_xai_billing_response(
            &json!({
                "config": {
                    "monthlyLimit": {"val": 2500},
                    "used": {"val": 1000},
                    "billingPeriodEnd": "2026-09-01T00:00:00Z"
                }
            }),
            1_775_000_000,
        )
        .expect("legacy monthly payload should parse");

        assert_eq!(metadata["usage_limit"], json!(25.0));
        assert_eq!(metadata["current_usage"], json!(10.0));
        assert_eq!(metadata["remaining"], json!(15.0));
        assert_eq!(metadata["usage_percentage"], json!(40.0));
    }
}

#[cfg(test)]
#[path = "quota/tests.rs"]
mod tests;

#[cfg(test)]
mod claude_code_quota_tests {
    use super::{parse_claude_code_oauth_usage_response, parse_claude_code_usage_headers};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn parses_cedar_ember_grants_into_reset_credits() {
        let parsed = parse_claude_code_oauth_usage_response(
            &json!({
                "five_hour": {"utilization": 65.0, "resets_at": "2027-01-15T08:00:00Z"},
                "cedar_ember": {
                    "eligible": true,
                    "grants": [
                        {"id": "launch", "resets_left": 2, "clears": ["five_hour"],
                         "ends_at": "2027-01-20T00:00:00Z"},
                        {"id": "later", "resets_left": 1, "clears": ["five_hour"],
                         "starts_at": "2027-02-01T00:00:00Z"},
                        {"id": "paused", "resets_left": 1, "clears": ["five_hour"], "paused": true},
                        {"id": "spent", "resets_left": 0, "clears": ["five_hour"]},
                        {"id": "expired", "resets_left": 1, "clears": ["five_hour"],
                         "ends_at": "2027-01-01T00:00:00Z"}
                    ]
                }
            }),
            1_800_000_000,
        )
        .expect("usage should parse");

        let credits = &parsed["reset_credits"];
        assert_eq!(credits["available_count"], json!(2));
        assert_eq!(credits["credits"].as_array().map(Vec::len), Some(1));
        assert_eq!(credits["credits"][0]["display_key"], json!("Key-1"));
        assert_eq!(credits["credits"][0]["expires_at"], json!(1_800_403_200u64));
        assert!(credits["credits"][0].get("id").is_none());
        assert!(!credits.to_string().contains("launch"));
    }

    #[test]
    fn real_cedar_ember_response_survives_metadata_redaction() {
        // Shape captured from a live Claude Pro account (unrelated fields trimmed).
        let parsed = parse_claude_code_oauth_usage_response(
            &json!({
                "five_hour": {"utilization": 32.0, "resets_at": "2026-09-29T19:19:59.933008+00:00"},
                "cedar_ember": {
                    "eligible": true,
                    "at_limit": false,
                    "grants": [{
                        "id": "opus55-launch-promax-20260921",
                        "label": "Claude Opus 5.5 launch",
                        "resets_total": 1,
                        "resets_left": 1,
                        "starts_at": "2026-09-22T16:00:00+00:00",
                        "ends_at": "2026-10-22T16:00:00+00:00",
                        "clears": ["five_hour", "seven_day"],
                        "paused": false,
                        "usable_now": true,
                        "use_requires_limit": false
                    }],
                    "next_grant_id": "opus55-launch-promax-20260921"
                }
            }),
            1_790_699_000,
        )
        .expect("usage should parse");
        assert_eq!(parsed["reset_credits"]["available_count"], json!(1));

        let safe = crate::provider::redaction::admin_provider_upstream_metadata_safe_json(Some(
            &json!({ "claude_code": parsed }),
        ));
        assert_eq!(
            safe["claude_code"]["reset_credits"]["available_count"],
            json!(1),
            "redaction dropped reset_credits: {safe}"
        );
    }

    #[test]
    fn cedar_ember_null_or_empty_omits_reset_credits() {
        let parsed = parse_claude_code_oauth_usage_response(
            &json!({"five_hour": {"utilization": 1.0}, "cedar_ember": null}),
            1,
        )
        .expect("usage should parse");
        assert!(parsed["reset_credits"].is_null());
    }

    #[test]
    fn parses_unified_ratelimit_headers_into_percent_and_reset() {
        let headers = BTreeMap::from([
            (
                "Anthropic-Ratelimit-Unified-5h-Utilization".to_string(),
                "0.42".to_string(),
            ),
            (
                "anthropic-ratelimit-unified-5h-reset".to_string(),
                "1800003600".to_string(),
            ),
            (
                "anthropic-ratelimit-unified-7d-utilization".to_string(),
                "1.0".to_string(),
            ),
            (
                "anthropic-ratelimit-unified-7d-reset".to_string(),
                "1800400000000".to_string(),
            ),
            (
                "anthropic-ratelimit-unified-7d_oi-utilization".to_string(),
                "0.1".to_string(),
            ),
        ]);
        let parsed =
            parse_claude_code_usage_headers(&headers, 1_800_000_000).expect("headers should parse");

        assert_eq!(parsed["five_hour_used_percent"], json!(42.0));
        assert_eq!(parsed["five_hour_reset_at"], json!(1_800_003_600u64));
        assert_eq!(parsed["seven_day_used_percent"], json!(100.0));
        // Millisecond timestamps are normalized to seconds.
        assert_eq!(parsed["seven_day_reset_at"], json!(1_800_400_000u64));
        assert_eq!(parsed["seven_day_fable_used_percent"], json!(10.0));
        assert!(parsed.get("seven_day_fable_reset_at").is_none());
        assert_eq!(parsed["updated_at"], json!(1_800_000_000u64));
    }

    #[test]
    fn unified_ratelimit_headers_absent_yield_none() {
        let headers =
            BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
        assert!(parse_claude_code_usage_headers(&headers, 1).is_none());
    }

    #[test]
    fn parses_windows_and_skips_null_ones() {
        let parsed = parse_claude_code_oauth_usage_response(
            &json!({
                "five_hour": {"utilization": 37.5, "resets_at": "2027-01-15T08:00:00.000000+00:00"},
                "seven_day": {"utilization": 12, "resets_at": "2027-01-20T00:00:00Z"},
                "seven_day_sonnet": null,
                "seven_day_overage_included": {"utilization": 3.0, "resets_at": null}
            }),
            1_800_000_000,
        )
        .expect("usage windows should parse");

        assert_eq!(parsed["updated_at"], json!(1_800_000_000u64));
        assert_eq!(parsed["five_hour_used_percent"], json!(37.5));
        assert_eq!(parsed["five_hour_reset_at"], json!(1_800_000_000u64));
        assert_eq!(parsed["seven_day_used_percent"], json!(12.0));
        assert!(parsed.get("seven_day_sonnet_used_percent").is_none());
        assert_eq!(parsed["seven_day_fable_used_percent"], json!(3.0));
        assert!(parsed.get("seven_day_fable_reset_at").is_none());
    }

    #[test]
    fn returns_none_without_any_window() {
        assert!(parse_claude_code_oauth_usage_response(&json!({}), 1).is_none());
        assert!(parse_claude_code_oauth_usage_response(&json!({"five_hour": null}), 1).is_none());
    }
}
