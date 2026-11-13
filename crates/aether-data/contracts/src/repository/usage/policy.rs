use super::{
    BodyEncoding, CapturedBody, StoredRequestUsageAudit, UpsertUsageRecord, UsageBodyCaptureState,
    UsageBodyField,
};
use serde_json::{Map, Value};

const MAX_USAGE_CANDIDATE_INDEX: u64 = i32::MAX as u64;
const MAX_USAGE_CANDIDATE_ID_LEN: usize = 128;
const MAX_USAGE_KEY_NAME_LEN: usize = 255;
const MAX_USAGE_PLANNER_KIND_LEN: usize = 64;
const MAX_USAGE_ROUTE_FAMILY_LEN: usize = 80;
const MAX_USAGE_ROUTE_KIND_LEN: usize = 80;
const MAX_USAGE_EXECUTION_PATH_LEN: usize = 80;
const MAX_USAGE_RUNTIME_MISS_REASON_LEN: usize = 120;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ApiKeyUsageContribution {
    pub api_key_id: String,
    pub total_requests: i64,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
    pub last_used_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ApiKeyUsageDelta {
    pub total_requests: i64,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
    pub candidate_last_used_at_unix_secs: Option<u64>,
    pub removed_last_used_at_unix_secs: Option<u64>,
}

impl ApiKeyUsageDelta {
    pub fn between(before: &ApiKeyUsageContribution, after: &ApiKeyUsageContribution) -> Self {
        Self {
            total_requests: after.total_requests - before.total_requests,
            total_tokens: after.total_tokens - before.total_tokens,
            total_cost_usd: after.total_cost_usd - before.total_cost_usd,
            candidate_last_used_at_unix_secs: newer_last_used_at(
                before.last_used_at_unix_secs,
                after.last_used_at_unix_secs,
            ),
            removed_last_used_at_unix_secs: None,
        }
    }

    pub fn addition(after: &ApiKeyUsageContribution) -> Self {
        Self {
            total_requests: after.total_requests,
            total_tokens: after.total_tokens,
            total_cost_usd: after.total_cost_usd,
            candidate_last_used_at_unix_secs: after.last_used_at_unix_secs,
            removed_last_used_at_unix_secs: None,
        }
    }

    pub fn removal(before: &ApiKeyUsageContribution) -> Self {
        Self {
            total_requests: -before.total_requests,
            total_tokens: -before.total_tokens,
            total_cost_usd: -before.total_cost_usd,
            candidate_last_used_at_unix_secs: None,
            removed_last_used_at_unix_secs: before.last_used_at_unix_secs,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.total_requests == 0
            && self.total_tokens == 0
            && self.total_cost_usd == 0.0
            && self.candidate_last_used_at_unix_secs.is_none()
            && self.removed_last_used_at_unix_secs.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelUsageContribution {
    pub model: String,
    pub request_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ModelUsageDelta {
    pub request_count: i64,
}

impl ModelUsageDelta {
    pub fn between(before: &ModelUsageContribution, after: &ModelUsageContribution) -> Self {
        Self {
            request_count: after.request_count - before.request_count,
        }
    }

    pub fn addition(after: &ModelUsageContribution) -> Self {
        Self {
            request_count: after.request_count,
        }
    }

    pub fn removal(before: &ModelUsageContribution) -> Self {
        Self {
            request_count: -before.request_count,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.request_count == 0
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProviderApiKeyUsageContribution {
    pub key_id: String,
    pub request_count: i64,
    pub success_count: i64,
    pub error_count: i64,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
    pub total_response_time_ms: i64,
    pub last_used_at_unix_secs: Option<u64>,
    pub usage_created_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ProviderApiKeyUsageDelta {
    pub request_count: i64,
    pub success_count: i64,
    pub error_count: i64,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
    pub total_response_time_ms: i64,
    pub candidate_last_used_at_unix_secs: Option<u64>,
    pub removed_last_used_at_unix_secs: Option<u64>,
    pub usage_created_at_unix_secs: Option<u64>,
}

impl ProviderApiKeyUsageDelta {
    pub fn between(
        before: &ProviderApiKeyUsageContribution,
        after: &ProviderApiKeyUsageContribution,
    ) -> Self {
        Self {
            request_count: after.request_count - before.request_count,
            success_count: after.success_count - before.success_count,
            error_count: after.error_count - before.error_count,
            total_tokens: after.total_tokens - before.total_tokens,
            total_cost_usd: after.total_cost_usd - before.total_cost_usd,
            total_response_time_ms: after.total_response_time_ms - before.total_response_time_ms,
            candidate_last_used_at_unix_secs: newer_last_used_at(
                before.last_used_at_unix_secs,
                after.last_used_at_unix_secs,
            ),
            removed_last_used_at_unix_secs: None,
            usage_created_at_unix_secs: after.usage_created_at_unix_secs,
        }
    }

    pub fn addition(after: &ProviderApiKeyUsageContribution) -> Self {
        Self {
            request_count: after.request_count,
            success_count: after.success_count,
            error_count: after.error_count,
            total_tokens: after.total_tokens,
            total_cost_usd: after.total_cost_usd,
            total_response_time_ms: after.total_response_time_ms,
            candidate_last_used_at_unix_secs: after.last_used_at_unix_secs,
            removed_last_used_at_unix_secs: None,
            usage_created_at_unix_secs: after.usage_created_at_unix_secs,
        }
    }

    pub fn removal(before: &ProviderApiKeyUsageContribution) -> Self {
        Self {
            request_count: -before.request_count,
            success_count: -before.success_count,
            error_count: -before.error_count,
            total_tokens: -before.total_tokens,
            total_cost_usd: -before.total_cost_usd,
            total_response_time_ms: -before.total_response_time_ms,
            candidate_last_used_at_unix_secs: None,
            removed_last_used_at_unix_secs: before.last_used_at_unix_secs,
            usage_created_at_unix_secs: before.usage_created_at_unix_secs,
        }
    }

    pub fn is_noop(&self) -> bool {
        self.request_count == 0
            && self.success_count == 0
            && self.error_count == 0
            && self.total_tokens == 0
            && self.total_cost_usd == 0.0
            && self.total_response_time_ms == 0
            && self.candidate_last_used_at_unix_secs.is_none()
            && self.removed_last_used_at_unix_secs.is_none()
    }
}

pub fn incoming_usage_can_recover_terminal_failure(
    incoming_status: &str,
    incoming_billing_status: &str,
) -> bool {
    incoming_billing_status == "pending" && incoming_status == "completed"
}

pub fn usage_can_recover_terminal_failure(
    existing_status: &str,
    existing_billing_status: &str,
    incoming_status: &str,
    incoming_billing_status: &str,
) -> bool {
    existing_billing_status == "void"
        && matches!(existing_status, "failed" | "cancelled")
        && incoming_usage_can_recover_terminal_failure(incoming_status, incoming_billing_status)
}

/// Decide whether an incoming lifecycle event may replace an existing usage revision.
///
/// `updated_at_unix_secs` is authoritative. `finalized_at_unix_secs` breaks ties when writers
/// observe multiple transitions in the same second. Equal pending revisions may still progress to
/// streaming or terminal states, while every terminal replay requires a strictly newer revision.
/// The explicit void-failure recovery remains available at an equal revision, but never for an
/// older event.
#[allow(clippy::too_many_arguments)]
pub fn usage_lifecycle_update_allowed(
    existing_status: &str,
    existing_billing_status: &str,
    existing_updated_at_unix_secs: u64,
    existing_finalized_at_unix_secs: Option<u64>,
    incoming_status: &str,
    incoming_billing_status: &str,
    incoming_updated_at_unix_secs: u64,
    incoming_finalized_at_unix_secs: Option<u64>,
) -> bool {
    let existing_revision = (
        existing_updated_at_unix_secs,
        existing_finalized_at_unix_secs.unwrap_or_default(),
    );
    let incoming_revision = (
        incoming_updated_at_unix_secs,
        incoming_finalized_at_unix_secs.unwrap_or_default(),
    );
    if incoming_revision < existing_revision {
        return false;
    }

    let can_recover = usage_can_recover_terminal_failure(
        existing_status,
        existing_billing_status,
        incoming_status,
        incoming_billing_status,
    );
    let existing_is_terminal = matches!(existing_status, "completed" | "failed" | "cancelled");
    let incoming_is_terminal = matches!(incoming_status, "completed" | "failed" | "cancelled");
    if existing_is_terminal && !incoming_is_terminal {
        return false;
    }
    if existing_status == "streaming" && incoming_status == "pending" {
        return false;
    }
    if incoming_revision == existing_revision && existing_is_terminal && incoming_is_terminal {
        return can_recover;
    }

    true
}

pub fn strip_deprecated_usage_display_fields(mut usage: UpsertUsageRecord) -> UpsertUsageRecord {
    usage.username = None;
    usage.api_key_name = None;
    usage
}

fn sanitize_usage_record_metadata(mut usage: UpsertUsageRecord) -> UpsertUsageRecord {
    usage = strip_deprecated_usage_display_fields(usage);
    sanitize_usage_routing_fields(&mut usage, None);
    usage.error_message = None;
    usage.error_category = sanitize_usage_error_category(usage.error_category);
    if usage.error_category.is_none() && usage.status == "failed" {
        usage.error_category = usage
            .status_code
            .map(usage_error_category_for_status_code)
            .map(str::to_string);
    }
    usage.request_metadata = super::sanitize_usage_request_metadata(usage.request_metadata);
    usage
}

pub fn sanitize_usage_for_persistence(usage: UpsertUsageRecord) -> UpsertUsageRecord {
    let mut usage = sanitize_usage_record_metadata(usage);
    usage.request_headers = None;
    usage.request_body = None;
    usage.provider_request_headers = None;
    usage.provider_request_body = None;
    usage.response_headers = None;
    usage.response_body = None;
    usage.client_response_headers = None;
    usage.client_response_body = None;
    usage
}

pub fn sanitize_usage_capture_controls_for_persistence(
    mut usage: UpsertUsageRecord,
) -> UpsertUsageRecord {
    usage = record_usage_body_capture_states(usage);
    let metadata = usage
        .request_metadata
        .as_ref()
        .and_then(Value::as_object)
        .cloned();
    sanitize_usage_routing_fields(&mut usage, metadata.as_ref());
    let mut usage = sanitize_usage_record_metadata(usage);
    for headers in [
        &mut usage.request_headers,
        &mut usage.provider_request_headers,
        &mut usage.response_headers,
        &mut usage.client_response_headers,
    ] {
        *headers = sanitize_usage_headers_for_persistence(headers.take());
    }
    // A side without bytes keeps its marker: the marker is what tells a
    // repository to withdraw whatever an earlier write had captured, and the
    // state it carries is the reason the empty side is explainable at all.
    usage
}

/// Writes `body_capture.<key>.state` for every side that carries no bytes.
/// Stamps `body_capture.<key>` for every side of the record.
///
/// The ledger is the read model's only answer to "was this side captured, and
/// how completely": detail reads never load the payload tables, so a side with
/// bytes has to say so here or a captured body shows up as never captured. A
/// byte-carrying side records its state and stored size; a side without bytes
/// records the reason it is empty.
fn record_usage_body_capture_states(mut usage: UpsertUsageRecord) -> UpsertUsageRecord {
    for field in [
        UsageBodyField::RequestBody,
        UsageBodyField::ProviderRequestBody,
        UsageBodyField::ResponseBody,
        UsageBodyField::ClientResponseBody,
    ] {
        match usage.body(field) {
            Some(body @ CapturedBody::Bytes { .. }) => {
                // Extract the payload facts before taking the metadata: the
                // body borrows the record the metadata field is mutated in.
                let state = body.state_label();
                let stored_bytes = body.bytes().map(|bytes| bytes.len() as u64);
                usage.request_metadata = record_body_capture_payload_entry(
                    usage.request_metadata.take(),
                    field,
                    state,
                    stored_bytes,
                );
            }
            Some(CapturedBody::File {
                encoding,
                truncated,
                stored_bytes,
                source_bytes,
            }) => {
                let encoding = *encoding;
                let truncated = *truncated;
                let stored_bytes = *stored_bytes;
                let source_bytes = *source_bytes;
                usage.request_metadata = record_body_capture_file_entry(
                    usage.request_metadata.take(),
                    field,
                    encoding,
                    truncated,
                    stored_bytes,
                    source_bytes,
                );
            }
            _ => {
                let Some(state) = usage.body(field).and_then(CapturedBody::state_label) else {
                    continue;
                };
                usage.request_metadata =
                    record_body_capture_state(usage.request_metadata.take(), field, state);
            }
        }
    }
    usage
}

/// Stamps the ledger entry of a side that carries bytes.
///
/// `state` and `stored_bytes` are facts of the payload itself, so they are
/// written even when a capture pass already created the entry; `source_bytes`
/// and `reason` describe the capture pass and are left untouched, so a
/// stamping pass never erases why a side was truncated.
fn record_body_capture_payload_entry(
    metadata: Option<Value>,
    field: UsageBodyField,
    state: Option<UsageBodyCaptureState>,
    stored_bytes: Option<u64>,
) -> Option<Value> {
    let (state, stored_bytes) = (state?, stored_bytes?);
    let mut metadata = metadata
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let capture = metadata
        .entry("body_capture".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(capture) = capture.as_object_mut() else {
        return Some(Value::Object(metadata));
    };
    let entry = capture
        .entry(field.as_capture_key().to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(entry) = entry.as_object_mut() {
        entry.insert(
            "state".to_string(),
            Value::String(state.as_str().to_string()),
        );
        entry.insert("stored_bytes".to_string(), Value::from(stored_bytes));
        // A switch back to the database must not leave a stale file marker.
        entry.remove("storage");
    }
    Some(Value::Object(metadata))
}

/// Records a file-backed side: the state, the storage marker and the sizes.
///
/// Unlike the disabled/unavailable path, this keeps `stored_bytes` and
/// `source_bytes` because the file store is the authoritative copy; the ledger
/// is the only DB-side record of it.
fn record_body_capture_file_entry(
    metadata: Option<Value>,
    field: UsageBodyField,
    encoding: BodyEncoding,
    truncated: bool,
    stored_bytes: u64,
    source_bytes: Option<u64>,
) -> Option<Value> {
    let mut metadata = metadata
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let capture = metadata
        .entry("body_capture".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(capture) = capture.as_object_mut() else {
        return Some(Value::Object(metadata));
    };
    let entry = capture
        .entry(field.as_capture_key().to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(entry) = entry.as_object_mut() {
        entry.insert(
            "state".to_string(),
            Value::String(
                if truncated {
                    UsageBodyCaptureState::Truncated
                } else {
                    UsageBodyCaptureState::Inline
                }
                .as_str()
                .to_string(),
            ),
        );
        entry.insert("storage".to_string(), Value::String("file".to_string()));
        // The extension is the encoding, so the ledger records it too: it is
        // the only way to tell which file is the current one when a crash left
        // more than one extension behind.
        entry.insert(
            "encoding".to_string(),
            Value::String(encoding.as_str().to_string()),
        );
        entry.insert("stored_bytes".to_string(), Value::from(stored_bytes));
        if let Some(source_bytes) = source_bytes {
            entry.insert("source_bytes".to_string(), Value::from(source_bytes));
        }
    }
    Some(Value::Object(metadata))
}

/// Records `body_capture.<key>.state` without disturbing what a producer
/// already wrote there.
fn record_body_capture_state(
    metadata: Option<Value>,
    field: UsageBodyField,
    state: UsageBodyCaptureState,
) -> Option<Value> {
    let mut metadata = metadata
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let capture = metadata
        .entry("body_capture".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let Some(capture) = capture.as_object_mut() else {
        return Some(Value::Object(metadata));
    };
    let entry = capture
        .entry(field.as_capture_key().to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Some(entry) = entry.as_object_mut() {
        // The marker is the latest word on this side, so it replaces whatever a
        // producer recorded when the side still had bytes.
        entry.insert(
            "state".to_string(),
            Value::String(state.as_str().to_string()),
        );
        entry.remove("encoding");
        entry.remove("stored_bytes");
        entry.remove("storage");
    }
    Some(Value::Object(metadata))
}

pub fn sanitize_usage_headers_for_persistence(value: Option<Value>) -> Option<Value> {
    value.filter(Value::is_object)
}

fn sanitize_usage_routing_fields(
    usage: &mut UpsertUsageRecord,
    metadata: Option<&Map<String, Value>>,
) {
    usage.candidate_id = sanitize_usage_routing_string_with_metadata(
        usage.candidate_id.take(),
        metadata,
        "candidate_id",
        MAX_USAGE_CANDIDATE_ID_LEN,
        false,
    );
    usage.candidate_index =
        sanitize_usage_routing_index_with_metadata(usage.candidate_index.take(), metadata);
    usage.key_name = sanitize_usage_routing_string_with_metadata(
        usage.key_name.take(),
        metadata,
        "key_name",
        MAX_USAGE_KEY_NAME_LEN,
        true,
    );
    usage.planner_kind = sanitize_usage_routing_string_with_metadata(
        usage.planner_kind.take(),
        metadata,
        "planner_kind",
        MAX_USAGE_PLANNER_KIND_LEN,
        false,
    );
    usage.route_family = sanitize_usage_routing_string_with_metadata(
        usage.route_family.take(),
        metadata,
        "route_family",
        MAX_USAGE_ROUTE_FAMILY_LEN,
        false,
    );
    usage.route_kind = sanitize_usage_routing_string_with_metadata(
        usage.route_kind.take(),
        metadata,
        "route_kind",
        MAX_USAGE_ROUTE_KIND_LEN,
        false,
    );
    usage.execution_path = sanitize_usage_routing_string_with_metadata(
        usage.execution_path.take(),
        metadata,
        "execution_path",
        MAX_USAGE_EXECUTION_PATH_LEN,
        false,
    );
    usage.local_execution_runtime_miss_reason = sanitize_usage_routing_string_with_metadata(
        usage.local_execution_runtime_miss_reason.take(),
        metadata,
        "local_execution_runtime_miss_reason",
        MAX_USAGE_RUNTIME_MISS_REASON_LEN,
        false,
    );
}

fn sanitize_usage_routing_string_with_metadata(
    typed: Option<String>,
    metadata: Option<&Map<String, Value>>,
    key: &str,
    max_len: usize,
    allow_spaces: bool,
) -> Option<String> {
    match typed {
        Some(value) => sanitize_usage_routing_string(Some(value), max_len, allow_spaces),
        None => metadata_routing_string(metadata, key, max_len, allow_spaces),
    }
}

fn sanitize_usage_routing_index_with_metadata(
    typed: Option<u64>,
    metadata: Option<&Map<String, Value>>,
) -> Option<u64> {
    match typed {
        Some(value) => sanitize_usage_routing_index(Some(value)),
        None => metadata
            .and_then(|object| object.get("candidate_index"))
            .and_then(|value| {
                value
                    .as_u64()
                    .or_else(|| value.as_i64().and_then(|value| u64::try_from(value).ok()))
                    .filter(|value| *value <= MAX_USAGE_CANDIDATE_INDEX)
            }),
    }
}

fn metadata_routing_string(
    metadata: Option<&Map<String, Value>>,
    key: &str,
    max_len: usize,
    allow_spaces: bool,
) -> Option<String> {
    metadata
        .and_then(|object| object.get(key))
        .and_then(Value::as_str)
        .and_then(|value| {
            sanitize_usage_routing_string(Some(value.to_string()), max_len, allow_spaces)
        })
}

fn sanitize_usage_routing_index(value: Option<u64>) -> Option<u64> {
    value.filter(|value| *value <= MAX_USAGE_CANDIDATE_INDEX)
}

fn sanitize_usage_routing_string(
    value: Option<String>,
    max_len: usize,
    allow_spaces: bool,
) -> Option<String> {
    let value = value?;
    let value = value.trim();
    if value.is_empty() || value.len() > max_len {
        return None;
    }
    if !value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || b"._:/@+-".contains(&byte) || (allow_spaces && byte == b' ')
    }) {
        return None;
    }
    Some(value.to_string())
}

pub(crate) fn sanitize_usage_error_category(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_ascii_lowercase();
    let category = match value.as_str() {
        "auth"
        | "cancelled"
        | "client_error"
        | "http_error"
        | "non_success_status"
        | "provider_error"
        | "rate_limit"
        | "redirect"
        | "server_error"
        | "stream_missing_terminal_event"
        | "stream_terminal_error"
        | "upstream_error" => value,
        "" => return None,
        _ => "other_error".to_string(),
    };
    Some(category)
}

/// Return the bounded error category represented by an HTTP status code.
///
/// Stale-request cleanup may have only a candidate status code available.  Do
/// not persist provider-supplied diagnostic text in that case; derive one of
/// the same fixed categories used by the usage writer instead.
pub fn usage_error_category_for_status_code(status_code: u16) -> &'static str {
    if status_code >= 500 {
        "server_error"
    } else if status_code >= 400 {
        "client_error"
    } else if status_code >= 300 {
        "redirect"
    } else {
        "non_success_status"
    }
}

pub fn provider_api_key_usage_is_success(
    status: &str,
    status_code: Option<u16>,
    error_message: Option<&str>,
) -> bool {
    matches!(
        status,
        "completed" | "success" | "ok" | "billed" | "settled"
    ) && status_code.is_none_or(|code| code < 400)
        && error_message.is_none_or(|value| value.trim().is_empty())
}

pub fn provider_api_key_usage_is_error(
    status: &str,
    status_code: Option<u16>,
    error_message: Option<&str>,
) -> bool {
    !matches!(status, "pending" | "streaming")
        && !provider_api_key_usage_is_success(status, status_code, error_message)
}

pub fn provider_api_key_usage_contribution(
    usage: &StoredRequestUsageAudit,
) -> Option<ProviderApiKeyUsageContribution> {
    let key_id = usage
        .provider_api_key_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    let is_in_flight = matches!(usage.status.as_str(), "pending" | "streaming");
    let is_success = provider_api_key_usage_is_success(
        usage.status.as_str(),
        usage.status_code,
        usage.error_message.as_deref(),
    );
    let is_error = provider_api_key_usage_is_error(
        usage.status.as_str(),
        usage.status_code,
        usage.error_message.as_deref(),
    );

    Some(ProviderApiKeyUsageContribution {
        key_id,
        request_count: 1,
        success_count: i64::from(is_success),
        error_count: i64::from(is_error),
        total_tokens: if is_in_flight || !usage.usage_available() {
            0
        } else {
            i64::try_from(usage.total_tokens).unwrap_or(i64::MAX)
        },
        total_cost_usd: if is_in_flight
            || !usage.usage_available()
            || !usage.usage_pricing_available()
        {
            0.0
        } else if usage.total_cost_usd.is_finite() {
            usage.total_cost_usd.max(0.0)
        } else {
            0.0
        },
        total_response_time_ms: if is_success {
            usage
                .response_time_ms
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or_default()
        } else {
            0
        },
        last_used_at_unix_secs: Some(usage.created_at_unix_ms),
        usage_created_at_unix_secs: Some(usage.created_at_unix_ms),
    })
}

pub fn model_usage_contribution(usage: &StoredRequestUsageAudit) -> Option<ModelUsageContribution> {
    if matches!(usage.status.as_str(), "pending" | "streaming") {
        return None;
    }
    let model = usage.model.trim();
    if model.is_empty() {
        return None;
    }
    Some(ModelUsageContribution {
        model: model.to_string(),
        request_count: 1,
    })
}

pub fn api_key_usage_contribution(
    usage: &StoredRequestUsageAudit,
) -> Option<ApiKeyUsageContribution> {
    if matches!(usage.status.as_str(), "pending" | "streaming") {
        return None;
    }
    let api_key_id = usage
        .api_key_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_string();
    Some(ApiKeyUsageContribution {
        api_key_id,
        total_requests: 1,
        total_tokens: if usage.usage_available() {
            i64::try_from(usage.total_tokens).unwrap_or(i64::MAX)
        } else {
            0
        },
        total_cost_usd: if !usage.usage_available() || !usage.usage_pricing_available() {
            0.0
        } else if usage.total_cost_usd.is_finite() {
            usage.total_cost_usd.max(0.0)
        } else {
            0.0
        },
        last_used_at_unix_secs: Some(usage.created_at_unix_ms),
    })
}

fn newer_last_used_at(before: Option<u64>, after: Option<u64>) -> Option<u64> {
    match (before, after) {
        (Some(before), Some(after)) if after > before => Some(after),
        (None, Some(after)) => Some(after),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        sanitize_usage_capture_controls_for_persistence, sanitize_usage_error_category,
        sanitize_usage_for_persistence, usage_error_category_for_status_code,
        usage_lifecycle_update_allowed,
    };
    use crate::repository::usage::{
        BodyEncoding, CapturedBody, UpsertUsageRecord, UsageBodyCaptureStorage, UsageBodyField,
    };

    fn usage_with_http_capture() -> UpsertUsageRecord {
        UpsertUsageRecord {
            capture_retention: Default::default(),
            request_id: "req-sensitive-capture".to_string(),
            user_id: Some("user-1".to_string()),
            api_key_id: Some("key-1".to_string()),
            username: Some("alice".to_string()),
            api_key_name: Some("primary".to_string()),
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: Some("provider-1".to_string()),
            provider_endpoint_id: None,
            provider_api_key_id: None,
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(false),
            is_stream: Some(false),
            input_tokens: Some(1),
            output_tokens: Some(2),
            total_tokens: Some(3),
            cache_creation_input_tokens: None,
            cache_creation_ephemeral_5m_input_tokens: None,
            cache_creation_ephemeral_1h_input_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_cost_usd: None,
            cache_read_cost_usd: None,
            output_price_per_1m: None,
            total_cost_usd: Some(0.01),
            actual_total_cost_usd: Some(0.01),
            status_code: Some(200),
            error_message: Some("Bearer secret".to_string()),
            error_category: Some("provider_error".to_string()),
            response_time_ms: Some(10),
            first_byte_time_ms: Some(5),
            status: "completed".to_string(),
            billing_status: "settled".to_string(),
            request_headers: Some(json!({"authorization": "Bearer secret"})),
            request_body: Some(CapturedBody::json(json!({"prompt": "private"}))),
            provider_request_headers: Some(json!({"x-api-key": "secret"})),
            provider_request_body: Some(CapturedBody::json(json!({"prompt": "private"}))),
            response_headers: Some(json!({"set-cookie": "secret"})),
            response_body: Some(CapturedBody::json(json!({"output": "private"}))),
            client_response_headers: Some(json!({"x-private": "secret"})),
            client_response_body: Some(CapturedBody::json(json!({"output": "private"}))),
            candidate_id: Some("candidate-1".to_string()),
            candidate_index: Some(0),
            key_name: None,
            planner_kind: None,
            route_family: None,
            route_kind: None,
            execution_path: None,
            local_execution_runtime_miss_reason: None,
            request_metadata: Some(json!({"client_ip": "203.0.113.8"})),
            finalized_at_unix_secs: Some(2),
            created_at_unix_ms: Some(1_000),
            updated_at_unix_secs: 2,
        }
    }

    #[test]
    fn usage_error_categories_are_bounded_to_controlled_values() {
        assert_eq!(
            sanitize_usage_error_category(Some(" Server_Error ".to_string())).as_deref(),
            Some("server_error")
        );
        assert_eq!(
            sanitize_usage_error_category(Some("Authorization: Bearer secret".to_string()))
                .as_deref(),
            Some("other_error")
        );
        assert_eq!(sanitize_usage_error_category(Some("  ".to_string())), None);
    }

    #[test]
    fn status_codes_map_to_bounded_error_categories() {
        assert_eq!(usage_error_category_for_status_code(599), "server_error");
        assert_eq!(usage_error_category_for_status_code(400), "client_error");
        assert_eq!(usage_error_category_for_status_code(302), "redirect");
        assert_eq!(
            usage_error_category_for_status_code(200),
            "non_success_status"
        );
    }

    #[test]
    fn persistence_derives_missing_failed_category_from_status_code() {
        let mut usage = usage_with_http_capture();
        usage.status = "failed".to_string();
        usage.status_code = Some(429);
        usage.error_category = None;

        let usage = sanitize_usage_for_persistence(usage);
        assert_eq!(usage.error_category.as_deref(), Some("client_error"));
    }

    #[test]
    fn persistence_boundary_drops_all_http_capture_material() {
        let usage = sanitize_usage_for_persistence(usage_with_http_capture());

        assert!(usage.username.is_none());
        assert!(usage.api_key_name.is_none());
        assert!(usage.error_message.is_none());
        assert!(usage.request_headers.is_none());
        assert!(usage.request_body.is_none());
        assert!(usage.request_body_state().is_none());
        assert!(usage.provider_request_headers.is_none());
        assert!(usage.provider_request_body.is_none());
        assert!(usage.provider_request_body_state().is_none());
        assert!(usage.response_headers.is_none());
        assert!(usage.response_body.is_none());
        assert!(usage.response_body_state().is_none());
        assert!(usage.client_response_headers.is_none());
        assert!(usage.client_response_body.is_none());
        assert!(usage.client_response_body_state().is_none());
        assert_eq!(usage.error_category.as_deref(), Some("provider_error"));
        assert_eq!(usage.candidate_id.as_deref(), Some("candidate-1"));
        assert_eq!(
            usage.request_metadata,
            Some(json!({"client_ip": "203.0.113.8"}))
        );
    }

    #[test]
    fn header_capture_preserves_original_values() {
        let headers = json!({
            "Authorization": "Bearer original-token",
            "originator": "codex-cli",
            "user-agent": "codex-tui/test",
            "session-id": "session-original",
            "thread-id": "thread-original",
            "x-client-request-id": "client-request-original",
            "x-codex-beta-features": "feature-original",
            "x-codex-parent-thread-id": "parent-thread-original",
            "x-codex-turn-metadata": "{\"turn_id\":\"turn-original\"}",
            "x-codex-turn-state": "turn-state-original",
            "x-codex-window-id": "window-original",
            "x-openai-internal-codex-responses-lite": "true",
            "x-openai-subagent": "reviewer",
            "set-cookie": ["session=original", "preference=original"],
            "x-custom-header": "original-value",
            "x-null": null
        });
        assert_eq!(
            super::sanitize_usage_headers_for_persistence(Some(headers.clone())),
            Some(headers)
        );
        for invalid in [json!(null), json!("headers"), json!(["headers"])] {
            assert!(super::sanitize_usage_headers_for_persistence(Some(invalid)).is_none());
        }
    }

    #[test]
    fn auxiliary_capture_projection_keeps_bodies_and_their_markers() {
        let input = usage_with_http_capture();

        let usage = sanitize_usage_capture_controls_for_persistence(input);

        assert_eq!(
            usage.request_headers,
            Some(json!({"authorization": "Bearer secret"}))
        );
        // The projection only normalizes capture controls; it never rewrites the
        // captured bytes, including the sides a policy marked as skipped.
        assert_eq!(
            usage.request_body,
            Some(CapturedBody::json(json!({"prompt": "private"})))
        );
        assert_eq!(
            usage.provider_request_body,
            Some(CapturedBody::json(json!({"prompt": "private"})))
        );
        assert_eq!(
            usage.response_body,
            Some(CapturedBody::json(json!({"output": "private"})))
        );
        assert_eq!(
            usage.client_response_body,
            Some(CapturedBody::json(json!({"output": "private"})))
        );
    }

    #[test]
    fn auxiliary_capture_projection_preserves_all_body_directions() {
        let mut input = usage_with_http_capture();
        input.request_headers =
            Some(json!({"Content-Type": "application/json", "Authorization": "Bearer secret"}));
        let captured = sanitize_usage_capture_controls_for_persistence(input.clone());
        assert_eq!(captured.request_body, input.request_body);
        assert_eq!(captured.provider_request_body, input.provider_request_body);
        assert_eq!(captured.response_body, input.response_body);
        assert_eq!(captured.client_response_body, input.client_response_body);
    }

    #[test]
    fn auxiliary_projection_promotes_only_bounded_routing_metadata() {
        let mut input = usage_with_http_capture();
        input.candidate_id = None;
        input.candidate_index = None;
        input.key_name = None;
        input.planner_kind = None;
        input.route_family = None;
        input.route_kind = None;
        input.execution_path = None;
        input.local_execution_runtime_miss_reason = None;
        input.request_metadata = Some(json!({
            "trace_id": "trace",
            "candidate_id": "candidate-from-metadata",
            "candidate_index": 7,
            "key_name": "primary key",
            "planner_kind": "fallback",
            "route_family": "chat",
            "route_kind": "remote",
            "execution_path": "execution_runtime_stream",
            "local_execution_runtime_miss_reason": "runtime_busy",
            "authorization": "Bearer should-not-persist",
        }));

        let usage = sanitize_usage_capture_controls_for_persistence(input);

        assert_eq!(
            usage.candidate_id.as_deref(),
            Some("candidate-from-metadata")
        );
        assert_eq!(usage.candidate_index, Some(7));
        assert_eq!(usage.key_name.as_deref(), Some("primary key"));
        assert_eq!(usage.planner_kind.as_deref(), Some("fallback"));
        assert_eq!(usage.route_family.as_deref(), Some("chat"));
        assert_eq!(usage.route_kind.as_deref(), Some("remote"));
        assert_eq!(
            usage.execution_path.as_deref(),
            Some("execution_runtime_stream")
        );
        assert_eq!(
            usage.local_execution_runtime_miss_reason.as_deref(),
            Some("runtime_busy")
        );
        let metadata = usage.request_metadata.expect("projected metadata");
        assert_eq!(metadata["trace_id"], "trace");
        // The sanitizer appends the bounded body-capture ledger; it is not
        // routing metadata, but it must survive alongside it.
        assert!(metadata["body_capture"].is_object());
        for key in [
            "candidate_id",
            "candidate_index",
            "key_name",
            "planner_kind",
            "route_family",
            "route_kind",
            "execution_path",
            "local_execution_runtime_miss_reason",
            "authorization",
        ] {
            assert!(metadata.get(key).is_none(), "{key} must not persist");
        }
    }

    #[test]
    fn routing_projection_rejects_unbounded_or_control_character_values() {
        let mut input = usage_with_http_capture();
        input.candidate_id = Some("candidate\nforged".to_string());
        input.candidate_index = Some(u64::MAX);
        input.key_name = Some("key\0name".to_string());
        input.planner_kind = Some("p".repeat(65));
        input.route_family = Some("route\tname".to_string());
        input.route_kind = Some("route-kind".to_string());
        input.execution_path = Some("execution-path".to_string());
        input.local_execution_runtime_miss_reason = Some("m".repeat(121));

        let usage = sanitize_usage_for_persistence(input);

        assert!(usage.candidate_id.is_none());
        assert!(usage.candidate_index.is_none());
        assert!(usage.key_name.is_none());
        assert!(usage.planner_kind.is_none());
        assert!(usage.route_family.is_none());
        assert_eq!(usage.route_kind.as_deref(), Some("route-kind"));
        assert_eq!(usage.execution_path.as_deref(), Some("execution-path"));
        assert!(usage.local_execution_runtime_miss_reason.is_none());
    }

    #[test]
    fn invalid_typed_routing_values_do_not_fall_back_to_metadata() {
        let mut input = usage_with_http_capture();
        input.candidate_id = Some("candidate\nforged".to_string());
        input.candidate_index = Some(u64::MAX);
        input.key_name = Some("key\0name".to_string());
        input.planner_kind = Some("p".repeat(65));
        input.route_family = Some("route\tname".to_string());
        input.route_kind = Some("route\nkind".to_string());
        input.execution_path = Some("execution\npath".to_string());
        input.local_execution_runtime_miss_reason = Some("m".repeat(121));
        input.request_metadata = Some(json!({
            "candidate_id": "metadata-candidate",
            "candidate_index": 7,
            "key_name": "metadata key",
            "planner_kind": "metadata-planner",
            "route_family": "metadata-family",
            "route_kind": "metadata-kind",
            "execution_path": "metadata-path",
            "local_execution_runtime_miss_reason": "metadata-reason",
        }));

        let usage = sanitize_usage_capture_controls_for_persistence(input);

        assert!(usage.candidate_id.is_none());
        assert!(usage.candidate_index.is_none());
        assert!(usage.key_name.is_none());
        assert!(usage.planner_kind.is_none());
        assert!(usage.route_family.is_none());
        assert!(usage.route_kind.is_none());
        assert!(usage.execution_path.is_none());
        assert!(usage.local_execution_runtime_miss_reason.is_none());
    }

    #[test]
    fn lifecycle_order_rejects_stale_and_equal_conflicting_terminal_events() {
        assert!(!usage_lifecycle_update_allowed(
            "completed",
            "pending",
            20,
            Some(20),
            "failed",
            "void",
            19,
            Some(19),
        ));
        assert!(!usage_lifecycle_update_allowed(
            "completed",
            "pending",
            20,
            Some(20),
            "failed",
            "void",
            20,
            Some(20),
        ));
        assert!(!usage_lifecycle_update_allowed(
            "completed",
            "pending",
            20,
            Some(20),
            "completed",
            "settled",
            20,
            Some(20),
        ));
        assert!(usage_lifecycle_update_allowed(
            "completed",
            "pending",
            20,
            Some(20),
            "failed",
            "void",
            21,
            Some(21),
        ));
    }

    #[test]
    fn lifecycle_order_allows_same_second_progress_and_fresh_void_recovery() {
        assert!(usage_lifecycle_update_allowed(
            "pending",
            "pending",
            20,
            None,
            "streaming",
            "pending",
            20,
            None,
        ));
        assert!(usage_lifecycle_update_allowed(
            "streaming",
            "pending",
            20,
            None,
            "completed",
            "pending",
            20,
            Some(20),
        ));
        assert!(usage_lifecycle_update_allowed(
            "failed",
            "void",
            20,
            Some(20),
            "completed",
            "pending",
            20,
            Some(20),
        ));
        assert!(!usage_lifecycle_update_allowed(
            "failed",
            "void",
            20,
            Some(21),
            "completed",
            "pending",
            20,
            Some(20),
        ));
    }

    use super::{api_key_usage_contribution, provider_api_key_usage_contribution};
    use crate::repository::usage::StoredRequestUsageAudit;

    fn authoritative_unpriced_usage() -> StoredRequestUsageAudit {
        let mut usage = StoredRequestUsageAudit::new(
            "usage-realtime".to_string(),
            "request-realtime".to_string(),
            None,
            Some("downstream-key".to_string()),
            None,
            None,
            "OpenAI".to_string(),
            "gpt-realtime".to_string(),
            None,
            Some("provider-realtime".to_string()),
            Some("endpoint-realtime".to_string()),
            Some("provider-key-realtime".to_string()),
            Some("realtime".to_string()),
            Some("openai:realtime".to_string()),
            Some("openai".to_string()),
            Some("realtime".to_string()),
            Some("openai:realtime".to_string()),
            Some("openai".to_string()),
            Some("realtime".to_string()),
            false,
            true,
            120,
            40,
            160,
            9.75,
            11.25,
            Some(200),
            None,
            None,
            Some(250),
            Some(30),
            "completed".to_string(),
            "void".to_string(),
            100,
            101,
            Some(102),
        )
        .expect("usage should build");
        usage.request_metadata = Some(json!({
            "usage_available": true,
            "usage_pricing_available": false,
            "realtime_session": {
                "input_audio_tokens": 20,
                "output_audio_tokens": 10,
            }
        }));
        usage
    }

    #[test]
    fn authoritative_unpriced_usage_contributes_tokens_but_never_cost() {
        let usage = authoritative_unpriced_usage();

        let provider = provider_api_key_usage_contribution(&usage)
            .expect("provider key contribution should exist");
        assert_eq!(provider.request_count, 1);
        assert_eq!(provider.success_count, 1);
        assert_eq!(provider.total_tokens, 160);
        assert_eq!(provider.total_cost_usd, 0.0);

        let downstream =
            api_key_usage_contribution(&usage).expect("API key contribution should exist");
        assert_eq!(downstream.total_requests, 1);
        assert_eq!(downstream.total_tokens, 160);
        assert_eq!(downstream.total_cost_usd, 0.0);
    }

    #[test]
    fn capture_control_ledger_stamps_byte_carrying_sides() {
        let mut usage = usage_with_http_capture();
        usage.provider_request_body = Some(CapturedBody::from_bytes(
            b"{\"model\": \"gpt".to_vec(),
            BodyEncoding::Json,
            true,
        ));
        usage.response_body = Some(CapturedBody::Disabled);
        usage.request_metadata = Some(json!({
            // A capture pass may already have explained the truncation; the
            // stamp must not erase it.
            "body_capture": {
                "provider_request": {
                    "state": "truncated",
                    "source_bytes": 4096,
                    "reason": "body_capture_limit_exceeded"
                }
            }
        }));

        let usage = sanitize_usage_capture_controls_for_persistence(usage);
        let capture = usage.request_metadata.as_ref().unwrap()["body_capture"]
            .as_object()
            .unwrap();

        let request = &capture["request"];
        assert_eq!(request["state"], json!("inline"));
        assert_eq!(
            request["stored_bytes"].as_u64(),
            Some(
                usage
                    .request_body
                    .as_ref()
                    .and_then(CapturedBody::bytes)
                    .map(|b| b.len() as u64)
                    .unwrap_or(0)
            )
        );

        let provider_request = &capture["provider_request"];
        assert_eq!(provider_request["state"], json!("truncated"));
        assert_eq!(provider_request["source_bytes"], json!(4096));
        assert_eq!(
            provider_request["reason"],
            json!("body_capture_limit_exceeded")
        );

        assert_eq!(capture["response"]["state"], json!("disabled"));
    }

    /// The extension is the encoding, so the ledger records it too. Without
    /// it, a crash that left two extensions behind has no way to tell which
    /// file is the current one.
    #[test]
    fn capture_control_ledger_records_file_encoding() {
        let mut usage = usage_with_http_capture();
        usage.request_body = Some(CapturedBody::File {
            encoding: BodyEncoding::Sse,
            truncated: true,
            stored_bytes: 7,
            source_bytes: Some(9),
        });

        let usage = sanitize_usage_capture_controls_for_persistence(usage);
        let entry = &usage.request_metadata.as_ref().unwrap()["body_capture"]["request"];
        assert_eq!(entry["storage"], json!("file"));
        assert_eq!(entry["encoding"], json!("sse"));
        assert_eq!(entry["state"], json!("truncated"));
        assert_eq!(entry["stored_bytes"], json!(7));
        assert_eq!(entry["source_bytes"], json!(9));
    }

    #[test]
    fn body_state_reads_the_ledger_when_payload_bytes_are_not_loaded() {
        // List reads never load the payload tables: the ledger alone has to
        // describe a captured side, including one whose capture stopped early.
        let mut usage = usage_with_http_capture();
        usage.request_body = None;
        usage.request_metadata = Some(json!({
            "body_capture": {
                "request": {"state": "truncated", "stored_bytes": 128, "source_bytes": 4096,
                            "reason": "body_capture_limit_exceeded"}
            }
        }));

        let result = usage.body_capture_result(UsageBodyField::RequestBody);
        assert!(result.available);
        assert_eq!(result.storage, UsageBodyCaptureStorage::Truncated);
        assert_eq!(result.state_label(), "truncated");

        // A side retired by retention keeps saying "captured" as well.
        usage.request_metadata = Some(json!({
            "body_capture": {"response": {"state": "inline", "stored_bytes": 64}}
        }));
        usage.response_body = None;
        let result = usage.body_capture_result(UsageBodyField::ResponseBody);
        assert!(result.available);
        assert_eq!(result.storage, UsageBodyCaptureStorage::Inline);
    }
}
