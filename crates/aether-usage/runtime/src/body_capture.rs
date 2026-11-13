use aether_data_contracts::repository::usage::{
    BodyEncoding, CapturedBody, UpsertUsageRecord, UsageBodyCaptureState, UsageBodyField,
};
use serde_json::{Map, Value};

use crate::event::UsageEvent;
use crate::runtime::{UsageBodyCapturePolicy, UsageRequestRecordLevel};

const BASE64_IMAGE_DATA_URL_PREFIX: &str = "data:image/";
const BASE64_DATA_URL_MARKER: &str = ";base64,";

/// The four capture slots a policy pass may rewrite.
struct UsageBodyCapturePayloadMut<'a> {
    request_body: &'a mut Option<CapturedBody>,
    provider_request_body: &'a mut Option<CapturedBody>,
    response_body: &'a mut Option<CapturedBody>,
    client_response_body: &'a mut Option<CapturedBody>,
    request_metadata: &'a mut Option<Value>,
}

impl<'a> UsageBodyCapturePayloadMut<'a> {
    fn from_event(event: &'a mut UsageEvent) -> Self {
        Self {
            request_body: &mut event.data.request_body,
            provider_request_body: &mut event.data.provider_request_body,
            response_body: &mut event.data.response_body,
            client_response_body: &mut event.data.client_response_body,
            request_metadata: &mut event.data.request_metadata,
        }
    }

    fn from_record(record: &'a mut UpsertUsageRecord) -> Self {
        Self {
            request_body: &mut record.request_body,
            provider_request_body: &mut record.provider_request_body,
            response_body: &mut record.response_body,
            client_response_body: &mut record.client_response_body,
            request_metadata: &mut record.request_metadata,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UsageBodyCaptureEngine {
    policy: UsageBodyCapturePolicy,
}

impl UsageBodyCaptureEngine {
    pub fn new(policy: UsageBodyCapturePolicy) -> Self {
        Self { policy }
    }

    pub fn apply_to_event(self, event: &mut UsageEvent) {
        let force_disabled = is_sensitive_oauth_exchange(
            event.data.api_format.as_deref(),
            event.data.endpoint_api_format.as_deref(),
        );
        self.apply_to_payload(
            UsageBodyCapturePayloadMut::from_event(event),
            force_disabled,
        );
    }

    pub fn apply_to_record(self, record: &mut UpsertUsageRecord) {
        let force_disabled = is_sensitive_oauth_exchange(
            record.api_format.as_deref(),
            record.endpoint_api_format.as_deref(),
        );
        self.apply_to_payload(
            UsageBodyCapturePayloadMut::from_record(record),
            force_disabled,
        );
    }

    fn apply_to_payload(self, payload: UsageBodyCapturePayloadMut<'_>, force_disabled: bool) {
        let disabled_reason = if force_disabled {
            Some("sensitive_oauth_exchange")
        } else if matches!(self.policy.record_level, UsageRequestRecordLevel::Basic) {
            Some("request_record_level_basic")
        } else {
            None
        };
        for (key, body) in [
            ("request", payload.request_body),
            ("provider_request", payload.provider_request_body),
            ("response", payload.response_body),
            ("client_response", payload.client_response_body),
        ] {
            apply_captured_body_policy(
                self.policy,
                key,
                body,
                payload.request_metadata,
                disabled_reason,
            );
        }
    }
}

/// Applies the record-level and size policies to one captured side.
///
/// The stored payload is bytes, so every policy has to be expressed on bytes:
/// disabling replaces the side with its reason, compaction only applies to JSON
/// (a data URL lives inside a JSON document), and a size limit truncates on a
/// character boundary for textual encodings so the value stays readable.
/// OAuth token exchanges carry credentials on both sides, so they are never
/// captured even at the `full` record level.
fn is_sensitive_oauth_exchange(
    api_format: Option<&str>,
    endpoint_api_format: Option<&str>,
) -> bool {
    let is_oauth = |value: Option<&str>| {
        value.is_some_and(|value| {
            let value = value.trim().to_ascii_lowercase().replace('-', "_");
            value.contains("oauth") || value.contains("token_exchange")
        })
    };
    is_oauth(api_format) || is_oauth(endpoint_api_format)
}

fn apply_captured_body_policy(
    policy: UsageBodyCapturePolicy,
    metadata_key: &str,
    body: &mut Option<CapturedBody>,
    request_metadata: &mut Option<Value>,
    disabled_reason: Option<&'static str>,
) {
    if let Some(reason) = disabled_reason {
        *body = Some(CapturedBody::Disabled);
        upsert_body_capture_metadata_value_entry(
            request_metadata,
            metadata_key,
            Some(UsageBodyCaptureState::Disabled),
            None,
            None,
            Some(reason),
        );
        return;
    }

    let Some(CapturedBody::Bytes {
        bytes,
        encoding,
        truncated,
    }) = body.as_mut()
    else {
        return;
    };

    let source_bytes = bytes.len() as u64;
    let mut reason = None;
    if policy.compact_base64_images && *encoding == BodyEncoding::Json {
        if let Some(compacted) = compact_captured_json_images(bytes) {
            *bytes = compacted;
            *truncated = true;
            reason = Some("base64_image_data_url_compacted");
        }
    }
    if let Some(limit) = policy.max_body_bytes {
        if bytes.len() > limit {
            let keep = truncate_on_char_boundary(bytes, limit, *encoding);
            bytes.truncate(keep);
            *truncated = true;
            reason = reason.or(Some("body_capture_limit_exceeded"));
        }
    }
    let state = if *truncated {
        UsageBodyCaptureState::Truncated
    } else {
        UsageBodyCaptureState::Inline
    };
    upsert_body_capture_metadata_value_entry(
        request_metadata,
        metadata_key,
        Some(state),
        Some(bytes.len() as u64),
        Some(source_bytes),
        reason,
    );
}

/// Compacts base64 image data URLs inside a JSON document, returning the new
/// bytes when anything changed.
fn compact_captured_json_images(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(bytes).ok()?;
    if compact_base64_image_data_urls(&mut value) == 0 {
        return None;
    }
    serde_json::to_vec(&value).ok()
}

/// Truncates at a UTF-8 boundary for textual encodings; binary is cut wherever
/// the limit falls.
pub(crate) fn truncate_on_char_boundary(
    bytes: &[u8],
    limit: usize,
    encoding: BodyEncoding,
) -> usize {
    if limit >= bytes.len() {
        return bytes.len();
    }
    match encoding {
        BodyEncoding::Binary => limit,
        BodyEncoding::Json | BodyEncoding::Text | BodyEncoding::Sse => {
            let mut end = limit;
            while end > 0 && std::str::from_utf8(&bytes[..end]).is_err() {
                end -= 1;
            }
            end
        }
    }
}

fn compact_base64_image_data_urls(value: &mut Value) -> usize {
    match value {
        Value::String(text) => {
            if let Some((compacted, count)) = compact_base64_image_data_urls_in_string(text) {
                *text = compacted;
                count
            } else {
                0
            }
        }
        Value::Array(items) => items.iter_mut().map(compact_base64_image_data_urls).sum(),
        Value::Object(object) => object
            .values_mut()
            .map(compact_base64_image_data_urls)
            .sum(),
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

fn compact_base64_image_data_urls_in_string(value: &str) -> Option<(String, usize)> {
    let mut cursor = 0usize;
    let mut compacted = String::new();
    let mut replacement_count = 0usize;

    while let Some(relative_start) = value[cursor..].find(BASE64_IMAGE_DATA_URL_PREFIX) {
        let start = cursor + relative_start;
        let metadata_start = start + BASE64_IMAGE_DATA_URL_PREFIX.len();
        let Some(relative_marker) = value[metadata_start..].find(BASE64_DATA_URL_MARKER) else {
            break;
        };
        let marker = metadata_start + relative_marker;
        let media_subtype = &value[metadata_start..marker];
        if media_subtype.is_empty()
            || media_subtype.len() > 127
            || !media_subtype
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'+' | b'-'))
        {
            cursor = metadata_start;
            continue;
        }

        let payload_start = marker + BASE64_DATA_URL_MARKER.len();
        let payload_len = value.as_bytes()[payload_start..]
            .iter()
            .take_while(|byte| {
                let byte = **byte;
                byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/' | b'=')
            })
            .count();
        if payload_len == 0 {
            cursor = payload_start;
            continue;
        }
        let payload_end = payload_start + payload_len;
        let payload = &value[payload_start..payload_end];
        let Some(original_bytes) = decoded_base64_len_hint(payload) else {
            cursor = payload_end;
            continue;
        };

        compacted.push_str(&value[cursor..start]);
        compacted.push_str(&format!(
            "<base64 image omitted; original size: {}>",
            format_base64_image_size(original_bytes)
        ));
        cursor = payload_end;
        replacement_count = replacement_count.saturating_add(1);
    }

    if replacement_count == 0 {
        return None;
    }
    compacted.push_str(&value[cursor..]);
    Some((compacted, replacement_count))
}

fn format_base64_image_size(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes as f64 / GIB as f64)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{bytes} B")
    }
}

pub(crate) fn mark_usage_event_capture_truncated(metadata: &mut Option<Value>, key: &str) {
    aether_data_contracts::repository::usage::mark_usage_capture_memory_omitted(metadata, key);
}

fn upsert_body_capture_metadata_value_entry(
    metadata: &mut Option<Value>,
    key: &str,
    state: Option<UsageBodyCaptureState>,
    stored_bytes: Option<u64>,
    source_bytes: Option<u64>,
    reason: Option<&str>,
) {
    let Some(state) = state else {
        return;
    };
    let Some(body_capture_object) = body_capture_value_object_mut(metadata, 1) else {
        return;
    };
    body_capture_object.insert(
        key.to_string(),
        build_body_capture_metadata_entry(state, stored_bytes, source_bytes, reason),
    );
}

fn build_body_capture_metadata_entry(
    state: UsageBodyCaptureState,
    stored_bytes: Option<u64>,
    source_bytes: Option<u64>,
    reason: Option<&str>,
) -> Value {
    let mut entry = Map::with_capacity(4);
    entry.insert(
        "state".to_string(),
        Value::String(
            match state {
                UsageBodyCaptureState::None => "none",
                UsageBodyCaptureState::Inline => "inline",
                UsageBodyCaptureState::Truncated => "truncated",
                UsageBodyCaptureState::Disabled => "disabled",
                UsageBodyCaptureState::Unavailable => "unavailable",
            }
            .to_string(),
        ),
    );
    if let Some(stored_bytes) = stored_bytes {
        entry.insert("stored_bytes".to_string(), Value::from(stored_bytes));
    }
    if let Some(source_bytes) = source_bytes {
        entry.insert("source_bytes".to_string(), Value::from(source_bytes));
    }
    if let Some(reason) = reason {
        entry.insert("reason".to_string(), Value::String(reason.to_string()));
    }
    Value::Object(entry)
}

fn body_capture_object_mut(
    metadata: &mut Map<String, Value>,
    capacity: usize,
) -> Option<&mut Map<String, Value>> {
    let body_capture = metadata
        .entry("body_capture".to_string())
        .or_insert_with(|| Value::Object(Map::with_capacity(capacity)));
    body_capture.as_object_mut()
}

fn body_capture_value_object_mut(
    metadata: &mut Option<Value>,
    capacity: usize,
) -> Option<&mut Map<String, Value>> {
    let metadata_object = metadata
        .get_or_insert_with(|| Value::Object(Map::with_capacity(1)))
        .as_object_mut();
    let metadata_object = metadata_object?;
    body_capture_object_mut(metadata_object, capacity)
}

/// Records that a side's bytes live in the body file store. The absence of
/// this key means the bytes live in `usage_http_bodies` (or the row predates
/// the file store).
pub(crate) fn mark_body_capture_storage_file(metadata: &mut Option<Value>, side: UsageBodyField) {
    let Some(entry) = body_capture_entry_object_mut(metadata, side) else {
        return;
    };
    entry.insert("storage".to_string(), Value::String("file".to_string()));
}

/// Degrades a side whose file write failed. The accounting write still lands;
/// only the diagnostic copy is marked unavailable.
pub(crate) fn mark_body_capture_unavailable(
    metadata: &mut Option<Value>,
    side: UsageBodyField,
    reason: &str,
) {
    let Some(entry) = body_capture_entry_object_mut(metadata, side) else {
        return;
    };
    entry.insert(
        "state".to_string(),
        Value::String("unavailable".to_string()),
    );
    entry.insert("reason".to_string(), Value::String(reason.to_string()));
    entry.remove("storage");
}

fn body_capture_entry_object_mut(
    metadata: &mut Option<Value>,
    side: UsageBodyField,
) -> Option<&mut Map<String, Value>> {
    let body_capture = body_capture_value_object_mut(metadata, 1)?;
    let entry = body_capture
        .entry(side.as_capture_key().to_string())
        .or_insert_with(|| Value::Object(Map::with_capacity(1)));
    entry.as_object_mut()
}

pub(crate) fn decoded_base64_len_hint(body_base64: &str) -> Option<u64> {
    let body_base64 = body_base64.trim();
    if body_base64.is_empty() {
        return None;
    }

    let usable_len = body_base64.len();
    if usable_len % 4 == 1 {
        return None;
    }

    let padding = body_base64
        .chars()
        .rev()
        .take_while(|char| *char == '=')
        .count();
    let full_quads = usable_len / 4;
    let remainder = usable_len % 4;
    let base_len = full_quads.saturating_mul(3);
    let remainder_len = match remainder {
        0 => 0,
        2 => 1,
        3 => 2,
        _ => return None,
    };
    let decoded_len = base_len
        .saturating_add(remainder_len)
        .saturating_sub(padding.min(2));

    Some(decoded_len as u64)
}

pub fn apply_usage_body_capture_policy_to_event(
    policy: UsageBodyCapturePolicy,
    event: &mut UsageEvent,
) {
    UsageBodyCaptureEngine::new(policy).apply_to_event(event);
}

pub fn apply_usage_body_capture_policy_to_record(
    policy: UsageBodyCapturePolicy,
    record: &mut UpsertUsageRecord,
) {
    UsageBodyCaptureEngine::new(policy).apply_to_record(record);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::UsageEventType;
    use aether_data_contracts::repository::usage::BodyEncoding;
    use serde_json::json;

    fn body(bytes: &[u8], encoding: BodyEncoding) -> CapturedBody {
        CapturedBody::from_bytes(bytes.to_vec(), encoding, false)
    }

    fn full_policy() -> UsageBodyCapturePolicy {
        UsageBodyCapturePolicy {
            record_level: UsageRequestRecordLevel::Full,
            max_body_bytes: None,
            compact_base64_images: false,
            file_store_enabled: false,
        }
    }

    fn test_record() -> UpsertUsageRecord {
        UpsertUsageRecord {
            request_id: "req".to_string(),
            provider_name: "provider".to_string(),
            model: "model".to_string(),
            status: "completed".to_string(),
            billing_status: "pending".to_string(),
            updated_at_unix_secs: 1,
            ..test_record_defaults()
        }
    }

    fn test_record_defaults() -> UpsertUsageRecord {
        // `UpsertUsageRecord` has no `Default`: every field is meaningful, so the
        // fixture spells out the ones a capture test reads.
        UpsertUsageRecord {
            request_id: String::new(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: String::new(),
            model: String::new(),
            target_model: None,
            provider_id: None,
            provider_endpoint_id: None,
            provider_api_key_id: None,
            request_type: None,
            api_format: None,
            api_family: None,
            endpoint_kind: None,
            endpoint_api_format: None,
            provider_api_family: None,
            provider_endpoint_kind: None,
            has_format_conversion: None,
            is_stream: None,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            cache_creation_input_tokens: None,
            cache_creation_ephemeral_5m_input_tokens: None,
            cache_creation_ephemeral_1h_input_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_cost_usd: None,
            cache_read_cost_usd: None,
            output_price_per_1m: None,
            total_cost_usd: None,
            actual_total_cost_usd: None,
            status_code: None,
            error_message: None,
            error_category: None,
            response_time_ms: None,
            first_byte_time_ms: None,
            status: String::new(),
            billing_status: String::new(),
            request_headers: None,
            request_body: None,
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
            candidate_id: None,
            candidate_index: None,
            key_name: None,
            planner_kind: None,
            route_family: None,
            route_kind: None,
            execution_path: None,
            local_execution_runtime_miss_reason: None,
            request_metadata: None,
            finalized_at_unix_secs: None,
            created_at_unix_ms: None,
            updated_at_unix_secs: 0,
            capture_retention: Default::default(),
        }
    }

    fn metadata_state(metadata: Option<&Value>, key: &str) -> Option<String> {
        metadata?
            .get("body_capture")?
            .get(key)?
            .get("state")?
            .as_str()
            .map(ToOwned::to_owned)
    }

    #[test]
    fn compact_base64_images_replaces_nested_data_urls_with_size_placeholders() {
        let mut value = json!({
            "image": format!("data:image/png;base64,{}", "A".repeat(64)),
            "nested": [{"other": "data:image/jpeg;base64,QUJD"}],
            "text": "keep me",
        });
        let compacted = compact_base64_image_data_urls(&mut value);

        assert_eq!(compacted, 2);
        assert_eq!(value["text"], json!("keep me"));
        let image = value["image"].as_str().expect("image placeholder");
        assert!(image.starts_with("<base64 image omitted;"));
        assert!(!image.contains("AAAAAAAA"));
        let nested = value["nested"][0]["other"]
            .as_str()
            .expect("nested placeholder");
        assert!(nested.starts_with("<base64 image omitted;"));
    }

    #[test]
    fn policy_truncates_text_bodies_on_a_character_boundary() {
        let policy = UsageBodyCapturePolicy {
            max_body_bytes: Some(5),
            ..full_policy()
        };
        let mut event = UsageEvent::new(UsageEventType::Completed, "req", Default::default());
        // "汉字" is 6 bytes in UTF-8, so a 5-byte cut would split the last glyph.
        event.data.response_body = Some(body("汉字".as_bytes(), BodyEncoding::Json));

        UsageBodyCaptureEngine::new(policy).apply_to_event(&mut event);

        let CapturedBody::Bytes {
            bytes, truncated, ..
        } = event.data.response_body.as_ref().expect("captured body")
        else {
            panic!("expected bytes");
        };
        assert!(truncated);
        assert_eq!(bytes, "汉".as_bytes());
        assert!(std::str::from_utf8(bytes).is_ok());
    }

    #[test]
    fn policy_cuts_binary_bodies_at_the_limit() {
        let policy = UsageBodyCapturePolicy {
            max_body_bytes: Some(2),
            ..full_policy()
        };
        let mut record = test_record();
        record.response_body = Some(body(&[0xff, 0xfe, 0xfd], BodyEncoding::Binary));

        UsageBodyCaptureEngine::new(policy).apply_to_record(&mut record);

        let CapturedBody::Bytes { bytes, .. } = record.response_body.as_ref().expect("body") else {
            panic!("expected bytes");
        };
        assert_eq!(bytes, &[0xff, 0xfe]);
    }

    #[test]
    fn policy_records_source_and_stored_bytes_in_metadata() {
        let mut event = UsageEvent::new(UsageEventType::Completed, "req", Default::default());
        event.data.request_body = Some(body(&[b'a'; 8], BodyEncoding::Text));

        UsageBodyCaptureEngine::new(full_policy()).apply_to_event(&mut event);

        let entry =
            &event.data.request_metadata.as_ref().expect("metadata")["body_capture"]["request"];
        assert_eq!(entry["state"], json!("inline"));
        assert_eq!(entry["stored_bytes"], json!(8));
        assert_eq!(entry["source_bytes"], json!(8));
    }

    #[test]
    fn policy_marks_disabled_sides_without_touching_bytes() {
        let policy = UsageBodyCapturePolicy {
            record_level: UsageRequestRecordLevel::Basic,
            ..full_policy()
        };
        let mut event = UsageEvent::new(UsageEventType::Completed, "req", Default::default());
        event.data.response_body = Some(body(b"{}", BodyEncoding::Json));

        UsageBodyCaptureEngine::new(policy).apply_to_event(&mut event);

        assert_eq!(event.data.response_body, Some(CapturedBody::Disabled));
        assert_eq!(
            metadata_state(event.data.request_metadata.as_ref(), "response").as_deref(),
            Some("disabled")
        );
    }

    #[test]
    fn sensitive_oauth_exchange_forces_disabled_even_for_full_policy() {
        let mut event = UsageEvent::new(UsageEventType::Completed, "req", Default::default());
        event.data.api_format = Some("kiro_oauth_exchange".to_string());
        event.data.request_body = Some(body(b"{}", BodyEncoding::Json));

        UsageBodyCaptureEngine::new(full_policy()).apply_to_event(&mut event);

        assert_eq!(event.data.request_body, Some(CapturedBody::Disabled));
    }

    #[test]
    fn upsert_body_capture_metadata_value_entry_preserves_existing_metadata_fields() {
        let mut metadata = Some(json!({"body_capture": {"request": {"state": "inline"}}}));
        upsert_body_capture_metadata_value_entry(
            &mut metadata,
            "request",
            Some(UsageBodyCaptureState::Truncated),
            Some(4),
            Some(9),
            Some("body_capture_limit_exceeded"),
        );

        let entry = &metadata.as_ref().expect("metadata")["body_capture"]["request"];
        assert_eq!(entry["state"], json!("truncated"));
        assert_eq!(entry["stored_bytes"], json!(4));
        assert_eq!(entry["source_bytes"], json!(9));
        assert_eq!(entry["reason"], json!("body_capture_limit_exceeded"));
    }
}
