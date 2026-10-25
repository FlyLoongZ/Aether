pub use aether_ai_formats::provider_compat::proxy::rules::*;

use http::{HeaderName, HeaderValue};
use serde_json::Value;

/// Validate a response rule before persistence or execution. Error messages must
/// not contain configured values, which may be secrets.
pub fn validate_response_header_rule(rule: &Value) -> Result<(), &'static str> {
    let rule = rule.as_object().ok_or("规则必须是对象")?;
    if rule.get("enabled").is_some_and(|value| !value.is_boolean()) {
        return Err("enabled 必须是布尔值");
    }
    // Allow operators to disable an invalid legacy rule without deleting it.
    if rule.get("enabled").and_then(Value::as_bool) == Some(false) {
        return Ok(());
    }
    let validate_name = |field| {
        let name = rule
            .get(field)
            .and_then(Value::as_str)
            .ok_or("响应头名称必须是字符串")?
            .trim();
        HeaderName::from_bytes(name.as_bytes()).map_err(|_| "响应头名称不符合 HTTP 规范")?;
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("content-encoding")
        {
            return Err("content-length / content-encoding 是系统保留的响应头");
        }
        Ok(())
    };
    match rule
        .get("action")
        .and_then(Value::as_str)
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("set") => {
            validate_name("key")?;
            let value = rule
                .get("value")
                .and_then(Value::as_str)
                .ok_or("响应头值必须是字符串")?;
            HeaderValue::from_str(value).map_err(|_| "响应头值包含非法 HTTP 字符")?;
        }
        Some("drop") => validate_name("key")?,
        Some("rename") => {
            validate_name("from")?;
            validate_name("to")?;
        }
        _ => return Err("action 必须是 set/drop/rename"),
    }
    Ok(())
}

pub fn validate_response_header_rules(rules: &Value) -> Result<(), String> {
    if rules.is_null() {
        return Ok(());
    }
    let rules = rules.as_array().ok_or("响应头规则必须是数组或 null")?;
    for (index, rule) in rules.iter().enumerate() {
        validate_response_header_rule(rule)
            .map_err(|reason| format!("第 {} 条响应头规则：{reason}", index + 1))?;
    }
    Ok(())
}

#[cfg(test)]
mod response_header_validation_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_invalid_names_values_and_rename_targets_without_disclosing_values() {
        for rule in [
            json!({"action": "set", "key": "bad header", "value": "secret"}),
            json!({"action": "set", "key": "x-test:", "value": "secret"}),
            json!({"action": "set", "key": "", "value": "secret"}),
            json!({"action": "set", "key": "x-test", "value": "secret\r\nInjected: true"}),
            json!({"action": "set", "key": "x-test", "value": "secret\u{0}"}),
            json!({"action": "set", "key": "x-test", "value": "secret\u{7f}"}),
            json!({"action": "drop", "key": "bad header"}),
            json!({"action": "rename", "from": "x-valid", "to": "bad header"}),
            json!({"action": "rename", "from": "bad header", "to": "x-valid"}),
            json!({"action": "rename", "from": "x-valid", "to": "Content-Length"}),
            json!({"action": "set", "key": "Content-Encoding", "value": "gzip"}),
            json!({"action": "rename", "from": "x-valid", "to": "content-encoding"}),
        ] {
            let error = validate_response_header_rules(&json!([rule])).unwrap_err();
            assert!(error.starts_with("第 1 条响应头规则："));
            assert!(!error.contains("secret"));
            assert!(!error.contains("Injected"));
        }
    }

    #[test]
    fn accepts_http_tokens_tabs_unicode_and_disabled_legacy_rules() {
        assert!(validate_response_header_rules(&json!([
            {"action": "set", "key": " X-Valid!#$%&'*+-.^_`|~ ", "value": "\t中文 value"},
            {"action": "set", "key": "x-empty", "value": ""},
            {"action": "drop", "key": "x-valid"},
            {"action": "rename", "from": "x-source", "to": "x-target"},
            {"enabled": false, "action": "set", "key": "bad header", "value": "legacy\r\nvalue"}
        ]))
        .is_ok());
        assert!(validate_response_header_rules(&Value::Null).is_ok());
        assert!(validate_response_header_rules(&json!({})).is_err());
    }
}
