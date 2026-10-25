use std::collections::BTreeMap;
use std::time::Duration;

use aether_contracts::ExecutionPlan;
use serde_json::{Map, Value};
use tracing::warn;

use crate::{AppState, GatewayError};

const RESPONSE_HEADER_RULES_KEY: &str = "response_header_rules";
const RESPONSE_HEADER_RULES_CAMEL_KEY: &str = "responseHeaderRules";
const PROVIDER_RESPONSE_HEADERS_CONTEXT_KEY: &str = "provider_response_headers";
const PROVIDER_REQUEST_STARTED_AT_UNIX_MS_CONTEXT_KEY: &str = "provider_request_started_at_unix_ms";
const PROVIDER_REQUEST_ORDER_ID_CONTEXT_KEY: &str = "provider_request_order_id";
const PROVIDER_RESPONSE_HEADERS_OBSERVED_AT_UNIX_MS_CONTEXT_KEY: &str =
    "provider_response_headers_observed_at_unix_ms";
const RESPONSE_HEADER_RULE_PROTECTED_KEYS: &[&str] = &["content-length", "content-encoding"];
const RESPONSE_HEADER_RULES_CACHE_TTL: Duration = Duration::from_secs(5);

fn endpoint_response_header_rules_from_config(config: Option<&Value>) -> Option<&Value> {
    let config = config?.as_object()?;
    config
        .get(RESPONSE_HEADER_RULES_KEY)
        .or_else(|| config.get(RESPONSE_HEADER_RULES_CAMEL_KEY))
        .filter(|value| !value.is_null())
}

async fn read_endpoint_response_header_rules(state: &AppState, endpoint_id: &str) -> Option<Value> {
    let endpoint_id = endpoint_id.trim();
    if endpoint_id.is_empty() {
        return None;
    }
    let endpoint_id = endpoint_id.to_string();

    let endpoint_id_for_load = endpoint_id.clone();
    match state
        .endpoint_response_header_rules_cache
        .get_or_load(endpoint_id, RESPONSE_HEADER_RULES_CACHE_TTL, || async {
            state
                .read_provider_catalog_endpoints_by_ids(std::slice::from_ref(&endpoint_id_for_load))
                .await
                .map(|endpoints| {
                    endpoints.into_iter().next().and_then(|endpoint| {
                        endpoint_response_header_rules_from_config(endpoint.config.as_ref())
                            .cloned()
                    })
                })
        })
        .await
    {
        Ok(rules) => rules,
        Err(err) => {
            warn!(
                event_name = "response_header_rules_endpoint_read_failed",
                log_type = "ops",
                endpoint_id = %endpoint_id_for_load,
                error = ?err,
                "gateway failed to read endpoint response header rules; skipping response header edits"
            );
            None
        }
    }
}

pub(crate) async fn apply_endpoint_response_header_rules(
    state: &AppState,
    plan: &ExecutionPlan,
    headers: &mut BTreeMap<String, String>,
    response_body: Option<&Value>,
) -> Result<(), GatewayError> {
    let Some(rules) = read_endpoint_response_header_rules(state, plan.endpoint_id.as_str()).await
    else {
        return Ok(());
    };

    let Some(rules) = rules.as_array() else {
        warn!(
            event_name = "response_header_rules_invalid_shape",
            log_type = "ops",
            endpoint_id = %plan.endpoint_id,
            "gateway skipped endpoint response header rules because response_header_rules is not an array"
        );
        return Ok(());
    };

    apply_response_header_rules(headers, rules, response_body, &plan.endpoint_id)
}

fn apply_response_header_rules(
    headers: &mut BTreeMap<String, String>,
    rules: &[Value],
    response_body: Option<&Value>,
    endpoint_id: &str,
) -> Result<(), GatewayError> {
    // Historical/imported configurations can bypass admin validation. Skip only
    // malformed rules, so valid drops and edits still protect the response.
    let rules = Value::Array(
        rules
            .iter()
            .enumerate()
            .filter_map(|(index, rule)| {
                match aether_provider_transport::rules::validate_response_header_rule(rule) {
                    Ok(()) => Some(rule.clone()),
                    Err(reason) => {
                        warn!(
                            event_name = "response_header_rule_invalid",
                            log_type = "ops",
                            endpoint_id,
                            rule_index = index + 1,
                            reason,
                            "gateway skipped an invalid response header rule"
                        );
                        None
                    }
                }
            })
            .collect(),
    );
    let empty_body = Value::Null;
    let body = response_body.unwrap_or(&empty_body);
    if !crate::provider_transport::apply_local_header_rules(
        headers,
        Some(&rules),
        RESPONSE_HEADER_RULE_PROTECTED_KEYS,
        body,
        response_body,
    ) {
        return Err(GatewayError::Internal(
            "response_header_rules 应用失败".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn attach_provider_response_headers_to_report_context(
    report_context: Option<Value>,
    provider_headers: &BTreeMap<String, String>,
    provider_request_started_at_unix_ms: u64,
    provider_response_headers_observed_at_unix_ms: u64,
    provider_request_order_id: &str,
) -> Option<Value> {
    let provider_headers = serde_json::to_value(provider_headers).ok()?;
    let mut object = match report_context {
        Some(Value::Object(object)) => object,
        Some(other) => Map::from_iter([("seed".to_string(), other)]),
        None => Map::new(),
    };
    let observation_is_absent = !object.contains_key(PROVIDER_RESPONSE_HEADERS_CONTEXT_KEY)
        && !object.contains_key(PROVIDER_REQUEST_STARTED_AT_UNIX_MS_CONTEXT_KEY)
        && !object.contains_key(PROVIDER_RESPONSE_HEADERS_OBSERVED_AT_UNIX_MS_CONTEXT_KEY)
        && !object.contains_key(PROVIDER_REQUEST_ORDER_ID_CONTEXT_KEY);
    if observation_is_absent {
        object.insert(
            PROVIDER_RESPONSE_HEADERS_CONTEXT_KEY.to_string(),
            provider_headers,
        );
        object.insert(
            PROVIDER_REQUEST_STARTED_AT_UNIX_MS_CONTEXT_KEY.to_string(),
            Value::from(provider_request_started_at_unix_ms),
        );
        object.insert(
            PROVIDER_RESPONSE_HEADERS_OBSERVED_AT_UNIX_MS_CONTEXT_KEY.to_string(),
            Value::from(provider_response_headers_observed_at_unix_ms),
        );
        object.insert(
            PROVIDER_REQUEST_ORDER_ID_CONTEXT_KEY.to_string(),
            Value::from(provider_request_order_id),
        );
    }
    Some(Value::Object(object))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn invalid_legacy_response_header_rules_do_not_break_client_responses() {
        let mut headers = BTreeMap::from([
            ("content-type".to_string(), "application/json".to_string()),
            ("x-private".to_string(), "provider-only".to_string()),
        ]);
        let rules = json!([
            {"action": "set", "key": "bad header", "value": "secret"},
            {"action": "set", "key": "x-bad-value", "value": "secret\r\nInjected: true"},
            {"action": "rename", "from": "content-type", "to": "bad target:"},
            {"action": "drop", "key": "x-private"},
            {"action": "set", "key": "x-marker", "value": "ok"},
            {"action": "rename", "from": "x-marker", "to": "x-renamed"},
            {"action": "set", "key": "content-encoding", "value": "gzip"},
            {"action": "rename", "from": "x-renamed", "to": "content-encoding"}
        ]);
        apply_response_header_rules(
            &mut headers,
            rules.as_array().unwrap(),
            None,
            "endpoint-test",
        )
        .expect("invalid rules should be skipped");
        let response = crate::api::response::build_client_response_from_parts(
            200,
            &headers,
            axum::body::Body::empty(),
            "trace-test",
            None,
        )
        .expect("legacy rules must not turn a completed request into a 500");
        assert_eq!(response.headers()["content-type"], "application/json");
        assert_eq!(response.headers()["x-renamed"], "ok");
        assert!(response.headers().get("x-private").is_none());
        assert!(response.headers().get("x-bad-value").is_none());
        assert!(response.headers().get("content-encoding").is_none());
    }

    #[test]
    fn provider_response_observation_is_first_write_wins() {
        let first_headers =
            BTreeMap::from([("x-codex-primary-used-percent".to_string(), "10".to_string())]);
        let second_headers =
            BTreeMap::from([("x-codex-primary-used-percent".to_string(), "20".to_string())]);

        let report_context = attach_provider_response_headers_to_report_context(
            Some(json!("seed-value")),
            &first_headers,
            100,
            200,
            "observation-1",
        );
        let report_context = attach_provider_response_headers_to_report_context(
            report_context,
            &second_headers,
            300,
            400,
            "observation-2",
        )
        .expect("report context should exist");

        assert_eq!(report_context["seed"], json!("seed-value"));
        assert_eq!(
            report_context["provider_response_headers"]["x-codex-primary-used-percent"],
            json!("10")
        );
        assert_eq!(
            report_context["provider_request_started_at_unix_ms"],
            json!(100)
        );
        assert_eq!(
            report_context["provider_response_headers_observed_at_unix_ms"],
            json!(200)
        );
        assert_eq!(
            report_context["provider_request_order_id"],
            json!("observation-1")
        );
    }

    #[test]
    fn provider_response_observation_does_not_complete_a_partial_triplet() {
        let report_context = attach_provider_response_headers_to_report_context(
            Some(json!({"provider_response_headers": {"x-existing": "1"}})),
            &BTreeMap::from([("x-new".to_string(), "2".to_string())]),
            300,
            400,
            "observation-2",
        )
        .expect("report context should exist");

        assert_eq!(
            report_context["provider_response_headers"]["x-existing"],
            json!("1")
        );
        assert!(report_context
            .get("provider_request_started_at_unix_ms")
            .is_none());
        assert!(report_context
            .get("provider_response_headers_observed_at_unix_ms")
            .is_none());
        assert!(report_context.get("provider_request_order_id").is_none());
    }
}
