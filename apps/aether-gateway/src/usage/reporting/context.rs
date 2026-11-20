use std::time::Duration;

use serde_json::Value;
use tokio::time::sleep;

use crate::request_candidate_runtime::resolve_locally_actionable_request_candidate_report_context;
use crate::AppState;

pub(crate) use aether_usage_runtime::report_context_is_locally_actionable;

const REQUEST_CANDIDATE_REPORT_CONTEXT_RETRY_ATTEMPTS: usize = 5;
const REQUEST_CANDIDATE_REPORT_CONTEXT_RETRY_DELAY_MS: u64 = 50;
const PLAN_USAGE_RESERVATION_TOKEN_FIELD: &str = "plan_usage_reservation_token";
const PLAN_USAGE_RESERVATION_DEFERRED_FIELD: &str = "plan_usage_reservation_deferred";

/// The funding decision is made after planning, so it must be applied by the
/// server after the planner has produced the report context.
pub(crate) async fn attach_plan_wallet_fallback_context(
    _state: &AppState,
    report_context: Option<Value>,
    wallet_fallback: bool,
) -> Result<Option<Value>, crate::GatewayError> {
    let Some(Value::Object(mut context)) = report_context else {
        return Ok(wallet_fallback.then(|| serde_json::json!({"plan_wallet_fallback": true})));
    };
    if !wallet_fallback && !context.contains_key("plan_wallet_fallback") {
        return Ok(Some(Value::Object(context)));
    }
    context.remove("plan_wallet_fallback");
    if wallet_fallback {
        context.insert("plan_wallet_fallback".to_string(), Value::Bool(true));
        context.remove(PLAN_USAGE_RESERVATION_TOKEN_FIELD);
        context.remove(PLAN_USAGE_RESERVATION_DEFERRED_FIELD);
    }
    Ok(Some(Value::Object(context)))
}

pub(crate) async fn resolve_locally_actionable_report_context(
    state: &AppState,
    report_context: Option<&Value>,
) -> Option<Value> {
    let context = report_context?.clone();
    if report_context_is_locally_actionable(Some(&context)) {
        return Some(context);
    }

    if let Some(resolved) =
        resolve_locally_actionable_request_candidate_report_context_with_retry(state, &context)
            .await
    {
        return Some(resolved);
    }

    if let Some(resolved) =
        resolve_locally_actionable_request_candidate_report_context_with_retry(state, &context)
            .await
    {
        return Some(resolved);
    }

    report_context_is_locally_actionable(Some(&context)).then_some(context)
}

async fn resolve_locally_actionable_request_candidate_report_context_with_retry(
    state: &AppState,
    context: &Value,
) -> Option<Value> {
    context
        .get("request_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    for attempt in 0..=REQUEST_CANDIDATE_REPORT_CONTEXT_RETRY_ATTEMPTS {
        if let Some(resolved) =
            resolve_locally_actionable_request_candidate_report_context(state, context).await
        {
            return Some(resolved);
        }

        if attempt < REQUEST_CANDIDATE_REPORT_CONTEXT_RETRY_ATTEMPTS {
            sleep(Duration::from_millis(
                REQUEST_CANDIDATE_REPORT_CONTEXT_RETRY_DELAY_MS,
            ))
            .await;
        }
    }

    None
}
