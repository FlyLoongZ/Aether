use std::time::Duration;

use aether_data_contracts::repository::video_tasks::VideoTaskLookupKey;
use aether_usage_runtime::build_locally_actionable_report_context_from_video_task;
use serde_json::Value;
use tokio::time::sleep;

use crate::request_candidate_runtime::resolve_locally_actionable_request_candidate_report_context;
use crate::video_tasks::{resolve_video_task_report_lookup, VideoTaskReportLookup};
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

    let context = resolve_locally_actionable_report_context_from_video_task(state, &context)
        .await
        .unwrap_or(context);

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

async fn resolve_locally_actionable_report_context_from_video_task(
    state: &AppState,
    context: &Value,
) -> Option<Value> {
    let requested_user_id = requested_report_user_id(context)?;
    let task = match resolve_video_task_report_lookup(context)? {
        VideoTaskReportLookup::Lookup(lookup) => {
            let task = state.data.find_video_task(lookup).await.ok()??;
            if !video_task_matches_requested_user(&task, requested_user_id) {
                return None;
            }
            task
        }
        VideoTaskReportLookup::TaskIdOrExternal { task_id, user_id } => {
            if let Some(task) = state
                .data
                .find_video_task(VideoTaskLookupKey::Id(task_id))
                .await
                .ok()?
                .filter(|task| video_task_matches_requested_user(task, requested_user_id))
            {
                task
            } else {
                let user_id = user_id?;
                let task = state
                    .data
                    .find_video_task(VideoTaskLookupKey::UserExternal {
                        user_id,
                        external_task_id: task_id,
                    })
                    .await
                    .ok()??;
                if !video_task_matches_requested_user(&task, requested_user_id) {
                    return None;
                }
                task
            }
        }
    };

    build_locally_actionable_report_context_from_video_task(context, &task)
}

fn requested_report_user_id(context: &Value) -> Option<Option<&str>> {
    match context.get("user_id") {
        None => Some(None),
        Some(value) => value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(Some),
    }
}

fn video_task_matches_requested_user(
    task: &aether_data_contracts::repository::video_tasks::StoredVideoTask,
    requested_user_id: Option<&str>,
) -> bool {
    let Some(requested_user_id) = requested_user_id else {
        return true;
    };
    task.user_id.as_deref().map(str::trim) == Some(requested_user_id)
}
