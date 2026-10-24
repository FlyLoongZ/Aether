pub(crate) mod candidate_loop;
mod orchestration;
mod outcome;
mod plan_fallback;
mod policy;
mod stream_path;
mod sync_path;

pub(crate) use candidate_loop::{
    execute_stream_plan_and_reports_with_transfer_tracker,
    execute_sync_plan_and_reports_with_transfer_tracker, ProviderTransferTracker,
};

pub(crate) use orchestration::*;
pub(crate) use outcome::{
    attach_deferred_usage_context, beautify_local_execution_client_error_message,
    build_local_execution_exhaustion, build_local_execution_runtime_miss_context,
    is_deferred_upstream_response, mark_deferred_upstream_response,
    record_failed_usage_for_deferred_response, record_failed_usage_for_exhausted_request,
    record_failed_usage_for_runtime_miss_request, LocalExecutionExhaustion,
    LocalExecutionRequestOutcome,
};

pub(crate) use plan_fallback::{
    maybe_execute_stream_via_plan_fallback, maybe_execute_sync_via_plan_fallback,
};
pub(crate) use policy::{build_direct_plan_bypass_cache_key, should_skip_direct_plan};

#[cfg(test)]
pub(crate) use policy::{
    should_bypass_execution_runtime_decision, should_bypass_execution_runtime_plan,
};
pub(crate) use stream_path::maybe_execute_via_stream_decision_path;
pub(crate) use sync_path::maybe_execute_via_sync_decision_path;
