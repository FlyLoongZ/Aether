#[cfg(test)]
use crate::provider_transport::provider_types::is_codex_cli_backend_url;
#[cfg(test)]
use crate::AiExecutionDecision;
#[cfg(test)]
use aether_contracts::ExecutionPlan;

/// Whether this decision asks for a backend that must not go through the
/// execution runtime's plan.
#[cfg(test)]
pub(crate) fn should_bypass_execution_runtime_decision(payload: &AiExecutionDecision) -> bool {
    let provider_api_format = payload
        .provider_api_format
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    if !crate::ai_serving::is_openai_responses_family_format(&provider_api_format) {
        return false;
    }

    payload
        .upstream_url
        .as_deref()
        .or(payload.upstream_base_url.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(is_codex_cli_backend_url)
}

/// The plan-level counterpart of
/// [`should_bypass_execution_runtime_decision`].
#[cfg(test)]
pub(crate) fn should_bypass_execution_runtime_plan(plan: &ExecutionPlan) -> bool {
    if !crate::ai_serving::is_openai_responses_family_format(&plan.provider_api_format) {
        return false;
    }

    is_codex_cli_backend_url(&plan.url)
}
