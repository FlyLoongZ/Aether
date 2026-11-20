use aether_ai_serving::{run_ai_sync_decision_path, AiSyncDecisionPathPort, AiSyncDecisionStep};
use async_trait::async_trait;

use crate::ai_serving::planner::common::{
    GEMINI_FILES_DELETE_PLAN_KIND, GEMINI_FILES_GET_PLAN_KIND, GEMINI_FILES_LIST_PLAN_KIND,
};
use crate::ai_serving::planner::route::resolve_execution_runtime_sync_plan_kind;
use crate::ai_serving::GatewayControlDecision;
use crate::{AiExecutionDecision, AppState, GatewayError};

pub(crate) async fn maybe_build_sync_decision_payload(
    state: &AppState,
    parts: &http::request::Parts,
    trace_id: &str,
    decision: &GatewayControlDecision,
    body_json: &serde_json::Value,
    body_base64: Option<&str>,
    body_is_empty: bool,
) -> Result<Option<AiExecutionDecision>, GatewayError> {
    let Some(plan_kind) = resolve_execution_runtime_sync_plan_kind(parts, decision) else {
        return Ok(None);
    };

    let port = GatewaySyncDecisionPathPort {
        state,
        parts,
        trace_id,
        decision,
        body_json,
        body_base64,
        body_is_empty,
        plan_kind,
    };

    run_ai_sync_decision_path(&port).await
}

struct GatewaySyncDecisionPathPort<'a> {
    state: &'a AppState,
    parts: &'a http::request::Parts,
    trace_id: &'a str,
    decision: &'a GatewayControlDecision,
    body_json: &'a serde_json::Value,
    body_base64: Option<&'a str>,
    body_is_empty: bool,
    plan_kind: &'a str,
}

#[async_trait]
impl AiSyncDecisionPathPort for GatewaySyncDecisionPathPort<'_> {
    type Decision = AiExecutionDecision;
    type Error = GatewayError;

    fn sync_decision_step_enabled(&self, step: AiSyncDecisionStep) -> bool {
        if step == AiSyncDecisionStep::LocalGeminiFiles {
            return matches!(
                self.plan_kind,
                GEMINI_FILES_LIST_PLAN_KIND
                    | GEMINI_FILES_GET_PLAN_KIND
                    | GEMINI_FILES_DELETE_PLAN_KIND
            );
        }
        true
    }

    async fn build_sync_decision_step(
        &self,
        step: AiSyncDecisionStep,
    ) -> Result<Option<Self::Decision>, Self::Error> {
        match step {
            AiSyncDecisionStep::LocalImage => {
                super::maybe_build_sync_local_image_decision_payload(
                    self.state,
                    self.parts,
                    self.body_json,
                    self.body_base64,
                    self.trace_id,
                    self.decision,
                    self.plan_kind,
                )
                .await
            }
            AiSyncDecisionStep::LocalOpenAiChat => {
                super::maybe_build_sync_local_decision_payload(
                    self.state,
                    self.parts,
                    self.trace_id,
                    self.decision,
                    self.body_json,
                    self.plan_kind,
                )
                .await
            }
            AiSyncDecisionStep::LocalOpenAiResponses => {
                super::maybe_build_sync_local_openai_responses_decision_payload(
                    self.state,
                    self.parts,
                    self.trace_id,
                    self.decision,
                    self.body_json,
                    self.plan_kind,
                )
                .await
            }
            AiSyncDecisionStep::LocalStandardFamily => {
                super::maybe_build_sync_local_standard_decision_payload(
                    self.state,
                    self.parts,
                    self.trace_id,
                    self.decision,
                    self.body_json,
                    self.plan_kind,
                )
                .await
            }
            AiSyncDecisionStep::LocalSameFormatProvider => {
                super::maybe_build_sync_local_same_format_provider_decision_payload(
                    self.state,
                    self.parts,
                    self.trace_id,
                    self.decision,
                    self.body_json,
                    self.plan_kind,
                )
                .await
            }
            AiSyncDecisionStep::LocalGeminiFiles => {
                super::maybe_build_sync_local_gemini_files_decision_payload(
                    self.state,
                    self.parts,
                    self.body_json,
                    self.body_base64,
                    self.body_is_empty,
                    self.trace_id,
                    self.decision,
                    self.plan_kind,
                )
                .await
            }
        }
    }
}
