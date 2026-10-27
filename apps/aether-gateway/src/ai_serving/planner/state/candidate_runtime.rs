use serde_json::Value;

use super::PlannerAppState;

impl<'a> PlannerAppState<'a> {
    pub(crate) async fn resolve_request_candidate_required_capabilities(
        self,
        user_id: &str,
        api_key_id: &str,
        requested_model: Option<&str>,
        explicit_required_capabilities: Option<&Value>,
        model_directive_base_model: Option<&str>,
    ) -> Option<Value> {
        crate::request_candidate_runtime::resolve_request_candidate_required_capabilities(
            self.app(),
            user_id,
            api_key_id,
            requested_model,
            explicit_required_capabilities,
            model_directive_base_model,
        )
        .await
    }

    pub(crate) async fn persist_available_local_candidate(
        self,
        input: crate::request_candidate_runtime::LocalCandidatePersistenceInput<'_>,
        created_at_unix_ms: u64,
    ) -> String {
        crate::request_candidate_runtime::persist_available_local_candidate(
            self.app(),
            input,
            created_at_unix_ms,
        )
        .await
    }

    pub(crate) async fn persist_skipped_local_candidate(
        self,
        input: crate::request_candidate_runtime::LocalCandidatePersistenceInput<'_>,
        skip_reason: &str,
        finished_at_unix_ms: u64,
    ) {
        crate::request_candidate_runtime::persist_skipped_local_candidate(
            self.app(),
            input,
            skip_reason,
            finished_at_unix_ms,
        )
        .await
    }
}
