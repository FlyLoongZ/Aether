use aether_data_contracts::repository::candidate_selection::StoredMinimalCandidateSelectionRow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum SchedulerPriorityMode {
    #[default]
    Provider,
    GlobalKey,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SchedulerMinimalCandidateSelectionCandidate {
    pub provider_id: String,
    pub provider_name: String,
    pub provider_type: String,
    pub provider_priority: i32,
    pub endpoint_id: String,
    pub endpoint_api_format: String,
    pub key_id: String,
    pub key_name: String,
    pub key_auth_type: String,
    pub key_internal_priority: i32,
    pub key_global_priority_for_format: Option<i32>,
    pub key_capabilities: Option<serde_json::Value>,
    pub model_id: String,
    pub global_model_id: String,
    pub global_model_name: String,
    pub selected_provider_model_name: String,
    pub supports_streaming: bool,
    pub mapping_matched_model: Option<String>,
}

impl aether_cache::CacheWeight for SchedulerMinimalCandidateSelectionCandidate {
    fn cache_weight(&self) -> usize {
        aether_cache::value_weight::<Self>(
            self.provider_id.capacity()
                + self.provider_name.capacity()
                + self.provider_type.capacity()
                + self.endpoint_id.capacity()
                + self.endpoint_api_format.capacity()
                + self.key_id.capacity()
                + self.key_name.capacity()
                + self.key_auth_type.capacity()
                + self.model_id.capacity()
                + self.global_model_id.capacity()
                + self.global_model_name.capacity()
                + self.selected_provider_model_name.capacity()
                + self
                    .mapping_matched_model
                    .as_ref()
                    .map_or(0, String::capacity)
                + self
                    .key_capabilities
                    .as_ref()
                    .map_or(0, aether_cache::CacheWeight::cache_weight),
        )
    }
}

pub struct EnumerateMinimalCandidateSelectionInput<'a> {
    pub rows: Vec<StoredMinimalCandidateSelectionRow>,
    pub normalized_api_format: &'a str,
    pub request_operation: Option<&'a str>,
    pub requested_model_name: &'a str,
    pub resolved_global_model_name: &'a str,
    pub require_streaming: bool,
    pub required_capabilities: Option<&'a serde_json::Value>,
    pub auth_constraints: Option<&'a crate::SchedulerAuthConstraints>,
}
