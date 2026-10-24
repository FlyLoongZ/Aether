use std::time::Duration;

use aether_routing_core::{ResolvedRoutingPolicy, RoutingSchedulingMode};
use aether_scheduler_core::SchedulerAffinityScope;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const SCHEDULER_AFFINITY_TTL: Duration = Duration::from_secs(300);
pub(crate) const SCHEDULER_AFFINITY_POLICY_REPORT_FIELD: &str = "scheduler_affinity_policy";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SchedulerAffinityPolicyContext {
    pub(crate) scheduling_mode: RoutingSchedulingMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) scope: Option<SchedulerAffinityScope>,
}

impl SchedulerAffinityPolicyContext {
    pub(crate) fn from_routing_policy(policy: &ResolvedRoutingPolicy) -> Self {
        let scope = policy
            .group_id
            .as_deref()
            .map(str::trim)
            .filter(|group_id| !group_id.is_empty())
            .map(|group_id| SchedulerAffinityScope::new(group_id, policy.group_version));
        Self {
            scheduling_mode: policy.scheduling_mode,
            scope,
        }
    }

    pub(crate) fn cache_affinity_enabled(&self) -> bool {
        self.scheduling_mode == RoutingSchedulingMode::CacheAffinity
    }
}

pub(crate) fn scheduler_affinity_policy_context_from_report_context(
    report_context: Option<&Value>,
) -> Option<SchedulerAffinityPolicyContext> {
    report_context
        .and_then(|context| context.get(SCHEDULER_AFFINITY_POLICY_REPORT_FIELD))
        .and_then(|value| serde_json::from_value(value.clone()).ok())
}

pub(crate) fn insert_scheduler_affinity_policy_report_context_field(
    extra_fields: &mut serde_json::Map<String, Value>,
    routing_policy: Option<&ResolvedRoutingPolicy>,
) {
    let Some(routing_policy) = routing_policy else {
        return;
    };
    let context = SchedulerAffinityPolicyContext::from_routing_policy(routing_policy);
    if let Ok(value) = serde_json::to_value(context) {
        extra_fields.insert(SCHEDULER_AFFINITY_POLICY_REPORT_FIELD.to_string(), value);
    }
}
