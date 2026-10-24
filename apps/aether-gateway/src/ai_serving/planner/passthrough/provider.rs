use crate::ai_serving::planner::plan_builders::{AiStreamAttempt, AiSyncAttempt};
use crate::ai_serving::GatewayControlDecision;
use crate::{AppState, GatewayError};

mod family;
mod plans;
mod request;

pub(crate) use self::family::{
    build_local_same_format_provider_candidate_attempt_source,
    maybe_build_local_same_format_provider_decision_payload_for_candidate,
    resolve_local_same_format_provider_decision_input, LocalSameFormatProviderCandidateAttempt,
    LocalSameFormatProviderCandidateAttemptSource, LocalSameFormatProviderDecisionInput,
    LocalSameFormatProviderFamily, LocalSameFormatProviderSpec,
};

pub(crate) use self::family::{
    maybe_build_pinned_stream_local_same_format_provider_decision_payload,
    maybe_build_stream_local_same_format_provider_decision_payload,
    maybe_build_sync_local_same_format_provider_decision_payload,
};
pub(crate) use self::plans::{build_local_stream_attempt_source, build_local_sync_attempt_source};

const ANTIGRAVITY_ENVELOPE_NAME: &str = "antigravity:v1internal";
