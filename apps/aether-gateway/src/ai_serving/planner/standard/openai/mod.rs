mod chat;
mod embedding;
mod image_intent;
mod responses;

pub(crate) use chat::{
    build_local_openai_chat_stream_attempt_source_for_kind,
    build_local_openai_chat_sync_attempt_source_for_kind,
    maybe_build_stream_local_decision_payload, maybe_build_sync_local_decision_payload,
    set_local_openai_chat_execution_exhausted_diagnostic,
};

pub(crate) use embedding::maybe_build_sync_local_openai_embedding_decision_payload;
use image_intent::openai_request_is_image_generation_intent;
pub(crate) use responses::{
    build_local_openai_responses_stream_attempt_source_for_kind,
    build_local_openai_responses_sync_attempt_source_for_kind,
    maybe_build_responses_websocket_decision,
    maybe_build_stream_local_openai_responses_decision_payload,
    maybe_build_sync_local_openai_responses_decision_payload, ResponsesWebSocketBodyNormalization,
    ResponsesWebSocketDecision, ResponsesWebSocketPinnedCandidate,
};
