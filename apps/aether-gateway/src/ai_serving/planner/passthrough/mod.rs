//! Requests that can stay in the same public/provider contract family.

mod provider;

pub(crate) use self::provider::{
    build_local_stream_attempt_source as build_local_same_format_stream_attempt_source,
    build_local_sync_attempt_source as build_local_same_format_sync_attempt_source,
    maybe_build_local_same_format_provider_decision_payload_for_candidate,
    maybe_build_pinned_stream_local_same_format_provider_decision_payload,
    maybe_build_stream_local_same_format_provider_decision_payload,
    maybe_build_sync_local_same_format_provider_decision_payload,
};
