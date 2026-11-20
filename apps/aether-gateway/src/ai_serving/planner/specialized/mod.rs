//! Non-matrix AI surfaces such as files and images.

mod files;
mod image;

pub(crate) use self::files::{
    build_local_gemini_files_stream_attempt_source_for_kind,
    build_local_gemini_files_sync_attempt_source_for_kind,
    maybe_build_stream_local_gemini_files_decision_payload,
    maybe_build_sync_local_gemini_files_decision_payload,
};

pub(crate) use self::image::{
    build_local_image_stream_attempt_source_for_kind,
    build_local_image_sync_attempt_source_for_kind,
    maybe_build_stream_local_image_decision_payload, maybe_build_sync_local_image_decision_payload,
    set_local_openai_image_execution_exhausted_diagnostic,
};
