use crate::GatewayError;

pub(crate) use self::service::VideoTaskService;
pub use self::types::VideoTaskTruthSourceMode;
pub(crate) use self::types::{
    LocalVideoTaskContentAction, LocalVideoTaskFollowUpPlan, LocalVideoTaskReadRefreshPlan,
    LocalVideoTaskReadResponse, LocalVideoTaskSnapshot, VideoTaskSyncReportMode,
};
pub(crate) use aether_video_tasks_core::{
    build_internal_finalize_video_plan, build_local_sync_finalize_read_response,
    build_local_sync_finalize_request_path, resolve_local_sync_error_background_report_kind,
    resolve_local_sync_success_background_report_kind,
};

pub(crate) use self::helpers::{
    extract_openai_task_id_from_content_path, resolve_video_task_hydration_lookup_key,
    resolve_video_task_read_lookup_key, resolve_video_task_report_lookup, VideoTaskReportLookup,
};

#[cfg(test)]
pub(crate) use crate::control::GatewayControlAuthContext;

pub(crate) mod helpers;
mod service;
mod store;
pub(crate) mod types;

pub(crate) fn not_found_body() -> serde_json::Value {
    serde_json::json!({"detail": "Video task not found"})
}

pub(crate) fn not_found_error() -> GatewayError {
    GatewayError::Client {
        status: http::StatusCode::NOT_FOUND,
        message: "Video task not found".to_string(),
    }
}

#[cfg(test)]
mod tests;
