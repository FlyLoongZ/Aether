use std::ops::Deref;
use std::path::PathBuf;

use crate::control::GatewayControlAuthContext;

use super::LocalVideoTaskFollowUpPlan;

#[derive(Debug)]
pub(crate) struct VideoTaskService(aether_video_tasks_core::VideoTaskService);

impl VideoTaskService {
    pub(crate) fn new() -> Self {
        Self(aether_video_tasks_core::VideoTaskService::new())
    }

    pub(crate) fn with_file_store(
        path: impl Into<PathBuf>,
        encryption_key: impl Into<String>,
    ) -> std::io::Result<Self> {
        Ok(Self(
            aether_video_tasks_core::VideoTaskService::with_file_store(path, encryption_key)?,
        ))
    }

    pub(crate) fn prepare_follow_up_sync_plan(
        &self,
        plan_kind: &str,
        request_path: &str,
        body_json: Option<&serde_json::Value>,
        auth_context: Option<&GatewayControlAuthContext>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        self.0.prepare_follow_up_sync_plan(
            plan_kind,
            request_path,
            body_json,
            auth_context.map(|value| value.user_id.as_str()),
            auth_context.map(|value| value.api_key_id.as_str()),
            trace_id,
        )
    }

    pub(crate) fn prepare_follow_up_sync_plan_for_user(
        &self,
        plan_kind: &str,
        request_path: &str,
        body_json: Option<&serde_json::Value>,
        auth_context: Option<&GatewayControlAuthContext>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        self.0.prepare_follow_up_sync_plan_for_user(
            plan_kind,
            request_path,
            body_json,
            auth_context.map(|value| value.user_id.as_str()),
            auth_context.map(|value| value.api_key_id.as_str()),
            trace_id,
        )
    }

    pub(crate) fn prepare_follow_up_sync_plan_for_user_id(
        &self,
        plan_kind: &str,
        request_path: &str,
        body_json: Option<&serde_json::Value>,
        user_id: &str,
        api_key_id: Option<&str>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        self.0.prepare_follow_up_sync_plan_for_user(
            plan_kind,
            request_path,
            body_json,
            Some(user_id),
            api_key_id,
            trace_id,
        )
    }
}

impl Deref for VideoTaskService {
    type Target = aether_video_tasks_core::VideoTaskService;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
