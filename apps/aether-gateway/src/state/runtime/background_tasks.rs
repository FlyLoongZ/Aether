use crate::{AppState, GatewayError};
use aether_data_contracts::repository::background_tasks::{
    BackgroundTaskListQuery, BackgroundTaskSummary, StoredBackgroundTaskEvent,
    StoredBackgroundTaskRun, StoredBackgroundTaskRunPage, UpdateBackgroundTaskRunProgress,
    UpsertBackgroundTaskEvent, UpsertBackgroundTaskRun,
};

impl AppState {
    pub(crate) async fn find_background_task_run(
        &self,
        run_id: &str,
    ) -> Result<Option<StoredBackgroundTaskRun>, GatewayError> {
        self.data
            .find_background_task_run(run_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_background_task_runs(
        &self,
        query: &BackgroundTaskListQuery,
    ) -> Result<StoredBackgroundTaskRunPage, GatewayError> {
        self.data
            .list_background_task_runs(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_background_task_events(
        &self,
        run_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<StoredBackgroundTaskEvent>, GatewayError> {
        self.data
            .list_background_task_events(run_id, offset, limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn summarize_background_task_runs(
        &self,
    ) -> Result<BackgroundTaskSummary, GatewayError> {
        self.data
            .summarize_background_task_runs()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_background_task_run(
        &self,
        run: UpsertBackgroundTaskRun,
    ) -> Result<Option<StoredBackgroundTaskRun>, GatewayError> {
        self.data
            .upsert_background_task_run(run)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn update_background_task_run_progress(
        &self,
        update: UpdateBackgroundTaskRunProgress,
    ) -> Result<bool, GatewayError> {
        self.data
            .update_background_task_run_progress(update)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn request_cancel_background_task_run(
        &self,
        run_id: &str,
        updated_at_unix_secs: u64,
    ) -> Result<bool, GatewayError> {
        self.data
            .request_cancel_background_task_run(run_id, updated_at_unix_secs)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_background_task_event(
        &self,
        event: UpsertBackgroundTaskEvent,
    ) -> Result<Option<StoredBackgroundTaskEvent>, GatewayError> {
        self.data
            .upsert_background_task_event(event)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }
}
