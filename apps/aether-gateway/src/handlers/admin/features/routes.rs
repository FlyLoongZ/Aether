use super::{background_tasks, gemini_files};
use crate::handlers::admin::request::{AdminRouteRequest, AdminRouteResult};

pub(crate) async fn maybe_build_local_admin_features_response(
    request: AdminRouteRequest<'_>,
) -> AdminRouteResult {
    if let Some(response) = background_tasks::maybe_build_local_admin_background_tasks_response(
        &request.state(),
        &request.request_context(),
        request.request_body(),
    )
    .await?
    {
        return Ok(Some(response));
    }

    if let Some(response) = gemini_files::maybe_build_local_admin_gemini_files_response(
        &request.state(),
        &request.request_context(),
        request.request_body(),
    )
    .await?
    {
        return Ok(Some(response));
    }

    Ok(None)
}
