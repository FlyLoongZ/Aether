use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Json;
use axum::Router;
use serde_json::json;

use crate::constants::READYZ_PATH;
use crate::AppState;

pub(crate) fn mount_core_routes(router: Router<AppState>) -> Router<AppState> {
    router.route(READYZ_PATH, get(readyz))
}

pub(crate) async fn readyz(State(_state): State<AppState>) -> impl IntoResponse {
    Json(json!({ "status": "ready" }))
}
