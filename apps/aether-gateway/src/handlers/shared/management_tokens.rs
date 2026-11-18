use aether_data::repository::management_tokens::{
    StoredManagementToken, StoredManagementTokenUserSummary,
};
use serde_json::json;

use crate::control::management_token_permission_mode_and_summary;

use super::unix_secs_to_rfc3339;

fn build_management_token_user_payload(
    user: &StoredManagementTokenUserSummary,
) -> serde_json::Value {
    json!({
        "id": user.id,
        "email": user.email,
        "username": user.username,
        "role": user.role,
    })
}

pub(crate) fn build_management_token_payload(
    token: &StoredManagementToken,
    user: Option<&StoredManagementTokenUserSummary>,
) -> serde_json::Value {
    let (permission_mode, permission_summary) =
        management_token_permission_mode_and_summary(token.permissions.as_ref());
    let mut payload = json!({
        "id": token.id,
        "user_id": token.user_id,
        "name": token.name,
        "description": token.description,
        "token_display": token.token_display(),
        "allowed_ips": token.allowed_ips,
        "permissions": token.permissions,
        "permission_mode": permission_mode,
        "permission_summary": permission_summary,
        "expires_at": token.expires_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "last_used_at": token.last_used_at_unix_secs.and_then(unix_secs_to_rfc3339),
        "last_used_ip": token.last_used_ip,
        "usage_count": token.usage_count,
        "is_active": token.is_active,
        "created_at": token.created_at_unix_ms.and_then(unix_secs_to_rfc3339),
        "updated_at": token.updated_at_unix_secs.and_then(unix_secs_to_rfc3339),
    });
    if let Some(user) = user {
        payload["user"] = build_management_token_user_payload(user);
    }
    payload
}
