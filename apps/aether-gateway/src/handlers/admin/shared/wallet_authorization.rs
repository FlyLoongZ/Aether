//! Authorization for wallet-shaped fields on user and API-key writes.
//!
//! `unlimited`, `initial_gift_usd`, `limit_mode` and `unlimited_balance` all mint
//! balance: an `unlimited` account pays for nothing, and a gift or an initial
//! balance is money created out of thin air. The route-level permission for
//! `create_user`/`update_user`/`update_api_key` is `admin:users:write` /
//! `admin:api_keys:write`, so without a field-level check any token that may edit
//! users could also fund them.
//!
//! The rule is: wallet fields take effect only for a caller allowed to adjust
//! admin wallet balances (`admin:wallets:write` or `admin:wallets:admin`).
//! Otherwise they are ignored and reported with a structured warning — the call
//! itself still succeeds, so a partially-authorized edit of unrelated fields does
//! not fail because of an unrelated capability. JWT (local admin) sessions carry
//! no management token and are always allowed.

use tracing::warn;

use crate::control::management_token_principal_has_permission;
use crate::handlers::admin::request::AdminRequestContext;

/// Whether the caller may change wallet balances and limits.
///
/// `None` means a local (JWT) admin session: those are not management tokens, so
/// permission scoping does not apply to them.
pub(crate) fn may_adjust_wallet_balances(request_context: &AdminRequestContext<'_>) -> bool {
    let Some(decision) = request_context.decision() else {
        return true;
    };
    // A local (JWT) admin session has no management token; permission scoping does
    // not apply to it, so it keeps the old behaviour.
    let is_management_token = decision
        .admin_principal
        .as_ref()
        .and_then(|principal| principal.management_token_id.as_deref())
        .is_some();
    if !is_management_token {
        return true;
    }
    management_token_principal_has_permission(decision, "admin:wallets:write")
        || management_token_principal_has_permission(decision, "admin:wallets:admin")
}

/// Logs which wallet fields were dropped because the caller may not mint balance.
///
/// Structured on purpose: an operator wondering why `unlimited: true` had no
/// effect needs to find this in the log, and an auditor needs to see that a
/// low-privileged token tried.
pub(crate) fn warn_dropped_wallet_fields(
    request_context: &AdminRequestContext<'_>,
    operation: &'static str,
    subject_id: Option<&str>,
    dropped_fields: &[&'static str],
) {
    if dropped_fields.is_empty() {
        return;
    }
    let management_token_id = request_context
        .decision()
        .and_then(|decision| decision.admin_principal.as_ref())
        .and_then(|principal| principal.management_token_id.as_deref())
        .unwrap_or("local-admin-session");
    warn!(
        event_name = "admin_wallet_field_ignored",
        log_type = "security",
        operation,
        management_token_id,
        subject_id = subject_id.unwrap_or("-"),
        dropped_fields = ?dropped_fields,
        required_permission = "admin:wallets:write",
        "wallet fields ignored: caller may not adjust admin wallet balances"
    );
}
