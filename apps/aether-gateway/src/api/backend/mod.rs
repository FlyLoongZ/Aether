mod admin;
mod oauth;
mod public;

pub(crate) use admin::mount_admin_routes;
pub(crate) use oauth::mount_oauth_routes;
pub(crate) use public::mount_public_support_routes;
