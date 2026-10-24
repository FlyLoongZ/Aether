pub(crate) mod endpoint_keys;
pub(crate) mod endpoints_admin;
pub(crate) mod oauth;
pub(crate) mod ops;
pub(crate) mod pool;
pub(crate) mod pool_admin;
pub(crate) mod shared;
pub(crate) mod summary;
pub(crate) mod write;

pub(crate) mod crud;
pub(crate) mod delete_task;
mod models;
pub(crate) mod query;
mod routes;
pub(crate) mod strategy;

pub(super) use self::routes::maybe_build_local_admin_provider_response;
