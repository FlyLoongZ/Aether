mod catalog;
mod runtime;
#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use aether_model_fetch::ModelFetchRunSummary;
pub(crate) use catalog::{
    codex_catalog_credential_scope_from_stored_key, codex_catalog_targets, load_codex_catalogs,
    normalize_codex_client_version, read_codex_management_catalog, refresh_codex_catalog_target,
    store_codex_management_catalog, CodexCatalogRuntime, CodexCatalogTarget,
    NormalizedCodexClientVersion,
};
#[cfg(test)]
pub(crate) use runtime::perform_model_fetch_once;
pub(crate) use runtime::state::ModelFetchRuntimeState;
pub(crate) use runtime::{
    perform_model_fetch_for_key, perform_model_fetch_for_keys, safe_model_fetch_error,
    spawn_model_fetch_worker,
};
