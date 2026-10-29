use super::payload::{
    provider_query_extract_api_key_id, provider_query_extract_api_key_ids,
    provider_query_extract_force_refresh, provider_query_extract_provider_id,
};
use super::response::{
    build_admin_provider_query_bad_request_response, build_admin_provider_query_not_found_response,
    ADMIN_PROVIDER_QUERY_API_KEY_NOT_FOUND_DETAIL, ADMIN_PROVIDER_QUERY_NO_ACTIVE_API_KEY_DETAIL,
    ADMIN_PROVIDER_QUERY_PROVIDER_ID_REQUIRED_DETAIL,
    ADMIN_PROVIDER_QUERY_PROVIDER_NOT_FOUND_DETAIL,
};
use crate::clock::current_unix_secs;
use crate::handlers::admin::provider::shared::model_test_capabilities::{
    admin_provider_model_supports_image_generation, admin_provider_model_test_capabilities_payload,
};
use crate::handlers::admin::request::AdminAppState;
use crate::model_fetch::{safe_model_fetch_error, ModelFetchRuntimeState};
use crate::{AppState, GatewayError};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_model_fetch::{
    aggregate_models_for_cache, fetch_models_from_transports_for_management,
    model_catalog_upstream_metadata, preset_models_for_provider, selected_models_fetch_endpoints,
    upstream_metadata_namespace_updates,
};
use axum::{
    body::Body,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::collections::BTreeSet;

const ADMIN_PROVIDER_QUERY_NO_ACTIVE_ENDPOINT_DETAIL: &str =
    "No active endpoints found for this provider";
const ADMIN_PROVIDER_QUERY_NO_MODELS_FROM_ENDPOINT_DETAIL: &str =
    "No models returned from any endpoint";
const ADMIN_PROVIDER_QUERY_NO_MODELS_FROM_KEY_DETAIL: &str = "No models returned from any key";
const ANTIGRAVITY_PROVIDER_CACHE_KEY_PREFIX: &str = "upstream_models_provider:";

#[derive(Debug)]
struct ProviderQueryKeyFetchResult {
    models: Vec<Value>,
    error: Option<String>,
    warning: Option<String>,
    from_cache: bool,
    has_success: bool,
}

fn provider_query_select_model_keys(
    keys: Vec<StoredProviderCatalogKey>,
    selected_key_ids: Option<&BTreeSet<String>>,
) -> Result<Vec<StoredProviderCatalogKey>, ()> {
    if selected_key_ids.is_some_and(|selected| {
        selected
            .iter()
            .any(|key_id| !keys.iter().any(|key| key.id == *key_id))
    }) {
        return Err(());
    }
    Ok(match selected_key_ids {
        Some(selected) => keys
            .into_iter()
            .filter(|key| selected.contains(&key.id))
            .collect(),
        None => keys.into_iter().filter(|key| key.is_active).collect(),
    })
}

fn provider_query_attach_model_test_capabilities(
    provider: &StoredProviderCatalogProvider,
    models: Vec<Value>,
) -> Vec<Value> {
    models
        .into_iter()
        .map(|mut model| {
            let Some(object) = model.as_object_mut() else {
                return model;
            };
            let model_id = object
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or_default()
                .to_string();
            let supports_image_generation = admin_provider_model_supports_image_generation(
                &provider.provider_type,
                &model_id,
                object
                    .get("supports_image_generation")
                    .or_else(|| object.get("effective_supports_image_generation"))
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            );
            object.insert(
                "model_test_capabilities".to_string(),
                admin_provider_model_test_capabilities_payload(
                    &provider.provider_type,
                    &model_id,
                    supports_image_generation,
                ),
            );
            model
        })
        .collect()
}

fn provider_query_codex_preset_fallback(
    provider: &StoredProviderCatalogProvider,
    fetch_error: &str,
) -> Option<ProviderQueryKeyFetchResult> {
    if !provider.provider_type.trim().eq_ignore_ascii_case("codex") {
        return None;
    }
    let models = preset_models_for_provider(&provider.provider_type)?;
    Some(ProviderQueryKeyFetchResult {
        models: aggregate_models_for_cache(&models),
        error: None,
        warning: Some(format!(
            "Codex 动态模型目录不可用，已使用内置模型卡：{fetch_error}"
        )),
        from_cache: false,
        has_success: true,
    })
}

fn provider_query_project_model_fetch_errors(
    errors: impl IntoIterator<Item = String>,
) -> Vec<String> {
    errors
        .into_iter()
        .map(|error| safe_model_fetch_error(&error))
        .collect()
}

async fn provider_query_persist_preset_models(
    state: &AdminAppState<'_>,
    provider: &StoredProviderCatalogProvider,
    key: &StoredProviderCatalogKey,
    models: &[Value],
) -> Result<(), GatewayError> {
    if models.is_empty() {
        return Ok(());
    }
    <AppState as ModelFetchRuntimeState>::write_upstream_models_cache(
        state.app(),
        &provider.id,
        &key.id,
        models,
    )
    .await;
    if let Some(catalog_metadata) = model_catalog_upstream_metadata(&provider.provider_type, models)
    {
        provider_query_persist_upstream_metadata(state, key, &catalog_metadata).await?;
    }
    Ok(())
}

async fn provider_query_persist_upstream_metadata(
    state: &AdminAppState<'_>,
    key: &StoredProviderCatalogKey,
    upstream_metadata: &Value,
) -> Result<(), GatewayError> {
    let updated_at = current_unix_secs();
    for (namespace, value) in
        upstream_metadata_namespace_updates(key.upstream_metadata.as_ref(), upstream_metadata)
    {
        state
            .app()
            .upsert_provider_catalog_key_upstream_metadata_namespace(
                &key.id,
                &namespace,
                &value,
                Some(updated_at),
            )
            .await?;
    }
    Ok(())
}

mod model_test;

pub(crate) use self::model_test::{
    build_admin_provider_query_test_model_failover_local_response,
    build_admin_provider_query_test_model_local_response,
};

fn provider_query_provider_payload(provider: &StoredProviderCatalogProvider) -> Value {
    json!({
        "id": provider.id.clone(),
        "name": provider.name.clone(),
        "display_name": provider.name.clone(),
        "provider_type": provider.provider_type.clone(),
    })
}

fn provider_query_key_display_name(key: &StoredProviderCatalogKey) -> String {
    let trimmed = key.name.trim();
    if trimmed.is_empty() {
        key.id.clone()
    } else {
        trimmed.to_string()
    }
}

async fn provider_query_read_cached_models(
    state: &AdminAppState<'_>,
    provider_id: &str,
    key_id: &str,
) -> Option<Vec<Value>> {
    let cache_key = format!("upstream_models:{provider_id}:{key_id}");
    let raw = state.runtime_state().kv_get(&cache_key).await.ok()??;
    let parsed = serde_json::from_str::<Vec<Value>>(&raw).ok()?;
    let models = aggregate_models_for_cache(&parsed);
    (!models.is_empty()).then_some(models)
}

async fn provider_query_read_provider_cached_models(
    state: &AdminAppState<'_>,
    provider_id: &str,
) -> Option<Vec<Value>> {
    let cache_key = format!("{ANTIGRAVITY_PROVIDER_CACHE_KEY_PREFIX}{provider_id}");
    let raw = state.runtime_state().kv_get(&cache_key).await.ok()??;
    let parsed = serde_json::from_str::<Vec<Value>>(&raw).ok()?;
    let models = aggregate_models_for_cache(&parsed);
    (!models.is_empty()).then_some(models)
}

async fn provider_query_write_provider_cached_models(
    state: &AdminAppState<'_>,
    provider_id: &str,
    models: &[Value],
) {
    let models = aggregate_models_for_cache(models);
    if models.is_empty() {
        return;
    }
    let Ok(serialized) = serde_json::to_string(&models) else {
        return;
    };
    let cache_key = format!("{ANTIGRAVITY_PROVIDER_CACHE_KEY_PREFIX}{provider_id}");
    let _ = state
        .runtime_state()
        .kv_set(
            &cache_key,
            serialized,
            Some(std::time::Duration::from_secs(
                aether_model_fetch::model_fetch_interval_minutes().saturating_mul(60),
            )),
        )
        .await;
}

fn provider_query_antigravity_tier_weight(raw_auth_config: Option<&str>) -> i32 {
    raw_auth_config
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| value.get("tier").cloned())
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .map(|tier| match tier.trim().to_ascii_lowercase().as_str() {
            "ultra" => 3,
            "pro" => 2,
            "free" => 1,
            _ => 0,
        })
        .unwrap_or(0)
}

async fn provider_query_sort_antigravity_keys(
    state: &AdminAppState<'_>,
    provider: &StoredProviderCatalogProvider,
    endpoints: &[StoredProviderCatalogEndpoint],
    keys: Vec<StoredProviderCatalogKey>,
) -> Result<Vec<StoredProviderCatalogKey>, GatewayError> {
    let mut ranked = Vec::new();
    for key in keys {
        let availability = if key.oauth_invalid_at_unix_secs.is_some() {
            0
        } else {
            1
        };
        let tier_weight = if let Some(endpoint) = selected_models_fetch_endpoints(endpoints, &key)
            .into_iter()
            .next()
        {
            state
                .app()
                .read_provider_transport_snapshot(&provider.id, &endpoint.id, &key.id)
                .await?
                .map(|transport| {
                    provider_query_antigravity_tier_weight(
                        transport.key.decrypted_auth_config.as_deref(),
                    )
                })
                .unwrap_or(0)
        } else {
            0
        };
        ranked.push(((availability, tier_weight), key));
    }
    ranked.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    Ok(ranked.into_iter().map(|(_, key)| key).collect())
}

async fn provider_query_fetch_models_for_key(
    state: &AdminAppState<'_>,
    provider: &StoredProviderCatalogProvider,
    endpoints: &[StoredProviderCatalogEndpoint],
    key: &StoredProviderCatalogKey,
    force_refresh: bool,
) -> Result<ProviderQueryKeyFetchResult, GatewayError> {
    let is_codex = provider.provider_type.trim().eq_ignore_ascii_case("codex");
    let codex_catalog = if is_codex {
        crate::model_fetch::read_codex_management_catalog(state.app(), &provider.id, &key.id).await
    } else {
        None
    };
    if !force_refresh {
        // Read through the live, credential-scoped directory before the admin cache.
        // Never reuse the old versionless cache for Codex (including cached presets).
        let cached_models = if is_codex {
            codex_catalog
                .as_ref()
                .and_then(|catalog| catalog.models.as_ref())
                .filter(|_| {
                    selected_models_fetch_endpoints(endpoints, key)
                        .iter()
                        .any(|endpoint| endpoint.api_format == "openai:responses")
                })
                .map(|models| {
                    aether_model_fetch::project_codex_models_for_legacy_cache([(
                        "openai:responses",
                        models.as_slice(),
                    )])
                })
        } else {
            provider_query_read_cached_models(state, &provider.id, &key.id).await
        };
        if let Some(cached_models) = cached_models {
            let models = cached_models;
            return Ok(ProviderQueryKeyFetchResult {
                models,
                error: None,
                warning: None,
                from_cache: true,
                has_success: true,
            });
        }
    }

    let selected_endpoints = selected_models_fetch_endpoints(endpoints, key);
    if selected_endpoints.is_empty() {
        if let Some(models) = preset_models_for_provider(&provider.provider_type) {
            let models = aggregate_models_for_cache(&models);
            provider_query_persist_preset_models(state, provider, key, &models).await?;
            let models = models;
            return Ok(ProviderQueryKeyFetchResult {
                models,
                error: None,
                warning: None,
                from_cache: false,
                has_success: true,
            });
        }
        return Ok(ProviderQueryKeyFetchResult {
            models: Vec::new(),
            error: Some(ADMIN_PROVIDER_QUERY_NO_ACTIVE_ENDPOINT_DETAIL.to_string()),
            warning: None,
            from_cache: false,
            has_success: false,
        });
    }

    let mut transports = Vec::new();
    let mut all_errors = Vec::new();
    for endpoint in selected_endpoints {
        let Some(transport) = state
            .app()
            .read_provider_transport_snapshot(&provider.id, &endpoint.id, &key.id)
            .await?
        else {
            all_errors.push(format!(
                "{} transport snapshot unavailable",
                endpoint.api_format.trim()
            ));
            continue;
        };
        transports.push(transport);
    }

    if transports.is_empty() {
        return Ok(ProviderQueryKeyFetchResult {
            models: Vec::new(),
            error: Some(all_errors.join("; ")),
            warning: None,
            from_cache: false,
            has_success: false,
        });
    }

    let dynamic_client_version = crate::ai_serving::api::codex_client_version();
    let client_version = is_codex.then(|| {
        codex_catalog
            .as_ref()
            .map(|catalog| catalog.client_version.as_str())
            .unwrap_or(dynamic_client_version.as_str())
    });
    let outcome =
        match fetch_models_from_transports_for_management(state.app(), &transports, client_version)
            .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                all_errors.extend(provider_query_project_model_fetch_errors([err]));
                if let Some(fallback) =
                    provider_query_codex_preset_fallback(provider, &all_errors.join("; "))
                {
                    provider_query_persist_preset_models(state, provider, key, &fallback.models)
                        .await?;
                    return Ok(fallback);
                }
                return Ok(ProviderQueryKeyFetchResult {
                    models: Vec::new(),
                    error: Some(all_errors.join("; ")),
                    warning: None,
                    from_cache: false,
                    has_success: false,
                });
            }
        };

    all_errors.extend(provider_query_project_model_fetch_errors(outcome.errors));
    let unique_models = outcome.legacy_models;
    if outcome.has_success && !unique_models.is_empty() {
        if all_errors.is_empty() && outcome.native_codex_catalog {
            if let Some(catalog) = codex_catalog.as_ref() {
                crate::model_fetch::store_codex_management_catalog(
                    state.app(),
                    catalog,
                    &transports,
                    outcome.cached_models,
                    outcome.etag.as_deref(),
                )
                .await;
            }
        }
        <AppState as ModelFetchRuntimeState>::write_upstream_models_cache(
            state.app(),
            &provider.id,
            &key.id,
            &unique_models,
        )
        .await;
    }
    if let Some(upstream_metadata) = outcome.upstream_metadata.as_ref() {
        provider_query_persist_upstream_metadata(state, key, upstream_metadata).await?;
    }

    if unique_models.is_empty() && !all_errors.is_empty() {
        if let Some(fallback) =
            provider_query_codex_preset_fallback(provider, &all_errors.join("; "))
        {
            provider_query_persist_preset_models(state, provider, key, &fallback.models).await?;
            return Ok(fallback);
        }
    }

    let has_models = !unique_models.is_empty();
    let mut error = if !has_models && !all_errors.is_empty() {
        Some(all_errors.join("; "))
    } else {
        None
    };
    let warning = if has_models && !all_errors.is_empty() {
        Some(all_errors.join("; "))
    } else {
        None
    };
    if !has_models && error.is_none() {
        error = Some(ADMIN_PROVIDER_QUERY_NO_MODELS_FROM_ENDPOINT_DETAIL.to_string());
    }

    Ok(ProviderQueryKeyFetchResult {
        models: unique_models,
        error,
        warning,
        from_cache: false,
        has_success: outcome.has_success,
    })
}

pub(crate) async fn build_admin_provider_query_models_response(
    state: &AdminAppState<'_>,
    payload: &serde_json::Value,
) -> Result<Response<Body>, GatewayError> {
    let Some(provider_id) = provider_query_extract_provider_id(payload) else {
        return Ok(build_admin_provider_query_bad_request_response(
            ADMIN_PROVIDER_QUERY_PROVIDER_ID_REQUIRED_DETAIL,
        ));
    };

    let Some(provider) = state
        .app()
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .find(|item| item.id == provider_id)
    else {
        return Ok(build_admin_provider_query_not_found_response(
            ADMIN_PROVIDER_QUERY_PROVIDER_NOT_FOUND_DETAIL,
        ));
    };

    let provider_ids = vec![provider.id.clone()];
    let endpoints = state
        .app()
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await?;
    let keys = state
        .app()
        .list_provider_catalog_keys_by_provider_ids(&provider_ids)
        .await?;
    let force_refresh = provider_query_extract_force_refresh(payload);

    if let Some(api_key_id) = provider_query_extract_api_key_id(payload) {
        let Some(selected_key) = keys.iter().find(|key| key.id == api_key_id) else {
            return Ok(build_admin_provider_query_not_found_response(
                ADMIN_PROVIDER_QUERY_API_KEY_NOT_FOUND_DETAIL,
            ));
        };

        let result = provider_query_fetch_models_for_key(
            state,
            &provider,
            &endpoints,
            selected_key,
            force_refresh,
        )
        .await?;
        let models = provider_query_attach_model_test_capabilities(&provider, result.models);
        let success = !models.is_empty();
        return Ok(Json(json!({
            "success": success,
            "data": {
                "models": models,
                "error": result.error,
                "warning": result.warning,
                "from_cache": result.from_cache,
            },
            "provider": provider_query_provider_payload(&provider),
        }))
        .into_response());
    }

    let selected_key_ids = provider_query_extract_api_key_ids(payload);
    let query_keys = match provider_query_select_model_keys(keys, selected_key_ids.as_ref()) {
        Ok(keys) => keys,
        Err(()) => {
            return Ok(build_admin_provider_query_not_found_response(
                ADMIN_PROVIDER_QUERY_API_KEY_NOT_FOUND_DETAIL,
            ));
        }
    };
    if query_keys.is_empty() {
        return Ok(build_admin_provider_query_bad_request_response(
            ADMIN_PROVIDER_QUERY_NO_ACTIVE_API_KEY_DETAIL,
        ));
    }
    let query_key_count = query_keys.len();

    if selected_key_ids.is_none()
        && provider
            .provider_type
            .trim()
            .eq_ignore_ascii_case("antigravity")
        && !force_refresh
    {
        if let Some(models) = provider_query_read_provider_cached_models(state, &provider.id).await
        {
            let models = provider_query_attach_model_test_capabilities(&provider, models);
            return Ok(Json(json!({
                "success": !models.is_empty(),
                "data": {
                    "models": models,
                    "error": serde_json::Value::Null,
                    "warning": serde_json::Value::Null,
                    "from_cache": true,
                    "keys_total": query_key_count,
                    "keys_cached": query_key_count,
                    "keys_fetched": 0,
                },
                "provider": provider_query_provider_payload(&provider),
            }))
            .into_response());
        }
    }

    let ordered_keys = if provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("antigravity")
    {
        provider_query_sort_antigravity_keys(state, &provider, &endpoints, query_keys).await?
    } else {
        query_keys
    };

    let mut all_models = Vec::new();
    let mut all_errors = Vec::new();
    let mut all_warnings = Vec::new();
    let mut cache_hit_count = 0usize;
    let mut fetch_count = 0usize;
    for key in &ordered_keys {
        let result =
            provider_query_fetch_models_for_key(state, &provider, &endpoints, key, force_refresh)
                .await?;
        all_models.extend(result.models);
        if let Some(error) = result.error {
            all_errors.push(format!(
                "Key {}: {}",
                provider_query_key_display_name(key),
                error
            ));
        }
        if let Some(warning) = result.warning {
            all_warnings.push(format!(
                "Key {}: {}",
                provider_query_key_display_name(key),
                warning
            ));
        }
        if result.from_cache {
            cache_hit_count += 1;
        } else {
            fetch_count += 1;
        }
        if provider
            .provider_type
            .trim()
            .eq_ignore_ascii_case("antigravity")
            && result.has_success
        {
            break;
        }
    }

    let models = aggregate_models_for_cache(&all_models);
    if selected_key_ids.is_none()
        && provider
            .provider_type
            .trim()
            .eq_ignore_ascii_case("antigravity")
        && !models.is_empty()
    {
        provider_query_write_provider_cached_models(state, &provider.id, &models).await;
    }
    let success = !models.is_empty();
    let mut all_issues = all_errors;
    all_issues.extend(all_warnings);
    let mut error = if !success && !all_issues.is_empty() {
        Some(all_issues.join("; "))
    } else {
        None
    };
    let warning = if success && !all_issues.is_empty() {
        Some(all_issues.join("; "))
    } else {
        None
    };
    if !success && error.is_none() {
        error = Some(ADMIN_PROVIDER_QUERY_NO_MODELS_FROM_KEY_DETAIL.to_string());
    }
    let models = provider_query_attach_model_test_capabilities(&provider, models);

    Ok(Json(json!({
        "success": success,
        "data": {
            "models": models,
            "error": error,
            "warning": warning,
            "from_cache": fetch_count == 0 && cache_hit_count > 0,
            "keys_total": query_key_count,
            "keys_cached": cache_hit_count,
            "keys_fetched": fetch_count,
        },
        "provider": provider_query_provider_payload(&provider),
    }))
    .into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_query_projects_transport_errors_before_exposing_them() {
        let projected = provider_query_project_model_fetch_errors(vec![
            "HTTP 401 response body: Authorization: Bearer upstream-secret".to_string(),
            "connection failed for https://user:password@example.test/v1/models?key=secret"
                .to_string(),
        ]);

        assert_eq!(
            projected,
            [
                "Upstream models fetch authentication failed (status 401)",
                "Upstream models fetch connection failed",
            ]
        );
        let exposed = projected.join("; ");
        for secret in [
            "upstream-secret",
            "Bearer",
            "user",
            "password",
            "example.test",
            "?key=secret",
        ] {
            assert!(!exposed.contains(secret));
        }
    }
}
