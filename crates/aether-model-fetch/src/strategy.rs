use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use aether_contracts::{
    ExecutionError, ExecutionErrorKind, ExecutionPlan, ExecutionResult, RequestBody,
};
use aether_crypto::{rsa_pkcs1_sha256_sign, RsaPkcs1Sha256Error};
use aether_provider_transport::antigravity::{
    resolve_local_antigravity_request_auth, AntigravityRequestAuthSupport,
};
use aether_provider_transport::vertex::{
    looks_like_vertex_ai_host, parse_vertex_service_account_auth_config,
};
use aether_provider_transport::{
    is_vertex_api_key_transport_context, resolve_transport_execution_timeouts,
    resolve_transport_profile, GatewayProviderTransportSnapshot,
};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use serde_json::{json, Value};

use crate::logic::{
    aggregate_models_for_cache, codex_model_identity, extract_error_message,
    merge_codex_models_preserving_cards, parse_codex_models_response_page,
    parse_models_response_page, preset_models_for_provider, project_codex_models_for_legacy_cache,
};
use crate::transport::{
    build_antigravity_fetch_available_models_plan, build_antigravity_load_code_assist_plan,
    build_kiro_list_available_models_plan,
    build_standard_models_fetch_execution_plan_for_client_version,
    build_vertex_models_fetch_execution_plan, ModelFetchTransportRuntime,
};

const ANTIGRAVITY_SANDBOX_BASE_URL: &str = "https://daily-cloudcode-pa.sandbox.googleapis.com";
const ANTIGRAVITY_DAILY_BASE_URL: &str = "https://daily-cloudcode-pa.googleapis.com";
const ANTIGRAVITY_PROD_BASE_URL: &str = "https://cloudcode-pa.googleapis.com";
const ANTIGRAVITY_BLOCKED_MODELS: &[&str] = &["chat_23310", "chat_20706"];
const VERTEX_API_BASE_URL: &str = "https://aiplatform.googleapis.com";
const VERTEX_MODEL_GARDEN_API_VERSION: &str = "v1beta1";
const VERTEX_PAGE_SIZE: &str = "100";
const VERTEX_MAX_PAGES: usize = 20;
const GOOGLE_CLOUD_PLATFORM_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

#[derive(Debug, Clone, PartialEq)]
pub struct ModelsFetchOutcome {
    pub fetched_model_ids: Vec<String>,
    /// Provider response cards. Versioned Codex catalogs remain byte-for-byte opaque here.
    pub cached_models: Vec<Value>,
    /// Provider cards projected into Aether's legacy admin/runtime-cache shape.
    pub legacy_models: Vec<Value>,
    pub errors: Vec<String>,
    pub has_success: bool,
    /// Only native `models` responses may populate the opaque Codex client catalog.
    pub native_codex_catalog: bool,
    pub upstream_metadata: Option<Value>,
    pub etag: Option<String>,
    pub upstream_status: Option<u16>,
}

#[derive(Debug)]
struct ConsistentValue<T> {
    value: Option<T>,
    observed: bool,
    consistent: bool,
}

impl<T> Default for ConsistentValue<T> {
    fn default() -> Self {
        Self {
            value: None,
            observed: false,
            consistent: true,
        }
    }
}

impl<T: PartialEq> ConsistentValue<T> {
    fn observe(&mut self, candidate: Option<T>) {
        if !self.observed {
            self.consistent = candidate.is_some();
            self.value = candidate;
            self.observed = true;
            return;
        }
        if self.value.as_ref() != candidate.as_ref() {
            self.consistent = false;
            self.value = None;
        }
    }

    fn finish(self) -> Option<T> {
        (self.observed && self.consistent)
            .then_some(self.value)
            .flatten()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelFetchStrategyKind {
    PresetCatalog,
    StandardTransport,
    Vertex,
    Antigravity,
    Kiro,
}

pub trait ModelFetchStrategy {
    fn provider_id(&self) -> &str;

    fn kind(&self) -> ModelFetchStrategyKind;
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectedModelFetchStrategy {
    provider_type: String,
    kind: ModelFetchStrategyKind,
    preset_models: Option<Vec<Value>>,
}

impl ModelFetchStrategy for SelectedModelFetchStrategy {
    fn provider_id(&self) -> &str {
        self.provider_type.as_str()
    }

    fn kind(&self) -> ModelFetchStrategyKind {
        self.kind
    }
}

pub async fn fetch_models_from_transports(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
) -> Result<ModelsFetchOutcome, String> {
    fetch_models_from_transports_for_client_version(runtime, transports, None).await
}

pub async fn fetch_models_from_transports_for_client_version(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
    codex_client_version: Option<&str>,
) -> Result<ModelsFetchOutcome, String> {
    let strategy = select_model_fetch_strategy(transports)
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    execute_model_fetch_strategy(
        runtime,
        transports,
        strategy,
        codex_client_version,
        codex_client_version.is_none(),
    )
    .await
    .map_err(|error| sanitize_model_fetch_error(&error))
}

/// Management also supports Codex-compatible proxies returning OpenAI `data` arrays,
/// independently of the fingerprint sent upstream. Public client catalogs stay strict.
pub async fn fetch_models_from_transports_for_management(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
    codex_client_version: Option<&str>,
) -> Result<ModelsFetchOutcome, String> {
    let strategy = select_model_fetch_strategy(transports)
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    execute_model_fetch_strategy(runtime, transports, strategy, codex_client_version, true)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))
}

fn select_model_fetch_strategy(
    transports: &[GatewayProviderTransportSnapshot],
) -> Result<SelectedModelFetchStrategy, String> {
    let Some(first_transport) = transports.first() else {
        return Err("No transport snapshots available for models fetch".to_string());
    };

    let provider_type = first_transport
        .provider
        .provider_type
        .trim()
        .to_ascii_lowercase();
    if let Some(models) = preset_models_for_provider(&provider_type) {
        if provider_type == "kiro" {
            return Ok(SelectedModelFetchStrategy {
                provider_type,
                kind: ModelFetchStrategyKind::Kiro,
                preset_models: None,
            });
        }
        if provider_type == "codex" {
            return Ok(SelectedModelFetchStrategy {
                provider_type,
                kind: ModelFetchStrategyKind::StandardTransport,
                preset_models: None,
            });
        }
        return Ok(SelectedModelFetchStrategy {
            provider_type,
            kind: ModelFetchStrategyKind::PresetCatalog,
            preset_models: Some(models),
        });
    }

    if transports.iter().any(is_vertex_api_key_transport_context) {
        return Ok(SelectedModelFetchStrategy {
            provider_type,
            kind: ModelFetchStrategyKind::Vertex,
            preset_models: None,
        });
    }

    let kind = match provider_type.as_str() {
        "antigravity" => ModelFetchStrategyKind::Antigravity,
        "vertex_ai" => ModelFetchStrategyKind::Vertex,
        _ => ModelFetchStrategyKind::StandardTransport,
    };
    Ok(SelectedModelFetchStrategy {
        provider_type,
        kind,
        preset_models: None,
    })
}

async fn execute_model_fetch_strategy(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
    strategy: SelectedModelFetchStrategy,
    codex_client_version: Option<&str>,
    allow_codex_legacy_response: bool,
) -> Result<ModelsFetchOutcome, String> {
    let Some(first_transport) = transports.first() else {
        return Err("No transport snapshots available for models fetch".to_string());
    };

    match strategy.kind() {
        ModelFetchStrategyKind::PresetCatalog => Ok(build_success_outcome(
            strategy.preset_models.unwrap_or_default(),
            None,
            true,
        )),
        ModelFetchStrategyKind::StandardTransport => {
            fetch_standard_models(
                runtime,
                transports,
                strategy.provider_id(),
                codex_client_version,
                allow_codex_legacy_response,
            )
            .await
        }
        ModelFetchStrategyKind::Vertex => fetch_vertex_models(runtime, transports).await,
        ModelFetchStrategyKind::Antigravity => {
            fetch_antigravity_models(runtime, first_transport).await
        }
        ModelFetchStrategyKind::Kiro => fetch_kiro_models(runtime, first_transport).await,
    }
}

async fn fetch_standard_models(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
    provider_type: &str,
    codex_client_version: Option<&str>,
    allow_codex_legacy_response: bool,
) -> Result<ModelsFetchOutcome, String> {
    let mut all_models = Vec::new();
    let mut successful_codex_catalogs = Vec::<(String, Vec<Value>)>::new();
    let mut errors = Vec::new();
    let mut has_success = false;
    let mut etag = ConsistentValue::default();
    let mut upstream_status = ConsistentValue::default();
    let is_codex = provider_type.trim().eq_ignore_ascii_case("codex");
    let mut native_codex_catalog = is_codex;

    for transport in transports {
        match fetch_standard_models_for_transport(
            runtime,
            transport,
            codex_client_version,
            allow_codex_legacy_response,
        )
        .await
        {
            Ok(outcome) => {
                native_codex_catalog &= outcome.native_codex_catalog;
                all_models.extend(outcome.cached_models.iter().cloned());
                if is_codex && outcome.has_success {
                    successful_codex_catalogs
                        .push((transport.endpoint.api_format.clone(), outcome.cached_models));
                }
                has_success |= outcome.has_success;
                if outcome.has_success {
                    etag.observe(outcome.etag);
                    upstream_status.observe(outcome.upstream_status);
                }
            }
            Err((err, status)) => {
                upstream_status.observe(status);
                let format_label = model_fetch_format_label(&transport.endpoint.api_format);
                errors.push(format!(
                    "{format_label}: {}",
                    sanitize_model_fetch_error(&err)
                ));
            }
        }
    }

    let merged_models = if is_codex {
        merge_codex_models_preserving_cards(&all_models)?
    } else {
        aggregate_models_for_cache(&all_models)
    };
    let codex_model_ids = is_codex.then(|| collect_codex_model_ids(&merged_models));
    let upstream_metadata =
        crate::logic::model_catalog_upstream_metadata(provider_type, &merged_models);
    let mut outcome = build_success_outcome(merged_models, upstream_metadata, has_success);
    outcome.native_codex_catalog = native_codex_catalog && has_success;
    if let Some(model_ids) = codex_model_ids {
        outcome.fetched_model_ids = model_ids;
        outcome.legacy_models = project_codex_models_for_legacy_cache(
            successful_codex_catalogs
                .iter()
                .map(|(api_format, models)| (api_format.as_str(), models.as_slice())),
        );
    }
    Ok(outcome
        .with_errors(errors)
        .with_etag(etag.finish())
        .with_upstream_status(upstream_status.finish()))
}

async fn fetch_standard_models_for_transport(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
    codex_client_version: Option<&str>,
    allow_codex_legacy_response: bool,
) -> Result<ModelsFetchOutcome, (String, Option<u16>)> {
    let mut all_models = Vec::new();
    let mut seen_ids = BTreeSet::new();
    let mut next_after_id = None;
    let mut has_success = false;
    let mut etag = ConsistentValue::default();
    let mut upstream_status = ConsistentValue::default();
    let is_codex = transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("codex");
    let mut native_codex_catalog = is_codex;

    for _ in 0..20 {
        let plan = build_standard_models_fetch_execution_plan_for_client_version(
            runtime,
            transport,
            next_after_id.as_deref(),
            codex_client_version,
        )
        .await
        .map_err(|err| (err, None))?;
        let result = runtime
            .execute_model_fetch_execution_plan(&plan)
            .await
            .map_err(|err| (err, None))?;
        upstream_status.observe(Some(result.status_code));
        let body_json =
            execution_result_json_body(&result).map_err(|err| (err, Some(result.status_code)))?;
        native_codex_catalog &= body_json.get("models").and_then(Value::as_array).is_some();
        let parsed = if is_codex {
            parse_codex_models_response_for_request(
                &transport.endpoint.api_format,
                &body_json,
                if allow_codex_legacy_response {
                    None
                } else {
                    codex_client_version
                },
            )
        } else {
            parse_models_response_page(&transport.endpoint.api_format, &body_json)
        }
        .map_err(|err| (err, Some(result.status_code)))?;
        etag.observe(execution_result_header(&result, "etag"));
        has_success = true;
        if is_codex {
            // Preserve every opaque card until the catalog-wide merge can distinguish exact
            // duplicates from conflicting `id`/`slug` identities across endpoint transports.
            all_models.extend(parsed.cached_models);
        } else {
            for model in parsed.cached_models {
                let Some(model_id) = model
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                else {
                    continue;
                };
                if !seen_ids.insert(model_id.to_string()) {
                    continue;
                }
                all_models.push(model);
            }
        }

        let Some(next_cursor) = parsed
            .has_more
            .then_some(parsed.next_after_id)
            .flatten()
            .filter(|value| next_after_id.as_deref() != Some(value.as_str()))
        else {
            break;
        };
        next_after_id = Some(next_cursor);
    }

    let mut outcome = build_success_outcome(all_models, None, has_success);
    outcome.native_codex_catalog = native_codex_catalog && has_success;
    Ok(outcome
        .with_etag(etag.finish())
        .with_upstream_status(upstream_status.finish()))
}

fn parse_codex_models_response_for_request(
    endpoint_api_format: &str,
    body: &Value,
    codex_client_version: Option<&str>,
) -> Result<crate::logic::ModelsFetchPage, String> {
    if codex_client_version.is_none()
        && (body.is_array() || body.get("data").and_then(Value::as_array).is_some())
    {
        return parse_models_response_page(endpoint_api_format, body);
    }
    parse_codex_models_response_page(body)
}

async fn fetch_antigravity_models(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
) -> Result<ModelsFetchOutcome, String> {
    let (project_id, hydrated_transport, project_metadata) =
        resolve_or_hydrate_antigravity_project(runtime, transport).await?;

    let mut errors = Vec::new();
    for base_url in [
        ANTIGRAVITY_DAILY_BASE_URL,
        ANTIGRAVITY_PROD_BASE_URL,
        ANTIGRAVITY_SANDBOX_BASE_URL,
    ] {
        let plan = match build_antigravity_fetch_available_models_plan(
            runtime,
            &hydrated_transport,
            base_url,
            &project_id,
        )
        .await
        {
            Ok(plan) => plan,
            Err(err) => return Err(sanitize_model_fetch_error(&err)),
        };

        let result = match runtime.execute_model_fetch_execution_plan(&plan).await {
            Ok(result) => result,
            Err(err) => {
                errors.push(format!(
                    "antigravity models fetch failed: {}",
                    sanitize_model_fetch_error(&err)
                ));
                continue;
            }
        };

        if (200..300).contains(&result.status_code) {
            let body_json = execution_result_json_body_allow_empty(&result)?;
            let (models, metadata) = parse_antigravity_models_response(&body_json)?;
            let metadata = metadata
                .map(|metadata| attach_antigravity_project_metadata(metadata, &project_id))
                .or(project_metadata.clone());
            return Ok(build_success_outcome(models, metadata, true));
        }

        let error = execution_result_error_message(&result);
        if should_fallback_antigravity_status(result.status_code) {
            errors.push(format!("antigravity models fetch failed: {error}"));
            continue;
        }
        return Err(sanitize_model_fetch_error(&error));
    }

    Ok(ModelsFetchOutcome {
        fetched_model_ids: Vec::new(),
        cached_models: Vec::new(),
        legacy_models: Vec::new(),
        errors,
        has_success: false,
        native_codex_catalog: false,
        upstream_metadata: None,
        etag: None,
        upstream_status: None,
    })
}

async fn resolve_or_hydrate_antigravity_project(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
) -> Result<(String, GatewayProviderTransportSnapshot, Option<Value>), String> {
    if let Some(project_id) = resolve_antigravity_project_id_from_transport(transport) {
        let metadata = Some(build_antigravity_project_metadata(&project_id));
        return Ok((project_id, transport.clone(), metadata));
    }

    let plan = build_antigravity_load_code_assist_plan(runtime, transport)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    let result = runtime
        .execute_model_fetch_execution_plan(&plan)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    if !(200..300).contains(&result.status_code) {
        return Err(format!(
            "antigravity: loadCodeAssist failed: {}",
            execution_result_error_message(&result)
        ));
    }
    let body_json = execution_result_json_body_allow_empty(&result)?;
    let project_id = extract_cloud_ai_companion_project_id(&body_json)
        .ok_or_else(|| "antigravity: loadCodeAssist response missing project_id".to_string())?;
    let metadata = build_antigravity_project_metadata(&project_id);
    let mut hydrated_transport = transport.clone();
    hydrated_transport.key.upstream_metadata = Some(metadata.clone());

    Ok((project_id, hydrated_transport, Some(metadata)))
}

fn resolve_antigravity_project_id_from_transport(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<String> {
    match resolve_local_antigravity_request_auth(transport) {
        AntigravityRequestAuthSupport::Supported(auth) => Some(auth.project_id),
        AntigravityRequestAuthSupport::Unsupported(_) => None,
    }
}

fn build_antigravity_project_metadata(project_id: &str) -> Value {
    json!({
        "antigravity": {
            "project_id": project_id,
            "updated_at": now_unix_secs(),
        }
    })
}

fn attach_antigravity_project_metadata(mut metadata: Value, project_id: &str) -> Value {
    let Value::Object(root) = &mut metadata else {
        return build_antigravity_project_metadata(project_id);
    };
    let antigravity = root
        .entry("antigravity".to_string())
        .or_insert_with(|| json!({}));
    let Some(object) = antigravity.as_object_mut() else {
        *antigravity = json!({
            "project_id": project_id,
            "updated_at": now_unix_secs(),
        });
        return metadata;
    };
    object
        .entry("project_id".to_string())
        .or_insert_with(|| Value::String(project_id.to_string()));
    object
        .entry("updated_at".to_string())
        .or_insert_with(|| Value::from(now_unix_secs()));
    metadata
}

async fn fetch_kiro_models(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
) -> Result<ModelsFetchOutcome, String> {
    let plan = build_kiro_list_available_models_plan(runtime, transport)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    let result = runtime
        .execute_model_fetch_execution_plan(&plan)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    if !(200..300).contains(&result.status_code) {
        return Err(execution_result_error_message(&result));
    }

    let body_json = execution_result_json_body_allow_empty(&result)?;
    let (models, metadata) = parse_kiro_available_models_response(&body_json)?;
    Ok(build_success_outcome(models, metadata, true))
}

async fn fetch_vertex_models(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
) -> Result<ModelsFetchOutcome, String> {
    let Some(first_transport) = transports.first() else {
        return Err("Vertex models fetch requires at least one transport".to_string());
    };
    let auth_config = transport_auth_config(first_transport);
    if looks_like_vertex_service_account(auth_config.as_ref()) {
        fetch_vertex_service_account_models(runtime, transports, auth_config.as_ref()).await
    } else {
        fetch_vertex_api_key_models(runtime, transports, auth_config.as_ref()).await
    }
}

async fn fetch_vertex_api_key_models(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
    auth_config: Option<&Value>,
) -> Result<ModelsFetchOutcome, String> {
    let Some(reference_transport) = select_transport_for_api_format(transports, "gemini:") else {
        return Err("vertex_ai(api_key): missing gemini endpoint".to_string());
    };
    let api_key = reference_transport.key.decrypted_api_key.trim();
    if api_key.is_empty() || api_key == "__placeholder__" {
        return Ok(ModelsFetchOutcome {
            fetched_model_ids: Vec::new(),
            cached_models: Vec::new(),
            legacy_models: Vec::new(),
            errors: vec!["vertex_ai(api_key): missing api key".to_string()],
            has_success: false,
            native_codex_catalog: false,
            upstream_metadata: None,
            etag: None,
            upstream_status: None,
        });
    }

    let mut all_models = Vec::new();
    let mut hard_errors = Vec::new();
    let mut soft_errors = Vec::new();
    let mut has_success = false;

    // The API key is a bearer-like cloud credential.  Endpoint records can be
    // imported or edited by administrators, so never send it to an arbitrary
    // custom host merely because it is listed alongside a Vertex transport.
    // Keep only the canonical Vertex host and its official regional variants.
    for base_url in iter_trusted_vertex_base_urls(transports) {
        let url = build_vertex_google_list_url(&base_url, api_key, None);
        let outcome = match fetch_vertex_models_from_url(
            runtime,
            reference_transport,
            &url,
            auth_config,
            "google",
            "gemini:generate_content",
            None,
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                hard_errors.push(format!(
                    "vertex google models fetch failed: {}",
                    sanitize_model_fetch_error(&err)
                ));
                continue;
            }
        };
        has_success |= outcome.has_success;
        if let Some(error) = outcome.error {
            let error = sanitize_model_fetch_error(&error);
            if is_soft_not_found(&error) {
                soft_errors.push(format!("vertex google models fetch failed: {error}"));
            } else {
                hard_errors.push(format!("vertex google models fetch failed: {error}"));
            }
            continue;
        }
        all_models.extend(outcome.models);
    }

    let deduped = dedupe_models_by_id_and_format(all_models);
    if !deduped.is_empty() {
        return Ok(build_success_outcome(deduped, None, true).with_errors(hard_errors));
    }

    let errors = if !hard_errors.is_empty() {
        hard_errors
    } else if !soft_errors.is_empty() {
        vec![soft_errors.remove(0)]
    } else {
        Vec::new()
    };
    Ok(ModelsFetchOutcome {
        fetched_model_ids: Vec::new(),
        cached_models: Vec::new(),
        legacy_models: Vec::new(),
        errors,
        has_success,
        native_codex_catalog: false,
        upstream_metadata: None,
        etag: None,
        upstream_status: None,
    })
}

async fn fetch_vertex_service_account_models(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transports: &[GatewayProviderTransportSnapshot],
    auth_config: Option<&Value>,
) -> Result<ModelsFetchOutcome, String> {
    let Some(auth_config) = auth_config else {
        return Ok(ModelsFetchOutcome {
            fetched_model_ids: Vec::new(),
            cached_models: Vec::new(),
            legacy_models: Vec::new(),
            errors: vec!["vertex_ai(service_account): missing auth_config".to_string()],
            has_success: false,
            native_codex_catalog: false,
            upstream_metadata: None,
            etag: None,
            upstream_status: None,
        });
    };
    let token = exchange_vertex_service_account_token(runtime, &transports[0], auth_config)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    let gemini_transport =
        select_transport_for_api_format(transports, "gemini:").unwrap_or(&transports[0]);
    let claude_transport =
        select_transport_for_api_format(transports, "claude:").unwrap_or(gemini_transport);

    let mut all_models = Vec::new();
    let mut hard_errors = Vec::new();
    let mut soft_errors = Vec::new();
    let mut has_success = false;

    // A service-account access token is a cloud credential.  Restrict its
    // model-garden requests to official Vertex hosts even when an endpoint
    // override exists under the provider.
    for base in iter_trusted_vertex_base_urls(transports) {
        for (publisher, transport, api_format) in [
            ("google", gemini_transport, "gemini:generate_content"),
            ("anthropic", claude_transport, "claude:messages"),
        ] {
            let url = build_vertex_service_account_list_url(&base, publisher, None);
            let outcome = match fetch_vertex_models_from_url(
                runtime,
                transport,
                &url,
                Some(auth_config),
                publisher,
                api_format,
                Some(("authorization".to_string(), format!("Bearer {token}"))),
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(err) => {
                    hard_errors.push(format!(
                        "vertex {publisher} models fetch failed: {}",
                        sanitize_model_fetch_error(&err)
                    ));
                    continue;
                }
            };
            has_success |= outcome.has_success;
            if let Some(error) = outcome.error {
                let error = sanitize_model_fetch_error(&error);
                let labeled = format!("vertex {publisher} models fetch failed: {error}");
                if is_soft_not_found(&error) {
                    soft_errors.push(labeled);
                } else {
                    hard_errors.push(labeled);
                }
                continue;
            }
            all_models.extend(outcome.models);
        }
    }

    let deduped = dedupe_models_by_id_and_format(all_models);
    if !deduped.is_empty() {
        return Ok(build_success_outcome(deduped, None, true).with_errors(hard_errors));
    }

    let errors = if !hard_errors.is_empty() {
        hard_errors
    } else if !soft_errors.is_empty() {
        vec![soft_errors.remove(0)]
    } else {
        Vec::new()
    };
    Ok(ModelsFetchOutcome {
        fetched_model_ids: Vec::new(),
        cached_models: Vec::new(),
        legacy_models: Vec::new(),
        errors,
        has_success,
        native_codex_catalog: false,
        upstream_metadata: None,
        etag: None,
        upstream_status: None,
    })
}

#[derive(Debug)]
struct VertexFetchPageOutcome {
    models: Vec<Value>,
    error: Option<String>,
    has_success: bool,
}

async fn fetch_vertex_models_from_url(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
    initial_url: &str,
    auth_config: Option<&Value>,
    fallback_publisher: &str,
    api_format: &str,
    auth_header: Option<(String, String)>,
) -> Result<VertexFetchPageOutcome, String> {
    let mut all_models = Vec::new();
    let mut has_success = false;
    let mut next_page_token = None;

    for _ in 0..VERTEX_MAX_PAGES {
        let url = next_page_token
            .as_deref()
            .map(|token| append_query_param(initial_url.to_string(), "pageToken", token))
            .unwrap_or_else(|| initial_url.to_string());
        let plan = build_vertex_models_fetch_execution_plan(
            runtime,
            transport,
            &url,
            api_format,
            auth_header.clone(),
        )
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
        let result = runtime
            .execute_model_fetch_execution_plan(&plan)
            .await
            .map_err(|error| sanitize_model_fetch_error(&error))?;
        if result.status_code != 200 {
            return Ok(VertexFetchPageOutcome {
                models: Vec::new(),
                error: Some(execution_result_error_message(&result)),
                has_success,
            });
        }

        has_success = true;
        let body_json = execution_result_json_body_allow_empty(&result)
            .map_err(|error| sanitize_model_fetch_error(&error))?;
        all_models.extend(parse_vertex_models_payload(
            &body_json,
            auth_config,
            fallback_publisher,
        ));
        next_page_token = body_json
            .get("nextPageToken")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned);
        if next_page_token.is_none() {
            break;
        }
    }

    Ok(VertexFetchPageOutcome {
        models: all_models,
        error: None,
        has_success,
    })
}

async fn exchange_vertex_service_account_token(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
    auth_config: &Value,
) -> Result<String, String> {
    // Reuse the provider transport parser here instead of trusting token_uri
    // from the raw credential JSON.  The parser pins the token endpoint to
    // Google's HTTPS OAuth endpoint and rejects credentials, ports, queries,
    // fragments, and lookalike hosts before a signed assertion is produced.
    let auth_config_json = serde_json::to_string(auth_config)
        .map_err(|_| "vertex_ai(service_account): invalid auth_config".to_string())?;
    let auth_config = parse_vertex_service_account_auth_config(Some(&auth_config_json))
        .ok_or_else(|| "vertex_ai(service_account): invalid auth_config".to_string())?;
    let token_url = auth_config.token_uri;
    let client_email = auth_config.client_email;
    let private_key = auth_config.private_key;
    let now = now_unix_secs();
    let assertion =
        build_vertex_service_account_assertion(&client_email, &private_key, &token_url, now)
            .map_err(|error| sanitize_model_fetch_error(&error))?;
    let body = format!(
        "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer&assertion={assertion}"
    );
    let transport_profile = resolve_transport_profile(transport);

    let plan = ExecutionPlan {
        request_id: format!("req-model-fetch-{}-vertex-sa-token", transport.key.id),
        candidate_id: None,
        provider_name: Some(transport.provider.name.clone()),
        provider_id: transport.provider.id.clone(),
        endpoint_id: transport.endpoint.id.clone(),
        key_id: transport.key.id.clone(),
        method: "POST".to_string(),
        url: token_url,
        headers: BTreeMap::from([(
            "content-type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        )]),
        content_type: Some("application/x-www-form-urlencoded".to_string()),
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: Some(STANDARD.encode(body.as_bytes())),
            body_ref: None,
        },
        stream: false,
        client_api_format: "gemini:generate_content".to_string(),
        provider_api_format: "vertex_ai:service_account_token".to_string(),
        model_name: Some("token".to_string()),
        proxy: runtime.resolve_model_fetch_proxy(transport).await,
        transport_profile,
        timeouts: resolve_transport_execution_timeouts(transport),
    };
    let result = runtime
        .execute_model_fetch_execution_plan(&plan)
        .await
        .map_err(|error| sanitize_model_fetch_error(&error))?;
    let body_json = execution_result_json_body(&result)?;
    body_json
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| "vertex_ai(service_account): auth failed: missing access_token".to_string())
}

fn build_vertex_service_account_assertion(
    client_email: &str,
    private_key_pem: &str,
    token_url: &str,
    now_unix_secs: u64,
) -> Result<String, String> {
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256","typ":"JWT"}"#);
    let payload = URL_SAFE_NO_PAD.encode(
        serde_json::to_string(&json!({
            "iss": client_email,
            "scope": GOOGLE_CLOUD_PLATFORM_SCOPE,
            "aud": token_url,
            "iat": now_unix_secs,
            "exp": now_unix_secs.saturating_add(3600),
        }))
        .map_err(|err| format!("vertex_ai(service_account): jwt payload encode failed: {err}"))?,
    );
    let message = format!("{header}.{payload}");
    let signature =
        rsa_pkcs1_sha256_sign(private_key_pem.as_bytes(), message.as_bytes()).map_err(|error| {
            match error {
                RsaPkcs1Sha256Error::InvalidPrivateKey => {
                    "vertex_ai(service_account): private_key parse failed".to_string()
                }
                _ => "vertex_ai(service_account): signing failed".to_string(),
            }
        })?;
    Ok(format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature)))
}

fn execution_result_json_body(result: &ExecutionResult) -> Result<Value, String> {
    if result.status_code != 200 {
        return Err(execution_result_error_message(result));
    }
    execution_result_json_body_allow_empty(result)
}

fn execution_result_json_body_allow_empty(result: &ExecutionResult) -> Result<Value, String> {
    result
        .body
        .as_ref()
        .and_then(|body| body.json_body.clone())
        .ok_or_else(|| "models fetch response body is missing JSON payload".to_string())
}

fn execution_result_header(result: &ExecutionResult, name: &str) -> Option<String> {
    result
        .headers
        .iter()
        .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

const MODEL_FETCH_ERROR_DETAIL_MAX_BYTES: usize = 256;

fn execution_result_error_message(result: &ExecutionResult) -> String {
    let detail = result
        .body
        .as_ref()
        .and_then(|body| body.json_body.as_ref())
        .and_then(extract_error_message)
        .or_else(|| {
            result.error.as_ref().and_then(|error| {
                let message = error.message.trim();
                (!message.is_empty()).then_some(message.to_string())
            })
        });
    let status = if !(200..300).contains(&result.status_code) {
        Some(result.status_code)
    } else {
        result
            .error
            .as_ref()
            .and_then(|error| error.upstream_status)
            .filter(|status| (400..600).contains(status))
    };
    let summary = model_fetch_error_summary(detail.as_deref(), result.error.as_ref(), status);
    match status {
        Some(status) => format!("HTTP {status}: {summary}"),
        None if detail.is_some() => summary,
        None => format!("HTTP {}: {summary}", result.status_code),
    }
}

/// Projects transport/upstream diagnostics into a bounded message that can cross the
/// model-fetch API boundary.  Upstream error bodies and HTTP client errors are untrusted: they
/// commonly contain authorization headers, credential-bearing URLs, or local file paths.
fn sanitize_model_fetch_error(error: &str) -> String {
    let trimmed = error.trim();
    if trimmed.is_empty() {
        return "upstream request failed".to_string();
    }
    match trimmed {
        "No transport snapshots available for models fetch"
        | "No supported endpoint for Rust models fetch"
        | "Provider transport snapshot unavailable" => return trimmed.to_string(),
        _ => {}
    }

    let status = model_fetch_status_from_text(trimmed);
    if let Some(detail) = sanitize_model_fetch_error_detail(trimmed) {
        if let Some(status) = status {
            let lower = trimmed.to_ascii_lowercase();
            if lower.starts_with("http ")
                || lower.starts_with("status ")
                || lower.starts_with("status=")
                || lower.starts_with("status:")
                || lower.starts_with("status_code=")
                || lower.starts_with("status_code:")
            {
                return format!("HTTP {status}: {detail}");
            }
        }
        return detail;
    }

    let summary = model_fetch_error_summary(Some(trimmed), None, status);
    status
        .map(|status| format!("HTTP {status}: {summary}"))
        .unwrap_or_else(|| summary.to_string())
}

fn model_fetch_error_summary(
    detail: Option<&str>,
    execution_error: Option<&ExecutionError>,
    status: Option<u16>,
) -> String {
    // Authentication, authorization, not-found, timeout, and rate-limit statuses are stable
    // public classifications.  Never let an upstream body replace them with free-form text.
    if matches!(status, Some(401 | 403 | 404 | 408 | 429)) {
        return model_fetch_error_category(detail, execution_error, status).to_string();
    }
    if let Some(detail) = detail.and_then(sanitize_model_fetch_error_detail) {
        return detail;
    }
    model_fetch_error_category(detail, execution_error, status).to_string()
}

fn model_fetch_error_category(
    detail: Option<&str>,
    execution_error: Option<&ExecutionError>,
    status: Option<u16>,
) -> &'static str {
    let lower = detail.unwrap_or_default().to_ascii_lowercase();

    match status {
        Some(401) => return "upstream authentication failed",
        Some(403) => return "upstream authorization failed",
        Some(404) => return "upstream endpoint not found",
        Some(408) => return "upstream request timed out",
        Some(429) => return "upstream rate limited",
        _ => {}
    }

    if let Some(error) = execution_error {
        match &error.kind {
            ExecutionErrorKind::ConnectTimeout
            | ExecutionErrorKind::FirstByteTimeout
            | ExecutionErrorKind::ReadTimeout => return "upstream request timed out",
            ExecutionErrorKind::TlsError => return "upstream TLS connection failed",
            ExecutionErrorKind::ProxyError => return "upstream proxy request failed",
            ExecutionErrorKind::ProtocolError => return "upstream response invalid",
            ExecutionErrorKind::Cancelled => return "upstream request cancelled",
            ExecutionErrorKind::Upstream4xx => return "upstream request rejected",
            ExecutionErrorKind::Upstream5xx => return "upstream service failed",
            ExecutionErrorKind::Internal => {}
        }
    }

    if lower.contains("timeout") || lower.contains("timed out") {
        return "upstream request timed out";
    }
    if lower.contains("unauthorized")
        || lower.contains("authentication")
        || lower.contains("invalid api key")
        || lower.contains("invalid token")
    {
        return "upstream authentication failed";
    }
    if lower.contains("forbidden") || lower.contains("authorization") {
        return "upstream authorization failed";
    }
    if lower.contains("rate limit") || lower.contains("too many requests") {
        return "upstream rate limited";
    }
    if lower.contains("response body")
        || lower.contains("invalid response")
        || lower.contains("malformed")
        || lower.contains("missing models")
        || lower.contains("missing data")
        || lower.contains("conflicting cards")
        || lower.contains("json")
        || lower.contains("parse")
    {
        return "upstream response invalid";
    }
    if lower.contains("connect")
        || lower.contains("connection")
        || lower.contains("network")
        || lower.contains("dns")
        || lower.contains("certificate")
    {
        return "upstream connection failed";
    }

    match status {
        Some(status) if (400..500).contains(&status) => "upstream request rejected",
        Some(status) if (500..600).contains(&status) => "upstream service failed",
        _ => "upstream request failed",
    }
}

fn sanitize_model_fetch_error_detail(detail: &str) -> Option<String> {
    let normalized = detail.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.is_empty()
        || normalized.len() > MODEL_FETCH_ERROR_DETAIL_MAX_BYTES
        || model_fetch_error_contains_sensitive_data(&normalized)
    {
        return None;
    }

    let lower = normalized.to_ascii_lowercase();
    if lower.contains("conflicting cards") {
        // The identity is upstream-controlled and the accepted model-ID alphabet also accepts
        // common bearer/API-key encodings. A malicious upstream can reflect the credential it
        // just received as a conflicting model ID, so the API boundary must omit it entirely.
        return Some("conflicting cards".to_string());
    }

    const SAFE_ERROR_PHRASES: &[&str] = &[
        "connection reset",
        "connect timeout",
        "connection timeout",
        "compact endpoint unavailable",
        "temporarily unavailable",
        "missing models array",
        "missing data array",
        "missing project_id",
        "missing clientmodelconfigs",
        "response body is missing json payload",
        "invalid response",
        "no models",
        "endpoint unavailable",
    ];
    SAFE_ERROR_PHRASES
        .iter()
        .find(|phrase| lower.contains(**phrase))
        .map(|phrase| (*phrase).to_string())
}

fn model_fetch_error_contains_sensitive_data(value: &str) -> bool {
    if value.chars().any(|character| character.is_control()) {
        return true;
    }

    let lower = value.to_ascii_lowercase();
    const SENSITIVE_MARKERS: &[&str] = &[
        "authorization",
        "proxy-authorization",
        "bearer",
        "basic ",
        "api_key",
        "api-key",
        "apikey",
        "access_token",
        "access-token",
        "refresh_token",
        "refresh-token",
        "session_token",
        "session-token",
        "sessionkey",
        "password",
        "passwd",
        "private_key",
        "private-key",
        "secret",
        "credential",
        "cookie",
        "set-cookie",
        "token=",
        "token:",
        "key=",
        "key:",
        "url",
        "uri",
        "path=",
    ];
    if SENSITIVE_MARKERS
        .iter()
        .any(|marker| lower.contains(marker))
    {
        return true;
    }

    if lower
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|word| {
            matches!(
                word,
                "token" | "apikey" | "password" | "passwd" | "secret" | "credential"
            )
        })
    {
        return true;
    }

    if lower
        .chars()
        .any(|character| matches!(character, '/' | '\\' | '?' | '#' | '@' | '%'))
    {
        return true;
    }

    // A host, IP literal, or version-like opaque identifier should not cross the boundary as
    // part of an error.  Model IDs do not need to be included in transport diagnostics.
    lower.split_whitespace().any(|token| {
        let token = token.trim_matches(|character: char| {
            !character.is_ascii_alphanumeric() && character != '.' && character != '-'
        });
        if token.len() > 64 {
            return true;
        }
        let parts = token.split('.').collect::<Vec<_>>();
        parts.len() >= 2
            && parts.last().is_some_and(|suffix| suffix.len() >= 2)
            && parts.iter().all(|part| {
                !part.is_empty()
                    && part
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '-')
            })
    })
}

fn model_fetch_status_from_text(error: &str) -> Option<u16> {
    error
        .split(|character: char| !character.is_ascii_digit())
        .filter(|token| token.len() == 3)
        .find_map(|token| {
            let status = token.parse::<u16>().ok()?;
            (400..600).contains(&status).then_some(status)
        })
}

fn model_fetch_format_label(api_format: &str) -> &'static str {
    let normalized = api_format.trim().to_ascii_lowercase();
    if normalized.starts_with("openai:") {
        "openai"
    } else if normalized.starts_with("claude:") {
        "claude"
    } else if normalized.starts_with("gemini:") {
        "gemini"
    } else {
        "models"
    }
}

fn parse_antigravity_models_response(body: &Value) -> Result<(Vec<Value>, Option<Value>), String> {
    let models_object = body
        .get("models")
        .and_then(Value::as_object)
        .ok_or_else(|| "antigravity: invalid response (missing models)".to_string())?;

    let mut models = Vec::new();
    let mut quota_by_model = serde_json::Map::new();
    for (model_id, model_data) in models_object {
        let model_id = model_id.trim();
        if !antigravity_model_id_is_routable(model_id) {
            continue;
        }
        let model_object = model_data.as_object().cloned().unwrap_or_default();
        let display_name = model_object
            .get("displayName")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(model_id);
        models.push(json!({
            "id": model_id,
            "object": "model",
            "owned_by": "antigravity",
            "display_name": display_name,
            "api_formats": ["gemini:generate_content"],
        }));

        let quota_payload = build_antigravity_quota_payload(model_object.get("quotaInfo"));
        if !quota_payload.is_empty() {
            quota_by_model.insert(model_id.to_string(), Value::Object(quota_payload));
        }
    }

    let upstream_metadata = (!quota_by_model.is_empty()).then(|| {
        json!({
            "antigravity": {
                "updated_at": now_unix_secs(),
                "quota_by_model": quota_by_model,
            }
        })
    });

    Ok((models, upstream_metadata))
}

pub fn antigravity_model_id_is_routable(model_id: &str) -> bool {
    let model_id = model_id.trim();
    !model_id.is_empty()
        && !ANTIGRAVITY_BLOCKED_MODELS
            .iter()
            .any(|blocked| blocked.eq_ignore_ascii_case(model_id))
}

fn parse_kiro_available_models_response(
    body: &Value,
) -> Result<(Vec<Value>, Option<Value>), String> {
    let items = body
        .get("models")
        .and_then(Value::as_array)
        .ok_or_else(|| "kiro: invalid response (missing models)".to_string())?;

    let mut seen = BTreeSet::new();
    let mut models = Vec::new();
    for item in items {
        let Some(model_id) = item
            .get("modelId")
            .or_else(|| item.get("id"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if !seen.insert(model_id.to_string()) {
            continue;
        }

        let display_name = item
            .get("modelName")
            .or_else(|| item.get("display_name"))
            .or_else(|| item.get("name"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(model_id);
        let mut model = item.as_object().cloned().unwrap_or_default();
        model.insert("id".to_string(), Value::String(model_id.to_string()));
        model.insert("object".to_string(), Value::String("model".to_string()));
        model.insert(
            "owned_by".to_string(),
            Value::String(infer_kiro_model_owner(model_id).to_string()),
        );
        model.insert(
            "display_name".to_string(),
            Value::String(display_name.to_string()),
        );
        model.insert(
            "api_formats".to_string(),
            Value::Array(vec![Value::String("claude:messages".to_string())]),
        );
        model.remove("api_format");
        models.push(Value::Object(model));
    }

    let default_model = body.get("defaultModel").and_then(|value| {
        json_string(value.get("modelId")).map(|model_id| {
            json!({
                "model_id": model_id,
                "model_name": json_string(value.get("modelName")),
            })
        })
    });
    let upstream_metadata = default_model.map(|default_model| {
        json!({
            "kiro": {
                "updated_at": now_unix_secs(),
                "default_model": default_model,
            }
        })
    });

    Ok((models, upstream_metadata))
}

fn infer_kiro_model_owner(model_id: &str) -> &'static str {
    let normalized = model_id.trim().to_ascii_lowercase();
    if normalized.starts_with("claude-") {
        "anthropic"
    } else if normalized.starts_with("deepseek-") {
        "deepseek"
    } else if normalized.starts_with("minimax-") {
        "minimax"
    } else if normalized.starts_with("glm-") {
        "zhipu"
    } else if normalized.starts_with("qwen") {
        "alibaba"
    } else {
        "kiro"
    }
}

fn build_antigravity_quota_payload(quota_info: Option<&Value>) -> serde_json::Map<String, Value> {
    let Some(quota_info) = quota_info.and_then(Value::as_object) else {
        return serde_json::Map::new();
    };
    let reset_time = quota_info
        .get("resetTime")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let remaining_fraction = quota_info
        .get("remainingFraction")
        .and_then(|value| {
            value.as_f64().or_else(|| {
                value
                    .as_str()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .and_then(|value| value.parse::<f64>().ok())
            })
        })
        .filter(|value| value.is_finite())
        .map(|value| value.clamp(0.0, 1.0));

    let mut payload = serde_json::Map::new();
    if let Some(remaining_fraction) = remaining_fraction {
        let used_percent = (1.0 - remaining_fraction) * 100.0;
        payload.insert(
            "remaining_fraction".to_string(),
            Value::from(remaining_fraction),
        );
        payload.insert("used_percent".to_string(), Value::from(used_percent));
    }
    if let Some(reset_time) = reset_time {
        payload.insert("reset_time".to_string(), Value::String(reset_time));
    }
    payload
}

fn should_fallback_antigravity_status(status_code: u16) -> bool {
    matches!(status_code, 404 | 408 | 429) || (500..600).contains(&status_code)
}

fn looks_like_vertex_service_account(auth_config: Option<&Value>) -> bool {
    let Some(auth_config) = auth_config.and_then(Value::as_object) else {
        return false;
    };
    ["client_email", "private_key", "project_id"]
        .into_iter()
        .all(|field| {
            auth_config
                .get(field)
                .and_then(Value::as_str)
                .map(str::trim)
                .is_some_and(|value| !value.is_empty())
        })
}

fn iter_vertex_base_urls(transports: &[GatewayProviderTransportSnapshot]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut urls = Vec::new();
    for transport in transports {
        let base_url = transport.endpoint.base_url.trim().trim_end_matches('/');
        if base_url.is_empty() || !seen.insert(base_url.to_string()) {
            continue;
        }
        urls.push(base_url.to_string());
    }
    if seen.insert(VERTEX_API_BASE_URL.to_string()) {
        urls.push(VERTEX_API_BASE_URL.to_string());
    }
    urls
}

fn iter_trusted_vertex_base_urls(transports: &[GatewayProviderTransportSnapshot]) -> Vec<String> {
    iter_vertex_base_urls(transports)
        .into_iter()
        .filter(|base_url| looks_like_vertex_ai_host(base_url))
        .collect()
}

fn build_vertex_google_list_url(base_url: &str, api_key: &str, page_token: Option<&str>) -> String {
    let url = build_vertex_publisher_models_list_base_url(base_url, "google");
    let mut url = append_query_param(url, "key", api_key);
    url = append_query_param(url, "pageSize", VERTEX_PAGE_SIZE);
    if let Some(page_token) = page_token {
        url = append_query_param(url, "pageToken", page_token);
    }
    url
}

fn build_vertex_service_account_list_url(
    base_url: &str,
    publisher: &str,
    page_token: Option<&str>,
) -> String {
    let mut url = build_vertex_publisher_models_list_base_url(base_url, publisher);
    url = append_query_param(url, "pageSize", VERTEX_PAGE_SIZE);
    if let Some(page_token) = page_token {
        url = append_query_param(url, "pageToken", page_token);
    }
    url
}

fn build_vertex_publisher_models_list_base_url(base_url: &str, publisher: &str) -> String {
    let path = format!("/{VERTEX_MODEL_GARDEN_API_VERSION}/publishers/{publisher}/models");
    build_vertex_model_garden_path_url(base_url, &path)
}

fn build_vertex_model_garden_path_url(base_url: &str, path: &str) -> String {
    let base = base_url
        .trim()
        .trim_end_matches('/')
        .trim_end_matches("/v1beta1")
        .trim_end_matches("/v1beta")
        .trim_end_matches("/v1");
    format!("{}{}", base, path.trim())
}

fn parse_vertex_models_payload(
    body: &Value,
    auth_config: Option<&Value>,
    fallback_publisher: &str,
) -> Vec<Value> {
    vertex_payload_items(body)
        .into_iter()
        .filter_map(|item| build_vertex_model(item, auth_config, fallback_publisher))
        .collect()
}

fn vertex_payload_items(body: &Value) -> Vec<&serde_json::Map<String, Value>> {
    if let Some(items) = body.as_array() {
        return items.iter().filter_map(Value::as_object).collect();
    }
    ["publisherModels", "models", "data", "items"]
        .iter()
        .find_map(|key| body.get(*key).and_then(Value::as_array))
        .map(|items| items.iter().filter_map(Value::as_object).collect())
        .unwrap_or_default()
}

fn build_vertex_model(
    item: &serde_json::Map<String, Value>,
    auth_config: Option<&Value>,
    fallback_publisher: &str,
) -> Option<Value> {
    let raw_name = item
        .get("id")
        .or_else(|| item.get("name"))
        .or_else(|| item.get("model"))
        .and_then(Value::as_str)?;
    let model_id = extract_vertex_model_id(raw_name);
    if model_id.is_empty() {
        return None;
    }
    let display_name = item
        .get("displayName")
        .or_else(|| item.get("display_name"))
        .or_else(|| item.get("title"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(model_id.as_str())
        .to_string();
    Some(json!({
        "id": model_id,
        "object": "model",
        "owned_by": extract_vertex_publisher(item, fallback_publisher),
        "display_name": display_name,
        "api_formats": [vertex_effective_format(&model_id, auth_config)],
    }))
}

fn extract_vertex_model_id(raw_name: &str) -> String {
    let trimmed = raw_name.trim();
    if let Some((_, suffix)) = trimmed.split_once("/models/") {
        return suffix.trim().to_string();
    }
    trimmed
        .strip_prefix("models/")
        .unwrap_or(trimmed)
        .trim()
        .to_string()
}

fn extract_vertex_publisher(
    item: &serde_json::Map<String, Value>,
    fallback_publisher: &str,
) -> String {
    item.get("publisher")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            item.get("name")
                .and_then(Value::as_str)
                .and_then(|name| name.split("/publishers/").nth(1))
                .and_then(|rest| rest.split('/').next())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| fallback_publisher.to_string())
}

fn vertex_effective_format(model_id: &str, auth_config: Option<&Value>) -> String {
    if let Some(config) = auth_config.and_then(Value::as_object) {
        if let Some(mapping) = config
            .get("model_format_mapping")
            .and_then(Value::as_object)
        {
            if let Some(api_format) = mapping.get(model_id).and_then(Value::as_str) {
                return normalize_api_format(api_format);
            }
            for (prefix, api_format) in mapping {
                if prefix.ends_with('-')
                    && model_id.starts_with(prefix)
                    && api_format.as_str().is_some()
                {
                    return normalize_api_format(
                        api_format.as_str().unwrap_or("gemini:generate_content"),
                    );
                }
            }
        }
        if let Some(default_format) = config.get("default_format").and_then(Value::as_str) {
            let normalized = normalize_api_format(default_format);
            if !normalized.is_empty() {
                return normalized;
            }
        }
    }
    if model_id.starts_with("claude-") {
        "claude:messages".to_string()
    } else {
        "gemini:generate_content".to_string()
    }
}

fn is_soft_not_found(error: &str) -> bool {
    error.trim().starts_with("HTTP 404:")
}

fn dedupe_models_by_id_and_format(models: Vec<Value>) -> Vec<Value> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for model in models {
        let Some(model_id) = model
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let api_format = model
            .get("api_formats")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(Value::as_str)
            .unwrap_or_default();
        let dedupe_key = format!("{model_id}:{api_format}");
        if !seen.insert(dedupe_key) {
            continue;
        }
        deduped.push(model);
    }
    deduped
}

fn build_success_outcome(
    cached_models: Vec<Value>,
    upstream_metadata: Option<Value>,
    has_success: bool,
) -> ModelsFetchOutcome {
    let legacy_models = cached_models.clone();
    ModelsFetchOutcome {
        fetched_model_ids: collect_model_ids(&cached_models),
        cached_models,
        legacy_models,
        errors: Vec::new(),
        has_success,
        native_codex_catalog: false,
        upstream_metadata,
        etag: None,
        upstream_status: None,
    }
}

fn collect_model_ids(models: &[Value]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    let mut ids = Vec::new();
    for model in models {
        let Some(model_id) = model
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if seen.insert(model_id.to_string()) {
            ids.push(model_id.to_string());
        }
    }
    ids
}

fn collect_codex_model_ids(models: &[Value]) -> Vec<String> {
    let mut seen = BTreeSet::new();
    models
        .iter()
        .filter_map(codex_model_identity)
        .filter(|model_id| seen.insert((*model_id).to_string()))
        .map(ToOwned::to_owned)
        .collect()
}

fn transport_auth_config(transport: &GatewayProviderTransportSnapshot) -> Option<Value> {
    transport
        .key
        .decrypted_auth_config
        .as_deref()
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
}

fn select_transport_for_api_format<'a>(
    transports: &'a [GatewayProviderTransportSnapshot],
    prefix: &str,
) -> Option<&'a GatewayProviderTransportSnapshot> {
    transports.iter().find(|transport| {
        transport
            .endpoint
            .api_format
            .trim()
            .to_ascii_lowercase()
            .starts_with(prefix)
    })
}

fn append_query_param(mut url: String, key: &str, value: &str) -> String {
    if key.trim().is_empty() || value.trim().is_empty() {
        return url;
    }
    let separator = if url.contains('?') { '&' } else { '?' };
    let encoded_key =
        url::form_urlencoded::byte_serialize(key.trim().as_bytes()).collect::<String>();
    let encoded_value =
        url::form_urlencoded::byte_serialize(value.trim().as_bytes()).collect::<String>();
    url.push(separator);
    url.push_str(&encoded_key);
    url.push('=');
    url.push_str(&encoded_value);
    url
}

fn json_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn normalize_api_format(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn extract_cloud_ai_companion_project_id(body: &Value) -> Option<String> {
    let raw = body
        .get("cloudaicompanionProject")
        .or_else(|| body.get("cloudAiCompanionProject"))?;
    if let Some(value) = raw.as_str() {
        let value = value.trim();
        if !value.is_empty() {
            return Some(value.to_string());
        }
    }
    raw.as_object()
        .and_then(|object| object.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

trait OutcomeExt {
    fn with_errors(self, errors: Vec<String>) -> Self;
    fn with_etag(self, etag: Option<String>) -> Self;
    fn with_upstream_status(self, upstream_status: Option<u16>) -> Self;
}

impl OutcomeExt for ModelsFetchOutcome {
    fn with_errors(mut self, errors: Vec<String>) -> Self {
        self.errors = errors;
        self
    }

    fn with_etag(mut self, etag: Option<String>) -> Self {
        self.etag = etag;
        self
    }

    fn with_upstream_status(mut self, upstream_status: Option<u16>) -> Self {
        self.upstream_status = upstream_status;
        self
    }
}

#[cfg(test)]
#[path = "strategy/tests.rs"]
mod tests;
