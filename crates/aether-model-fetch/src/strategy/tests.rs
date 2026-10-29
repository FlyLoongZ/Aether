use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use aether_contracts::{
    redact_url_for_debug, ExecutionError, ExecutionErrorKind, ExecutionPhase, ExecutionResult,
    ResponseBody,
};
use aether_provider_transport::snapshot::{
    GatewayProviderTransportEndpoint, GatewayProviderTransportKey,
    GatewayProviderTransportProvider, GatewayProviderTransportSnapshot,
};
use async_trait::async_trait;
use aws_lc_rs::encoding::{AsDer, Pkcs8V1Der};
use aws_lc_rs::rsa::{KeyPair as AwsRsaKeyPair, KeySize};
use aws_lc_rs::signature::{KeyPair as _, UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256};
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine as _;
use serde_json::{json, Value};

use super::{
    build_vertex_google_list_url, build_vertex_service_account_assertion,
    build_vertex_service_account_list_url, parse_antigravity_models_response,
    parse_codex_models_response_for_request, select_model_fetch_strategy, ModelFetchStrategy,
    ModelFetchStrategyKind,
};
use crate::transport::ModelFetchTransportRuntime;
use crate::{fetch_models_from_transports, fetch_models_from_transports_for_client_version};

type RouteResult = Result<(u16, Value), String>;
type ModelFetchRoute = (String, RouteResult);

#[test]
fn vertex_assertion_accepts_bare_base64_pkcs8_and_verifies() {
    let key_pair = AwsRsaKeyPair::generate(KeySize::Rsa2048)
        .expect("2048-bit test RSA private key should generate");
    let pkcs8 = AsDer::<Pkcs8V1Der<'static>>::as_der(&key_pair)
        .expect("test RSA private key should encode as PKCS#8");
    let assertion = build_vertex_service_account_assertion(
        "svc@example.iam.gserviceaccount.com",
        &STANDARD.encode(pkcs8.as_ref()),
        "https://oauth2.googleapis.com/token",
        1_700_000_000,
    )
    .expect("bare-base64 PKCS#8 key should sign");
    let parts = assertion.split('.').collect::<Vec<_>>();
    assert_eq!(parts.len(), 3);
    let message = format!("{}.{}", parts[0], parts[1]);
    let signature = URL_SAFE_NO_PAD
        .decode(parts[2])
        .expect("JWT signature should decode");
    UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, key_pair.public_key().as_ref())
        .verify(message.as_bytes(), &signature)
        .expect("AWS-LC signature should verify");
}

struct TestRuntime {
    executed_urls: Arc<Mutex<Vec<String>>>,
    response_body: Value,
    status_code: u16,
    response_headers: BTreeMap<String, String>,
}

struct RoutingTestRuntime {
    executed_urls: Arc<Mutex<Vec<String>>>,
    routes: Vec<ModelFetchRoute>,
}

struct OAuthRoutingTestRuntime {
    executed_urls: Arc<Mutex<Vec<String>>>,
    routes: Vec<ModelFetchRoute>,
}

#[async_trait]
impl ModelFetchTransportRuntime for TestRuntime {
    async fn resolve_local_oauth_request_auth(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Result<Option<aether_provider_transport::LocalResolvedOAuthRequestAuth>, String> {
        Ok(None)
    }

    async fn resolve_model_fetch_proxy(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Option<aether_contracts::ProxySnapshot> {
        None
    }

    async fn execute_model_fetch_execution_plan(
        &self,
        plan: &aether_contracts::ExecutionPlan,
    ) -> Result<ExecutionResult, String> {
        self.executed_urls
            .lock()
            .expect("executed_urls lock")
            .push(plan.url.clone());
        Ok(ExecutionResult {
            request_id: plan.request_id.clone(),
            candidate_id: plan.candidate_id.clone(),
            status_code: self.status_code,
            headers: self.response_headers.clone(),
            response_observation: None,
            body: Some(ResponseBody {
                json_body: Some(self.response_body.clone()),
                body_bytes_b64: None,
            }),
            telemetry: None,
            error: None,
        })
    }
}

#[async_trait]
impl ModelFetchTransportRuntime for RoutingTestRuntime {
    async fn resolve_local_oauth_request_auth(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Result<Option<aether_provider_transport::LocalResolvedOAuthRequestAuth>, String> {
        Ok(None)
    }

    async fn resolve_model_fetch_proxy(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Option<aether_contracts::ProxySnapshot> {
        None
    }

    async fn execute_model_fetch_execution_plan(
        &self,
        plan: &aether_contracts::ExecutionPlan,
    ) -> Result<ExecutionResult, String> {
        self.executed_urls
            .lock()
            .expect("executed_urls lock")
            .push(plan.url.clone());
        let Some((_, route_result)) = self
            .routes
            .iter()
            .find(|(url_part, _)| plan.url.contains(url_part))
        else {
            return Err(format!(
                "unexpected models fetch URL {}",
                redact_url_for_debug(&plan.url)
            ));
        };
        let (status_code, response_body) = match route_result {
            Ok((status_code, response_body)) => (*status_code, response_body.clone()),
            Err(err) => return Err(err.clone()),
        };
        Ok(ExecutionResult {
            request_id: plan.request_id.clone(),
            candidate_id: plan.candidate_id.clone(),
            status_code,
            headers: BTreeMap::new(),
            response_observation: None,
            body: Some(ResponseBody {
                json_body: Some(response_body),
                body_bytes_b64: None,
            }),
            telemetry: None,
            error: None,
        })
    }
}

#[async_trait]
impl ModelFetchTransportRuntime for OAuthRoutingTestRuntime {
    async fn resolve_local_oauth_request_auth(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Result<Option<aether_provider_transport::LocalResolvedOAuthRequestAuth>, String> {
        Ok(Some(
            aether_provider_transport::LocalResolvedOAuthRequestAuth::Header {
                name: "authorization".to_string(),
                value: "Bearer oauth-token".to_string(),
            },
        ))
    }

    async fn resolve_model_fetch_proxy(
        &self,
        _transport: &GatewayProviderTransportSnapshot,
    ) -> Option<aether_contracts::ProxySnapshot> {
        None
    }

    async fn execute_model_fetch_execution_plan(
        &self,
        plan: &aether_contracts::ExecutionPlan,
    ) -> Result<ExecutionResult, String> {
        self.executed_urls
            .lock()
            .expect("executed_urls lock")
            .push(plan.url.clone());
        let Some((_, route_result)) = self
            .routes
            .iter()
            .find(|(url_part, _)| plan.url.contains(url_part))
        else {
            return Err(format!(
                "unexpected models fetch URL {}",
                redact_url_for_debug(&plan.url)
            ));
        };
        let (status_code, response_body) = match route_result {
            Ok((status_code, response_body)) => (*status_code, response_body.clone()),
            Err(err) => return Err(err.clone()),
        };
        Ok(ExecutionResult {
            request_id: plan.request_id.clone(),
            candidate_id: plan.candidate_id.clone(),
            status_code,
            headers: BTreeMap::new(),
            response_observation: None,
            body: Some(ResponseBody {
                json_body: Some(response_body),
                body_bytes_b64: None,
            }),
            telemetry: None,
            error: None,
        })
    }
}

fn sample_custom_aiplatform_transport() -> GatewayProviderTransportSnapshot {
    GatewayProviderTransportSnapshot {
        provider: GatewayProviderTransportProvider {
            id: "provider-1".to_string(),
            name: "Vertex".to_string(),
            provider_type: "custom".to_string(),
            website: None,
            is_active: true,
            keep_priority_on_conversion: false,
            enable_format_conversion: true,
            concurrent_limit: None,
            max_retries: None,
            proxy: None,
            request_timeout_secs: None,
            stream_first_byte_timeout_secs: None,
            config: None,
        },
        endpoint: GatewayProviderTransportEndpoint {
            id: "endpoint-1".to_string(),
            provider_id: "provider-1".to_string(),
            api_format: "gemini:generate_content".to_string(),
            api_family: Some("gemini".to_string()),
            endpoint_kind: Some("generate_content".to_string()),
            is_active: true,
            base_url: "https://aiplatform.googleapis.com".to_string(),
            header_rules: None,
            body_rules: None,
            max_retries: None,
            custom_path: Some("/v1/publishers/google/models/{model}:{action}".to_string()),
            config: None,
            format_acceptance_config: None,
            proxy: None,
        },
        key: GatewayProviderTransportKey {
            id: "key-1".to_string(),
            provider_id: "provider-1".to_string(),
            name: "key".to_string(),
            auth_type: "api_key".to_string(),
            is_active: true,
            api_formats: Some(vec!["gemini:generate_content".to_string()]),
            auth_type_by_format: None,
            allow_auth_channel_mismatch_formats: None,

            allowed_models: None,
            capabilities: None,
            rate_multipliers: None,
            global_priority_by_format: None,
            expires_at_unix_secs: None,
            proxy: None,
            fingerprint: None,
            upstream_metadata: None,
            decrypted_api_key: "vertex-secret".to_string(),
            decrypted_auth_config: None,
        },
    }
}

fn sample_codex_transport() -> GatewayProviderTransportSnapshot {
    let mut transport = sample_custom_aiplatform_transport();
    transport.provider.provider_type = "codex".to_string();
    transport.provider.name = "Codex".to_string();
    transport.endpoint.api_format = "openai:responses".to_string();
    transport.endpoint.api_family = Some("openai".to_string());
    transport.endpoint.endpoint_kind = Some("responses".to_string());
    transport.endpoint.base_url = "https://chatgpt.com/backend-api/codex".to_string();
    transport.endpoint.custom_path = Some("/responses".to_string());
    transport.key.api_formats = Some(vec!["openai:responses".to_string()]);
    transport.key.decrypted_api_key = "access-token".to_string();
    transport
}

fn sample_codex_transport_for_base(
    endpoint_id: &str,
    base_url: &str,
) -> GatewayProviderTransportSnapshot {
    let mut transport = sample_codex_transport();
    transport.endpoint.id = endpoint_id.to_string();
    transport.endpoint.base_url = base_url.to_string();
    transport
}

fn sample_kiro_transport() -> GatewayProviderTransportSnapshot {
    let mut transport = sample_custom_aiplatform_transport();
    transport.provider.provider_type = "kiro".to_string();
    transport.provider.name = "Kiro".to_string();
    transport.endpoint.api_format = "claude:messages".to_string();
    transport.endpoint.api_family = Some("claude".to_string());
    transport.endpoint.endpoint_kind = Some("messages".to_string());
    transport.endpoint.base_url = "https://q.{region}.amazonaws.com".to_string();
    transport.endpoint.custom_path = None;
    transport.key.auth_type = "oauth".to_string();
    transport.key.api_formats = Some(vec!["claude:messages".to_string()]);
    transport.key.decrypted_api_key = "__placeholder__".to_string();
    transport.key.decrypted_auth_config = Some(
        r#"{
            "access_token":"cached-token",
            "expires_at":4102444800,
            "profile_arn":"arn:aws:codewhisperer:us-east-1:123456789012:profile/demo",
            "api_region":"us-east-1",
            "machine_id":"123e4567-e89b-12d3-a456-426614174000"
        }"#
        .to_string(),
    );
    transport
}

fn sample_gemini_cli_transport() -> GatewayProviderTransportSnapshot {
    let mut transport = sample_custom_aiplatform_transport();
    transport.provider.provider_type = "gemini_cli".to_string();
    transport.provider.name = "Gemini CLI".to_string();
    transport.endpoint.base_url = "https://cloudcode-pa.googleapis.com".to_string();
    transport.key.auth_type = "bearer".to_string();
    transport.key.decrypted_api_key = "gemini-cli-access-token".to_string();
    transport
}

fn sample_antigravity_transport_without_project() -> GatewayProviderTransportSnapshot {
    let mut transport = sample_custom_aiplatform_transport();
    transport.provider.provider_type = "antigravity".to_string();
    transport.provider.name = "Antigravity".to_string();
    transport.endpoint.base_url = "https://daily-cloudcode-pa.googleapis.com".to_string();
    transport.key.auth_type = "oauth".to_string();
    transport.key.decrypted_api_key = "__placeholder__".to_string();
    transport.key.decrypted_auth_config =
        Some(r#"{"provider_type":"antigravity","refresh_token":"rt"}"#.to_string());
    transport
}

fn sample_openai_transport(
    endpoint_id: &str,
    api_format: &str,
    base_url: &str,
) -> GatewayProviderTransportSnapshot {
    let mut transport = sample_custom_aiplatform_transport();
    transport.provider.provider_type = "custom".to_string();
    transport.provider.name = "OpenAI Compat".to_string();
    transport.endpoint.id = endpoint_id.to_string();
    transport.endpoint.api_format = api_format.to_string();
    transport.endpoint.api_family = Some("openai".to_string());
    transport.endpoint.endpoint_kind = api_format
        .split_once(':')
        .map(|(_, endpoint_kind)| endpoint_kind.to_string());
    transport.endpoint.base_url = base_url.to_string();
    transport.endpoint.custom_path = None;
    transport.key.api_formats = Some(vec![api_format.to_string()]);
    transport.key.decrypted_api_key = "openai-secret".to_string();
    transport
}

#[test]
fn strategy_selection_keeps_codex_on_standard_transport_fetch() {
    let strategy =
        select_model_fetch_strategy(&[sample_codex_transport()]).expect("strategy should select");

    assert_eq!(strategy.provider_id(), "codex");
    assert_eq!(strategy.kind(), ModelFetchStrategyKind::StandardTransport);
}

#[test]
fn unversioned_codex_parser_accepts_top_level_openai_compatible_array() {
    let parsed = parse_codex_models_response_for_request(
        "openai:responses",
        &json!([{"id": "gpt-array-compatible"}]),
        None,
    )
    .expect("unversioned admin fetch should retain the top-level array fallback");

    assert_eq!(parsed.fetched_model_ids, vec!["gpt-array-compatible"]);
    assert_eq!(
        parsed.cached_models[0]["api_formats"],
        json!(["openai:responses"])
    );

    let error = parse_codex_models_response_for_request(
        "openai:responses",
        &json!([{"id": "gpt-array-compatible"}]),
        Some("0.145.2"),
    )
    .expect_err("versioned catalogs must use the opaque models-array schema");
    assert!(error.contains("missing models array"));
}

#[test]
fn strategy_selection_uses_preset_catalog_for_claude_code() {
    let mut transport = sample_custom_aiplatform_transport();
    transport.provider.provider_type = "claude_code".to_string();
    transport.endpoint.api_format = "claude:messages".to_string();

    let strategy = select_model_fetch_strategy(&[transport]).expect("strategy should select");

    assert_eq!(strategy.provider_id(), "claude_code");
    assert_eq!(strategy.kind(), ModelFetchStrategyKind::PresetCatalog);
}

#[test]
fn strategy_selection_uses_kiro_upstream_fetch() {
    let strategy =
        select_model_fetch_strategy(&[sample_kiro_transport()]).expect("strategy should select");

    assert_eq!(strategy.provider_id(), "kiro");
    assert_eq!(strategy.kind(), ModelFetchStrategyKind::Kiro);
}

#[tokio::test]
async fn custom_aiplatform_transport_uses_vertex_models_fetch_path_and_normalizes_chat_format() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        response_body: json!({
            "models": [{
                "name": "publishers/google/models/gemini-3.1-pro-preview"
            }]
        }),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };
    let outcome = fetch_models_from_transports(&runtime, &[sample_custom_aiplatform_transport()])
        .await
        .expect("models fetch should succeed");

    let urls = executed_urls.lock().expect("executed_urls lock");
    assert_eq!(
        urls.as_slice(),
        &["https://aiplatform.googleapis.com/v1beta1/publishers/google/models?key=vertex-secret&pageSize=100"]
    );
    assert_eq!(outcome.fetched_model_ids, vec!["gemini-3.1-pro-preview"]);
    assert_eq!(outcome.cached_models.len(), 1);
    assert_eq!(
        outcome.cached_models[0]["api_formats"][0].as_str(),
        Some("gemini:generate_content")
    );
}

#[tokio::test]
async fn standard_transport_merges_successful_endpoint_models_when_one_endpoint_fails() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = RoutingTestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        routes: vec![
            (
                "https://bad.example.com/models".to_string(),
                Err("connection reset".to_string()),
            ),
            (
                "https://chat.example.com/models".to_string(),
                Ok((
                    200,
                    json!({
                        "data": [{ "id": "shared-model" }]
                    }),
                )),
            ),
            (
                "https://responses.example.com/models".to_string(),
                Ok((
                    200,
                    json!({
                        "data": [
                            { "id": "shared-model" },
                            { "id": "responses-only" }
                        ]
                    }),
                )),
            ),
        ],
    };
    let transports = vec![
        sample_openai_transport("endpoint-bad", "openai:chat", "https://bad.example.com"),
        sample_openai_transport("endpoint-chat", "openai:chat", "https://chat.example.com"),
        sample_openai_transport(
            "endpoint-responses",
            "openai:responses",
            "https://responses.example.com",
        ),
    ];

    let outcome = fetch_models_from_transports(&runtime, &transports)
        .await
        .expect("models fetch should keep successful endpoint results");

    assert!(outcome.has_success);
    assert_eq!(
        outcome.fetched_model_ids,
        vec!["responses-only", "shared-model"]
    );
    assert_eq!(outcome.cached_models.len(), 2);
    assert_eq!(outcome.legacy_models, outcome.cached_models);
    assert_eq!(outcome.errors.len(), 1);
    assert!(outcome.errors[0].contains("connection reset"));
    let shared_model = outcome
        .cached_models
        .iter()
        .find(|model| model.get("id").and_then(Value::as_str) == Some("shared-model"))
        .expect("shared model should be cached once");
    assert_eq!(
        shared_model.get("api_formats"),
        Some(&json!(["openai:chat", "openai:responses"]))
    );
}

#[tokio::test]
async fn vertex_models_fetch_continues_when_one_base_url_errors() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = RoutingTestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        routes: vec![
            (
                "https://us-central1-aiplatform.googleapis.com/v1beta1/publishers/google/models"
                    .to_string(),
                Err("connect timeout".to_string()),
            ),
            (
                "https://aiplatform.googleapis.com/v1beta1/publishers/google/models".to_string(),
                Ok((
                    200,
                    json!({
                        "models": [{
                            "name": "publishers/google/models/gemini-3.1-pro-preview"
                        }]
                    }),
                )),
            ),
        ],
    };
    let mut failing_transport = sample_custom_aiplatform_transport();
    failing_transport.endpoint.base_url =
        "https://us-central1-aiplatform.googleapis.com".to_string();
    let mut successful_transport = sample_custom_aiplatform_transport();
    successful_transport.endpoint.id = "endpoint-2".to_string();
    successful_transport.endpoint.base_url = "https://aiplatform.googleapis.com".to_string();

    let outcome =
        fetch_models_from_transports(&runtime, &[failing_transport, successful_transport])
            .await
            .expect("vertex models fetch should keep successful base URL results");

    assert!(outcome.has_success);
    assert_eq!(outcome.fetched_model_ids, vec!["gemini-3.1-pro-preview"]);
    assert_eq!(outcome.cached_models.len(), 1);
    assert_eq!(outcome.errors.len(), 1);
    assert!(outcome.errors[0].contains("connect timeout"));
}

#[test]
fn execution_result_error_projection_discards_body_credentials_and_urls() {
    const BEARER_SECRET: &str = "super-secret-bearer";
    const QUERY_SECRET: &str = "query-secret";
    let result = ExecutionResult {
        request_id: "req-model-fetch-security-body".to_string(),
        candidate_id: None,
        status_code: 401,
        headers: BTreeMap::new(),
        response_observation: None,
        body: Some(ResponseBody {
            json_body: Some(json!({
                "error": {
                    "message": format!(
                        "Authorization: Bearer {BEARER_SECRET}; https://user:pass@example.test/v1/models?key={QUERY_SECRET}"
                    )
                }
            })),
            body_bytes_b64: None,
        }),
        telemetry: None,
        error: None,
    };

    let projected = super::execution_result_error_message(&result);
    assert_eq!(projected, "HTTP 401: upstream authentication failed");
    for secret in [
        BEARER_SECRET,
        QUERY_SECRET,
        "Authorization",
        "Bearer",
        "user",
        "pass",
        "example.test",
        "/v1/models",
    ] {
        assert!(
            !projected.contains(secret),
            "error projection leaked {secret}"
        );
    }
}

#[test]
fn execution_result_error_projection_discards_execution_error_message() {
    const SECRET: &str = "transport-secret";
    let result = ExecutionResult {
        request_id: "req-model-fetch-security-error".to_string(),
        candidate_id: None,
        status_code: 502,
        headers: BTreeMap::new(),
        response_observation: None,
        body: None,
        telemetry: None,
        error: Some(ExecutionError {
            kind: ExecutionErrorKind::Upstream5xx,
            phase: ExecutionPhase::Connect,
            message: format!(
                "connection failed for https://user:pass@example.test/v1/models?token={SECRET}"
            ),
            upstream_status: Some(502),
            retryable: true,
            failover_recommended: true,
        }),
    };

    let projected = super::execution_result_error_message(&result);
    assert_eq!(projected, "HTTP 502: upstream service failed");
    for secret in [SECRET, "https://", "user", "pass", "example.test", "token"] {
        assert!(
            !projected.contains(secret),
            "error projection leaked {secret}"
        );
    }
}

#[test]
fn model_fetch_transport_error_projection_discards_urls_and_credentials() {
    let projected = super::sanitize_model_fetch_error(
        "connection failed for https://user:password@example.test/v1/models?key=query-secret; Authorization: Bearer transport-secret",
    );
    assert_eq!(projected, "upstream authorization failed");
    for secret in [
        "user",
        "password",
        "query-secret",
        "transport-secret",
        "Bearer",
        "example.test",
    ] {
        assert!(
            !projected.contains(secret),
            "error projection leaked {secret}"
        );
    }
}

#[test]
fn model_fetch_error_projection_drops_reflected_secret_like_model_identity() {
    const REFLECTED_API_KEY: &str = "sk-proj-AbCdEfGhIjKlMnOpQrStUvWxYz0123456789";
    let projected = super::sanitize_model_fetch_error(&format!(
        "Codex models response contains conflicting cards for identity '{REFLECTED_API_KEY}'"
    ));

    assert_eq!(projected, "conflicting cards");
    assert!(!projected.contains(REFLECTED_API_KEY));
}

#[tokio::test]
async fn vertex_service_account_rejects_untrusted_token_uri_before_signing() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        response_body: json!({}),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };
    let transport = sample_custom_aiplatform_transport();
    let auth_config = json!({
        "client_email": "svc@example.iam.gserviceaccount.com",
        "private_key": "not-a-real-key",
        "project_id": "project-1",
        "token_uri": "https://attacker.example/token"
    });

    let error = super::exchange_vertex_service_account_token(&runtime, &transport, &auth_config)
        .await
        .expect_err("untrusted token URI must be rejected");
    assert_eq!(error, "vertex_ai(service_account): invalid auth_config");
    assert!(executed_urls.lock().expect("executed_urls lock").is_empty());
}

#[test]
fn vertex_service_account_model_fetch_ignores_untrusted_endpoint_bases() {
    let mut untrusted = sample_custom_aiplatform_transport();
    untrusted.endpoint.base_url = "https://attacker.example".to_string();
    assert_eq!(
        super::iter_trusted_vertex_base_urls(&[untrusted]),
        vec!["https://aiplatform.googleapis.com".to_string()]
    );
}

#[tokio::test]
async fn vertex_api_key_model_fetch_uses_only_trusted_endpoint_bases() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        response_body: json!({"models": []}),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };
    let mut untrusted = sample_custom_aiplatform_transport();
    untrusted.provider.provider_type = "vertex_ai".to_string();
    untrusted.endpoint.base_url = "https://attacker.example".to_string();

    fetch_models_from_transports(&runtime, &[untrusted])
        .await
        .expect("Vertex API-key model fetch should use the canonical fallback");

    let urls = executed_urls.lock().expect("executed_urls lock");
    assert_eq!(urls.len(), 1);
    assert!(urls[0].starts_with("https://aiplatform.googleapis.com/"));
    assert!(urls[0].contains("key=vertex-secret"));
    assert!(!urls[0].contains("attacker.example"));
}

#[test]
fn pagination_query_values_are_percent_encoded() {
    assert_eq!(
        super::append_query_param(
            "https://aiplatform.googleapis.com/v1beta1/models?key=secret".to_string(),
            "pageToken",
            "cursor&next=1#fragment",
        ),
        "https://aiplatform.googleapis.com/v1beta1/models?key=secret&pageToken=cursor%26next%3D1%23fragment"
    );
}

#[test]
fn vertex_model_fetch_uses_model_garden_list_endpoint() {
    assert_eq!(
        build_vertex_google_list_url(
            "https://aiplatform.googleapis.com",
            "vertex-secret",
            None,
        ),
        "https://aiplatform.googleapis.com/v1beta1/publishers/google/models?key=vertex-secret&pageSize=100"
    );
    assert_eq!(
        build_vertex_service_account_list_url(
            "https://aiplatform.googleapis.com",
            "google",
            Some("page-2"),
        ),
        "https://aiplatform.googleapis.com/v1beta1/publishers/google/models?pageSize=100&pageToken=page-2"
    );
}

#[test]
fn vertex_publisher_models_list_url_uses_model_garden_resource_not_runtime_resource() {
    let url = super::build_vertex_service_account_list_url(
        "https://aiplatform.googleapis.com",
        "google",
        None,
    );

    assert_eq!(
        url,
        "https://aiplatform.googleapis.com/v1beta1/publishers/google/models?pageSize=100"
    );
    assert!(
        !url.contains("/projects/") && !url.contains("/locations/"),
        "Model Garden publisher list must not use Vertex runtime project/location path"
    );
}

#[test]
fn vertex_service_account_fetches_model_garden_publishers_without_project_prefix() {
    assert_eq!(
        super::build_vertex_service_account_list_url(
            "https://us-central1-aiplatform.googleapis.com",
            "google",
            None
        ),
        "https://us-central1-aiplatform.googleapis.com/v1beta1/publishers/google/models?pageSize=100"
    );
    assert_eq!(
        super::build_vertex_service_account_list_url(
            "https://aiplatform.googleapis.com/v1",
            "anthropic",
            Some("next")
        ),
        "https://aiplatform.googleapis.com/v1beta1/publishers/anthropic/models?pageSize=100&pageToken=next"
    );
    assert_eq!(
        super::build_vertex_google_list_url(
            "https://aiplatform.googleapis.com/v1beta1",
            "vertex-secret",
            Some("next")
        ),
        "https://aiplatform.googleapis.com/v1beta1/publishers/google/models?key=vertex-secret&pageSize=100&pageToken=next"
    );
}

#[tokio::test]
async fn codex_transport_fetches_upstream_models_instead_of_preset_catalog() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        response_body: json!({
            "models": [{
                "id": "gpt-5.6-future",
                "slug": "gpt-5.6-future",
                "api_format": "opaque-future-field",
                "default_reasoning_level": "high",
                "supported_reasoning_levels": [{"effort": "high"}],
                "future_capability": {"mode": "preserve-me"}
            }]
        }),
        status_code: 200,
        response_headers: BTreeMap::from([(
            "ETag".to_string(),
            "\"codex-models-0.145.2\"".to_string(),
        )]),
    };
    let outcome = fetch_models_from_transports_for_client_version(
        &runtime,
        &[sample_codex_transport()],
        Some("0.145.2"),
    )
    .await
    .expect("models fetch should succeed");

    let urls = executed_urls.lock().expect("executed_urls lock");
    assert_eq!(
        urls.as_slice(),
        &["https://chatgpt.com/backend-api/codex/models?client_version=0.145.2"]
    );
    assert_eq!(outcome.etag.as_deref(), Some("\"codex-models-0.145.2\""));
    assert_eq!(outcome.upstream_status, Some(200));
    assert_eq!(outcome.fetched_model_ids, vec!["gpt-5.6-future"]);
    assert_eq!(outcome.cached_models.len(), 1);
    assert_eq!(
        outcome.cached_models[0]["api_format"],
        "opaque-future-field"
    );
    assert!(outcome.cached_models[0].get("api_formats").is_none());
    assert_eq!(outcome.legacy_models.len(), 1);
    assert_eq!(outcome.legacy_models[0]["id"], "gpt-5.6-future");
    assert_eq!(
        outcome.legacy_models[0]["api_formats"],
        json!(["openai:responses"])
    );
    assert_eq!(
        outcome.legacy_models[0]["api_format"],
        "opaque-future-field"
    );
    let card = &outcome
        .upstream_metadata
        .as_ref()
        .expect("Codex model catalog metadata")["codex_models"]["cards"]["gpt-5.6-future"];
    assert_eq!(card["default_reasoning_level"], "high");
    assert_eq!(card["future_capability"]["mode"], "preserve-me");
}

#[tokio::test]
async fn codex_transport_reports_slug_only_ids_without_rewriting_opaque_cards() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let card = json!({
        "slug": "gpt-slug-only-future",
        "model_messages": {"instructions_template": "Slug-only instructions"},
        "future_capability": {"opaque": true}
    });
    let runtime = TestRuntime {
        executed_urls,
        response_body: json!({"models": [card.clone()]}),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };

    let outcome = fetch_models_from_transports_for_client_version(
        &runtime,
        &[sample_codex_transport()],
        Some("0.145.2"),
    )
    .await
    .expect("slug-only Codex card should fetch");

    assert_eq!(outcome.fetched_model_ids, vec!["gpt-slug-only-future"]);
    assert_eq!(outcome.cached_models, vec![card]);
    assert!(outcome.cached_models[0].get("id").is_none());
    assert_eq!(outcome.legacy_models[0]["id"], "gpt-slug-only-future");
    assert_eq!(
        outcome.legacy_models[0]["api_formats"],
        json!(["openai:responses"])
    );
    assert_eq!(
        outcome.legacy_models[0]["future_capability"]["opaque"],
        true
    );
}

#[tokio::test]
async fn codex_legacy_projection_merges_only_formats_from_successful_transports() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let card = json!({
        "slug": "gpt-multi-format-future",
        "api_format": "opaque-upstream-protocol",
        "future_capability": {"opaque": true}
    });
    let runtime = RoutingTestRuntime {
        executed_urls,
        routes: vec![
            (
                "chat.example.com/backend-api/codex/models".to_string(),
                Ok((200, json!({"models": [card.clone()]}))),
            ),
            (
                "responses.example.com/backend-api/codex/models".to_string(),
                Ok((200, json!({"models": [card.clone()]}))),
            ),
            (
                "compact.example.com/backend-api/codex/models".to_string(),
                Err("compact endpoint unavailable".to_string()),
            ),
        ],
    };
    let mut chat = sample_codex_transport_for_base(
        "endpoint-chat",
        "https://chat.example.com/backend-api/codex",
    );
    chat.endpoint.api_format = "openai:chat".to_string();
    let responses = sample_codex_transport_for_base(
        "endpoint-responses",
        "https://responses.example.com/backend-api/codex",
    );
    let mut compact = sample_codex_transport_for_base(
        "endpoint-compact",
        "https://compact.example.com/backend-api/codex",
    );
    compact.endpoint.api_format = "openai:responses:compact".to_string();

    let outcome = fetch_models_from_transports_for_client_version(
        &runtime,
        &[chat, responses, compact],
        Some("0.145.2"),
    )
    .await
    .expect("successful endpoint catalogs should survive a sibling failure");

    assert_eq!(outcome.cached_models, vec![card]);
    assert_eq!(outcome.legacy_models.len(), 1);
    assert_eq!(
        outcome.legacy_models[0]["api_formats"],
        json!(["openai:chat", "openai:responses"])
    );
    assert_eq!(
        outcome.legacy_models[0]["api_format"],
        "opaque-upstream-protocol"
    );
    assert_eq!(outcome.errors.len(), 1);
    assert!(outcome.errors[0].contains("compact endpoint unavailable"));
}

#[tokio::test]
async fn unversioned_codex_admin_fetch_keeps_openai_compatible_data_fallback() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls,
        response_body: json!({
            "data": [{
                "id": "gpt-legacy-compatible",
                "future_capability": {"preserved": true}
            }]
        }),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };

    let outcome = fetch_models_from_transports(&runtime, &[sample_codex_transport()])
        .await
        .expect("unversioned admin fetch should retain the generic parser fallback");

    assert!(outcome.has_success);
    assert_eq!(outcome.fetched_model_ids, vec!["gpt-legacy-compatible"]);
    assert_eq!(outcome.cached_models[0]["id"], "gpt-legacy-compatible");
    assert!(!outcome.native_codex_catalog);
    let versioned = crate::fetch_models_from_transports_for_management(
        &runtime,
        &[sample_codex_transport()],
        Some("0.153.3"),
    )
    .await
    .expect("management retains data-array compatibility with a new fingerprint");
    assert!(versioned.has_success);
    assert!(
        !versioned.native_codex_catalog,
        "generic responses cannot replace opaque catalogs"
    );
    assert_eq!(versioned.fetched_model_ids, outcome.fetched_model_ids);
    assert_eq!(
        outcome.legacy_models[0]["api_formats"],
        json!(["openai:responses"])
    );
}

#[tokio::test]
async fn versioned_codex_catalog_does_not_accept_openai_compatible_data_fallback() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls,
        response_body: json!({"data": [{"id": "gpt-not-an-opaque-card"}]}),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };

    let outcome = fetch_models_from_transports_for_client_version(
        &runtime,
        &[sample_codex_transport()],
        Some("0.145.2"),
    )
    .await
    .expect("transport failures are returned as observable outcomes");

    assert!(!outcome.has_success);
    assert!(outcome.cached_models.is_empty());
    assert!(outcome.legacy_models.is_empty());
    assert_eq!(outcome.errors.len(), 1);
    assert!(outcome.errors[0].contains("missing models array"));
}

#[tokio::test]
async fn codex_transport_merges_exact_duplicate_cards_across_endpoints() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let card = json!({
        "id": "gpt-exact-duplicate",
        "slug": "gpt-exact-duplicate",
        "model_messages": {"instructions_template": "Opaque instructions"},
        "future_capability": {"opaque": true}
    });
    let runtime = RoutingTestRuntime {
        executed_urls,
        routes: vec![
            (
                "first.example.com/backend-api/codex/models".to_string(),
                Ok((200, json!({"models": [card.clone()]}))),
            ),
            (
                "second.example.com/backend-api/codex/models".to_string(),
                Ok((200, json!({"models": [card.clone()]}))),
            ),
        ],
    };
    let transports = vec![
        sample_codex_transport_for_base(
            "endpoint-first",
            "https://first.example.com/backend-api/codex",
        ),
        sample_codex_transport_for_base(
            "endpoint-second",
            "https://second.example.com/backend-api/codex",
        ),
    ];

    let outcome =
        fetch_models_from_transports_for_client_version(&runtime, &transports, Some("0.145.2"))
            .await
            .expect("exact duplicate endpoint catalogs should merge");

    assert!(outcome.has_success);
    assert!(outcome.errors.is_empty());
    assert_eq!(outcome.fetched_model_ids, vec!["gpt-exact-duplicate"]);
    assert_eq!(outcome.cached_models, vec![card]);
}

#[tokio::test]
async fn codex_transport_rejects_cross_identity_conflicts_across_endpoints() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = RoutingTestRuntime {
        executed_urls,
        routes: vec![
            (
                "first.example.com/backend-api/codex/models".to_string(),
                Ok((
                    200,
                    json!({
                        "models": [{
                            "id": "gpt-id-one",
                            "slug": "gpt-cross-identity",
                            "future_capability": {"source": "first"}
                        }]
                    }),
                )),
            ),
            (
                "second.example.com/backend-api/codex/models".to_string(),
                Ok((
                    200,
                    json!({
                        "models": [{
                            "id": "gpt-cross-identity",
                            "slug": "gpt-slug-two",
                            "future_capability": {"source": "second"}
                        }]
                    }),
                )),
            ),
        ],
    };
    let transports = vec![
        sample_codex_transport_for_base(
            "endpoint-first",
            "https://first.example.com/backend-api/codex",
        ),
        sample_codex_transport_for_base(
            "endpoint-second",
            "https://second.example.com/backend-api/codex",
        ),
    ];

    let error =
        fetch_models_from_transports_for_client_version(&runtime, &transports, Some("0.145.2"))
            .await
            .expect_err("conflicting endpoint catalogs must fail");

    assert_eq!(error, "conflicting cards");
    assert!(!error.contains("gpt-cross-identity"));
}

#[tokio::test]
async fn codex_transport_reports_non_success_upstream_status() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls,
        response_body: json!({
            "error": { "message": "temporarily unavailable" }
        }),
        status_code: 503,
        response_headers: BTreeMap::new(),
    };

    let outcome = fetch_models_from_transports_for_client_version(
        &runtime,
        &[sample_codex_transport()],
        Some("0.145.2"),
    )
    .await
    .expect("models fetch should return an observable failed outcome");

    assert!(!outcome.has_success);
    assert_eq!(outcome.upstream_status, Some(503));
    assert_eq!(outcome.etag, None);
    assert_eq!(outcome.errors.len(), 1);
    assert!(outcome.errors[0].contains("HTTP 503: temporarily unavailable"));
}

#[tokio::test]
async fn gemini_cli_load_code_assist_preserves_paid_tier_credits() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        response_body: json!({
            "cloudaicompanionProject": {
                "id": "project-from-load-code-assist"
            },
            "currentTier": {
                "id": "free-tier"
            },
            "paidTier": {
                "id": "g1-pro-tier",
                "availableCredits": 123.5,
                "consumedCredits": 7,
                "totalCredits": 200,
                "privateField": {
                    "ignored": true
                }
            }
        }),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };
    let outcome = fetch_models_from_transports(&runtime, &[sample_gemini_cli_transport()])
        .await
        .expect("models fetch should succeed");

    let urls = executed_urls.lock().expect("executed_urls lock");
    assert_eq!(
        urls.as_slice(),
        &["https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist"]
    );
    assert_eq!(
        outcome
            .upstream_metadata
            .as_ref()
            .and_then(|value| value.pointer("/gemini_cli/project_id")),
        Some(&json!("project-from-load-code-assist"))
    );
    assert_eq!(
        outcome
            .upstream_metadata
            .as_ref()
            .and_then(|value| value.pointer("/gemini_cli/plan_type")),
        Some(&json!("g1-pro-tier"))
    );
    assert_eq!(
        outcome
            .upstream_metadata
            .as_ref()
            .and_then(|value| value.pointer("/gemini_cli/paidTier/availableCredits")),
        Some(&json!(123.5))
    );
    assert!(outcome
        .upstream_metadata
        .as_ref()
        .and_then(|value| value.pointer("/gemini_cli/paidTier/privateField"))
        .is_none());
}

#[tokio::test]
async fn antigravity_model_fetch_hydrates_project_from_prod_load_code_assist() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = OAuthRoutingTestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        routes: vec![
            (
                "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist".to_string(),
                Ok((
                    200,
                    json!({
                        "cloudaicompanionProject": {
                            "id": "project-from-antigravity-load"
                        }
                    }),
                )),
            ),
            (
                "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels"
                    .to_string(),
                Ok((
                    200,
                    json!({
                        "models": {
                            "chat_12345": {
                                "displayName": "Antigravity Chat",
                                "quotaInfo": {
                                    "remainingFraction": 0.75
                                }
                            }
                        }
                    }),
                )),
            ),
        ],
    };

    let outcome =
        fetch_models_from_transports(&runtime, &[sample_antigravity_transport_without_project()])
            .await
            .expect("antigravity models fetch should hydrate project and succeed");

    let urls = executed_urls.lock().expect("executed_urls lock");
    assert_eq!(
        urls.as_slice(),
        &[
            "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist",
            "https://daily-cloudcode-pa.googleapis.com/v1internal:fetchAvailableModels",
        ]
    );
    assert_eq!(outcome.fetched_model_ids, vec!["chat_12345"]);
    assert_eq!(
        outcome
            .upstream_metadata
            .as_ref()
            .and_then(|value| value.pointer("/antigravity/project_id")),
        Some(&json!("project-from-antigravity-load"))
    );
    assert_eq!(
        outcome.upstream_metadata.as_ref().and_then(
            |value| value.pointer("/antigravity/quota_by_model/chat_12345/remaining_fraction")
        ),
        Some(&json!(0.75))
    );
}

#[test]
fn antigravity_models_without_explicit_quota_are_not_marked_exhausted() {
    let (models, metadata) = parse_antigravity_models_response(&json!({
        "models": {
            "gemini-3.7-flash-tiered": {
                "displayName": "Gemini 3.7 Flash"
            },
            "gemini-3.7-flash-high": {
                "displayName": "Gemini 3.7 Flash High",
                "quotaInfo": {
                    "remainingFraction": "0.75",
                    "resetTime": "2030-01-01T00:00:00Z"
                }
            },
            "gemini-3.7-flash-low": {
                "displayName": "Gemini 3.7 Flash Low",
                "quotaInfo": {
                    "remainingFraction": 0.0
                }
            }
        }
    }))
    .expect("Antigravity models should parse");

    assert_eq!(models.len(), 3);
    let metadata = metadata.expect("explicit quota should produce metadata");
    let antigravity = &metadata["antigravity"];
    assert!(antigravity["quota_by_model"]
        .get("gemini-3.7-flash-tiered")
        .is_none());
    assert_eq!(
        antigravity["quota_by_model"]["gemini-3.7-flash-high"]["remaining_fraction"],
        json!(0.75)
    );
    assert_eq!(
        antigravity["quota_by_model"]["gemini-3.7-flash-high"]["used_percent"],
        json!(25.0)
    );
    assert_eq!(
        antigravity["quota_by_model"]["gemini-3.7-flash-low"]["used_percent"],
        json!(100.0)
    );
}

#[tokio::test]
async fn kiro_transport_fetches_list_available_models() {
    let executed_urls = Arc::new(Mutex::new(Vec::new()));
    let runtime = TestRuntime {
        executed_urls: Arc::clone(&executed_urls),
        response_body: json!({
            "defaultModel": {
                "modelId": "auto",
                "modelName": "Auto"
            },
            "models": [
                {
                    "modelId": "auto",
                    "modelName": "Auto",
                    "tokenLimits": {
                        "maxInputTokens": 1000000,
                        "maxOutputTokens": 64000
                    }
                },
                {
                    "modelId": "claude-opus-4.7",
                    "modelName": "Claude Opus 4.7",
                    "description": "Experimental preview"
                }
            ]
        }),
        status_code: 200,
        response_headers: BTreeMap::new(),
    };
    let outcome = fetch_models_from_transports(&runtime, &[sample_kiro_transport()])
        .await
        .expect("models fetch should succeed");

    let urls = executed_urls.lock().expect("executed_urls lock");
    assert_eq!(
        urls.as_slice(),
        &["https://q.us-east-1.amazonaws.com/ListAvailableModels?origin=AI_EDITOR"]
    );
    assert_eq!(
        outcome.fetched_model_ids,
        vec!["auto".to_string(), "claude-opus-4.7".to_string()]
    );
    assert_eq!(outcome.cached_models.len(), 2);
    assert_eq!(
        outcome.cached_models[1]["display_name"].as_str(),
        Some("Claude Opus 4.7")
    );
    assert_eq!(
        outcome.cached_models[1]["owned_by"].as_str(),
        Some("anthropic")
    );
    assert_eq!(
        outcome.cached_models[1]["api_formats"],
        json!(["claude:messages"])
    );
    assert_eq!(
        outcome.upstream_metadata.as_ref().and_then(|value| {
            value
                .get("kiro")
                .and_then(|value| value.get("default_model"))
                .and_then(|value| value.get("model_id"))
        }),
        Some(&json!("auto"))
    );
}
