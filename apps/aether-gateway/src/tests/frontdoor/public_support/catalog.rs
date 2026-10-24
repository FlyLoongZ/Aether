use super::*;

#[tokio::test]
async fn gateway_handles_public_catalog_site_info_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::disabled().with_system_config_values_for_tests(
                    vec![
                        ("site_name".to_string(), json!("Aether Local")),
                        ("site_subtitle".to_string(), json!("Rust Only")),
                    ],
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/public/site-info"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["site_name"], "Aether Local");
    assert_eq!(payload["site_subtitle"], "Rust Only");
    assert_eq!(payload.as_object().map(|object| object.len()), Some(2));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_catalog_providers_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let mut inactive_provider = sample_provider("provider-draft", "draft", 30);
    inactive_provider.is_active = false;
    let mut inactive_endpoint = sample_endpoint(
        "endpoint-openai-disabled",
        "provider-openai",
        "openai:responses",
        "https://internal-openai.example",
    );
    inactive_endpoint.is_active = false;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai", "openai", 10),
            sample_provider("provider-claude", "claude", 20),
            inactive_provider,
        ],
        vec![
            sample_endpoint(
                "endpoint-openai",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example",
            ),
            sample_endpoint(
                "endpoint-claude",
                "provider-claude",
                "claude:messages",
                "https://api.anthropic.example",
            ),
            sample_endpoint(
                "endpoint-claude-cli",
                "provider-claude",
                "claude:messages",
                "https://api.anthropic.example",
            ),
            inactive_endpoint,
        ],
        vec![],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_reader_for_tests(
                    provider_catalog_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/public/providers?is_active=false&limit=10"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let providers = payload.as_array().expect("providers should be an array");
    assert_eq!(providers.len(), 2);
    assert_eq!(providers[0]["id"], "provider-openai");
    assert!(providers[0].get("name").is_none());
    assert!(providers[0].get("description").is_none());
    assert!(providers[0].get("website").is_none());
    assert_eq!(providers[0]["provider_priority"], 10);
    assert_eq!(providers[0]["endpoints_count"], 1);
    assert_eq!(providers[0]["active_endpoints_count"], 1);
    assert_eq!(providers[0]["models_count"], 0);
    assert_eq!(providers[0]["active_models_count"], 0);
    assert_eq!(providers[1]["id"], "provider-claude");
    assert_eq!(providers[1]["endpoints_count"], 2);
    assert_eq!(providers[1]["active_endpoints_count"], 2);
    assert!(providers
        .iter()
        .all(|provider| provider["id"] != "provider-draft"));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_catalog_models_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::<StoredPublicGlobalModel>::new())
            .with_public_catalog_models(vec![
                sample_public_catalog_model(
                    "model-openai-gpt5",
                    "provider-openai",
                    "openai",
                    "gpt-5-preview",
                    "gpt-5",
                    "GPT 5",
                ),
                sample_public_catalog_model(
                    "model-claude-sonnet",
                    "provider-claude",
                    "claude",
                    "claude-sonnet-4-5-20251001",
                    "claude-sonnet-4-5",
                    "Claude Sonnet 4.5",
                ),
            ]),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_global_model_reader_for_tests(
                    global_model_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/public/models?provider_id=provider-openai&limit=10"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload.as_array().expect("models should be an array");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["id"], "model-openai-gpt5");
    assert!(models[0].get("provider_id").is_none());
    assert!(models[0].get("provider_name").is_none());
    assert_eq!(models[0]["name"], "gpt-5");
    assert_eq!(models[0]["display_name"], "GPT 5");
    assert_eq!(models[0]["tags"], serde_json::Value::Null);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn public_catalog_excludes_unsupported_embedding_provider() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let mut active_embedding = sample_public_catalog_model(
        "model-openai-embedding-small",
        "provider-openai",
        "openai",
        "text-embedding-3-small",
        "text-embedding-3-small",
        "Text Embedding 3 Small",
    );
    active_embedding.supports_embedding = Some(true);
    active_embedding.supports_streaming = Some(false);
    let mut inactive_embedding = sample_public_catalog_model(
        "model-openai-embedding-inactive",
        "provider-openai",
        "openai",
        "text-embedding-3-large",
        "text-embedding-3-large",
        "Text Embedding 3 Large",
    );
    inactive_embedding.supports_embedding = Some(true);
    inactive_embedding.is_active = false;
    let mut unsupported_embedding = sample_public_catalog_model(
        "model-unsupported-embedding",
        "provider-openai",
        "openai",
        "legacy-embedding",
        "legacy-embedding",
        "Legacy Embedding",
    );
    unsupported_embedding.supports_embedding = Some(false);
    unsupported_embedding.is_active = false;

    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::<StoredPublicGlobalModel>::new())
            .with_public_catalog_models(vec![
                active_embedding,
                inactive_embedding,
                unsupported_embedding,
            ]),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_global_model_reader_for_tests(
                    global_model_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/public/models?provider_id=provider-openai&limit=10"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload.as_array().expect("models should be an array");
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["id"], "model-openai-embedding-small");
    assert_eq!(models[0]["supports_embedding"], true);
    assert_eq!(models[0]["supports_streaming"], false);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_catalog_search_models_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::<StoredPublicGlobalModel>::new())
            .with_public_catalog_models(vec![
                sample_public_catalog_model(
                    "model-openai-gpt5",
                    "provider-openai",
                    "openai",
                    "gpt-5-preview",
                    "gpt-5",
                    "GPT 5",
                ),
                sample_public_catalog_model(
                    "model-claude-sonnet",
                    "provider-claude",
                    "claude",
                    "claude-sonnet-4-5-20251001",
                    "claude-sonnet-4-5",
                    "Claude Sonnet 4.5",
                ),
            ]),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_global_model_reader_for_tests(
                    global_model_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/public/search/models?q=sonnet&limit=20"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload.as_array().expect("models should be an array");
    assert_eq!(models.len(), 1);
    assert!(models[0].get("provider_id").is_none());
    assert!(models[0].get("provider_name").is_none());
    assert_eq!(models[0]["name"], "claude-sonnet-4-5");
    assert_eq!(models[0]["display_name"], "Claude Sonnet 4.5");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_catalog_stats_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai", "openai", 10),
            sample_provider("provider-claude", "claude", 20),
        ],
        vec![
            sample_endpoint(
                "endpoint-openai",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example",
            ),
            sample_endpoint(
                "endpoint-claude",
                "provider-claude",
                "claude:messages",
                "https://api.anthropic.example",
            ),
        ],
        vec![],
    ));
    let candidate_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            sample_models_candidate_row("provider-openai", "openai", "openai:chat", "gpt-5", 10),
            sample_models_candidate_row("provider-openai", "openai", "openai:chat", "gpt-4.1", 10),
            sample_models_candidate_row(
                "provider-claude",
                "claude",
                "claude:messages",
                "claude-3-7-sonnet",
                20,
            ),
        ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_and_minimal_candidate_selection_for_tests(
                    provider_catalog_repository,
                    candidate_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/public/stats"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total_providers"], 2);
    assert_eq!(payload["active_providers"], 2);
    assert_eq!(payload["total_models"], 3);
    assert_eq!(payload["active_models"], 3);
    assert_eq!(
        payload["supported_formats"],
        json!(["claude:messages", "openai:chat"])
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_global_models_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let mut gpt_model = sample_public_global_model("gm-3", "gpt-5", "GPT 5", true);
    gpt_model.config = Some(json!({
        "description": "Public description",
        "streaming": true,
        "api_formats": ["openai:responses", {"secret": "capability-leak"}],
        "billing": {
            "video": {
                "price_per_second_by_resolution": {
                    "720p": 0.12,
                    "internal": "video-billing-secret"
                },
                "private_key": "video-secret"
            }
        },
        "client_secret": "config-secret",
        "model_mappings": ["gpt-5-upstream"],
        "provider_model_mappings": [{"name": "provider-gpt-5"}],
    }));
    gpt_model.supported_capabilities = Some(json!(["vision", {"secret": "capability-secret"}]));
    gpt_model.default_tiered_pricing = Some(json!({
        "tiers": [{
            "up_to": null,
            "input_price_per_1m": 3.0,
            "output_price_per_1m": 15.0,
            "internal_note": "tier-secret",
            "cache_ttl_pricing": [{
                "ttl_minutes": 60,
                "cache_creation_price_per_1m": 4.0,
                "secret": "ttl-secret"
            }]
        }],
        "internal_pricing": {"secret": "pricing-secret"}
    }));
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(vec![
        sample_public_global_model("gm-1", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
        sample_public_global_model("gm-2", "disabled-model", "Disabled Model", false),
        gpt_model,
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_global_model_reader_for_tests(
                    global_model_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/public/global-models?is_active=false&search=gpt"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 1);
    assert_eq!(payload["models"][0]["name"], "gpt-5");
    assert_eq!(payload["models"][0]["display_name"], "GPT 5");
    assert_eq!(payload["models"][0]["usage_count"], 0);
    assert_eq!(
        payload["models"][0]["config"]["description"],
        "Public description"
    );
    assert!(payload["models"][0]["config"]
        .get("model_mappings")
        .is_none());
    assert!(payload["models"][0]["config"]
        .get("provider_model_mappings")
        .is_none());
    assert!(payload["models"][0]["config"]
        .get("client_secret")
        .is_none());
    assert_eq!(
        payload["models"][0]["config"]["api_formats"],
        json!(["openai:responses"])
    );
    assert_eq!(
        payload["models"][0]["config"]["billing"]["video"]["price_per_second_by_resolution"],
        json!({"720p": 0.12})
    );
    assert!(payload["models"][0]["config"]["billing"]["video"]
        .get("private_key")
        .is_none());
    assert_eq!(
        payload["models"][0]["supported_capabilities"],
        json!(["vision"])
    );
    assert!(payload["models"][0]["default_tiered_pricing"]
        .get("internal_pricing")
        .is_none());
    assert!(payload["models"][0]["default_tiered_pricing"]["tiers"][0]
        .get("internal_note")
        .is_none());
    assert!(
        payload["models"][0]["default_tiered_pricing"]["tiers"][0]["cache_ttl_pricing"][0]
            .get("secret")
            .is_none()
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_capabilities_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router().expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/capabilities"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["capabilities"][0]["name"], "cache_1h");
    assert_eq!(payload["capabilities"][1]["name"], "context_1m");
    assert_eq!(payload["capabilities"][2]["name"], "gemini_files");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_user_configurable_capabilities_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router().expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/capabilities/user-configurable"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload, json!({ "capabilities": [] }));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_model_capabilities_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(vec![
        sample_public_global_model_with_capabilities(
            "gm-1",
            "gpt-5",
            "GPT 5",
            json!(["cache_1h", "missing_cap", "gemini_files"]),
        ),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_global_model_reader_for_tests(
                    global_model_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/capabilities/model/gpt-5"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["model"], "gpt-5");
    assert_eq!(payload["global_model_name"], "gpt-5");
    assert_eq!(
        payload["supported_capabilities"],
        json!(["cache_1h", "missing_cap", "gemini_files"])
    );
    assert_eq!(
        payload["capability_details"].as_array().map(Vec::len),
        Some(2)
    );
    assert_eq!(payload["capability_details"][0]["name"], "cache_1h");
    assert_eq!(payload["capability_details"][1]["name"], "gemini_files");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_missing_public_model_capabilities_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router().expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/capabilities/model/missing-model"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["model"], "missing-model");
    assert_eq!(payload["supported_capabilities"], json!([]));
    assert_eq!(payload["capability_details"], json!([]));
    assert_eq!(payload["error"], "模型不存在");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_providers_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-2", "anthropic", 20),
            sample_provider("provider-1", "openai", 10),
        ],
        vec![],
        vec![],
    ));
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_reader_for_tests(
                    provider_catalog_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/providers"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert!(payload["providers"][0].get("name").is_none());
    assert_eq!(payload["providers"][0]["provider_priority"], 10);
    assert!(payload["providers"][1].get("name").is_none());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_provider_detail_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let mut inactive_provider = sample_provider("provider-disabled", "disabled", 20);
    inactive_provider.is_active = false;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-1", "openai", 10),
            inactive_provider,
        ],
        vec![],
        vec![],
    ));
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_reader_for_tests(
                    provider_catalog_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/providers/provider-1"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["id"], "provider-1");
    assert!(payload.get("name").is_none());
    assert_eq!(payload["provider_priority"], 10);

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/providers/openai"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "Provider not found");

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/providers/provider-disabled?include_endpoints=true"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "Provider not found");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_providers_with_endpoints_without_proxying_upstream() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("proxied"))
            }
        }),
    );

    let mut inactive_endpoint = sample_endpoint(
        "endpoint-disabled",
        "provider-1",
        "openai:responses",
        "https://internal-openai.example",
    );
    inactive_endpoint.is_active = false;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-1", "openai", 10)],
        vec![
            sample_endpoint(
                "endpoint-1",
                "provider-1",
                "openai:chat",
                "https://api.openai.example",
            ),
            inactive_endpoint,
        ],
        vec![],
    ));
    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_provider_catalog_reader_for_tests(
                    provider_catalog_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/providers?include_endpoints=true"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["providers"][0]["endpoints"][0]["id"], "endpoint-1");
    assert!(payload["providers"][0]["endpoints"][0]
        .get("base_url")
        .is_none());
    assert_eq!(
        payload["providers"][0]["endpoints"][0]["api_format"],
        "openai:chat"
    );
    assert_eq!(
        payload["providers"][0]["endpoints"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_unauthenticated_test_connection_before_using_provider_credentials() {
    let provider_hits = Arc::new(Mutex::new(0usize));
    let provider_hits_clone = Arc::clone(&provider_hits);
    let provider = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let provider_hits_inner = Arc::clone(&provider_hits_clone);
            async move {
                *provider_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({"id": "must-not-run"})).into_response()
            }
        }),
    );
    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-auth-required", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-auth-required",
            "provider-auth-required",
            "openai:chat",
            &provider_url,
        )],
        vec![sample_key(
            "key-auth-required",
            "provider-auth-required",
            "openai:chat",
            "stored-provider-secret",
        )],
    ));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEVELOPMENT_ENCRYPTION_KEY,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/test-connection?provider=provider-auth-required&model=gpt-5&api_format=openai:chat"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "缺少用户凭证");
    assert_eq!(*provider_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    provider_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_non_admin_test_connection_before_using_provider_credentials() {
    let provider_hits = Arc::new(Mutex::new(0usize));
    let provider_hits_clone = Arc::clone(&provider_hits);
    let provider = Router::new().route(
        "/{*path}",
        any(move |_request: Request| {
            let provider_hits_inner = Arc::clone(&provider_hits_clone);
            async move {
                *provider_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({"id": "must-not-run"})).into_response()
            }
        }),
    );
    let (provider_url, provider_handle) = start_server(provider).await;
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![sample_provider("provider-admin-only", "openai", 10)],
        vec![sample_endpoint(
            "endpoint-admin-only",
            "provider-admin-only",
            "openai:chat",
            &provider_url,
        )],
        vec![sample_key(
            "key-admin-only",
            "provider-admin-only",
            "openai:chat",
            "stored-provider-secret",
        )],
    ));

    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-test-connection-non-admin"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_transport_reader_for_tests(
                    provider_catalog_repository,
                    DEVELOPMENT_ENCRYPTION_KEY,
                )
                .with_user_reader(user_repository),
            )
            .with_auth_session_for_tests(sample_auth_session(
                "user-auth-1",
                "session-test-connection-non-admin",
                "device-test-connection-non-admin",
                "refresh-token-placeholder",
                now,
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/v1/test-connection?provider=provider-admin-only&model=gpt-5&api_format=openai:chat"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-test-connection-non-admin",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "仅管理员可以测试供应商连接");
    assert_eq!(*provider_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    provider_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_usage_search_with_model_and_api_key_keywords() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-usage-multi-keyword-search"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-users-me-completed-multi-keyword-search",
            "req-users-me-completed-multi-keyword-search",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            now - chrono::Duration::minutes(20),
        ),
        sample_user_usage_audit(
            "usage-users-me-failed-multi-keyword-search",
            "req-users-me-failed-multi-keyword-search",
            "user-auth-1",
            "claude-3.5-sonnet",
            "Anthropic",
            "failed",
            now - chrono::Duration::minutes(10),
        ),
        sample_user_usage_audit(
            "usage-users-me-streaming-multi-keyword-search",
            "req-users-me-streaming-multi-keyword-search",
            "user-auth-1",
            "gpt-4.1-mini",
            "OpenAI",
            "streaming",
            now - chrono::Duration::minutes(5),
        ),
    ]));
    let auth_repository = Arc::new(InMemoryAuthApiKeySnapshotRepository::seed(vec![(
        Some("hash-api-key-user-1".to_string()),
        sample_usage_auth_snapshot("api-key-user-1", "user-auth-1", "renamed-key"),
    )]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_user_wallet_and_usage_for_tests(
                Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![
                    user.clone()
                ])),
                Arc::new(InMemoryWalletRepository::seed(vec![sample_auth_wallet(
                    "user-auth-1",
                    now,
                )])),
                Arc::clone(&usage_repository),
            )
            .with_auth_api_key_reader(auth_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-usage-multi-keyword-search",
                    "device-users-me-usage-multi-keyword-search",
                    "refresh-token-users-me-usage-multi-keyword-search",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/usage?limit=10&offset=0&search=gpt%20renamed-key"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-usage-multi-keyword-search",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["pagination"]["total"], 2);
    assert_eq!(
        payload["records"].as_array().expect("records array").len(),
        2
    );
    assert_eq!(payload["records"][0]["model"], "gpt-4.1-mini");
    assert_eq!(payload["records"][1]["model"], "gpt-4.1");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_providers_locally_without_proxying_upstream() {
    let now = Utc::now();
    let mut user = sample_auth_user(now);
    user.allowed_providers = Some(vec!["claude".to_string()]);
    user.allowed_providers_mode = "specific".to_string();
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-providers"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai", "openai", 10),
            sample_provider("provider-claude", "claude", 20),
        ],
        vec![
            sample_endpoint(
                "endpoint-openai-1",
                "provider-openai",
                "openai:chat",
                "https://api.openai.example",
            ),
            sample_endpoint(
                "endpoint-claude-1",
                "provider-claude",
                "claude:messages",
                "https://api.claude.example",
            ),
        ],
        vec![],
    ));
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(Vec::<StoredPublicGlobalModel>::new())
            .with_public_catalog_models(vec![
                sample_public_catalog_model(
                    "model-openai-gpt5",
                    "provider-openai",
                    "openai",
                    "gpt-5-preview",
                    "gpt-5",
                    "GPT 5",
                ),
                sample_public_catalog_model(
                    "model-claude-sonnet",
                    "provider-claude",
                    "claude",
                    "claude-sonnet-4-5-20251001",
                    "claude-sonnet-4-5",
                    "Claude Sonnet 4.5",
                ),
            ]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "OpenAI only".to_string(),
            description: None,
            priority: 0,
            allowed_providers: Some(vec!["openai".to_string()]),
            allowed_providers_mode: "specific".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "unrestricted".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    user_repository
        .add_user_to_group(&group.id, "user-auth-1")
        .await
        .expect("group membership should create");

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-providers",
                    "device-users-me-providers",
                    "refresh-token-users-me-providers",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/providers"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-providers")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let providers = payload.as_array().expect("providers should be array");
    assert_eq!(providers.len(), 1);
    assert_eq!(providers[0]["id"], "provider-openai");
    assert!(providers[0].get("name").is_none());
    assert!(providers[0].get("description").is_none());
    assert_eq!(providers[0]["endpoints"][0]["id"], "endpoint-openai-1");
    assert!(providers[0]["endpoints"][0].get("base_url").is_none());
    assert_eq!(providers[0]["models"][0]["name"], "gpt-5");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_available_models_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-available-models"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(vec![
            sample_public_global_model("gm-1", "gpt-5", "GPT 5", true),
            sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
            sample_public_global_model("gm-3", "disabled-model", "Disabled Model", false),
        ])
        .with_active_global_model_refs(vec![
            StoredProviderActiveGlobalModel::new("provider-openai".to_string(), "gm-1".to_string())
                .expect("active global model ref should build"),
            StoredProviderActiveGlobalModel::new("provider-claude".to_string(), "gm-2".to_string())
                .expect("active global model ref should build"),
        ]),
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![
            sample_provider("provider-openai", "openai", 10),
            sample_provider("provider-claude", "claude", 20),
        ],
        vec![],
        vec![],
    ));
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-available-models",
                    "device-users-me-available-models",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/users/me/available-models?search=gpt&skip=0&limit=10"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-available-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload["models"]
        .as_array()
        .expect("models should be an array");
    assert_eq!(payload["total"], 1);
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["id"], "gm-1");
    assert_eq!(models[0]["name"], "gpt-5");
    assert_eq!(models[0]["display_name"], "GPT 5");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_refreshes_users_me_available_models_after_group_assignment() {
    let now = Utc::now();
    let mut user = sample_auth_user(now);
    user.allowed_providers = None;
    user.allowed_providers_mode = "unrestricted".to_string();
    // Keep the legacy gpt-5 personal policy from sample_auth_user. Group policies are the
    // authority now, so this stale field must not narrow the user catalog.
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-group-models"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let mut allowed_model =
        sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true);
    allowed_model.config = Some(json!({
        "description": "Claude detail",
        "model_mappings": ["claude-upstream"]
    }));
    let mut blocked_model = sample_public_global_model("gm-1", "gpt-5", "GPT 5", true);
    blocked_model.config = Some(json!({
        "description": "GPT detail",
        "model_mappings": ["gpt-upstream"]
    }));
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(vec![
        blocked_model,
        allowed_model,
    ]));
    let user_repository: Arc<dyn UserReadRepository> =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "Claude only".to_string(),
            description: None,
            priority: 0,
            allowed_providers: None,
            allowed_providers_mode: "unrestricted".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: Some(vec!["claude-sonnet-4-5".to_string()]),
            allowed_models_mode: "specific".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    let data_state =
        crate::data::GatewayDataState::with_global_model_reader_for_tests(global_model_repository)
            .with_user_reader(Arc::clone(&user_repository));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(data_state)
        .with_auth_sessions_for_tests([sample_auth_session(
            "user-auth-1",
            "session-users-me-group-models",
            "device-users-me-group-models",
            "refresh-token-placeholder",
            now,
        )]);
    let mutation_state = state.clone();
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| state).await;

    let client = reqwest::Client::new();
    let response = client
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-group-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 2);

    mutation_state
        .replace_user_groups_for_user("user-auth-1", std::slice::from_ref(&group.id))
        .await
        .expect("group membership should create");

    let response = client
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-group-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed after group assignment");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    let models = payload["models"]
        .as_array()
        .expect("models should be an array");
    assert_eq!(payload["total"], 1);
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["name"], "claude-sonnet-4-5");
    assert_eq!(models[0]["config"]["description"], "Claude detail");
    assert!(models[0]["config"].get("model_mappings").is_none());
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_no_users_me_available_models_when_group_denies_all_models() {
    let now = Utc::now();
    let mut user = sample_auth_user(now);
    user.allowed_providers = None;
    user.allowed_providers_mode = "unrestricted".to_string();
    user.allowed_models = None;
    user.allowed_models_mode = "unrestricted".to_string();
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-deny-all-models"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let global_model_repository = Arc::new(InMemoryGlobalModelReadRepository::seed(vec![
        sample_public_global_model("gm-1", "gpt-5", "GPT 5", true),
        sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
    ]));
    let user_repository: Arc<dyn UserReadRepository> =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "No models".to_string(),
            description: None,
            priority: 0,
            allowed_providers: None,
            allowed_providers_mode: "unrestricted".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "deny_all".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    user_repository
        .add_user_to_group(&group.id, "user-auth-1")
        .await
        .expect("group membership should create");

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_user_reader(Arc::clone(&user_repository));
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-deny-all-models",
                    "device-users-me-deny-all-models",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-deny-all-models")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 0);
    assert_eq!(payload["models"].as_array().map(Vec::len), Some(0));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_available_models_without_provider_catalog(
) {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-available-models-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let global_model_repository = Arc::new(
        InMemoryGlobalModelReadRepository::seed(vec![
            sample_public_global_model("gm-1", "gpt-5", "GPT 5", true),
            sample_public_global_model("gm-2", "claude-sonnet-4-5", "Claude Sonnet 4.5", true),
        ])
        .with_active_global_model_refs(vec![StoredProviderActiveGlobalModel::new(
            "provider-openai".to_string(),
            "gm-1".to_string(),
        )
        .expect("active global model ref should build")]),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let group = user_repository
        .create_user_group(UpsertUserGroupRecord {
            name: "OpenAI provider only".to_string(),
            description: None,
            priority: 0,
            allowed_providers: Some(vec!["openai".to_string()]),
            allowed_providers_mode: "specific".to_string(),
            allowed_api_formats: None,
            allowed_api_formats_mode: "unrestricted".to_string(),
            allowed_models: None,
            allowed_models_mode: "unrestricted".to_string(),
            rate_limit: None,
            rate_limit_mode: "system".to_string(),
        })
        .await
        .expect("group should create")
        .expect("group should exist");
    user_repository
        .add_user_to_group(&group.id, "user-auth-1")
        .await
        .expect("group membership should create");
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state = crate::data::GatewayDataState::with_global_model_reader_for_tests(
                global_model_repository,
            )
            .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-available-models-unavailable",
                    "device-users-me-available-models-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/available-models"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-available-models-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_model_capabilities_get_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-model-cap-get"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-model-cap-get",
                    "device-users-me-model-cap-get",
                    "refresh-token-placeholder",
                    now,
                )])
                .with_auth_user_model_capability_settings_for_tests(
                    "user-auth-1",
                    json!({"gpt-5": {"cache_1h": true}}),
                )
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-model-cap-get")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["model_capability_settings"],
        json!({"gpt-5": {"cache_1h": true}})
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_updates_users_me_model_capabilities_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-model-cap-put"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-model-cap-put",
                    "device-users-me-model-cap-put",
                    "refresh-token-placeholder",
                    now,
                )])
                .with_auth_user_model_capability_settings_for_tests(
                    "user-auth-1",
                    json!({"disabled-model": {"disabled_flag": true}}),
                )
        })
        .await;

    let client = reqwest::Client::new();
    let put_response = client
        .put(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-model-cap-put")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "model_capability_settings": {}
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(put_response.status(), StatusCode::OK);
    let put_payload: serde_json::Value = put_response.json().await.expect("json body should parse");
    assert_eq!(put_payload["message"], "模型能力配置已更新");
    assert_eq!(put_payload["model_capability_settings"], json!({}));

    let get_response = client
        .get(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-users-me-model-cap-put")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(get_response.status(), StatusCode::OK);
    let get_payload: serde_json::Value = get_response.json().await.expect("json body should parse");
    assert_eq!(get_payload["model_capability_settings"], json!({}));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_model_capabilities_update_without_storage(
) {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-users-me-model-cap-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let user_repository =
        Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]).read_only());
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_user_reader_for_tests(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-model-cap-unavailable",
                    "device-users-me-model-cap-unavailable",
                    "refresh-token-placeholder",
                    now,
                )])
                .without_auth_user_model_capability_store_for_tests()
        })
        .await;

    let response = reqwest::Client::new()
        .put(format!("{gateway_url}/api/users/me/model-capabilities"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-model-cap-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "model_capability_settings": {}
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
