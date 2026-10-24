use super::*;
use crate::tests::DEV_FERNET_KEY;

#[test]
fn gateway_handles_admin_provider_query_embedding_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_embedding_model_test",
        gateway_handles_admin_provider_query_embedding_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_embedding_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-siliconflow");
            assert_eq!(plan.endpoint_id, "endpoint-siliconflow-embedding");
            assert_eq!(plan.key_id, "key-siliconflow-embedding");
            assert_eq!(plan.client_api_format, "openai:embedding");
            assert_eq!(plan.provider_api_format, "openai:embedding");
            assert_eq!(plan.url, "https://api.siliconflow.example/embeddings");
            assert_eq!(plan.model_name.as_deref(), Some("Qwen/Qwen3-Embedding-4B"));
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-siliconflow-embedding")
            );
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(body["model"], json!("Qwen/Qwen3-Embedding-4B"));
            assert_eq!(body["input"], json!("This is a test embedding input."));
            assert!(
                body.get("stream").is_none(),
                "embedding provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "object": "list",
                        "model": "Qwen/Qwen3-Embedding-4B",
                        "data": [{
                            "object": "embedding",
                            "index": 0,
                            "embedding": [0.1, 0.2, 0.3]
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 24
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-siliconflow", "SiliconFlow", 10);
    provider.provider_type = "custom".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-siliconflow-embedding",
            "provider-siliconflow",
            "openai:embedding",
            "https://api.siliconflow.example",
        )],
        vec![sample_bound_key(
            "key-siliconflow-embedding",
            "provider-siliconflow",
            "openai:embedding",
            "sk-siliconflow-embedding",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-siliconflow",
            "model": "Qwen/Qwen3-Embedding-4B",
            "api_format": "openai:embedding",
            "endpoint_id": "endpoint-siliconflow-embedding",
            "request_body": {
                "model": "Qwen/Qwen3-Embedding-4B",
                "input": "This is a test embedding input."
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["response_body"]["data"][0]["object"],
        json!("embedding")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_doubao_text_embedding_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_doubao_text_embedding_model_test",
        gateway_handles_admin_provider_query_doubao_text_embedding_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_doubao_text_embedding_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-doubao");
            assert_eq!(plan.endpoint_id, "endpoint-doubao-embedding");
            assert_eq!(plan.key_id, "key-doubao-embedding");
            assert_eq!(plan.client_api_format, "openai:embedding");
            assert_eq!(plan.provider_api_format, "doubao:embedding");
            assert_eq!(plan.url, "https://ark.volces.example/api/v3/embeddings");
            assert_eq!(
                plan.model_name.as_deref(),
                Some("doubao-embedding-text-240515")
            );
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-doubao-embedding")
            );
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(body["model"], json!("doubao-embedding-text-240515"));
            assert_eq!(body["input"], json!(["This is a test embedding input."]));
            assert!(
                body.get("stream").is_none(),
                "doubao embedding provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "object": "list",
                        "model": "doubao-embedding-text-240515",
                        "data": [{
                            "object": "embedding",
                            "index": 0,
                            "embedding": [0.1, 0.2, 0.3]
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 28
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-doubao", "Doubao", 10);
    provider.provider_type = "doubao".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-doubao-embedding",
            "provider-doubao",
            "doubao:embedding",
            "https://ark.volces.example/api/v3",
        )],
        vec![sample_bound_key(
            "key-doubao-embedding",
            "provider-doubao",
            "doubao:embedding",
            "sk-doubao-embedding",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-doubao",
            "model": "doubao-embedding-text-240515",
            "api_format": "doubao:embedding",
            "endpoint_id": "endpoint-doubao-embedding",
            "request_body": {
                "model": "doubao-embedding-text-240515",
                "input": "This is a test embedding input."
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["request_body"]["input"],
        json!(["This is a test embedding input."])
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_gemini_embedding_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_gemini_embedding_model_test",
        gateway_handles_admin_provider_query_gemini_embedding_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_gemini_embedding_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-gemini");
            assert_eq!(plan.endpoint_id, "endpoint-gemini-embedding");
            assert_eq!(plan.key_id, "key-gemini-embedding");
            assert_eq!(plan.client_api_format, "openai:embedding");
            assert_eq!(plan.provider_api_format, "gemini:embedding");
            assert_eq!(
                plan.url,
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent"
            );
            assert_eq!(plan.model_name.as_deref(), Some("gemini-embedding-001"));
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("x-goog-api-key").map(String::as_str),
                Some("sk-gemini-embedding")
            );
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(body["model"], json!("gemini-embedding-001"));
            assert_eq!(
                body["content"]["parts"][0]["text"],
                json!("This is a test embedding input.")
            );
            assert!(body.get("requests").is_none());
            assert!(
                body.get("stream").is_none(),
                "gemini embedding provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "model": "gemini-embedding-001",
                        "embedding": {
                            "values": [0.1, 0.2, 0.3]
                        }
                    }
                },
                "telemetry": {
                    "elapsed_ms": 27
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-gemini", "Gemini", 10);
    provider.provider_type = "gemini".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-gemini-embedding",
            "provider-gemini",
            "gemini:embedding",
            "https://generativelanguage.googleapis.com/v1beta",
        )],
        vec![sample_bound_key(
            "key-gemini-embedding",
            "provider-gemini",
            "gemini:embedding",
            "sk-gemini-embedding",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-gemini",
            "model": "gemini-embedding-001",
            "api_format": "gemini:embedding",
            "endpoint_id": "endpoint-gemini-embedding",
            "request_body": {
                "model": "gemini-embedding-001",
                "input": "This is a test embedding input."
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["response_body"]["embedding"]["values"][0],
        json!(0.1)
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_vertex_gemini_embedding_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_vertex_gemini_embedding_model_test",
        gateway_handles_admin_provider_query_vertex_gemini_embedding_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_vertex_gemini_embedding_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-vertex-ai");
            assert_eq!(plan.endpoint_id, "endpoint-vertex-gemini-embedding");
            assert_eq!(plan.key_id, "key-vertex-gemini-embedding");
            assert_eq!(plan.client_api_format, "openai:embedding");
            assert_eq!(plan.provider_api_format, "gemini:embedding");
            assert_eq!(
                plan.url,
                "https://aiplatform.googleapis.com/v1/publishers/google/models/gemini-embedding-2:predict?key=sk-vertex-gemini-embedding"
            );
            assert_eq!(plan.model_name.as_deref(), Some("gemini-embedding-2"));
            assert!(!plan.stream);
            let body = plan.body.json_body.as_ref().expect("json body");
            assert!(
                body.get("model").is_none(),
                "Vertex predict carries the model in the URL path; the test body must not repeat it"
            );
            assert_eq!(
                body["instances"][0]["content"],
                json!("This is a test embedding input.")
            );
            assert!(body.get("content").is_none());
            assert!(body.get("requests").is_none());
            assert!(
                body.get("stream").is_none(),
                "gemini embedding provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "predictions": [
                            {
                                "embeddings": {
                                    "values": [0.1, 0.2, 0.3]
                                }
                            }
                        ],
                        "deployedModelId": "gemini-embedding-2"
                    }
                },
                "telemetry": {
                    "elapsed_ms": 27
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-vertex-ai", "Vertex AI", 10);
    provider.provider_type = "vertex_ai".to_string();
    let mut key = sample_bound_key(
        "key-vertex-gemini-embedding",
        "provider-vertex-ai",
        "gemini:embedding",
        "sk-vertex-gemini-embedding",
    );
    key.allowed_models = Some(json!(["gemini-embedding-2"]));
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-vertex-gemini-embedding",
            "provider-vertex-ai",
            "gemini:embedding",
            "https://aiplatform.googleapis.com",
        )],
        vec![key],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-vertex-ai",
            "model": "gemini-embedding-2",
            "api_format": "gemini:embedding",
            "endpoint_id": "endpoint-vertex-gemini-embedding",
            "request_body": {
                "model": "gemini-embedding-2",
                "input": "This is a test embedding input."
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["request_body"]["instances"][0]["content"],
        json!("This is a test embedding input.")
    );
    assert_eq!(
        payload["attempts"][0]["endpoint_product"],
        json!("Vertex AI")
    );
    assert_eq!(
        payload["attempts"][0]["endpoint_variant"],
        json!("vertex_native")
    );
    assert_eq!(payload["attempts"][0]["endpoint_action"], json!("predict"));
    assert_eq!(
        payload["attempts"][0]["endpoint_batch_strategy"],
        json!("single_instance")
    );
    assert!(
        payload["attempts"][0]["request_body"]
            .get("model")
            .is_none(),
        "attempt debug payload must expose the exact Vertex body without a duplicate model"
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_jina_embedding_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_jina_embedding_model_test",
        gateway_handles_admin_provider_query_jina_embedding_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_jina_embedding_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-jina-embedding");
            assert_eq!(plan.endpoint_id, "endpoint-jina-embedding");
            assert_eq!(plan.key_id, "key-jina-embedding");
            assert_eq!(plan.client_api_format, "openai:embedding");
            assert_eq!(plan.provider_api_format, "jina:embedding");
            assert_eq!(plan.url, "https://api.jina.example/embeddings");
            assert_eq!(plan.model_name.as_deref(), Some("jina-embeddings-v3"));
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-jina-embedding")
            );
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(body["model"], json!("jina-embeddings-v3"));
            assert_eq!(body["task"], json!("text-matching"));
            assert_eq!(body["input"], json!("This is a test embedding input."));
            assert!(
                body.get("stream").is_none(),
                "jina embedding provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "model": "jina-embeddings-v3",
                        "data": [{
                            "object": "embedding",
                            "index": 0,
                            "embedding": [0.1, 0.2, 0.3]
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 29
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-jina-embedding", "Jina", 10);
    provider.provider_type = "jina".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-jina-embedding",
            "provider-jina-embedding",
            "jina:embedding",
            "https://api.jina.example",
        )],
        vec![sample_bound_key(
            "key-jina-embedding",
            "provider-jina-embedding",
            "jina:embedding",
            "sk-jina-embedding",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-jina-embedding",
            "model": "jina-embeddings-v3",
            "api_format": "jina:embedding",
            "endpoint_id": "endpoint-jina-embedding",
            "request_body": {
                "model": "jina-embeddings-v3",
                "input": "This is a test embedding input."
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["request_body"]["task"],
        json!("text-matching")
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_openai_rerank_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_openai_rerank_model_test",
        gateway_handles_admin_provider_query_openai_rerank_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_openai_rerank_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-openai-rerank");
            assert_eq!(plan.endpoint_id, "endpoint-openai-rerank");
            assert_eq!(plan.key_id, "key-openai-rerank");
            assert_eq!(plan.client_api_format, "openai:rerank");
            assert_eq!(plan.provider_api_format, "openai:rerank");
            assert_eq!(plan.url, "https://api.openai.example/v1/rerank");
            assert_eq!(plan.model_name.as_deref(), Some("bge-reranker-base"));
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-openai-rerank")
            );
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(body["model"], json!("bge-reranker-base"));
            assert_eq!(body["query"], json!("Apple"));
            assert_eq!(
                body["documents"],
                json!(["apple", "banana", "fruit", "vegetable"])
            );
            assert_eq!(body["return_documents"], json!(true));
            assert_eq!(body["top_n"], json!(4));
            assert!(
                body.get("stream").is_none(),
                "openai rerank provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "model": "bge-reranker-base",
                        "results": [{
                            "index": 0,
                            "relevance_score": 0.91
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 32
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-openai-rerank", "OpenAI Rerank", 10);
    provider.provider_type = "openai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-openai-rerank",
            "provider-openai-rerank",
            "openai:rerank",
            "https://api.openai.example/v1",
        )],
        vec![sample_bound_key(
            "key-openai-rerank",
            "provider-openai-rerank",
            "openai:rerank",
            "sk-openai-rerank",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-openai-rerank",
            "model": "bge-reranker-base",
            "api_format": "openai:rerank",
            "endpoint_id": "endpoint-openai-rerank",
            "request_body": {
                "model": "bge-reranker-base",
                "query": "Apple",
                "documents": ["apple", "banana", "fruit", "vegetable"],
                "return_documents": true,
                "top_n": 4
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["response_body"]["results"][0]["relevance_score"],
        json!(0.91)
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_handles_admin_provider_query_rerank_model_test() {
    run_provider_query_test(
        "gateway_handles_admin_provider_query_rerank_model_test",
        gateway_handles_admin_provider_query_rerank_model_test_impl,
    );
}

async fn gateway_handles_admin_provider_query_rerank_model_test_impl() {
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| async move {
            assert_eq!(plan.provider_id, "provider-jina");
            assert_eq!(plan.endpoint_id, "endpoint-jina-rerank");
            assert_eq!(plan.key_id, "key-jina-rerank");
            assert_eq!(plan.client_api_format, "openai:rerank");
            assert_eq!(plan.provider_api_format, "jina:rerank");
            assert_eq!(plan.url, "https://api.jina.example/rerank");
            assert_eq!(
                plan.model_name.as_deref(),
                Some("jina-reranker-v2-base-multilingual")
            );
            assert!(!plan.stream);
            assert_eq!(
                plan.headers.get("authorization").map(String::as_str),
                Some("Bearer sk-jina-rerank")
            );
            let body = plan.body.json_body.as_ref().expect("json body");
            assert_eq!(body["model"], json!("jina-reranker-v2-base-multilingual"));
            assert_eq!(body["query"], json!("Apple"));
            assert_eq!(
                body["documents"],
                json!(["apple", "banana", "fruit", "vegetable"])
            );
            assert_eq!(body["return_documents"], json!(true));
            assert_eq!(body["top_n"], json!(4));
            assert!(
                body.get("stream").is_none(),
                "rerank provider body must not carry stream"
            );
            Json(json!({
                "request_id": plan.request_id,
                "candidate_id": plan.candidate_id,
                "status_code": 200,
                "headers": {
                    "content-type": "application/json"
                },
                "body": {
                    "json_body": {
                        "model": "jina-reranker-v2-base-multilingual",
                        "results": [{
                            "index": 0,
                            "relevance_score": 0.93
                        }]
                    }
                },
                "telemetry": {
                    "elapsed_ms": 31
                }
            }))
        }),
    );

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let mut provider = sample_provider("provider-jina", "Jina", 10);
    provider.provider_type = "jina".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![sample_endpoint(
            "endpoint-jina-rerank",
            "provider-jina",
            "jina:rerank",
            "https://api.jina.example",
        )],
        vec![sample_bound_key(
            "key-jina-rerank",
            "provider-jina",
            "jina:rerank",
            "sk-jina-rerank",
        )],
    ));

    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(GatewayDataState::with_provider_transport_reader_for_tests(
                provider_catalog_repository,
                DEV_FERNET_KEY.to_string(),
            )),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/admin/provider-query/test-model"))
        .header(GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "provider_id": "provider-jina",
            "model": "jina-reranker-v2-base-multilingual",
            "api_format": "jina:rerank",
            "endpoint_id": "endpoint-jina-rerank",
            "request_body": {
                "model": "jina-reranker-v2-base-multilingual",
                "query": "Apple",
                "documents": ["apple", "banana", "fruit", "vegetable"],
                "return_documents": true,
                "top_n": 4
            }
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["success"], json!(true));
    assert_eq!(payload["error"], serde_json::Value::Null);
    assert_eq!(payload["attempts"][0]["status"], json!("success"));
    assert_eq!(
        payload["attempts"][0]["response_body"]["results"][0]["relevance_score"],
        json!(0.93)
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}
