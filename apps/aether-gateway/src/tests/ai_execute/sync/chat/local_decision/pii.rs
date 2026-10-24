use super::*;
use crate::tests::DEV_FERNET_KEY;

large_stack_async_test!(
    proxy_pii_redaction_local_openai_chat_runtime_masks_headers_and_restores_sync_response,
    proxy_pii_redaction_local_openai_chat_runtime_masks_headers_and_restores_sync_response_impl
);

async fn proxy_pii_redaction_local_openai_chat_runtime_masks_headers_and_restores_sync_response_impl(
) {
    #[derive(Debug, Clone)]
    struct SeenProviderRequest {
        body: serde_json::Value,
        authorization: String,
        accept_encoding: String,
    }

    fn auth_snapshot() -> StoredAuthApiKeySnapshot {
        StoredAuthApiKeySnapshot::new(
            "user-redaction-1".to_string(),
            "alice".to_string(),
            Some("alice@example.com".to_string()),
            "user".to_string(),
            "local".to_string(),
            true,
            false,
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
            "api-key-redaction-1".to_string(),
            Some("default".to_string()),
            true,
            false,
            false,
            Some(60),
            Some(5),
            Some(4_102_444_800),
            Some(serde_json::json!(["openai"])),
            Some(serde_json::json!(["openai:chat"])),
            Some(serde_json::json!(["gpt-5"])),
        )
        .expect("auth snapshot should build")
    }

    fn auth_export_record(
        snapshot: &StoredAuthApiKeySnapshot,
        key_hash: String,
        feature_settings: Option<serde_json::Value>,
    ) -> aether_data::repository::auth::StoredAuthApiKeyExportRecord {
        aether_data::repository::auth::StoredAuthApiKeyExportRecord::new(
            snapshot.user_id.clone(),
            snapshot.api_key_id.clone(),
            key_hash,
            None,
            snapshot.api_key_name.clone(),
            snapshot
                .api_key_allowed_providers
                .as_ref()
                .map(|value| serde_json::json!(value)),
            snapshot
                .api_key_allowed_api_formats
                .as_ref()
                .map(|value| serde_json::json!(value)),
            snapshot
                .api_key_allowed_models
                .as_ref()
                .map(|value| serde_json::json!(value)),
            snapshot.api_key_rate_limit,
            snapshot.api_key_concurrent_limit,
            None,
            snapshot.api_key_is_active,
            snapshot
                .api_key_expires_at_unix_secs
                .map(|value| value as i64),
            false,
            0,
            0,
            0.0,
            snapshot.api_key_is_standalone,
        )
        .expect("auth api key export record should build")
        .with_feature_settings(feature_settings)
    }

    fn auth_repository_with_redaction_feature_settings() -> Arc<InMemoryAuthApiKeySnapshotRepository>
    {
        let snapshot = auth_snapshot();
        let key_hash = hash_api_key("sk-client-redaction");
        Arc::new(
            InMemoryAuthApiKeySnapshotRepository::seed(vec![(
                Some(key_hash.clone()),
                snapshot.clone(),
            )])
            .with_export_records(vec![auth_export_record(
                &snapshot,
                key_hash,
                Some(json!({
                    "chat_pii_redaction": {
                        "enabled": true,
                    }
                })),
            )]),
        )
    }

    fn candidate_row() -> StoredMinimalCandidateSelectionRow {
        StoredMinimalCandidateSelectionRow {
            provider_id: "provider-redaction-1".to_string(),
            provider_name: "openai".to_string(),
            provider_type: "custom".to_string(),
            provider_priority: 10,
            provider_is_active: true,
            endpoint_id: "endpoint-redaction-1".to_string(),
            endpoint_api_format: "openai:chat".to_string(),
            endpoint_api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_is_active: true,
            key_id: "key-redaction-1".to_string(),
            key_name: "prod".to_string(),
            key_auth_type: "api_key".to_string(),
            key_is_active: true,
            key_api_formats: Some(vec!["openai:chat".to_string()]),
            key_allowed_models: None,
            key_capabilities: None,
            key_internal_priority: 5,
            key_global_priority_by_format: Some(serde_json::json!({"openai:chat": 1})),
            model_id: "model-redaction-1".to_string(),
            global_model_id: "global-model-redaction-1".to_string(),
            global_model_name: "gpt-5".to_string(),
            global_model_mappings: None,
            global_model_supports_streaming: Some(true),
            model_provider_model_name: "gpt-5-upstream".to_string(),
            model_provider_model_mappings: Some(vec![StoredProviderModelMapping {
                name: "gpt-5-upstream".to_string(),
                priority: 1,
                api_formats: Some(vec!["openai:chat".to_string()]),
                endpoint_ids: Some(vec!["endpoint-redaction-1".to_string()]),
                operations: None,
            }]),
            model_supports_streaming: Some(true),
            model_is_active: true,
            model_is_available: true,
        }
    }

    fn provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-redaction-1".to_string(),
            "openai".to_string(),
            Some("https://example.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
        .with_transport_fields(
            true,
            false,
            true,
            None,
            Some(2),
            None,
            Some(20.0),
            None,
            Some(serde_json::json!({"chat_pii_redaction": {"enabled": true}})),
        )
    }

    fn endpoint(base_url: String) -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-redaction-1".to_string(),
            "provider-redaction-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
        .with_transport_fields(base_url, None, None, Some(2), None, None, None, None)
        .expect("endpoint transport should build")
    }

    fn key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "key-redaction-1".to_string(),
            "provider-redaction-1".to_string(),
            "prod".to_string(),
            "api_key".to_string(),
            None,
            true,
        )
        .expect("key should build")
        .with_transport_fields(
            Some(serde_json::json!(["openai:chat"])),
            encrypt_python_fernet_plaintext(DEV_FERNET_KEY, "sk-upstream-redaction")
                .expect("api key should encrypt"),
            None,
            None,
            Some(serde_json::json!({"openai:chat": 1})),
            None,
            None,
            None,
            None,
        )
        .expect("key transport should build")
    }

    fn sentinel_from_body(body: &serde_json::Value) -> String {
        let messages = body["messages"].as_array().expect("messages should exist");
        let user_content = messages
            .iter()
            .find(|message| message["role"] == "user")
            .and_then(|message| message["content"].as_str())
            .expect("user content should be text");
        let start = user_content
            .find("<AETHER:EMAIL:")
            .expect("redacted content should include email sentinel");
        let end = user_content[start..]
            .find('>')
            .map(|index| start + index + 1)
            .expect("sentinel should close");
        user_content[start..end].to_string()
    }

    let seen_provider_request = Arc::new(Mutex::new(None::<SeenProviderRequest>));
    let seen_provider_request_clone = Arc::clone(&seen_provider_request);
    let provider_app = Router::new().route(
        "/chat/completions",
        any(move |request: Request| {
            let seen_provider_request_inner = Arc::clone(&seen_provider_request_clone);
            async move {
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                let payload: serde_json::Value =
                    serde_json::from_slice(&raw_body).expect("provider payload should parse");
                let sentinel = sentinel_from_body(&payload);
                *seen_provider_request_inner
                    .lock()
                    .expect("mutex should lock") = Some(SeenProviderRequest {
                    body: payload,
                    authorization: crate::tests::support::header_string(
                        &parts.headers,
                        http::header::AUTHORIZATION,
                    ),
                    accept_encoding: crate::tests::support::header_string(
                        &parts.headers,
                        http::header::ACCEPT_ENCODING,
                    ),
                });
                Json(json!({
                    "id": "chatcmpl-redaction-1",
                    "object": "chat.completion",
                    "model": "gpt-5-upstream",
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": format!("restored {sentinel}")},
                        "finish_reason": "stop"
                    }],
                    "usage": {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 5}
                }))
            }
        }),
    );
    let (provider_url, provider_handle) = start_server(provider_app).await;
    let auth_repository = auth_repository_with_redaction_feature_settings();
    let candidate_selection_repository =
        Arc::new(InMemoryMinimalCandidateSelectionReadRepository::seed(vec![
            candidate_row(),
        ]));
    let request_candidate_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider()],
        vec![endpoint(provider_url)],
        vec![key()],
    ));
    let data_state = crate::data::GatewayDataState::with_auth_candidate_selection_provider_catalog_and_request_candidate_repository_for_tests(
        auth_repository,
        candidate_selection_repository,
        provider_catalog_repository,
        Arc::clone(&request_candidate_repository),
        DEV_FERNET_KEY,
    )
    .with_system_config_values_for_tests(vec![
        ("module.chat_pii_redaction.enabled".to_string(), json!(true)),
        (
            "module.chat_pii_redaction.rules".to_string(),
            json!([{
                "id": "email",
                "name": "邮箱",
                "pattern": r"(?i)[A-Z0-9._%+-]{1,64}@[A-Z0-9.-]{1,253}\.[A-Z]{2,63}",
                "enabled": true,
                "features": {"validator": "email"},
                "system": true
            }]),
        ),
        (
            "module.chat_pii_redaction.cache_ttl_seconds".to_string(),
            json!(300),
        ),
    ]);
    let gateway_state = AppState::new()
        .expect("gateway state should build")
        .with_data_state_for_tests(data_state);
    let gateway = build_router_with_state(gateway_state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions"))
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::AUTHORIZATION, "Bearer sk-client-redaction")
        .header(http::header::ACCEPT_ENCODING, "gzip")
        .header(TRACE_ID_HEADER, "trace-proxy-pii-redaction-sync")
        .body(
            r#"{"model":"gpt-5","messages":[{"role":"user","content":"Email alice@example.com"}]}"#,
        )
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let execution_path = response
        .headers()
        .get(EXECUTION_PATH_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let response_text = response.text().await.expect("body should read");
    assert_eq!(status, StatusCode::OK, "{response_text}");
    assert_eq!(
        execution_path.as_deref(),
        Some(EXECUTION_PATH_EXECUTION_RUNTIME_SYNC)
    );
    let response_json: serde_json::Value =
        serde_json::from_str(&response_text).expect("body should parse");
    assert_eq!(
        response_json["choices"][0]["message"]["content"],
        "restored alice@example.com"
    );

    let seen = seen_provider_request
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("provider request should be captured");
    assert_eq!(seen.authorization, "Bearer sk-upstream-redaction");
    assert_eq!(seen.accept_encoding, "identity");
    let provider_body_text = serde_json::to_string(&seen.body).expect("body should serialize");
    assert!(!provider_body_text.contains("alice@example.com"));
    assert!(provider_body_text.contains("<AETHER:EMAIL:"));
    assert_eq!(seen.body["messages"][0]["role"], "user");

    let stored_candidates = request_candidate_repository
        .list_by_request_id("trace-proxy-pii-redaction-sync")
        .await
        .expect("request candidate trace should read");
    assert_eq!(stored_candidates.len(), 1);
    assert_eq!(stored_candidates[0].status, RequestCandidateStatus::Success);

    gateway_handle.abort();
    provider_handle.abort();
}
