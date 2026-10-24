use super::*;
use crate::tests::DEV_FERNET_KEY;

#[test]
fn gateway_handles_admin_provider_oauth_batch_import_task_status_locally_with_trusted_admin_principal(
) {
    run_admin_oauth_test(
        "gateway_handles_admin_provider_oauth_batch_import_task_status_locally_with_trusted_admin_principal",
        gateway_handles_admin_provider_oauth_batch_import_task_status_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_handles_admin_provider_oauth_batch_import_task_status_locally_with_trusted_admin_principal_impl(
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/task-123",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_provider_oauth_batch_task_entry_for_tests(
                "task-123",
                json!({
                    "task_id": "task-123",
                    "provider_id": "provider-codex",
                    "provider_type": "codex",
                    "status": "completed",
                    "total": 2,
                    "processed": 2,
                    "success": 1,
                    "failed": 1,
                    "created_count": 0,
                    "replaced_count": 1,
                    "progress_percent": 100,
                    "message": "导入完成：成功 1，失败 1",
                    "error": null,
                    "error_samples": [{
                        "index": 1,
                        "status": "error",
                        "error": "Token 验证失败: invalid_grant",
                        "replaced": false
                    }],
                    "created_at": 1700000001u64,
                    "started_at": 1700000002u64,
                    "finished_at": 1700000003u64,
                    "updated_at": 1700000004u64,
                }),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/task-123"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["task_id"], "task-123");
    assert_eq!(payload["provider_id"], "provider-codex");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["status"], "completed");
    assert_eq!(payload["total"], 2);
    assert_eq!(payload["processed"], 2);
    assert_eq!(payload["success"], 1, "payload={payload}");
    assert_eq!(payload["failed"], 1);
    assert_eq!(payload["created_count"], 0);
    assert_eq!(payload["replaced_count"], 1);
    assert_eq!(payload["progress_percent"], 100);
    assert_eq!(payload["error_samples"].as_array().map(Vec::len), Some(1));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_batch_imports_admin_provider_oauth_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_batch_imports_admin_provider_oauth_locally_with_trusted_admin_principal",
        gateway_batch_imports_admin_provider_oauth_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_batch_imports_admin_provider_oauth_locally_with_trusted_admin_principal_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let token_hits = Arc::new(Mutex::new(Vec::<String>::new()));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                let raw_body = String::from_utf8(
                    to_bytes(request.into_body(), usize::MAX)
                        .await
                        .expect("body should read")
                        .to_vec(),
                )
                .expect("body should be utf8");
                token_hits_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(raw_body.clone());
                if raw_body.contains("refresh_token=batch-refresh-success") {
                    Json(json!({
                        "access_token": "batch-imported-codex-access-token",
                        "refresh_token": "batch-imported-codex-refresh-token",
                        "token_type": "Bearer",
                        "expires_in": 1800,
                        "scope": "openid email profile offline_access",
                        "email": "batch-alice@example.com",
                        "account_id": "acct-batch-123",
                        "account_user_id": "acct-user-batch-123",
                        "plan_type": "plus",
                        "user_id": "user-batch-123",
                    }))
                    .into_response()
                } else {
                    (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": "invalid_grant",
                            "error_description": "refresh token invalid",
                        })),
                    )
                        .into_response()
                }
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut existing_key = sample_key(
        "key-codex-batch-duplicate",
        "provider-codex",
        "openai:chat",
        "stale-batch-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = false;
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","email":"batch-alice@example.com","account_id":"acct-batch-123","account_user_id":"acct-user-batch-123","plan_type":"plus","refresh_token":"old-refresh-token"}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "batch-refresh-success\nbatch-refresh-error\n",
            "proxy_node_id": "proxy-node-batch-import"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 2);
    assert_eq!(payload["success"], 1, "payload={payload}");
    assert_eq!(payload["failed"], 1);
    let results = payload["results"]
        .as_array()
        .expect("results should be array");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["status"], "success");
    assert_eq!(results[0]["key_id"], "key-codex-batch-duplicate");
    assert_eq!(results[0]["replaced"], true);
    assert_eq!(results[1]["status"], "error");
    assert!(
        results[1]["error"]
            .as_str()
            .expect("error should be string")
            .contains("Token 验证失败"),
        "payload={payload}"
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(token_hits.lock().expect("mutex should lock").len(), 2);

    let reloaded = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-batch-duplicate".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert!(persisted.is_active);
    assert_eq!(
        persisted.proxy,
        Some(json!({"node_id": "proxy-node-batch-import", "enabled": true}))
    );
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, "batch-imported-codex-access-token");

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_batch_imports_chatgpt_web_access_tokens_with_pool_hints() {
    run_admin_oauth_test(
        "gateway_batch_imports_chatgpt_web_access_tokens_with_pool_hints",
        gateway_batch_imports_chatgpt_web_access_tokens_with_pool_hints_impl,
    );
}

async fn gateway_batch_imports_chatgpt_web_access_tokens_with_pool_hints_impl() {
    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "unexpected refresh exchange"})),
                )
            }
        }),
    );

    let mut provider = sample_provider("provider-chatgpt-web", "chatgpt_web", 10);
    provider.provider_type = "chatgpt_web".to_string();
    provider.config = Some(json!({"pool_advanced": {}}));
    let endpoint = sample_endpoint(
        "endpoint-chatgpt-web-image",
        "provider-chatgpt-web",
        "openai:image",
        "https://chatgpt.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let pool_score_repository = Arc::new(InMemoryPoolMemberScoreRepository::default());

    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_pool_score_repository_for_tests(Arc::clone(&pool_score_repository))
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests(
                "chatgpt_web",
                format!("{token_url}/oauth/token"),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let credentials = json!([
        {
            "accessToken": "chatgpt-web-batch-access-token",
            "expiresAt": 2_100_000_000u64,
            "email": "pool-image@example.com",
            "accountId": "acct-pool-image",
            "accountUserId": "user-pool-image__acct-pool-image",
            "planType": "plus",
            "userId": "user-pool-image"
        }
    ])
    .to_string();

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-chatgpt-web/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": credentials,
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["total"], 1);
    assert_eq!(payload["success"], 1, "payload={payload}");
    assert_eq!(payload["failed"], 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 0);

    let reloaded = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-chatgpt-web".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert_eq!(persisted.expires_at_unix_secs, Some(2_100_000_000));
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, "chatgpt-web-batch-access-token");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["provider_type"], "chatgpt_web");
    assert_eq!(auth_config["access_token_import_temporary"], true);
    assert_eq!(auth_config["email"], "pool-image@example.com");
    assert_eq!(auth_config["account_id"], "acct-pool-image");
    assert_eq!(
        auth_config["account_user_id"],
        "user-pool-image__acct-pool-image"
    );
    assert_eq!(auth_config["plan_type"], "plus");
    assert_eq!(auth_config["user_id"], "user-pool-image");

    let score_scope = provider_key_pool_score_scope();
    let score_identity =
        PoolMemberIdentity::provider_api_key("provider-chatgpt-web", persisted.id.clone());
    let scores = pool_score_repository
        .get_pool_member_scores_by_ids(&GetPoolMemberScoresByIdsQuery {
            ids: vec![provider_key_pool_score_id(&score_identity, &score_scope)],
        })
        .await
        .expect("pool score should load");
    assert_eq!(scores.len(), 1);
    assert_eq!(scores[0].member_id, persisted.id);
    assert_eq!(scores[0].hard_state, PoolMemberHardState::Unknown);
    assert!(scores[0].score > 0.0);

    gateway_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_starts_admin_provider_oauth_batch_import_task_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_starts_admin_provider_oauth_batch_import_task_locally_with_trusted_admin_principal",
        gateway_starts_admin_provider_oauth_batch_import_task_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_starts_admin_provider_oauth_batch_import_task_locally_with_trusted_admin_principal_impl(
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "access_token": "task-imported-codex-access-token",
                    "refresh_token": "task-imported-codex-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                    "email": "task-alice@example.com",
                    "account_id": "acct-task-123",
                    "account_user_id": "acct-user-task-123",
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let submit_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "task-batch-refresh-success"
        }))
        .send()
        .await
        .expect("submit request should succeed");

    assert_eq!(submit_response.status(), StatusCode::OK);
    let submit_payload: serde_json::Value = submit_response
        .json()
        .await
        .expect("submit payload should parse");
    assert_eq!(submit_payload["status"], "submitted");
    assert_eq!(submit_payload["total"], 1);
    let task_id = submit_payload["task_id"]
        .as_str()
        .expect("task id should exist")
        .to_string();

    let mut status_payload = serde_json::Value::Null;
    for _ in 0..40 {
        let response = client
            .get(format!(
                "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/{task_id}"
            ))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .send()
            .await
            .expect("status request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        status_payload = response.json().await.expect("status payload should parse");
        if status_payload["status"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(
        status_payload["status"], "completed",
        "payload={status_payload}"
    );
    assert_eq!(status_payload["total"], 1);
    assert_eq!(status_payload["processed"], 1);
    assert_eq!(status_payload["success"], 1);
    assert_eq!(status_payload["failed"], 0);
    assert_eq!(status_payload["created_count"], 1);
    assert_eq!(status_payload["replaced_count"], 0);
    assert_eq!(status_payload["progress_percent"], 100);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 1);

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_updates_admin_provider_oauth_batch_import_task_progress() {
    run_admin_oauth_test(
        "gateway_updates_admin_provider_oauth_batch_import_task_progress",
        gateway_updates_admin_provider_oauth_batch_import_task_progress_impl,
    );
}

async fn gateway_updates_admin_provider_oauth_batch_import_task_progress_impl() {
    let upstream = Router::new().fallback(any(|| async {
        (StatusCode::OK, Body::from("quota refresh body"))
    }));

    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |_request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            async move {
                let hit = {
                    let mut guard = token_hits_inner.lock().expect("mutex should lock");
                    *guard += 1;
                    *guard
                };
                if hit == 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Json(json!({
                    "access_token": format!("progress-codex-access-token-{hit}"),
                    "refresh_token": format!("progress-codex-refresh-token-{hit}"),
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                    "email": format!("progress-{hit}@example.com"),
                    "account_id": format!("acct-progress-{hit}"),
                    "account_user_id": format!("acct-user-progress-{hit}"),
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository,
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let submit_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "progress-refresh-one\nprogress-refresh-two"
        }))
        .send()
        .await
        .expect("submit request should succeed");

    assert_eq!(submit_response.status(), StatusCode::OK);
    let submit_payload: serde_json::Value = submit_response
        .json()
        .await
        .expect("submit payload should parse");
    let task_id = submit_payload["task_id"]
        .as_str()
        .expect("task id should exist")
        .to_string();

    let mut progress_payload = serde_json::Value::Null;
    for _ in 0..50 {
        let response = client
            .get(format!(
                "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/{task_id}"
            ))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .send()
            .await
            .expect("status request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        progress_payload = response.json().await.expect("status payload should parse");
        if progress_payload["status"] == "processing" && progress_payload["processed"] == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    assert_eq!(
        progress_payload["status"], "processing",
        "payload={progress_payload}"
    );
    assert_eq!(progress_payload["total"], 2);
    assert_eq!(progress_payload["processed"], 1);
    assert_eq!(progress_payload["success"], 1);
    assert_eq!(progress_payload["failed"], 0);
    assert_eq!(progress_payload["created_count"], 1);
    assert_eq!(progress_payload["progress_percent"], 50);

    let mut completed_payload = progress_payload;
    for _ in 0..50 {
        let response = client
            .get(format!(
                "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/{task_id}"
            ))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .send()
            .await
            .expect("status request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        completed_payload = response.json().await.expect("status payload should parse");
        if completed_payload["status"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    assert_eq!(
        completed_payload["status"], "completed",
        "payload={completed_payload}"
    );
    assert_eq!(completed_payload["processed"], 2);
    assert_eq!(completed_payload["success"], 2);
    assert_eq!(completed_payload["progress_percent"], 100);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 2);

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_completes_admin_provider_oauth_key_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_completes_admin_provider_oauth_key_locally_with_trusted_admin_principal",
        gateway_completes_admin_provider_oauth_key_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_completes_admin_provider_oauth_key_locally_with_trusted_admin_principal_impl() {
    #[derive(Debug, Clone)]
    struct SeenTokenRequest {
        content_type: String,
        body: String,
    }

    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let seen_token = Arc::new(Mutex::new(None::<SeenTokenRequest>));
    let seen_token_clone = Arc::clone(&seen_token);
    let namespace_race_repository = Arc::new(Mutex::new(
        None::<Arc<InMemoryProviderCatalogReadRepository>>,
    ));
    let namespace_race_repository_clone = Arc::clone(&namespace_race_repository);
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            let seen_token_inner = Arc::clone(&seen_token_clone);
            let namespace_race_repository_inner = Arc::clone(&namespace_race_repository_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                *seen_token_inner.lock().expect("mutex should lock") = Some(SeenTokenRequest {
                    content_type: parts
                        .headers
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    body: String::from_utf8(raw_body.to_vec())
                        .expect("token request body should be utf8"),
                });
                let repository = namespace_race_repository_inner
                    .lock()
                    .expect("mutex should lock")
                    .clone()
                    .expect("provider catalog repository should be installed");
                repository
                    .upsert_key_upstream_metadata_namespace(
                        "key-codex-oauth",
                        "codex",
                        &json!({
                            "primary_used_percent": 80.0,
                            "account_quota_request_id": "concurrent-refresh"
                        }),
                        Some(1_800_000_001),
                    )
                    .await
                    .expect("concurrent Codex namespace update should succeed");
                Json(json!({
                    "access_token": "new-codex-access-token",
                    "refresh_token": "new-codex-refresh-token",
                    "token_type": "Bearer",
                    "expiresAt": 4_102_444_800u64,
                    "email": "alice@example.com",
                    "account_id": "acct-codex-123",
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    provider.config = Some(json!({"pool_advanced": {}}));

    let mut key = sample_key(
        "key-codex-oauth",
        "provider-codex",
        "openai:chat",
        "__placeholder__",
    );
    key.auth_type = "oauth".to_string();
    key.oauth_invalid_at_unix_secs = Some(1_700_000_000);
    key.oauth_invalid_reason = Some("[ACCOUNT_BLOCK] token invalid".to_string());
    key.error_count = Some(7);
    key.health_by_format = Some(json!({
        "openai:chat": {"consecutive_failures": 3}
    }));
    key.circuit_breaker_by_format = Some(json!({
        "openai:chat": {"state": "open"}
    }));
    key.upstream_metadata = Some(json!({
        "codex": {
            "primary_used_percent": 100.0,
            "primary_reset_at": 4_200_000_000u64,
            "account_quota_reset_fence_unix_ms": 1_800_000_000_000u64,
            "account_quota_reset_fence_id": "old-account-fence",
            "account_quota_reset_processed_ids": ["old-account-reset"],
            "account_quota_reset_pending": true
        },
        "unrelated_runtime": {
            "preserved": true
        }
    }));
    key.status_snapshot = Some(json!({
        "oauth": {
            "code": "invalid"
        },
        "quota": {
            "provider_type": "codex",
            "usage_ratio": 1.0,
            "windows": [{
                "kind": "primary",
                "usage": 100.0,
                "reset_at": 4_200_000_000u64
            }]
        }
    }));

    let score_identity = PoolMemberIdentity::provider_api_key("provider-codex", "key-codex-oauth");
    let score_scope = provider_key_pool_score_scope();
    let invalid_score = build_provider_key_pool_score_upsert(
        &key,
        "codex",
        None,
        1_700_000_000,
        aether_pool_core::PoolMemberScoreRules::default(),
    )
    .into_stored();
    assert_eq!(invalid_score.hard_state, PoolMemberHardState::AuthInvalid);

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![key],
    ));
    *namespace_race_repository.lock().expect("mutex should lock") =
        Some(Arc::clone(&provider_catalog_repository));
    let pool_score_repository =
        Arc::new(InMemoryPoolMemberScoreRepository::seed(vec![invalid_score]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_pool_score_repository_for_tests(Arc::clone(&pool_score_repository))
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_state_entry_for_tests(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                json!({
                    "nonce": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "key_id": "key-codex-oauth",
                    "provider_id": "provider-codex",
                    "provider_type": "codex",
                    "pkce_verifier": "verifier-codex-123",
                    "initiated_by_user_id": "admin-user-123",
                    "initiated_by_session_id": "session-123",
                    "created_at": aether_admin::provider::state::current_unix_secs(),
                }),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let wrong_session_response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth/complete"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-attacker")
        .json(&json!({
            "callback_url": "http://localhost:1455/auth/callback?code=code-codex-123&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }))
        .send()
        .await
        .expect("mismatched-session request should succeed");
    assert_eq!(wrong_session_response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 0);

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/keys/key-codex-oauth/complete"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "callback_url": "http://localhost:1455/auth/callback?code=code-codex-123&state=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["expires_at"], 4_102_444_800u64);
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["account_state_recheck_attempted"], false);
    assert_eq!(
        payload["account_state_recheck_error"],
        serde_json::Value::Null
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let seen_token = seen_token
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("token request should be recorded");
    assert_eq!(seen_token.content_type, "application/x-www-form-urlencoded");
    assert!(seen_token.body.contains("grant_type=authorization_code"));
    assert!(seen_token
        .body
        .contains("client_id=app_EMoamEEZ73f0CkXaXp7hrann"));
    assert!(seen_token.body.contains("code=code-codex-123"));
    assert!(seen_token.body.contains("code_verifier=verifier-codex-123"));

    let reloaded = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-oauth".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert_eq!(persisted.expires_at_unix_secs, Some(4_102_444_800));
    assert!(persisted.is_active);
    assert_eq!(persisted.oauth_invalid_at_unix_secs, None);
    assert_eq!(persisted.oauth_invalid_reason, None);
    assert_eq!(persisted.error_count, Some(0));
    assert_eq!(persisted.health_by_format, Some(json!({})));
    assert_eq!(persisted.circuit_breaker_by_format, Some(json!({})));
    assert_eq!(
        persisted
            .upstream_metadata
            .as_ref()
            .and_then(Value::as_object)
            .and_then(|metadata| metadata.get("codex"))
            .and_then(Value::as_object)
            .map(|codex| codex.keys().cloned().collect::<Vec<_>>()),
        Some(vec!["credential_generation".to_string()]),
        "explicit Codex reauthorization must not carry quota/reset state across accounts"
    );
    assert!(persisted
        .upstream_metadata
        .as_ref()
        .and_then(|metadata| metadata.pointer("/codex/credential_generation"))
        .and_then(Value::as_str)
        .is_some_and(|generation| !generation.is_empty()));
    assert_eq!(
        persisted
            .upstream_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("unrelated_runtime")),
        Some(&json!({"preserved": true}))
    );
    assert_eq!(
        persisted
            .status_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("quota")),
        Some(&Value::Null)
    );
    let scores = pool_score_repository
        .get_pool_member_scores_by_ids(&GetPoolMemberScoresByIdsQuery {
            ids: vec![provider_key_pool_score_id(&score_identity, &score_scope)],
        })
        .await
        .expect("pool score should load");
    assert_eq!(scores.len(), 1);
    assert!(
        scores[0].hard_state.schedulable(),
        "OAuth completion should replace AuthInvalid with a schedulable score"
    );
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, "new-codex-access-token");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["provider_type"], "codex");
    assert_eq!(auth_config["refresh_token"], "new-codex-refresh-token");
    assert_eq!(auth_config["expires_at"], 4_102_444_800u64);
    assert_eq!(auth_config["email"], "alice@example.com");
    assert_eq!(auth_config["account_id"], "acct-codex-123");
    assert_eq!(auth_config["plan_type"], "plus");

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_completes_admin_provider_oauth_provider_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_completes_admin_provider_oauth_provider_locally_with_trusted_admin_principal",
        gateway_completes_admin_provider_oauth_provider_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_completes_admin_provider_oauth_provider_locally_with_trusted_admin_principal_impl()
{
    #[derive(Debug, Clone)]
    struct SeenTokenRequest {
        content_type: String,
        body: String,
    }

    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let token_hits = Arc::new(Mutex::new(0usize));
    let token_hits_clone = Arc::clone(&token_hits);
    let seen_token = Arc::new(Mutex::new(None::<SeenTokenRequest>));
    let seen_token_clone = Arc::clone(&seen_token);
    let token_server = Router::new().route(
        "/oauth/token",
        any(move |request: Request| {
            let token_hits_inner = Arc::clone(&token_hits_clone);
            let seen_token_inner = Arc::clone(&seen_token_clone);
            async move {
                *token_hits_inner.lock().expect("mutex should lock") += 1;
                let (parts, body) = request.into_parts();
                let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                *seen_token_inner.lock().expect("mutex should lock") = Some(SeenTokenRequest {
                    content_type: parts
                        .headers
                        .get(http::header::CONTENT_TYPE)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_string(),
                    body: String::from_utf8(raw_body.to_vec())
                        .expect("token request body should be utf8"),
                });
                Json(json!({
                    "access_token": "provider-codex-access-token",
                    "refresh_token": "provider-codex-refresh-token",
                    "token_type": "Bearer",
                    "expires_at": 4_102_444_800u64,
                    "email": "alice@example.com",
                    "account_id": "acct-codex-123",
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut existing_key = sample_key(
        "key-codex-inactive-duplicate",
        "provider-codex",
        "openai:chat",
        "stale-codex-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = false;
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","refresh_token":"old-refresh-token"}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (token_url, token_handle) = start_server(token_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_state_entry_for_tests(
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                json!({
                    "nonce": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "key_id": "",
                    "provider_id": "provider-codex",
                    "provider_type": "codex",
                    "pkce_verifier": "verifier-provider-codex-123",
                    "initiated_by_user_id": "admin-user-123",
                    "initiated_by_session_id": "session-123",
                    "created_at": aether_admin::provider::state::current_unix_secs(),
                }),
            )
            .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/complete"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "callback_url": "http://localhost:1455/auth/callback?code=provider-code-123&state=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "proxy_node_id": "proxy-node-codex-oauth",
            "name": "should-not-override-inactive-name"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["key_id"], "key-codex-inactive-duplicate");
    assert_eq!(payload["provider_type"], "codex");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["expires_at"], 4_102_444_800u64);
    assert_eq!(payload["email"], "alice@example.com");
    assert_eq!(payload["replaced"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*token_hits.lock().expect("mutex should lock"), 1);

    let seen_token = seen_token
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("token request should be recorded");
    assert_eq!(seen_token.content_type, "application/x-www-form-urlencoded");
    assert!(seen_token.body.contains("grant_type=authorization_code"));
    assert!(seen_token.body.contains("code=provider-code-123"));
    assert!(seen_token
        .body
        .contains("code_verifier=verifier-provider-codex-123"));

    let reloaded = provider_catalog_repository
        .list_keys_by_ids(&["key-codex-inactive-duplicate".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert!(persisted.is_active);
    assert_eq!(persisted.expires_at_unix_secs, Some(4_102_444_800));
    assert_eq!(
        persisted.proxy,
        Some(json!({"node_id": "proxy-node-codex-oauth", "enabled": true}))
    );
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, "provider-codex-access-token");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["provider_type"], "codex");
    assert_eq!(auth_config["refresh_token"], "provider-codex-refresh-token");
    assert_eq!(auth_config["expires_at"], 4_102_444_800u64);
    assert_eq!(auth_config["email"], "alice@example.com");
    assert_eq!(auth_config["account_id"], "acct-codex-123");
    assert_eq!(auth_config["plan_type"], "plus");

    gateway_handle.abort();
    token_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_names_new_antigravity_oauth_account_from_google_userinfo_email() {
    run_admin_oauth_test(
        "gateway_names_new_antigravity_oauth_account_from_google_userinfo_email",
        || {
            assert_antigravity_oauth_account_uses_google_userinfo_email(
                "complete",
                json!({
                    "callback_url": "http://localhost:51121/oauth2callback?code=antigravity-code-123&state=cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                }),
            )
        },
    );
}

#[test]
fn gateway_names_imported_antigravity_oauth_account_from_google_userinfo_email() {
    run_admin_oauth_test(
        "gateway_names_imported_antigravity_oauth_account_from_google_userinfo_email",
        || {
            assert_antigravity_oauth_account_uses_google_userinfo_email(
                "import-refresh-token",
                json!({"refresh_token": "antigravity-import-refresh-token"}),
            )
        },
    );
}

#[test]
fn gateway_names_batch_imported_antigravity_oauth_account_from_google_userinfo_email() {
    run_admin_oauth_test(
        "gateway_names_batch_imported_antigravity_oauth_account_from_google_userinfo_email",
        || {
            assert_antigravity_oauth_account_uses_google_userinfo_email(
                "batch-import",
                json!({"credentials": "antigravity-import-refresh-token"}),
            )
        },
    );
}

#[test]
fn gateway_imports_codex_header_authorization_without_overwriting_payload_access_token() {
    run_admin_oauth_test(
        "gateway_imports_codex_header_authorization_without_overwriting_payload_access_token",
        gateway_imports_codex_header_authorization_without_overwriting_payload_access_token_impl,
    );
}

async fn gateway_imports_codex_header_authorization_without_overwriting_payload_access_token_impl()
{
    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let access_token =
        sample_codex_access_token_with_profile_email("profile@example.com", "acct-profile-123");

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "access_token": access_token,
            "headers": {
                "authorization": "Bearer imported-session-token",
                "chatgpt-account-id": "acct-header"
            },
            "name": "temporary-codex-header-auth",
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");

    let reloaded = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    let decrypted_api_key = decrypt_persisted_provider_api_key(persisted);
    assert_eq!(decrypted_api_key, access_token);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["email"], "profile@example.com");
    assert_eq!(auth_config["access_token"], access_token);
    assert_eq!(
        auth_config["headers"]["authorization"],
        "Bearer imported-session-token"
    );
    assert_eq!(auth_config["headers"]["chatgpt-account-id"], "acct-header");

    gateway_handle.abort();
}

#[test]
fn gateway_imports_codex_access_token_with_payload_expires_at_when_token_has_no_exp() {
    run_admin_oauth_test(
        "gateway_imports_codex_access_token_with_payload_expires_at_when_token_has_no_exp",
        gateway_imports_codex_access_token_with_payload_expires_at_when_token_has_no_exp_impl,
    );
}

async fn gateway_imports_codex_access_token_with_payload_expires_at_when_token_has_no_exp_impl() {
    let access_token = sample_codex_access_token_with_profile_email_and_expiry(
        "opaque@example.com",
        "acct-opaque-123",
        None,
    );
    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-responses",
        "provider-codex",
        "openai:responses",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "access_token": access_token,
            "expiresAt": 2_100_000_000u64,
            "name": "temporary-codex-opaque-access-token",
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["expires_at"], 2_100_000_000u64);

    let reloaded = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    let persisted = reloaded.first().expect("persisted key should exist");
    assert_eq!(persisted.expires_at_unix_secs, Some(2_100_000_000));
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(persisted);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config json should parse");
    assert_eq!(auth_config["expires_at"], 2_100_000_000u64);
    assert_eq!(auth_config["access_token_import_temporary"], true);

    gateway_handle.abort();
}

#[test]
fn gateway_import_invalidate_cached_oauth_entry_before_followup_resolution() {
    run_admin_oauth_test(
        "gateway_import_invalidate_cached_oauth_entry_before_followup_resolution",
        gateway_import_invalidate_cached_oauth_entry_before_followup_resolution_impl,
    );
}

async fn gateway_import_invalidate_cached_oauth_entry_before_followup_resolution_impl() {
    let token_server = Router::new().route(
        "/oauth/token",
        post(move |body: Bytes| async move {
            let body_text = String::from_utf8(body.to_vec()).unwrap_or_default();
            if body_text.contains("refresh_token=old-refresh-token") {
                Json(json!({
                    "access_token": "cached-old-codex-access-token",
                    "refresh_token": "cached-old-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                    "email": "alice@example.com",
                    "account_id": "acct-codex-123",
                    "plan_type": "plus",
                }))
            } else {
                assert!(
                    body_text.contains("refresh_token=provider-import-refresh-token"),
                    "unexpected token request body: {body_text}"
                );
                Json(json!({
                    "access_token": "imported-fresh-codex-access-token",
                    "refresh_token": "imported-fresh-refresh-token",
                    "token_type": "Bearer",
                    "expires_in": 1800,
                    "scope": "openid email profile offline_access",
                    "email": "alice@example.com",
                    "account_id": "acct-codex-123",
                    "plan_type": "plus",
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-chat",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );

    let mut existing_key = sample_key(
        "key-codex-import-cache-duplicate",
        "provider-codex",
        "openai:chat",
        "stale-imported-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.expires_at_unix_secs = Some(1);
    existing_key.oauth_invalid_at_unix_secs = Some(1_700_000_000);
    existing_key.oauth_invalid_reason = Some("[OAUTH_EXPIRED] token invalidated".to_string());
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"codex","email":"alice@example.com","account_id":"acct-codex-123","plan_type":"plus","refresh_token":"old-refresh-token","expires_at":1}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (token_url, token_handle) = start_server(token_server).await;
    let oauth_refresh =
        crate::provider_transport::LocalOAuthRefreshCoordinator::with_adapters_for_tests(vec![
            Arc::new(
                crate::provider_transport::oauth_refresh::GenericOAuthRefreshAdapter::default()
                    .with_token_url_for_tests("codex", format!("{token_url}/oauth/token")),
            ),
        ]);
    let app_state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_provider_oauth_token_url_for_tests("codex", format!("{token_url}/oauth/token"))
        .with_oauth_refresh_coordinator_for_tests(oauth_refresh);

    let stale_transport = app_state
        .read_provider_transport_snapshot(
            "provider-codex",
            "endpoint-codex-chat",
            "key-codex-import-cache-duplicate",
        )
        .await
        .expect("transport should load")
        .expect("transport should exist");
    let cached_entry = app_state
        .force_local_oauth_refresh_entry(&stale_transport)
        .await
        .expect("initial refresh should succeed")
        .expect("initial refresh should return cached entry");
    assert_eq!(
        cached_entry.auth_header_value,
        "Bearer cached-old-codex-access-token"
    );
    provider_catalog_repository
        .update_key_oauth_runtime_state(
            "key-codex-import-cache-duplicate",
            Some(1_700_000_000),
            Some("[OAUTH_EXPIRED] token invalidated"),
            Some(1_700_000_000),
        )
        .await
        .expect("oauth invalid marker should be seeded through the runtime mutation");

    let gateway = build_router_with_state(app_state.clone());
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-codex/import-refresh-token"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "refresh_token": "provider-import-refresh-token",
            "name": "should-not-override-cache-duplicate-name"
        }))
        .send()
        .await
        .expect("request should succeed");

    let status = response.status();
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["key_id"], "key-codex-import-cache-duplicate");
    assert_eq!(payload["replaced"], true);

    let fresh_transport = app_state
        .read_provider_transport_snapshot(
            "provider-codex",
            "endpoint-codex-chat",
            "key-codex-import-cache-duplicate",
        )
        .await
        .expect("transport should load")
        .expect("transport should exist");
    let resolved = app_state
        .resolve_local_oauth_request_auth(&fresh_transport)
        .await
        .expect("oauth auth should resolve")
        .expect("oauth auth should exist");
    match resolved {
        crate::provider_transport::LocalResolvedOAuthRequestAuth::Header { value, .. } => {
            assert_eq!(value, "Bearer imported-fresh-codex-access-token");
        }
        crate::provider_transport::LocalResolvedOAuthRequestAuth::Kiro(_) => {
            panic!("codex should resolve to header auth")
        }
    }

    gateway_handle.abort();
    token_handle.abort();
}

#[test]
fn gateway_batch_imports_admin_provider_oauth_kiro_locally_with_trusted_admin_principal() {
    run_admin_oauth_test(
        "gateway_batch_imports_admin_provider_oauth_kiro_locally_with_trusted_admin_principal",
        gateway_batch_imports_admin_provider_oauth_kiro_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_batch_imports_admin_provider_oauth_kiro_locally_with_trusted_admin_principal_impl()
{
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let refresh_hits = Arc::new(Mutex::new(0usize));
    let refresh_hits_clone = Arc::clone(&refresh_hits);
    let refresh_server = Router::new().route(
        "/refreshToken",
        post(move |_request: Request| {
            let refresh_hits_inner = Arc::clone(&refresh_hits_clone);
            async move {
                *refresh_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "accessToken": sample_kiro_device_access_token("kiro-batch@example.com"),
                    "refreshToken": "kiro-batch-refresh-token-new",
                    "expiresIn": 1800,
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let endpoint = sample_endpoint(
        "endpoint-kiro-chat",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "https://service.kiro.dev",
    );

    let mut existing_key = sample_key(
        "key-kiro-batch-duplicate",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "stale-kiro-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = false;
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"kiro","auth_method":"social","email":"kiro-batch@example.com","refresh_token":"kiro-batch-refresh-old"}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (refresh_url, refresh_handle) = start_server(refresh_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests(
                "kiro_social_refresh",
                refresh_url.to_string(),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "kiro-batch-refresh-token",
            "proxy_node_id": "proxy-node-kiro-batch"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["total"], 1);
    assert_eq!(payload["success"], 1, "payload={payload}");
    assert_eq!(payload["failed"], 0);
    let results = payload["results"]
        .as_array()
        .expect("results should be array");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["status"], "success");
    assert_eq!(results[0]["key_id"], "key-kiro-batch-duplicate");
    assert_eq!(results[0]["auth_method"], "social");
    assert_eq!(results[0]["replaced"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*refresh_hits.lock().expect("mutex should lock"), 1);

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-kiro-batch-duplicate".to_string()])
        .await
        .expect("keys should load")
        .into_iter()
        .next()
        .expect("persisted key should exist");
    assert!(stored_key.is_active);
    assert_eq!(
        stored_key.proxy,
        Some(json!({"node_id": "proxy-node-kiro-batch", "enabled": true}))
    );
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "kiro");
    assert_eq!(auth_config["auth_method"], "social");
    assert_eq!(auth_config["email"], "kiro-batch@example.com");
    assert_eq!(auth_config["refresh_token"], "kiro-batch-refresh-token-new");

    gateway_handle.abort();
    refresh_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_batch_imports_admin_provider_oauth_kiro_over_active_expired_duplicate() {
    run_admin_oauth_test(
        "gateway_batch_imports_admin_provider_oauth_kiro_over_active_expired_duplicate",
        gateway_batch_imports_admin_provider_oauth_kiro_over_active_expired_duplicate_impl,
    );
}

async fn gateway_batch_imports_admin_provider_oauth_kiro_over_active_expired_duplicate_impl() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let refresh_hits = Arc::new(Mutex::new(0usize));
    let refresh_hits_clone = Arc::clone(&refresh_hits);
    let refresh_server = Router::new().route(
        "/refreshToken",
        post(move |_request: Request| {
            let refresh_hits_inner = Arc::clone(&refresh_hits_clone);
            async move {
                *refresh_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "accessToken": sample_kiro_device_access_token("kiro-batch@example.com"),
                    "refreshToken": "kiro-batch-refresh-token-replaced",
                    "expiresIn": 1800,
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let endpoint = sample_endpoint(
        "endpoint-kiro-chat",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "https://service.kiro.dev",
    );

    let mut existing_key = sample_key(
        "key-kiro-batch-expired-duplicate",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "stale-kiro-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = true;
    existing_key.oauth_invalid_at_unix_secs = Some(1_700_000_000);
    existing_key.oauth_invalid_reason = Some("Kiro Token 无效或已过期".to_string());
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"kiro","auth_method":"social","email":"kiro-batch@example.com","refresh_token":"kiro-batch-refresh-old"}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (refresh_url, refresh_handle) = start_server(refresh_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests(
                "kiro_social_refresh",
                refresh_url.to_string(),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "kiro-batch-refresh-token",
            "proxy_node_id": "proxy-node-kiro-batch"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["total"], 1);
    assert_eq!(payload["success"], 1, "payload={payload}");
    assert_eq!(payload["failed"], 0);
    let results = payload["results"]
        .as_array()
        .expect("results should be array");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["status"], "success");
    assert_eq!(results[0]["key_id"], "key-kiro-batch-expired-duplicate");
    assert_eq!(results[0]["auth_method"], "social");
    assert_eq!(results[0]["replaced"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*refresh_hits.lock().expect("mutex should lock"), 1);

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-kiro-batch-expired-duplicate".to_string()])
        .await
        .expect("keys should load")
        .into_iter()
        .next()
        .expect("persisted key should exist");
    assert!(stored_key.is_active);
    assert_eq!(
        stored_key.proxy,
        Some(json!({"node_id": "proxy-node-kiro-batch", "enabled": true}))
    );
    assert_eq!(stored_key.oauth_invalid_at_unix_secs, None);
    assert_eq!(stored_key.oauth_invalid_reason, None);
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["provider_type"], "kiro");
    assert_eq!(auth_config["auth_method"], "social");
    assert_eq!(auth_config["email"], "kiro-batch@example.com");
    assert_eq!(
        auth_config["refresh_token"],
        "kiro-batch-refresh-token-replaced"
    );

    gateway_handle.abort();
    refresh_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_batch_imports_admin_provider_oauth_kiro_via_execution_runtime_proxy_node() {
    run_admin_oauth_test(
        "gateway_batch_imports_admin_provider_oauth_kiro_via_execution_runtime_proxy_node",
        gateway_batch_imports_admin_provider_oauth_kiro_via_execution_runtime_proxy_node_impl,
    );
}

async fn gateway_batch_imports_admin_provider_oauth_kiro_via_execution_runtime_proxy_node_impl() {
    let execution_plans = Arc::new(Mutex::new(Vec::<ExecutionPlan>::new()));
    let execution_plans_clone = Arc::clone(&execution_plans);
    let execution_runtime = Router::new().route(
        "/v1/execute/sync",
        any(move |Json(plan): Json<ExecutionPlan>| {
            let execution_plans_inner = Arc::clone(&execution_plans_clone);
            async move {
                execution_plans_inner
                    .lock()
                    .expect("mutex should lock")
                    .push(plan.clone());
                assert_eq!(
                    plan.proxy
                        .as_ref()
                        .and_then(|proxy| proxy.node_id.as_deref()),
                    Some("proxy-node-kiro-batch-runtime")
                );
                if plan.request_id == "provider-oauth:kiro-social-refresh" {
                    assert_eq!(plan.url, "https://oauth.example/refreshToken");
                    assert_eq!(
                        plan.headers
                            .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
                            .map(String::as_str),
                        Some("true")
                    );
                    return Json(json!({
                        "request_id": plan.request_id,
                        "status_code": 200,
                        "headers": {
                            "content-type": "application/json"
                        },
                        "body": {
                            "json_body": {
                                "accessToken": sample_kiro_device_access_token("kiro-runtime@example.com"),
                                "refreshToken": "kiro-runtime-refresh-token-new",
                                "expiresIn": 1800,
                            }
                        }
                    }));
                }

                assert_eq!(plan.request_id, "kiro-quota:key-kiro-batch-runtime");
                Json(json!({
                    "request_id": plan.request_id,
                    "status_code": 200,
                    "headers": {
                        "content-type": "application/json"
                    },
                    "body": {
                        "json_body": {
                            "subscriptionInfo": {
                                "subscriptionTitle": "KIRO PRO+"
                            },
                            "usageBreakdownList": [{
                                "currentUsageWithPrecision": 5.0,
                                "usageLimitWithPrecision": 20.0,
                                "nextDateReset": 1_900_000_000u64
                            }],
                            "desktopUserInfo": {
                                "email": "kiro-runtime@example.com"
                            }
                        }
                    }
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let endpoint = sample_endpoint(
        "endpoint-kiro-chat",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "https://service.kiro.dev",
    );

    let mut existing_key = sample_key(
        "key-kiro-batch-runtime",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "stale-kiro-runtime-access-token",
    );
    existing_key.auth_type = "oauth".to_string();
    existing_key.is_active = false;
    existing_key.encrypted_auth_config = Some(
        encrypt_python_fernet_plaintext(
            DEV_FERNET_KEY,
            r#"{"provider_type":"kiro","auth_method":"social","email":"kiro-runtime@example.com","refresh_token":"kiro-runtime-refresh-token-old"}"#,
        )
        .expect("auth config ciphertext should build"),
    );

    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![existing_key],
    ));
    let mut manual_node = sample_proxy_node("proxy-node-kiro-batch-runtime");
    manual_node.status = "online".to_string();
    manual_node.proxy_url = Some("http://proxy.example:8080".to_string());
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![manual_node]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let gateway = build_router_with_state(
        build_state_with_execution_runtime_override(execution_runtime_url)
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .attach_proxy_node_repository_for_tests(proxy_node_repository)
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests(
                "kiro_social_refresh",
                "https://oauth.example",
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/batch-import"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "kiro-runtime-refresh-token-old",
            "proxy_node_id": "proxy-node-kiro-batch-runtime"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("payload should parse");
    assert_eq!(payload["total"], 1);
    assert_eq!(payload["success"], 1);
    assert_eq!(payload["failed"], 0);
    assert_eq!(payload["results"][0]["status"], "success");
    assert_eq!(payload["results"][0]["key_id"], "key-kiro-batch-runtime");
    assert_eq!(payload["results"][0]["replaced"], true);

    for _ in 0..40 {
        let plan_count = execution_plans.lock().expect("mutex should lock").len();
        if plan_count == 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    {
        let plans = execution_plans.lock().expect("mutex should lock");
        assert_eq!(plans.len(), 2);
    }

    let stored_key = provider_catalog_repository
        .list_keys_by_ids(&["key-kiro-batch-runtime".to_string()])
        .await
        .expect("keys should load")
        .into_iter()
        .next()
        .expect("persisted key should exist");
    assert!(stored_key.is_active);
    assert_eq!(
        stored_key.proxy,
        Some(json!({"node_id": "proxy-node-kiro-batch-runtime", "enabled": true}))
    );
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&stored_key);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["email"], "kiro-runtime@example.com");
    assert_eq!(
        auth_config["refresh_token"],
        "kiro-runtime-refresh-token-new"
    );

    gateway_handle.abort();
    execution_runtime_handle.abort();
}

#[test]
fn gateway_starts_admin_provider_oauth_kiro_batch_import_task_locally_with_trusted_admin_principal()
{
    run_admin_oauth_test(
        "gateway_starts_admin_provider_oauth_kiro_batch_import_task_locally_with_trusted_admin_principal",
        gateway_starts_admin_provider_oauth_kiro_batch_import_task_locally_with_trusted_admin_principal_impl,
    );
}

async fn gateway_starts_admin_provider_oauth_kiro_batch_import_task_locally_with_trusted_admin_principal_impl(
) {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().fallback(any(move |_request: Request| {
        let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
        async move {
            *upstream_hits_inner.lock().expect("mutex should lock") += 1;
            (StatusCode::OK, Body::from("unexpected upstream hit"))
        }
    }));

    let refresh_hits = Arc::new(Mutex::new(0usize));
    let refresh_hits_clone = Arc::clone(&refresh_hits);
    let refresh_server = Router::new().route(
        "/refreshToken",
        post(move |_request: Request| {
            let refresh_hits_inner = Arc::clone(&refresh_hits_clone);
            async move {
                *refresh_hits_inner.lock().expect("mutex should lock") += 1;
                Json(json!({
                    "accessToken": sample_kiro_device_access_token("kiro-task@example.com"),
                    "refreshToken": "kiro-task-refresh-token-new",
                    "expiresIn": 1800,
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-kiro", "kiro", 10);
    provider.provider_type = "kiro".to_string();
    let endpoint = sample_endpoint(
        "endpoint-kiro-chat",
        "provider-kiro",
        "kiro:generateAssistantResponse",
        "https://service.kiro.dev",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let (refresh_url, refresh_handle) = start_server(refresh_server).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                GatewayDataState::with_provider_catalog_repository_for_tests(
                    provider_catalog_repository.clone(),
                )
                .with_encryption_key_for_tests(DEV_FERNET_KEY),
            )
            .with_provider_oauth_token_url_for_tests(
                "kiro_social_refresh",
                refresh_url.to_string(),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let submit_response = client
        .post(format!(
            "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/batch-import/tasks"
        ))
        .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
        .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
        .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
        .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
        .json(&json!({
            "credentials": "kiro-task-refresh-token"
        }))
        .send()
        .await
        .expect("submit request should succeed");

    assert_eq!(submit_response.status(), StatusCode::OK);
    let submit_payload: serde_json::Value = submit_response
        .json()
        .await
        .expect("submit payload should parse");
    assert_eq!(submit_payload["status"], "submitted");
    let task_id = submit_payload["task_id"]
        .as_str()
        .expect("task id should exist")
        .to_string();

    let mut status_payload = serde_json::Value::Null;
    for _ in 0..40 {
        let response = client
            .get(format!(
                "{gateway_url}/api/admin/provider-oauth/providers/provider-kiro/batch-import/tasks/{task_id}"
            ))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .header(TRUSTED_ADMIN_USER_ID_HEADER, "admin-user-123")
            .header(TRUSTED_ADMIN_USER_ROLE_HEADER, "admin")
            .header(TRUSTED_ADMIN_SESSION_ID_HEADER, "session-123")
            .send()
            .await
            .expect("status request should succeed");
        assert_eq!(response.status(), StatusCode::OK);
        status_payload = response.json().await.expect("status payload should parse");
        if status_payload["status"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(
        status_payload["status"], "completed",
        "payload={status_payload}"
    );
    assert_eq!(status_payload["total"], 1);
    assert_eq!(status_payload["processed"], 1);
    assert_eq!(status_payload["success"], 1);
    assert_eq!(status_payload["failed"], 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    assert_eq!(*refresh_hits.lock().expect("mutex should lock"), 1);

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-kiro".to_string()])
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 1);

    gateway_handle.abort();
    refresh_handle.abort();
    upstream_handle.abort();
}

#[test]
fn gateway_prevents_pool_write_token_from_importing_agent_identity_via_batch_routes() {
    run_admin_oauth_test(
        "gateway_prevents_pool_write_token_from_importing_agent_identity_via_batch_routes",
        gateway_prevents_pool_write_token_from_importing_agent_identity_via_batch_routes_impl,
    );
}

async fn gateway_prevents_pool_write_token_from_importing_agent_identity_via_batch_routes_impl() {
    let raw_token = "ae-provider-oauth-agent-identity-pool-write";
    let state = AppState::new().expect("gateway should build");
    let admin_user = state
        .create_local_auth_user_with_settings(
            Some("provider-oauth-agent-identity-pool@example.com".to_string()),
            true,
            "admin".to_string(),
            "hash".to_string(),
            "admin".to_string(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("admin user should be created")
        .expect("admin user should exist");
    let mut management_token = sample_management_token(
        "token-provider-oauth-agent-identity-pool",
        &admin_user.id,
        "provider-oauth-agent-identity-pool",
        true,
    );
    management_token.token.allowed_ips = None;
    management_token.token.permissions = Some(json!(["admin:pool:read", "admin:pool:write"]));
    let management_token_repository =
        Arc::new(InMemoryManagementTokenRepository::seed_with_hashes(
            vec![management_token],
            vec![(
                hash_management_token(raw_token),
                "token-provider-oauth-agent-identity-pool".to_string(),
            )],
        ));

    let mut provider = sample_provider("provider-codex", "codex", 10);
    provider.provider_type = "codex".to_string();
    let endpoint = sample_endpoint(
        "endpoint-codex-agent-identity",
        "provider-codex",
        "openai:chat",
        "https://chatgpt.com/backend-api/codex",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let data_state =
        GatewayDataState::with_management_token_repository_for_tests(management_token_repository)
            .attach_provider_catalog_repository_for_tests(provider_catalog_repository.clone())
            .with_encryption_key_for_tests(DEV_FERNET_KEY);
    let gateway = build_router_with_state(state.with_data_state_for_tests(data_state));
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let credentials = json!({
        "auth_mode": "agentIdentity",
        "agent_runtime_id": "runtime-rbac-guard",
        "agent_private_key": "MC4CAQAwBQYDK2VwBCIEIAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "task_id": "task-rbac-guard"
    })
    .to_string();
    let client = reqwest::Client::new();
    for path in [
        "/api/admin/provider-oauth/providers/provider-codex/batch-import",
        "/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks",
        "/api/admin/provider-oauth/providers/provider-codex/agent-identity-import/tasks",
    ] {
        let response = client
            .post(format!("{gateway_url}{path}"))
            .header(crate::constants::GATEWAY_HEADER, "rust-phase3b")
            .bearer_auth(raw_token)
            .json(&json!({ "credentials": credentials }))
            .send()
            .await
            .expect("request should succeed");

        let status = response.status();
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "path={path} payload={payload}"
        );
        assert_eq!(payload["detail"], "management token permission denied");
        assert_eq!(payload["required_permission"], "admin:provider_oauth:admin");
    }

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-codex".to_string()])
        .await
        .expect("keys should load");
    assert!(keys.is_empty());

    gateway_handle.abort();
}
