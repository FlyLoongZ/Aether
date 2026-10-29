use super::*;
use crate::tests::DEV_FERNET_KEY;

#[test]
fn gateway_authorizes_claude_cookie_without_persisting_cookie() {
    run_admin_oauth_test(
        "gateway_authorizes_claude_cookie_without_persisting_cookie",
        gateway_authorizes_claude_cookie_without_persisting_cookie_impl,
    );
}

async fn gateway_authorizes_claude_cookie_without_persisting_cookie_impl() {
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
                let json_body = match plan.request_id.as_str() {
                    "provider-oauth:claude-cookie-organizations" => json!([
                        {"uuid": "org-personal", "raven_type": "personal"},
                        {"uuid": "org-team", "raven_type": "team"}
                    ]),
                    "provider-oauth:claude-cookie-authorize" => {
                        let state = plan
                            .body
                            .json_body
                            .as_ref()
                            .and_then(|body| body.get("state"))
                            .and_then(serde_json::Value::as_str)
                            .expect("authorize plan should contain state");
                        let mut redirect =
                            url::Url::parse("https://platform.claude.com/oauth/code/callback")
                                .expect("redirect URL should parse");
                        redirect
                            .query_pairs_mut()
                            .append_pair("code", "claude-authorization-code")
                            .append_pair("state", state);
                        json!({"redirect_uri": redirect.to_string()})
                    }
                    "provider-oauth:exchange-code" => json!({
                        "access_token": "sk-ant-oat01-created",
                        "refresh_token": "sk-ant-ort01-created",
                        "expires_in": 3600,
                        "organization": {"uuid": "org-team"},
                        "account": {
                            "uuid": "account-claude-123",
                            "email_address": "claude@example.com"
                        }
                    }),
                    // Newly authorized accounts get their 5H/weekly quota fetched right away.
                    quota if quota.starts_with("claude-code-quota:") => {
                        json!({"five_hour": {"utilization": 10.0}})
                    }
                    unexpected => panic!("unexpected execution plan: {unexpected}"),
                };
                Json(json!({
                    "request_id": plan.request_id,
                    "status_code": 200,
                    "headers": {"content-type": "application/json"},
                    "body": {"json_body": json_body}
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-claude", "claude", 10);
    provider.provider_type = "claude_code".to_string();
    provider.proxy = Some(json!({
        "node_id": "proxy-node-claude",
        "url": "http://proxy.example:8080",
        "enabled": true
    }));
    let endpoint = sample_endpoint(
        "endpoint-claude-messages",
        "provider-claude",
        "claude:messages",
        "https://api.anthropic.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let mut proxy_node = sample_proxy_node("proxy-node-claude");
    proxy_node.status = "online".to_string();
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![proxy_node]));

    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .attach_proxy_node_repository_for_tests(proxy_node_repository)
            .with_encryption_key_for_tests(DEV_FERNET_KEY),
        )
        .with_provider_oauth_token_url_for_tests(
            "claude_code_cookie_base_url",
            "https://claude.example",
        )
        .with_provider_oauth_token_url_for_tests(
            "claude_code",
            "https://platform.example/v1/oauth/token",
        );

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-claude/cookie-authorize",
        Some(json!({
            "cookie": "Cookie: other=value; sessionKey=sk-ant-sid01-secret; theme=dark",
            "name": "claude-cookie-account"
        })),
    )
    .await;
    let status = response.status();
    let payload: serde_json::Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read"),
    )
    .expect("response should be JSON");
    assert_eq!(status, StatusCode::OK, "payload={payload}");
    assert_eq!(payload["provider_type"], "claude_code");
    assert_eq!(payload["has_refresh_token"], true);
    assert_eq!(payload["temporary"], false);
    assert_eq!(payload["email"], "claude@example.com");

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-claude".to_string()])
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 1);
    let decrypted_api_key = decrypt_persisted_provider_api_key(&keys[0]);
    assert_eq!(decrypted_api_key, "sk-ant-oat01-created");
    let decrypted_auth_config = decrypt_persisted_provider_auth_config(&keys[0]);
    let auth_config: serde_json::Value =
        serde_json::from_str(&decrypted_auth_config).expect("auth config should parse");
    assert_eq!(auth_config["refresh_token"], "sk-ant-ort01-created");
    assert_eq!(auth_config["org_uuid"], "org-team");
    assert_eq!(auth_config["account_uuid"], "account-claude-123");
    assert_eq!(auth_config["email"], "claude@example.com");
    assert!(!decrypted_auth_config.contains("sk-ant-sid01-secret"));
    assert!(!decrypted_auth_config.contains("sessionKey"));
    assert!(auth_config.get("cookie").is_none());

    let plans = execution_plans.lock().expect("mutex should lock");
    assert_eq!(plans.len(), 3);
    for plan in &plans[..2] {
        assert_eq!(
            plan.headers.get("cookie").map(String::as_str),
            Some("sessionKey=sk-ant-sid01-secret")
        );
        assert_eq!(
            plan.headers
                .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
                .map(String::as_str),
            Some("false")
        );
        assert_eq!(
            plan.proxy.as_ref().and_then(|proxy| proxy.mode.as_deref()),
            Some("http")
        );
    }
    let token_plan = &plans[2];
    assert_eq!(token_plan.request_id, "provider-oauth:exchange-code");
    assert!(!token_plan.headers.contains_key("cookie"));
    assert!(token_plan
        .body
        .json_body
        .as_ref()
        .is_some_and(|body| body.get("scope").is_none()));

    execution_runtime_handle.abort();
}

#[test]
fn gateway_batch_authorizes_claude_cookies_as_redacted_task() {
    run_admin_oauth_test(
        "gateway_batch_authorizes_claude_cookies_as_redacted_task",
        gateway_batch_authorizes_claude_cookies_as_redacted_task_impl,
    );
}

async fn gateway_batch_authorizes_claude_cookies_as_redacted_task_impl() {
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
                let json_body = match plan.request_id.as_str() {
                    "provider-oauth:claude-cookie-organizations" => {
                        json!([{"uuid": "org-team", "raven_type": "team"}])
                    }
                    "provider-oauth:claude-cookie-authorize" => {
                        let cookie = plan
                            .headers
                            .get("cookie")
                            .map(String::as_str)
                            .expect("authorize plan should contain cookie");
                        let label = if cookie.contains("batch-sid-one") {
                            "one"
                        } else if cookie.contains("batch-sid-two") {
                            "two"
                        } else {
                            panic!("unexpected Cookie authorization input")
                        };
                        let state = plan
                            .body
                            .json_body
                            .as_ref()
                            .and_then(|body| body.get("state"))
                            .and_then(serde_json::Value::as_str)
                            .expect("authorize plan should contain state");
                        let mut redirect =
                            url::Url::parse("https://platform.claude.com/oauth/code/callback")
                                .expect("redirect URL should parse");
                        redirect
                            .query_pairs_mut()
                            .append_pair("code", format!("code-{label}").as_str())
                            .append_pair("state", state);
                        json!({"redirect_uri": redirect.to_string()})
                    }
                    "provider-oauth:exchange-code" => {
                        let code = plan
                            .body
                            .json_body
                            .as_ref()
                            .and_then(|body| body.get("code"))
                            .and_then(serde_json::Value::as_str)
                            .expect("token plan should contain code");
                        let label = code.strip_prefix("code-").expect("code should be tagged");
                        json!({
                            "access_token": format!("sk-ant-oat01-{label}"),
                            "refresh_token": format!("sk-ant-ort01-{label}"),
                            "expires_in": 3600,
                            "organization": {"uuid": "org-team"},
                            "account": {
                                "uuid": format!("account-{label}"),
                                "email_address": format!("{label}@example.com")
                            }
                        })
                    }
                    unexpected => panic!("unexpected execution plan: {unexpected}"),
                };
                Json(json!({
                    "request_id": plan.request_id,
                    "status_code": 200,
                    "headers": {"content-type": "application/json"},
                    "body": {"json_body": json_body}
                }))
            }
        }),
    );

    let mut provider = sample_provider("provider-claude", "claude", 10);
    provider.provider_type = "claude_code".to_string();
    provider.proxy = Some(json!({
        "node_id": "proxy-node-claude",
        "url": "http://proxy.example:8080",
        "enabled": true
    }));
    let endpoint = sample_endpoint(
        "endpoint-claude-messages",
        "provider-claude",
        "claude:messages",
        "https://api.anthropic.com",
    );
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![endpoint],
        vec![],
    ));
    let background_task_repository = Arc::new(InMemoryBackgroundTaskRepository::default());
    let mut proxy_node = sample_proxy_node("proxy-node-claude");
    proxy_node.status = "online".to_string();
    let proxy_node_repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![proxy_node]));
    let (execution_runtime_url, execution_runtime_handle) = start_server(execution_runtime).await;
    let state = build_state_with_execution_runtime_override(execution_runtime_url)
        .with_data_state_for_tests(
            GatewayDataState::with_provider_catalog_repository_for_tests(
                provider_catalog_repository.clone(),
            )
            .attach_proxy_node_repository_for_tests(proxy_node_repository)
            .with_encryption_key_for_tests(DEV_FERNET_KEY)
            .with_background_task_repository_for_tests(background_task_repository.clone()),
        )
        .with_provider_oauth_token_url_for_tests(
            "claude_code_cookie_base_url",
            "https://claude.example",
        )
        .with_provider_oauth_token_url_for_tests(
            "claude_code",
            "https://platform.example/v1/oauth/token",
        );

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-claude/cookie-authorize/tasks",
        Some(json!({
            "cookies": [
                "sessionKey=batch-sid-one",
                "foo=bar",
                "Cookie: sessionKey=batch-sid-one",
                "sessionKey=batch-sid-two"
            ]
        })),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let submitted: serde_json::Value = serde_json::from_slice(
        &to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("submitted body should read"),
    )
    .expect("submitted body should parse");
    assert_eq!(submitted["status"], "submitted");
    assert_eq!(submitted["total"], 4);
    assert_eq!(submitted["import_kind"], "cookie_authorize");
    let task_id = submitted["task_id"]
        .as_str()
        .expect("task id should exist")
        .to_string();
    assert!(task_id.starts_with("claude-cookie-"));

    let mut status_payload = Value::Null;
    for _ in 0..80 {
        let response = local_admin_provider_oauth_response(
            &state,
            http::Method::GET,
            format!(
                "/api/admin/provider-oauth/providers/provider-claude/cookie-authorize/tasks/{task_id}"
            )
            .as_str(),
            None,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        status_payload = serde_json::from_slice(
            &to_bytes(response.into_body(), usize::MAX)
                .await
                .expect("status body should read"),
        )
        .expect("status body should parse");
        if status_payload["status"] == "completed" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    assert_eq!(status_payload["status"], "completed");
    assert_eq!(status_payload["import_kind"], "cookie_authorize");
    assert_eq!(status_payload["total"], 4);
    assert_eq!(status_payload["processed"], 4);
    assert_eq!(status_payload["success"], 2);
    assert_eq!(status_payload["failed"], 2);
    assert_eq!(status_payload["created_count"], 2);
    assert_eq!(status_payload["replaced_count"], 0);
    let error_samples = status_payload["error_samples"]
        .as_array()
        .expect("error samples should be an array");
    assert_eq!(error_samples.len(), 2);
    assert_eq!(error_samples[0]["index"], 1);
    assert_eq!(error_samples[1]["index"], 2);

    let status_text = status_payload.to_string();
    for forbidden in ["sessionKey", "batch-sid-one", "batch-sid-two"] {
        assert!(!status_text.contains(forbidden), "forbidden={forbidden}");
    }
    let raw_task_state = state
        .load_provider_oauth_batch_task_for_tests(
            format!("provider_oauth_batch_task:{task_id}").as_str(),
        )
        .expect("raw task state should exist");
    for forbidden in ["sessionKey", "batch-sid-one", "batch-sid-two"] {
        assert!(
            !raw_task_state.contains(forbidden),
            "raw task state contains {forbidden}"
        );
    }
    let persisted_task_state = state
        .runtime_kv_get(format!("provider_oauth_batch_task:{task_id}").as_str())
        .await
        .expect("runtime task state lookup should succeed")
        .expect("runtime task state should exist");
    assert!(crate::handlers::shared::runtime_secret_payload_is_sealed(
        &persisted_task_state
    ));
    for forbidden in ["sessionKey", "batch-sid-one", "batch-sid-two"] {
        assert!(
            !persisted_task_state.contains(forbidden),
            "persisted task state contains {forbidden}"
        );
    }
    let background_run = background_task_repository
        .find_run(&task_id)
        .await
        .expect("background run should load")
        .expect("background run should exist");
    let background_text = serde_json::to_string(&background_run)
        .expect("background run should serialize for assertion");
    for forbidden in ["sessionKey", "batch-sid-one", "batch-sid-two"] {
        assert!(
            !background_text.contains(forbidden),
            "forbidden={forbidden}"
        );
    }

    let keys = provider_catalog_repository
        .list_keys_by_provider_ids(&["provider-claude".to_string()])
        .await
        .expect("keys should load");
    assert_eq!(keys.len(), 2);
    for key in &keys {
        let auth_config = decrypt_persisted_provider_auth_config(key);
        assert!(!auth_config.contains("batch-sid"));
        assert!(!auth_config.contains("sessionKey"));
    }

    let plans = execution_plans.lock().expect("mutex should lock");
    // Post-authorization quota refreshes are fire-and-forget, so their count is not
    // deterministic here; only the OAuth flow plans are asserted.
    assert_eq!(
        plans
            .iter()
            .filter(|plan| !plan.request_id.starts_with("claude-code-quota:"))
            .count(),
        6
    );
    assert_eq!(
        plans
            .iter()
            .filter(|plan| plan.request_id == "provider-oauth:claude-cookie-organizations")
            .count(),
        2
    );
    assert_eq!(
        plans
            .iter()
            .filter(|plan| plan.request_id == "provider-oauth:claude-cookie-authorize")
            .count(),
        2
    );
    assert_eq!(
        plans
            .iter()
            .filter(|plan| plan.request_id == "provider-oauth:exchange-code")
            .count(),
        2
    );
    assert!(plans.iter().all(|plan| {
        plan.proxy.as_ref().and_then(|proxy| proxy.mode.as_deref()) == Some("http")
    }));

    execution_runtime_handle.abort();
}

#[test]
fn gateway_rejects_generic_oauth_start_for_xai_provider() {
    run_admin_oauth_test(
        "gateway_rejects_generic_oauth_start_for_xai_provider",
        gateway_rejects_generic_oauth_start_for_xai_provider_impl,
    );
}

async fn gateway_rejects_generic_oauth_start_for_xai_provider_impl() {
    let mut provider = sample_provider("provider-xai", "xai", 10);
    provider.provider_type = "xai".to_string();
    let provider_catalog_repository = Arc::new(InMemoryProviderCatalogReadRepository::seed(
        vec![provider],
        vec![],
        vec![],
    ));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_provider_catalog_reader_for_tests(
            provider_catalog_repository,
        ));

    let response = local_admin_provider_oauth_response(
        &state,
        http::Method::POST,
        "/api/admin/provider-oauth/providers/provider-xai/start",
        None,
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should read");
    let payload: serde_json::Value = serde_json::from_slice(&body).expect("json body should parse");
    assert!(
        payload["detail"].as_str().is_some_and(|detail| {
            detail.contains("设备授权") || detail.contains("导入凭据")
        }),
        "payload={payload}"
    );
}

#[test]
fn local_admin_provider_oauth_batch_task_status_attaches_audit_only_for_terminal_states() {
    run_admin_oauth_test(
        "local_admin_provider_oauth_batch_task_status_attaches_audit_only_for_terminal_states",
        local_admin_provider_oauth_batch_task_status_attaches_audit_only_for_terminal_states_impl,
    );
}

async fn local_admin_provider_oauth_batch_task_status_attaches_audit_only_for_terminal_states_impl()
{
    let completed_state = AppState::new()
        .expect("gateway should build")
        .with_provider_oauth_batch_task_entry_for_tests(
            "task-completed",
            json!({
                "task_id": "task-completed",
                "provider_id": "provider-codex",
                "provider_type": "codex",
                "status": "completed",
                "total": 1,
                "processed": 1,
                "success": 1,
                "failed": 0,
                "progress_percent": 100,
                "message": "导入完成",
                "error": null,
                "error_samples": [],
                "created_at": 1700000001u64,
                "started_at": 1700000002u64,
                "finished_at": 1700000003u64,
                "updated_at": 1700000004u64,
            }),
        );
    let completed_response = local_admin_provider_oauth_response(
        &completed_state,
        http::Method::GET,
        "/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/task-completed",
        None,
    )
    .await;
    assert_eq!(completed_response.status(), StatusCode::OK);
    let completed_audit = completed_response
        .extensions()
        .get::<AdminAuditEvent>()
        .expect("completed task should attach audit");
    assert_eq!(
        completed_audit.event_name,
        "admin_provider_oauth_batch_task_completed_viewed"
    );
    assert_eq!(
        completed_audit.action,
        "view_provider_oauth_batch_task_terminal_state"
    );
    assert_eq!(completed_audit.target_type, "provider_oauth_batch_task");
    assert_eq!(completed_audit.target_id, "provider-codex:task-completed");

    let processing_state = AppState::new()
        .expect("gateway should build")
        .with_provider_oauth_batch_task_entry_for_tests(
            "task-processing",
            json!({
                "task_id": "task-processing",
                "provider_id": "provider-codex",
                "provider_type": "codex",
                "status": "processing",
                "total": 3,
                "processed": 1,
                "success": 1,
                "failed": 0,
                "progress_percent": 33,
                "message": "处理中",
                "error": null,
                "error_samples": [],
                "created_at": 1700000001u64,
                "started_at": 1700000002u64,
                "finished_at": null,
                "updated_at": 1700000003u64,
            }),
        );
    let processing_response = local_admin_provider_oauth_response(
        &processing_state,
        http::Method::GET,
        "/api/admin/provider-oauth/providers/provider-codex/batch-import/tasks/task-processing",
        None,
    )
    .await;
    assert_eq!(processing_response.status(), StatusCode::OK);
    assert!(
        processing_response
            .extensions()
            .get::<AdminAuditEvent>()
            .is_none(),
        "processing task status should not attach audit"
    );
}
