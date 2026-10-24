use super::*;

#[tokio::test]
async fn gateway_handles_users_me_management_token_reads_locally_without_proxying_upstream() {
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
                json!("session-users-me-management-token-reads"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_users_me_management_token("mt-user-1", "user-auth-1", "alice", true),
        sample_users_me_management_token("mt-user-2", "user-auth-2", "bob", true),
    ]));
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_management_token_repository_for_tests(
                    Arc::clone(&repository),
                )
                .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-management-token-reads",
                    "device-users-me-management-token-reads",
                    "refresh-token-users-me-management-token-reads",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let list_response = client
        .get(format!(
            "{gateway_url}/api/me/management-tokens?is_active=true&skip=0&limit=50"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(list_response.status(), StatusCode::OK);
    let list_payload: serde_json::Value =
        list_response.json().await.expect("json body should parse");
    assert_eq!(list_payload["total"], 1);
    assert_eq!(list_payload["quota"]["used"], 1);
    assert_eq!(list_payload["quota"]["max"], 20);
    let items = list_payload["items"]
        .as_array()
        .expect("items should be array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], "mt-user-1");
    assert!(items[0].get("user").is_none());

    let detail_response = client
        .get(format!("{gateway_url}/api/me/management-tokens/mt-user-1"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["id"], "mt-user-1");
    assert_eq!(detail_payload["token_display"], "ae_test...****");

    let foreign_response = client
        .get(format!("{gateway_url}/api/me/management-tokens/mt-user-2"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(foreign_response.status(), StatusCode::NOT_FOUND);

    let foreign_before = repository
        .get_management_token_with_user("mt-user-2")
        .await
        .expect("foreign token lookup should succeed")
        .expect("foreign token should exist");
    let update_response = client
        .put(format!("{gateway_url}/api/me/management-tokens/mt-user-2"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "name": "cross-user-update" }))
        .send()
        .await
        .expect("cross-user token update should complete");
    assert_eq!(update_response.status(), StatusCode::FORBIDDEN);

    let toggle_response = client
        .patch(format!(
            "{gateway_url}/api/me/management-tokens/mt-user-2/status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("cross-user token toggle should complete");
    assert_eq!(toggle_response.status(), StatusCode::FORBIDDEN);

    let regenerate_response = client
        .post(format!(
            "{gateway_url}/api/me/management-tokens/mt-user-2/regenerate"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("cross-user token regenerate should complete");
    assert_eq!(regenerate_response.status(), StatusCode::FORBIDDEN);

    let delete_response = client
        .delete(format!("{gateway_url}/api/me/management-tokens/mt-user-2"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("cross-user token delete should complete");
    assert_eq!(delete_response.status(), StatusCode::FORBIDDEN);

    let foreign_after = repository
        .get_management_token_with_user("mt-user-2")
        .await
        .expect("foreign token lookup should succeed")
        .expect("foreign token should still exist");
    assert_eq!(foreign_after, foreign_before);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_users_me_management_token_nested_get_path_as_local_not_found_without_hitting_upstream(
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
                json!("session-users-me-management-token-reads"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_users_me_management_token("mt-user-1", "user-auth-1", "alice", true),
    ]));
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_management_token_repository_for_tests(
                    repository,
                )
                .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-management-token-reads",
                    "device-users-me-management-token-reads",
                    "refresh-token-users-me-management-token-reads",
                    now,
                )])
        })
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/me/management-tokens/mt-user-1/status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-reads",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_local_route_not_found_response(response).await;
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_users_me_management_token_writes_locally_without_proxying_upstream() {
    let now = Utc::now();
    let mut user = sample_auth_user(now);
    user.role = "admin".to_string();
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
                json!("session-users-me-management-token-writes"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_users_me_management_token("mt-existing-1", "user-auth-1", "alice", true),
    ]));
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_management_token_repository_for_tests(
                    repository,
                )
                .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-management-token-writes",
                    "device-users-me-management-token-writes",
                    "refresh-token-users-me-management-token-writes",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/me/management-tokens"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-writes",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "writer-token",
            "description": "writer token",
            "allowed_ips": ["127.0.0.1"],
            "permissions": ["admin:usage:read"],
            "expires_at": (now + chrono::Duration::days(1)).to_rfc3339(),
        }))
        .send()
        .await
        .expect("request should succeed");

    let create_status = create_response.status();
    let create_body = create_response.text().await.expect("body should read");
    assert_eq!(create_status, StatusCode::CREATED, "{create_body}");
    let create_payload: serde_json::Value =
        serde_json::from_str(&create_body).expect("json body should parse");
    let created_id = create_payload["data"]["id"]
        .as_str()
        .expect("created id should exist")
        .to_string();
    assert_eq!(create_payload["message"], "Management Token 创建成功");
    assert_eq!(create_payload["data"]["name"], "writer-token");
    assert_eq!(
        create_payload["data"]["permissions"],
        json!(["admin:usage:read"])
    );
    let created_token = create_payload["token"].as_str().unwrap_or_default();
    let created_token_random = created_token.strip_prefix("ae-").unwrap_or_default();
    assert_eq!(created_token_random.len(), 32);
    assert!(created_token_random
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric()));

    let update_response = client
        .put(format!(
            "{gateway_url}/api/me/management-tokens/{created_id}"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-writes",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "writer-token-renamed",
            "description": null,
            "allowed_ips": ["10.0.0.1"],
            "permissions": ["admin:usage:read", "admin:pool:write"],
            "expires_at": (now + chrono::Duration::days(2)).to_rfc3339(),
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(update_response.status(), StatusCode::OK);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["message"], "更新成功");
    assert_eq!(update_payload["data"]["name"], "writer-token-renamed");
    assert_eq!(
        update_payload["data"]["description"],
        serde_json::Value::Null
    );
    assert_eq!(
        update_payload["data"]["permissions"],
        json!(["admin:pool:write", "admin:usage:read"])
    );

    let toggle_response = client
        .patch(format!(
            "{gateway_url}/api/me/management-tokens/{created_id}/status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-writes",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(toggle_response.status(), StatusCode::OK);
    let toggle_payload: serde_json::Value = toggle_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(toggle_payload["message"], "Token 已禁用");
    assert_eq!(toggle_payload["data"]["is_active"], false);

    let regenerate_response = client
        .post(format!(
            "{gateway_url}/api/me/management-tokens/{created_id}/regenerate"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-writes",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(regenerate_response.status(), StatusCode::OK);
    let regenerate_payload: serde_json::Value = regenerate_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(regenerate_payload["message"], "Token 已重新生成");
    let regenerated_token = regenerate_payload["token"].as_str().unwrap_or_default();
    let regenerated_token_random = regenerated_token.strip_prefix("ae-").unwrap_or_default();
    assert_eq!(regenerated_token_random.len(), 32);
    assert!(regenerated_token_random
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric()));

    let delete_response = client
        .delete(format!(
            "{gateway_url}/api/me/management-tokens/{created_id}"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-writes",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(delete_response.status(), StatusCode::OK);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["message"], "删除成功");

    let missing_response = client
        .get(format!(
            "{gateway_url}/api/me/management-tokens/{created_id}"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-writes",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(missing_response.status(), StatusCode::NOT_FOUND);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_users_me_management_token_writes_for_non_admin_user_without_proxying_upstream(
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
                json!("session-users-me-management-token-create-denied"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let repository = Arc::new(InMemoryManagementTokenRepository::default());
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_management_token_repository_for_tests(
                    repository,
                )
                .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-management-token-create-denied",
                    "device-users-me-management-token-create-denied",
                    "refresh-token-users-me-management-token-create-denied",
                    now,
                )])
        })
        .await;

    let create_response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/me/management-tokens"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-create-denied",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "ordinary-user-token",
            "description": "ordinary user token",
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(create_response.status(), StatusCode::FORBIDDEN);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(
        create_payload["detail"],
        json!("仅管理员可以创建 Management Token")
    );

    let client = reqwest::Client::new();
    for (method, path) in [
        (reqwest::Method::PUT, "/api/me/management-tokens/mt-owned"),
        (
            reqwest::Method::PATCH,
            "/api/me/management-tokens/mt-owned/status",
        ),
        (
            reqwest::Method::POST,
            "/api/me/management-tokens/mt-owned/regenerate",
        ),
        (
            reqwest::Method::DELETE,
            "/api/me/management-tokens/mt-owned",
        ),
    ] {
        let response = client
            .request(method, format!("{gateway_url}{path}"))
            .header("authorization", format!("Bearer {access_token}"))
            .header(
                "x-client-device-id",
                "device-users-me-management-token-create-denied",
            )
            .header("user-agent", "AetherTest/1.0")
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(
            payload["detail"],
            json!("仅管理员可以管理 Management Token")
        );
    }
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_management_token_reads_without_reader() {
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
                json!("session-users-me-management-token-read-unavailable"),
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
                    "session-users-me-management-token-read-unavailable",
                    "device-users-me-management-token-read-unavailable",
                    "refresh-token-users-me-management-token-read-unavailable",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let list_response = client
        .get(format!("{gateway_url}/api/me/management-tokens"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-read-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(list_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let list_payload: serde_json::Value =
        list_response.json().await.expect("json body should parse");
    assert_eq!(list_payload["detail"], "服务暂不可用，请稍后重试");

    let detail_response = client
        .get(format!("{gateway_url}/api/me/management-tokens/mt-missing"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-read-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_users_me_management_token_writes_without_writer() {
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
                json!("session-users-me-management-token-write-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let repository = Arc::new(InMemoryManagementTokenRepository::seed(vec![
        sample_users_me_management_token("mt-existing-1", "user-auth-1", "alice", true),
    ]));
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));

    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_builder(|| {
            let data_state =
                crate::data::GatewayDataState::with_management_token_reader_for_tests(repository)
                    .with_user_reader(user_repository);
            AppState::new()
                .expect("gateway should build")
                .with_data_state_for_tests(data_state)
                .with_auth_sessions_for_tests([sample_auth_session(
                    "user-auth-1",
                    "session-users-me-management-token-write-unavailable",
                    "device-users-me-management-token-write-unavailable",
                    "refresh-token-users-me-management-token-write-unavailable",
                    now,
                )])
        })
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/me/management-tokens"))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-write-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "readonly-token",
            "description": "readonly token",
            "allowed_ips": ["127.0.0.1"],
            "expires_at": (now + chrono::Duration::days(1)).to_rfc3339(),
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(create_payload["detail"], "服务暂不可用，请稍后重试");

    let update_response = client
        .put(format!(
            "{gateway_url}/api/me/management-tokens/mt-existing-1"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-write-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "name": "readonly-token-renamed",
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(update_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["detail"], "服务暂不可用，请稍后重试");

    let toggle_response = client
        .patch(format!(
            "{gateway_url}/api/me/management-tokens/mt-existing-1/status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-write-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(toggle_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let toggle_payload: serde_json::Value = toggle_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(toggle_payload["detail"], "服务暂不可用，请稍后重试");

    let regenerate_response = client
        .post(format!(
            "{gateway_url}/api/me/management-tokens/mt-existing-1/regenerate"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-write-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(
        regenerate_response.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let regenerate_payload: serde_json::Value = regenerate_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(regenerate_payload["detail"], "服务暂不可用，请稍后重试");

    let delete_response = client
        .delete(format!(
            "{gateway_url}/api/me/management-tokens/mt-existing-1"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-users-me-management-token-write-unavailable",
        )
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(delete_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["detail"], "服务暂不可用，请稍后重试");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}
