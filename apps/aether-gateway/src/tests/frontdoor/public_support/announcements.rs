use super::*;

#[tokio::test]
async fn gateway_handles_public_announcements_list_without_proxying_upstream() {
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

    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-pinned".to_string(),
            "置顶维护".to_string(),
            "维护窗口".to_string(),
            "maintenance".to_string(),
            5,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            1_711_000_100,
            1_711_000_100,
        )
        .expect("announcement should build"),
        StoredAnnouncement::new(
            "announcement-normal".to_string(),
            "普通公告".to_string(),
            "普通内容".to_string(),
            "info".to_string(),
            3,
            true,
            false,
            false,
            Some("admin-2".to_string()),
            Some("ops".to_string()),
            None,
            None,
            1_711_000_000,
            1_711_000_000,
        )
        .expect("announcement should build"),
        StoredAnnouncement::new(
            "announcement-inactive".to_string(),
            "已停用".to_string(),
            "不应返回".to_string(),
            "warning".to_string(),
            100,
            false,
            true,
            false,
            Some("admin-3".to_string()),
            Some("root".to_string()),
            None,
            None,
            1_711_000_200,
            1_711_000_200,
        )
        .expect("announcement should build"),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_reader_for_tests(
                    announcement_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/announcements?active_only=false&limit=50&offset=0"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 2);
    let items = payload["items"].as_array().expect("items should be array");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["id"], "announcement-pinned");
    assert_eq!(items[0]["is_active"], true);
    assert_eq!(items[1]["id"], "announcement-normal");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_active_announcements_without_proxying_upstream() {
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

    let now = Utc::now().timestamp().max(0) as u64;
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-active".to_string(),
            "有效公告".to_string(),
            "当前生效".to_string(),
            "important".to_string(),
            50,
            true,
            false,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            Some((now.saturating_sub(60)) as i64),
            Some((now.saturating_add(3600)) as i64),
            now as i64,
            now as i64,
        )
        .expect("announcement should build"),
        StoredAnnouncement::new(
            "announcement-future".to_string(),
            "未来公告".to_string(),
            "尚未生效".to_string(),
            "info".to_string(),
            10,
            true,
            false,
            false,
            Some("admin-2".to_string()),
            Some("ops".to_string()),
            Some((now.saturating_add(3600)) as i64),
            Some((now.saturating_add(7200)) as i64),
            now as i64,
            now as i64,
        )
        .expect("announcement should build"),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_reader_for_tests(
                    announcement_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/announcements/active"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 1);
    let items = payload["items"].as_array().expect("items should be array");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], "announcement-active");
    assert_eq!(items[0]["type"], "important");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_public_announcement_detail_without_proxying_upstream() {
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

    let now = Utc::now().timestamp().max(0) as u64;
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-1".to_string(),
            "系统维护".to_string(),
            "今天晚些时候维护".to_string(),
            "maintenance".to_string(),
            10,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            Some(now.saturating_sub(60) as i64),
            Some(now.saturating_add(3600) as i64),
            1_711_000_000,
            1_711_000_100,
        )
        .expect("announcement should build"),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_reader_for_tests(
                    announcement_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/announcements/announcement-1"))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["id"], "announcement-1");
    assert_eq!(payload["title"], "系统维护");
    assert_eq!(payload["type"], "maintenance");
    assert_eq!(payload["author"]["id"], "admin-1");
    assert_eq!(payload["author"]["username"], "admin");
    assert_eq!(payload["created_at"], "2024-03-21T05:46:40Z");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_hides_non_public_announcement_details() {
    let now = Utc::now().timestamp().max(0) as u64;
    let announcements = [
        ("inactive", false, None, None),
        ("future", true, Some(now.saturating_add(3600) as i64), None),
        ("expired", true, None, Some(now.saturating_sub(1) as i64)),
    ]
    .into_iter()
    .map(|(id, is_active, starts_at, ends_at)| {
        StoredAnnouncement::new(
            format!("announcement-{id}"),
            format!("{id} title"),
            format!("{id} private content"),
            "info".to_string(),
            1,
            is_active,
            false,
            false,
            Some("admin-private".to_string()),
            Some("admin".to_string()),
            starts_at,
            ends_at,
            now as i64,
            now as i64,
        )
        .expect("announcement should build")
    })
    .collect::<Vec<_>>();
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(announcements));
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_reader_for_tests(
                    announcement_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    for id in ["inactive", "future", "expired"] {
        let response = client
            .get(format!("{gateway_url}/api/announcements/announcement-{id}"))
            .send()
            .await
            .expect("request should succeed");
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{id}");
        let payload: serde_json::Value = response.json().await.expect("json body should parse");
        assert_eq!(payload["detail"], "Announcement not found", "{id}");
    }

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_creates_announcement_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/announcements",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_repository_for_tests(Arc::clone(
                    &announcement_repository,
                )),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/announcements"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "title": "系统维护",
            "content": "今晚维护",
            "type": "maintenance",
            "priority": 8,
            "is_pinned": true,
            "start_time": "2026-03-30T00:00:00Z",
            "end_time": "2026-03-31T00:00:00Z"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["title"], "系统维护");
    assert_eq!(payload["message"], "公告创建成功");

    let announcement_id = payload["id"]
        .as_str()
        .expect("created announcement id should be string");
    let created = announcement_repository
        .find_by_id(announcement_id)
        .await
        .expect("query should succeed")
        .expect("announcement should exist");
    assert_eq!(created.kind, "maintenance");
    assert_eq!(created.priority, 8);
    assert!(created.is_pinned);
    assert_eq!(
        created.author_id.as_deref(),
        Some(crate::tests::TEST_ADMIN_USER_ID)
    );
    assert_eq!(created.start_time_unix_secs, Some(1_774_828_800));
    assert_eq!(created.end_time_unix_secs, Some(1_774_915_200));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_updates_announcement_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/announcements/{announcement_id}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-1".to_string(),
            "系统维护".to_string(),
            "今天维护".to_string(),
            "maintenance".to_string(),
            10,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            1_711_000_000,
            1_711_000_100,
        )
        .expect("announcement should build"),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_repository_for_tests(Arc::clone(
                    &announcement_repository,
                )),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .put(format!("{gateway_url}/api/announcements/announcement-1"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "title": "系统升级",
            "content": "维护延长",
            "type": "important",
            "priority": 99,
            "is_active": false,
            "is_pinned": false,
            "start_time": "2026-03-30T12:00:00Z",
            "end_time": "2026-03-31T12:00:00Z"
        }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["message"], "公告更新成功");

    let updated = announcement_repository
        .find_by_id("announcement-1")
        .await
        .expect("query should succeed")
        .expect("announcement should exist");
    assert_eq!(updated.title, "系统升级");
    assert_eq!(updated.content, "维护延长");
    assert_eq!(updated.kind, "important");
    assert_eq!(updated.priority, 99);
    assert!(!updated.is_active);
    assert!(!updated.is_pinned);
    assert_eq!(updated.start_time_unix_secs, Some(1_774_872_000));
    assert_eq!(updated.end_time_unix_secs, Some(1_774_958_400));
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_deletes_announcement_locally_with_trusted_admin_principal() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/announcements/{announcement_id}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-1".to_string(),
            "系统维护".to_string(),
            "今天维护".to_string(),
            "maintenance".to_string(),
            10,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            1_711_000_000,
            1_711_000_100,
        )
        .expect("announcement should build"),
    ]));

    let (_upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_repository_for_tests(Arc::clone(
                    &announcement_repository,
                )),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .delete(format!("{gateway_url}/api/announcements/announcement-1"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["message"], "公告已删除");

    let deleted = announcement_repository
        .find_by_id("announcement-1")
        .await
        .expect("query should succeed");
    assert!(deleted.is_none());

    let page = announcement_repository
        .list_announcements(&AnnouncementListQuery {
            active_only: false,
            offset: 0,
            limit: 10,
            now_unix_secs: None,
        })
        .await
        .expect("list should succeed");
    assert_eq!(page.total, 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_admin_announcement_writes_without_writer() {
    let upstream_hits = Arc::new(Mutex::new(0usize));
    let upstream_hits_clone = Arc::clone(&upstream_hits);
    let upstream = Router::new().route(
        "/api/announcements/{*path}",
        any(move |_request: Request| {
            let upstream_hits_inner = Arc::clone(&upstream_hits_clone);
            async move {
                *upstream_hits_inner.lock().expect("mutex should lock") += 1;
                (StatusCode::OK, Body::from("unexpected upstream hit"))
            }
        }),
    );

    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-1".to_string(),
            "系统维护".to_string(),
            "今天维护".to_string(),
            "maintenance".to_string(),
            10,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            1_711_000_000,
            1_711_000_100,
        )
        .expect("announcement should build"),
    ]));

    let (_fallback_probe_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_state(
        AppState::new()
            .expect("gateway should build")
            .with_data_state_for_tests(
                crate::data::GatewayDataState::with_announcement_reader_for_tests(
                    announcement_repository,
                ),
            ),
    );
    let (gateway_url, gateway_handle) = start_server(gateway).await;
    let client = reqwest::Client::new();

    let create_response = client
        .post(format!("{gateway_url}/api/announcements"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "title": "系统维护",
            "content": "今晚维护",
            "type": "maintenance"
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(create_payload["detail"], "公告写入暂不可用");

    let update_response = client
        .put(format!("{gateway_url}/api/announcements/announcement-1"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .json(&json!({
            "title": "系统升级"
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(update_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let update_payload: serde_json::Value = update_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(update_payload["detail"], "公告写入暂不可用");

    let delete_response = client
        .delete(format!("{gateway_url}/api/announcements/announcement-1"))
        .bearer_auth(crate::tests::test_admin_access_token())
        .header("x-client-device-id", crate::tests::TEST_ADMIN_DEVICE_ID)
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(delete_response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let delete_payload: serde_json::Value = delete_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(delete_payload["detail"], "公告写入暂不可用");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_reads_announcement_unread_count_locally_without_proxying_upstream() {
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
                json!("session-announcement-user-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed_with_reads(
        vec![
            StoredAnnouncement::new(
                "announcement-unread".to_string(),
                "未读公告".to_string(),
                "仍然未读".to_string(),
                "info".to_string(),
                10,
                true,
                false,
                false,
                Some("admin-1".to_string()),
                Some("admin".to_string()),
                None,
                None,
                now.timestamp(),
                now.timestamp(),
            )
            .expect("announcement should build"),
            StoredAnnouncement::new(
                "announcement-read".to_string(),
                "已读公告".to_string(),
                "已经读过".to_string(),
                "maintenance".to_string(),
                8,
                true,
                true,
                false,
                Some("admin-1".to_string()),
                Some("admin".to_string()),
                None,
                None,
                now.timestamp(),
                now.timestamp(),
            )
            .expect("announcement should build"),
            StoredAnnouncement::new(
                "announcement-inactive".to_string(),
                "停用公告".to_string(),
                "不计入未读".to_string(),
                "warning".to_string(),
                6,
                false,
                false,
                false,
                Some("admin-1".to_string()),
                Some("admin".to_string()),
                None,
                None,
                now.timestamp(),
                now.timestamp(),
            )
            .expect("announcement should build"),
        ],
        [("user-auth-1".to_string(), "announcement-read".to_string())],
    ));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-user-1",
                "device-announcement-user-1",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/announcements/users/me/unread-count"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["unread_count"], 1);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_lists_required_unread_announcements_locally_without_proxying_upstream() {
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
                json!("session-announcement-required-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed_with_reads(
        vec![
            StoredAnnouncement::new(
                "announcement-required".to_string(),
                "必读公告".to_string(),
                "需要确认".to_string(),
                "important".to_string(),
                20,
                true,
                false,
                true,
                Some("admin-1".to_string()),
                Some("admin".to_string()),
                None,
                None,
                now.timestamp(),
                now.timestamp(),
            )
            .expect("announcement should build"),
            StoredAnnouncement::new(
                "announcement-normal".to_string(),
                "普通公告".to_string(),
                "不需要弹窗".to_string(),
                "info".to_string(),
                10,
                true,
                false,
                false,
                Some("admin-1".to_string()),
                Some("admin".to_string()),
                None,
                None,
                now.timestamp(),
                now.timestamp(),
            )
            .expect("announcement should build"),
            StoredAnnouncement::new(
                "announcement-read-required".to_string(),
                "已读必读公告".to_string(),
                "已经确认".to_string(),
                "warning".to_string(),
                8,
                true,
                false,
                true,
                Some("admin-1".to_string()),
                Some("admin".to_string()),
                None,
                None,
                now.timestamp(),
                now.timestamp(),
            )
            .expect("announcement should build"),
        ],
        [(
            "user-auth-1".to_string(),
            "announcement-read-required".to_string(),
        )],
    ));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-required-1",
                "device-announcement-required-1",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!(
            "{gateway_url}/api/announcements/users/me/required-unread"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-required-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["total"], 1);
    assert_eq!(payload["items"][0]["id"], "announcement-required");
    assert_eq!(payload["items"][0]["requires_ack"], true);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_marks_announcement_read_status_locally_without_proxying_upstream() {
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
                json!("session-announcement-user-2"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-to-read".to_string(),
            "待读公告".to_string(),
            "读完就归零".to_string(),
            "important".to_string(),
            20,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-user-2",
                "device-announcement-user-2",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;
    let client = reqwest::Client::new();

    let patch_response = client
        .patch(format!(
            "{gateway_url}/api/announcements/announcement-to-read/read-status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-2")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_read": true }))
        .send()
        .await
        .expect("patch request should succeed");

    assert_eq!(patch_response.status(), StatusCode::OK);
    let patch_payload: serde_json::Value =
        patch_response.json().await.expect("json body should parse");
    assert_eq!(patch_payload["message"], "公告已标记为已读");

    let unread_response = client
        .get(format!(
            "{gateway_url}/api/announcements/users/me/unread-count"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-2")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("count request should succeed");

    assert_eq!(unread_response.status(), StatusCode::OK);
    let unread_payload: serde_json::Value = unread_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(unread_payload["unread_count"], 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_marks_all_announcements_read_locally_without_proxying_upstream() {
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
                json!("session-announcement-user-3"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-a".to_string(),
            "公告 A".to_string(),
            "内容 A".to_string(),
            "info".to_string(),
            10,
            true,
            false,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
        StoredAnnouncement::new(
            "announcement-b".to_string(),
            "公告 B".to_string(),
            "内容 B".to_string(),
            "warning".to_string(),
            8,
            false,
            false,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
        StoredAnnouncement::new(
            "announcement-c".to_string(),
            "公告 C".to_string(),
            "内容 C".to_string(),
            "important".to_string(),
            6,
            true,
            true,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-user-3",
                "device-announcement-user-3",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;
    let client = reqwest::Client::new();

    let mark_response = client
        .post(format!("{gateway_url}/api/announcements/read-all"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-3")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("post request should succeed");

    assert_eq!(mark_response.status(), StatusCode::OK);
    let mark_payload: serde_json::Value =
        mark_response.json().await.expect("json body should parse");
    assert_eq!(mark_payload["message"], "已全部标记为已读");

    let unread_response = client
        .get(format!(
            "{gateway_url}/api/announcements/users/me/unread-count"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-3")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("count request should succeed");

    assert_eq!(unread_response.status(), StatusCode::OK);
    let unread_payload: serde_json::Value = unread_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(unread_payload["unread_count"], 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_announcement_user_routes_with_trailing_slash_locally() {
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
                json!("session-announcement-user-4"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-trailing".to_string(),
            "尾斜杠公告".to_string(),
            "路径一致性".to_string(),
            "info".to_string(),
            10,
            true,
            false,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-user-4",
                "device-announcement-user-4",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;
    let client = reqwest::Client::new();

    let unread_before = client
        .get(format!(
            "{gateway_url}/api/announcements/users/me/unread-count/"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-4")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(unread_before.status(), StatusCode::OK);
    let unread_before_payload: serde_json::Value =
        unread_before.json().await.expect("json body should parse");
    assert_eq!(unread_before_payload["unread_count"], 1);

    let patch_response = client
        .patch(format!(
            "{gateway_url}/api/announcements/announcement-trailing/read-status/"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-4")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_read": true }))
        .send()
        .await
        .expect("patch request should succeed");
    assert_eq!(patch_response.status(), StatusCode::OK);

    let unread_after = client
        .get(format!(
            "{gateway_url}/api/announcements/users/me/unread-count/"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-4")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(unread_after.status(), StatusCode::OK);
    let unread_after_payload: serde_json::Value =
        unread_after.json().await.expect("json body should parse");
    assert_eq!(unread_after_payload["unread_count"], 0);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_not_found_for_missing_announcement_read_status_locally() {
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
                json!("session-announcement-user-3"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-user-3",
                "device-announcement-user-3",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;

    let response = reqwest::Client::new()
        .patch(format!(
            "{gateway_url}/api/announcements/missing-announcement/read-status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-user-3")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_read": true }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "Announcement not found");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_cannot_mark_future_required_announcement_as_read() {
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
                json!("session-announcement-future"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let starts_at = now.timestamp().max(0) as u64 + 3600;
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-future-required".to_string(),
            "Future required notice".to_string(),
            "Not public yet".to_string(),
            "warning".to_string(),
            10,
            true,
            false,
            true,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            Some(starts_at as i64),
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-future",
                "device-announcement-future",
                "refresh-token-placeholder",
                now,
            )],
            Arc::clone(&announcement_repository),
        )
        .await;

    let response = reqwest::Client::new()
        .patch(format!(
            "{gateway_url}/api/announcements/announcement-future-required/read-status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-announcement-future")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_read": true }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["detail"], "Announcement not found");
    assert_eq!(
        announcement_repository
            .count_unread_active_announcements("user-auth-1", starts_at)
            .await
            .expect("unread count should load"),
        1
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_invalid_nested_announcement_paths_as_local_not_found_without_hitting_upstream(
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
                json!("session-announcement-user-invalid-nested"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let announcement_repository = Arc::new(InMemoryAnnouncementReadRepository::seed(vec![
        StoredAnnouncement::new(
            "announcement-nested".to_string(),
            "嵌套路由公告".to_string(),
            "用于校验无效 announcement 子路径".to_string(),
            "info".to_string(),
            10,
            true,
            false,
            false,
            Some("admin-1".to_string()),
            Some("admin".to_string()),
            None,
            None,
            now.timestamp(),
            now.timestamp(),
        )
        .expect("announcement should build"),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_announcement_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-announcement-user-invalid-nested",
                "device-announcement-user-invalid-nested",
                "refresh-token-placeholder",
                now,
            )],
            announcement_repository,
        )
        .await;
    let client = reqwest::Client::new();

    let detail_response = client
        .get(format!(
            "{gateway_url}/api/announcements/announcement-nested/history"
        ))
        .send()
        .await
        .expect("request should succeed");

    assert_local_route_not_found_response(detail_response).await;

    let read_status_response = client
        .patch(format!(
            "{gateway_url}/api/announcements/announcement-nested/history/read-status"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header(
            "x-client-device-id",
            "device-announcement-user-invalid-nested",
        )
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "is_read": true }))
        .send()
        .await
        .expect("request should succeed");

    assert_local_route_not_found_response(read_status_response).await;

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}
