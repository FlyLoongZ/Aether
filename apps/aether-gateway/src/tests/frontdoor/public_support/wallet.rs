use super::*;

#[tokio::test]
async fn gateway_redeems_wallet_code_locally() {
    let now = Utc::now();
    let user = sample_auth_user(now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-wallet-redeem-1")),
        ]),
        now + chrono::Duration::hours(1),
    );

    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![sample_auth_wallet(
        "user-auth-1",
        now,
    )]));
    let batch = wallet_repository
        .create_admin_redeem_code_batch(
            aether_data::repository::wallet::CreateAdminRedeemCodeBatchInput {
                name: "测试兑换".to_string(),
                amount_usd: 6.5,
                currency: "USD".to_string(),
                balance_bucket: "gift".to_string(),
                total_count: 1,
                expires_at_unix_secs: None,
                description: Some("public support".to_string()),
                created_by: Some("admin-user-1".to_string()),
            },
        )
        .await
        .expect("batch should create");
    let redeem_code = batch.codes[0].code.clone();

    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(
            crate::data::GatewayDataState::with_user_and_wallet_for_tests(
                user_repository,
                wallet_repository,
            ),
        )
        .with_auth_sessions_for_tests([sample_auth_session(
            "user-auth-1",
            "session-wallet-redeem-1",
            "device-wallet-redeem-1",
            "refresh-token-wallet-redeem-1",
            now,
        )]);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/wallet/redeem"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-redeem-1")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "code": redeem_code }))
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["batch_name"], "测试兑换");
    assert_eq!(payload["amount_usd"], 6.5);
    assert_eq!(payload["order"]["payment_method"], "gift_code");
    assert_eq!(payload["order"]["refundable_amount_usd"], 0.0);
    assert_eq!(payload["wallet"]["total_recharged"], 26.5);
    assert_eq!(payload["wallet"]["recharge_balance"], 12.5);
    assert_eq!(payload["wallet"]["gift_balance"], 9.5);
    assert_eq!(payload["wallet"]["refundable_balance"], 12.5);

    gateway_handle.abort();
}

#[tokio::test]
async fn gateway_creates_wallet_recharge_orders_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-wallet-recharge-create".to_string(),
        Some("wallet-recharge-create@example.com".to_string()),
        true,
        "wallet_recharge_create_user".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build");
    let wallet = StoredWalletSnapshot::new(
        "wallet-recharge-create".to_string(),
        Some(user.id.clone()),
        None,
        12.5,
        3.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        20.0,
        4.5,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build");
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-wallet-recharge-create-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            wallet,
            [sample_auth_session(
                "user-wallet-recharge-create",
                "session-wallet-recharge-create-1",
                "device-wallet-recharge-create-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/wallet/recharge"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-recharge-create-1")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "amount_usd": 10.0,
            "payment_method": "manual",
            "pay_amount": 72.5,
            "pay_currency": "CNY",
            "exchange_rate": 7.25,
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    assert_eq!(
        create_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    let order_id = create_payload["order"]["id"]
        .as_str()
        .expect("order id should exist")
        .to_string();
    assert_eq!(
        create_payload["order"]["wallet_id"],
        "wallet-recharge-create"
    );
    assert_eq!(create_payload["order"]["amount_usd"], 10.0);
    assert_eq!(create_payload["order"]["pay_amount"], 72.5);
    assert_eq!(create_payload["order"]["pay_currency"], "CNY");
    assert_eq!(create_payload["order"]["exchange_rate"], 7.25);
    assert_eq!(create_payload["order"]["payment_method"], "manual");
    assert_eq!(create_payload["order"]["status"], "pending");
    assert_eq!(create_payload["payment_instructions"]["gateway"], "manual");
    assert!(create_payload["payment_instructions"]["payment_url"].is_null());
    assert!(create_payload["payment_instructions"]["qr_code"].is_null());
    assert_eq!(
        create_payload["payment_instructions"]["instructions"],
        "请线下确认到账后由管理员处理"
    );

    let list_response = client
        .get(format!(
            "{gateway_url}/api/wallet/recharge?limit=20&offset=0"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-recharge-create-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(list_response.status(), StatusCode::OK);
    let list_payload: serde_json::Value =
        list_response.json().await.expect("json body should parse");
    assert_eq!(list_payload["total"], 1);
    assert_eq!(
        list_payload["items"]
            .as_array()
            .and_then(|items| items.first())
            .and_then(|item| item.get("id"))
            .and_then(serde_json::Value::as_str),
        Some(order_id.as_str())
    );

    let detail_response = client
        .get(format!("{gateway_url}/api/wallet/recharge/{order_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-recharge-create-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::OK);
    assert_eq!(
        detail_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["order"]["id"], order_id);
    assert_eq!(
        detail_payload["order"]["gateway_response"]["gateway"],
        "manual"
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_reuses_pending_billing_plan_checkout_order_without_proxying_upstream() {
    let now = Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-billing-checkout-reuse".to_string(),
        Some("billing-checkout-reuse@example.com".to_string()),
        true,
        "billing_checkout_reuse_user".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build");
    let wallet = StoredWalletSnapshot::new(
        "wallet-billing-checkout-reuse".to_string(),
        Some(user.id.clone()),
        None,
        12.5,
        3.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        20.0,
        4.5,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build");
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-billing-checkout-reuse"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let billing_repository = Arc::new(InMemoryBillingReadRepository::seed(Vec::new()));
    let encrypted_key = encrypt_python_fernet_plaintext(DEVELOPMENT_ENCRYPTION_KEY, "epay-secret")
        .expect("merchant key should encrypt");
    let AdminBillingMutationOutcome::Applied(_) = billing_repository
        .upsert_payment_gateway_config(&PaymentGatewayConfigWriteInput {
            provider: "epay".to_string(),
            enabled: true,
            endpoint_url: "https://pay.example.com/".to_string(),
            callback_base_url: Some("https://app.example.com".to_string()),
            merchant_id: "merchant-1".to_string(),
            merchant_key_encrypted: Some(encrypted_key),
            preserve_existing_secret: false,
            pay_currency: "CNY".to_string(),
            usd_exchange_rate: 7.25,
            min_recharge_usd: 1.0,
            channels_json: json!([
                {
                    "channel": "alipay",
                    "display_name": "支付宝"
                }
            ]),
        })
        .await
        .expect("gateway config should create")
    else {
        panic!("gateway config should apply");
    };
    let plan = match billing_repository
        .create_billing_plan(&BillingPlanWriteInput {
            title: "每日额度月卡".to_string(),
            description: Some("测试套餐".to_string()),
            price_amount: 100.0,
            price_currency: "CNY".to_string(),
            duration_unit: "month".to_string(),
            duration_value: 1,
            enabled: true,
            sort_order: 1,
            max_active_per_user: 1,
            purchase_limit_scope: "active_period".to_string(),
            entitlements_json: json!([
                {
                    "type": "daily_quota",
                    "daily_quota_usd": 50.0,
                    "reset_timezone": "Asia/Shanghai",
                    "allow_wallet_overage": false
                }
            ]),
        })
        .await
        .expect("billing plan should create")
    {
        AdminBillingMutationOutcome::Applied(plan) => plan,
        other => panic!("billing plan should apply, got {other:?}"),
    };

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
    let user_repository = Arc::new(InMemoryUserReadRepository::seed_auth_users(vec![user]));
    let wallet_repository = Arc::new(InMemoryWalletRepository::seed(vec![wallet]));
    let state = AppState::new()
        .expect("gateway should build")
        .with_data_state_for_tests(GatewayDataState::with_user_billing_and_wallet_for_tests(
            user_repository,
            billing_repository,
            wallet_repository,
        ))
        .with_auth_sessions_for_tests([sample_auth_session(
            "user-billing-checkout-reuse",
            "session-billing-checkout-reuse",
            "device-billing-checkout-reuse",
            "refresh-token-placeholder",
            now,
        )]);
    let gateway = build_router_with_state(state);
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let checkout_body = json!({
        "payment_provider": "epay",
        "payment_method": "epay",
        "payment_channel": "alipay",
    });
    let first_response = client
        .post(format!(
            "{gateway_url}/api/billing/plans/{}/checkout",
            plan.id
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-billing-checkout-reuse")
        .header("user-agent", "AetherTest/1.0")
        .json(&checkout_body)
        .send()
        .await
        .expect("first checkout request should succeed");
    assert_eq!(first_response.status(), StatusCode::OK);
    assert_eq!(
        first_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let first_payload: serde_json::Value = first_response
        .json()
        .await
        .expect("first checkout json should parse");
    let first_order_id = first_payload["order"]["id"]
        .as_str()
        .expect("first order id should exist")
        .to_string();
    assert_eq!(first_payload["order"]["status"], "pending");
    assert_eq!(first_payload["order"]["product_id"], plan.id);
    assert_eq!(
        first_payload["reused_pending_order"],
        serde_json::Value::Null
    );

    let second_response = client
        .post(format!(
            "{gateway_url}/api/billing/plans/{}/checkout",
            plan.id
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-billing-checkout-reuse")
        .header("user-agent", "AetherTest/1.0")
        .json(&checkout_body)
        .send()
        .await
        .expect("second checkout request should succeed");
    assert_eq!(second_response.status(), StatusCode::OK);
    assert_eq!(
        second_response
            .headers()
            .get("cache-control")
            .and_then(|value| value.to_str().ok()),
        Some("no-store")
    );
    let second_payload: serde_json::Value = second_response
        .json()
        .await
        .expect("second checkout json should parse");
    assert_eq!(second_payload["order"]["id"], first_order_id);
    assert_eq!(second_payload["reused_pending_order"], true);
    assert_eq!(
        second_payload["payment_instructions"],
        first_payload["payment_instructions"]
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_creates_wallet_refunds_locally_without_proxying_upstream() {
    let now = Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-wallet-refund-create".to_string(),
        Some("refund-create@example.com".to_string()),
        true,
        "refund_create_user".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build");
    let wallet = StoredWalletSnapshot::new(
        "wallet-refund-create".to_string(),
        Some(user.id.clone()),
        None,
        12.5,
        3.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        20.0,
        4.5,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build");
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-wallet-refund-create"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            wallet,
            [sample_auth_session(
                "user-wallet-refund-create",
                "session-wallet-refund-create",
                "device-wallet-refund-create",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/wallet/refunds"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-refund-create")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "amount_usd": 5.0,
            "reason": "need refund",
            "idempotency_key": "wallet-refund-create-key",
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    let refund_id = create_payload["id"]
        .as_str()
        .expect("refund id should exist")
        .to_string();
    assert_eq!(create_payload["status"], "pending_approval");
    assert_eq!(create_payload["source_type"], "wallet_balance");
    assert_eq!(create_payload["refund_mode"], "offline_payout");
    assert_eq!(create_payload["amount_usd"], 5.0);
    assert_eq!(create_payload["reason"], "need refund");

    let idempotent_response = client
        .post(format!("{gateway_url}/api/wallet/refunds"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-refund-create")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "amount_usd": 5.0,
            "reason": "need refund",
            "idempotency_key": "wallet-refund-create-key",
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(idempotent_response.status(), StatusCode::OK);
    let idempotent_payload: serde_json::Value = idempotent_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(idempotent_payload["id"], refund_id);

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_wallet_refunds_that_exceed_available_balance_locally() {
    let now = Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-wallet-refund-overdraft".to_string(),
        Some("refund-overdraft@example.com".to_string()),
        true,
        "refund_overdraft_user".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build");
    let wallet = StoredWalletSnapshot::new(
        "wallet-refund-overdraft".to_string(),
        Some(user.id.clone()),
        None,
        12.5,
        3.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        20.0,
        4.5,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build");
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-wallet-refund-overdraft"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            wallet,
            [sample_auth_session(
                "user-wallet-refund-overdraft",
                "session-wallet-refund-overdraft",
                "device-wallet-refund-overdraft",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/api/wallet/refunds"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-refund-overdraft")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({ "amount_usd": 20.0 }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(
        payload["detail"],
        "refund amount exceeds available refundable recharge balance"
    );
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_wallet_read_routes_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-wallet-read-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-read-1",
                "device-wallet-read-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();

    let transactions_response = client
        .get(format!(
            "{gateway_url}/api/wallet/transactions?limit=20&offset=0"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-read-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(transactions_response.status(), StatusCode::OK);
    let transactions_payload: serde_json::Value = transactions_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(transactions_payload["wallet"]["id"], "wallet-auth-1");
    assert_eq!(transactions_payload["total"], 0);
    assert_eq!(
        transactions_payload["items"].as_array().map(Vec::len),
        Some(0)
    );

    let flow_response = client
        .get(format!("{gateway_url}/api/wallet/flow?limit=20&offset=0"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-read-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(flow_response.status(), StatusCode::OK);
    let flow_payload: serde_json::Value =
        flow_response.json().await.expect("json body should parse");
    assert_eq!(flow_payload["wallet"]["id"], "wallet-auth-1");
    assert_eq!(flow_payload["today_entry"]["timezone"], "Asia/Shanghai");
    assert_eq!(flow_payload["today_entry"]["is_today"], true);
    assert_eq!(flow_payload["total"], 0);
    assert_eq!(flow_payload["items"].as_array().map(Vec::len), Some(0));

    let refunds_response = client
        .get(format!(
            "{gateway_url}/api/wallet/refunds?limit=20&offset=0"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-read-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(refunds_response.status(), StatusCode::OK);
    let refunds_payload: serde_json::Value = refunds_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(refunds_payload["wallet"]["id"], "wallet-auth-1");
    assert_eq!(refunds_payload["total"], 0);
    assert_eq!(refunds_payload["items"].as_array().map(Vec::len), Some(0));

    let refund_detail_response = client
        .get(format!("{gateway_url}/api/wallet/refunds/refund-1"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-read-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(refund_detail_response.status(), StatusCode::NOT_FOUND);
    let refund_detail_payload: serde_json::Value = refund_detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(refund_detail_payload["detail"], "Refund request not found");

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_wallet_recharge_read_routes_locally_without_proxying_upstream() {
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
                json!("session-wallet-recharge-read-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-recharge-read-1",
                "device-wallet-recharge-read-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let list_response = client
        .get(format!(
            "{gateway_url}/api/wallet/recharge?limit=20&offset=0"
        ))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-recharge-read-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(list_response.status(), StatusCode::OK);
    let list_payload: serde_json::Value =
        list_response.json().await.expect("json body should parse");
    assert_eq!(list_payload["wallet"]["id"], "wallet-auth-1");
    assert_eq!(list_payload["total"], 0);
    assert_eq!(list_payload["items"].as_array().map(Vec::len), Some(0));

    let detail_response = client
        .get(format!("{gateway_url}/api/wallet/recharge/order-1"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-recharge-read-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::NOT_FOUND);
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["detail"], "Payment order not found");

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_invalid_wallet_nested_detail_paths_as_local_not_found_without_hitting_upstream(
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
                json!("session-wallet-invalid-detail-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-invalid-detail-1",
                "device-wallet-invalid-detail-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();

    let recharge_response = client
        .get(format!("{gateway_url}/api/wallet/recharge/order-1/confirm"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-invalid-detail-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_local_route_not_found_response(recharge_response).await;

    let refund_response = client
        .get(format!("{gateway_url}/api/wallet/refunds/refund-1/status"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-invalid-detail-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_local_route_not_found_response(refund_response).await;

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_wallet_balance_locally_without_proxying_upstream() {
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
            ("session_id".to_string(), json!("session-wallet-balance-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-balance-1",
                "device-wallet-balance-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/wallet/balance"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-balance-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["balance"], 15.5);
    assert_eq!(payload["recharge_balance"], 12.5);
    assert_eq!(payload["gift_balance"], 3.0);
    assert_eq!(payload["refundable_balance"], 12.5);
    assert_eq!(payload["currency"], "USD");
    assert_eq!(payload["limit_mode"], "finite");
    assert_eq!(payload["unlimited"], false);
    assert_eq!(payload["pending_refund_count"], 0);
    assert_eq!(payload["wallet"]["id"], "wallet-auth-1");
    assert_eq!(payload["wallet"]["status"], "active");
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_handles_wallet_today_cost_locally_without_proxying_upstream() {
    let auth_now = Utc::now();
    let usage_now = wallet_today_usage_test_time();
    let user = sample_auth_user(auth_now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-wallet-today-1")),
        ]),
        auth_now + chrono::Duration::hours(1),
    );
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-wallet-today-1",
            "req-wallet-today-1",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            usage_now - chrono::Duration::minutes(30),
        ),
        sample_user_usage_audit(
            "usage-wallet-old-1",
            "req-wallet-old-1",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            usage_now - chrono::Duration::days(1),
        ),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_usage_state(
            user,
            sample_auth_wallet("user-auth-1", auth_now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-today-1",
                "device-wallet-today-1",
                "refresh-token-placeholder",
                auth_now,
            )],
            usage_repository,
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/wallet/today-cost"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-today-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["is_today"], true);
    assert_eq!(payload["total_requests"], 1);
    assert_eq!(payload["input_tokens"], 120);
    assert_eq!(payload["cache_read_tokens"], 15);
    assert_eq!(payload["total_cost"], 1.25);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_wallet_flow_today_entry_uses_live_settled_usage() {
    let auth_now = Utc::now();
    let usage_now = wallet_today_usage_test_time();
    let user = sample_auth_user(auth_now);
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id)),
            ("role".to_string(), json!(user.role)),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-wallet-flow-today")),
        ]),
        auth_now + chrono::Duration::hours(1),
    );
    let usage_repository = Arc::new(InMemoryUsageReadRepository::seed(vec![
        sample_user_usage_audit(
            "usage-wallet-flow-today",
            "req-wallet-flow-today",
            "user-auth-1",
            "gpt-4.1",
            "OpenAI",
            "completed",
            usage_now,
        ),
    ]));
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_usage_state(
            user,
            sample_auth_wallet("user-auth-1", auth_now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-flow-today",
                "device-wallet-flow-today",
                "refresh-token-placeholder",
                auth_now,
            )],
            usage_repository,
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/wallet/flow?limit=20&offset=0"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-flow-today")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    let payload: serde_json::Value = response.json().await.expect("json body should parse");
    assert_eq!(payload["today_entry"]["is_today"], true);
    assert_eq!(payload["today_entry"]["timezone"], "Asia/Shanghai");
    assert_eq!(payload["today_entry"]["total_requests"], 1);
    assert_eq!(payload["today_entry"]["input_tokens"], 120);
    assert_eq!(payload["today_entry"]["cache_read_tokens"], 15);
    assert_eq!(payload["today_entry"]["total_cost"], 1.25);
    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_returns_service_unavailable_for_wallet_today_cost_without_usage_reader() {
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
                json!("session-wallet-today-unavailable"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            sample_auth_wallet("user-auth-1", now),
            [sample_auth_session(
                "user-auth-1",
                "session-wallet-today-unavailable",
                "device-wallet-today-unavailable",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/api/wallet/today-cost"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-today-unavailable")
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
async fn gateway_handles_payment_callback_route_locally_without_proxying_upstream() {
    let _env_lock = payment_callback_env_lock().lock().await;
    let _secret_guard = set_test_env_var("PAYMENT_CALLBACK_SECRET", TEST_PAYMENT_CALLBACK_SECRET);
    let now = Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-wallet-callback".to_string(),
        Some("wallet-callback@example.com".to_string()),
        true,
        "wallet_callback_user".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build");
    let wallet = StoredWalletSnapshot::new(
        "wallet-callback".to_string(),
        Some(user.id.clone()),
        None,
        0.0,
        0.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        0.0,
        0.0,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build");
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            ("session_id".to_string(), json!("session-wallet-callback-1")),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            wallet,
            [sample_auth_session(
                "user-wallet-callback",
                "session-wallet-callback-1",
                "device-wallet-callback-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/wallet/recharge"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-callback-1")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "amount_usd": 10.0,
            "payment_method": "manual",
            "pay_amount": 72.5,
            "pay_currency": "CNY",
            "exchange_rate": 7.25,
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    let order_id = create_payload["order"]["id"]
        .as_str()
        .expect("order id should exist")
        .to_string();
    let order_no = create_payload["order"]["order_no"]
        .as_str()
        .expect("order no should exist")
        .to_string();

    let callback_body = json!({
        "callback_key": "callback-key-1",
        "order_no": order_no,
        "gateway_order_id": create_payload["order"]["gateway_order_id"],
        "amount_usd": 10.0,
        "pay_amount": 72.5,
        "pay_currency": "CNY",
        "exchange_rate": 7.25,
        "payload": serde_json::Value::Null,
    });
    let callback_signature =
        build_test_payment_callback_signature(&callback_body, TEST_PAYMENT_CALLBACK_SECRET);
    let callback_response = client
        .post(format!("{gateway_url}/api/payment/callback/manual"))
        .header("x-payment-callback-token", TEST_PAYMENT_CALLBACK_SECRET)
        .header("x-payment-callback-signature", callback_signature)
        .json(&callback_body)
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(callback_response.status(), StatusCode::OK);
    let callback_payload: serde_json::Value = callback_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(callback_payload, json!({ "ok": true }));

    let detail_response = client
        .get(format!("{gateway_url}/api/wallet/recharge/{order_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-callback-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["order"]["status"], "credited");
    assert_eq!(detail_payload["order"]["refundable_amount_usd"], 10.0);

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_rejects_payment_callback_with_mismatched_payment_method_locally() {
    let _env_lock = payment_callback_env_lock().lock().await;
    let _secret_guard = set_test_env_var("PAYMENT_CALLBACK_SECRET", TEST_PAYMENT_CALLBACK_SECRET);
    let now = Utc::now();
    let user = StoredUserAuthRecord::new(
        "user-wallet-callback-mismatch".to_string(),
        Some("wallet-callback-mismatch@example.com".to_string()),
        true,
        "wallet_callback_mismatch_user".to_string(),
        Some("$2y$10$.OBQfixAECpsb8V/VS3csOMf00x2E/jD/gnud20t6RG0yiQosyOZ2".to_string()),
        "user".to_string(),
        "local".to_string(),
        Some(json!(["openai"])),
        Some(json!(["openai:chat"])),
        Some(json!(["gpt-5"])),
        true,
        false,
        Some(now),
        Some(now),
    )
    .expect("auth user should build");
    let wallet = StoredWalletSnapshot::new(
        "wallet-callback-mismatch".to_string(),
        Some(user.id.clone()),
        None,
        0.0,
        0.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        0.0,
        0.0,
        0.0,
        0.0,
        now.timestamp(),
    )
    .expect("wallet should build");
    let access_token = build_test_auth_token(
        "access",
        serde_json::Map::from_iter([
            ("user_id".to_string(), json!(user.id.clone())),
            ("role".to_string(), json!(user.role.clone())),
            (
                "created_at".to_string(),
                json!(user.created_at.map(|value| value.to_rfc3339())),
            ),
            (
                "session_id".to_string(),
                json!("session-wallet-callback-mismatch-1"),
            ),
        ]),
        now + chrono::Duration::hours(1),
    );
    let (gateway_url, upstream_hits, gateway_handle, upstream_handle) =
        start_auth_gateway_with_state(
            user,
            wallet,
            [sample_auth_session(
                "user-wallet-callback-mismatch",
                "session-wallet-callback-mismatch-1",
                "device-wallet-callback-mismatch-1",
                "refresh-token-placeholder",
                now,
            )],
        )
        .await;

    let client = reqwest::Client::new();
    let create_response = client
        .post(format!("{gateway_url}/api/wallet/recharge"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-callback-mismatch-1")
        .header("user-agent", "AetherTest/1.0")
        .json(&json!({
            "amount_usd": 10.0,
            "payment_method": "alipay",
            "pay_amount": 72.5,
            "pay_currency": "CNY",
            "exchange_rate": 7.25,
        }))
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(create_response.status(), StatusCode::OK);
    let create_payload: serde_json::Value = create_response
        .json()
        .await
        .expect("json body should parse");
    let order_id = create_payload["order"]["id"]
        .as_str()
        .expect("order id should exist")
        .to_string();
    let order_no = create_payload["order"]["order_no"]
        .as_str()
        .expect("order no should exist")
        .to_string();

    let callback_body = json!({
        "callback_key": "callback-key-mismatch-1",
        "order_no": order_no,
        "gateway_order_id": create_payload["order"]["gateway_order_id"],
        "amount_usd": 10.0,
        "pay_amount": 72.5,
        "pay_currency": "CNY",
        "exchange_rate": 7.25,
        "payload": serde_json::Value::Null,
    });
    let callback_signature =
        build_test_payment_callback_signature(&callback_body, TEST_PAYMENT_CALLBACK_SECRET);
    let callback_response = client
        .post(format!("{gateway_url}/api/payment/callback/wechat"))
        .header("x-payment-callback-token", TEST_PAYMENT_CALLBACK_SECRET)
        .header("x-payment-callback-signature", callback_signature)
        .json(&callback_body)
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(callback_response.status(), StatusCode::OK);
    let callback_payload: serde_json::Value = callback_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(callback_payload["ok"], false);
    assert_eq!(callback_payload["error"], "payment callback rejected");
    assert_eq!(
        callback_payload.as_object().map(|value| value.len()),
        Some(2)
    );

    let detail_response = client
        .get(format!("{gateway_url}/api/wallet/recharge/{order_id}"))
        .header("authorization", format!("Bearer {access_token}"))
        .header("x-client-device-id", "device-wallet-callback-mismatch-1")
        .header("user-agent", "AetherTest/1.0")
        .send()
        .await
        .expect("request should succeed");
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_payload: serde_json::Value = detail_response
        .json()
        .await
        .expect("json body should parse");
    assert_eq!(detail_payload["order"]["status"], "pending");
    assert_eq!(detail_payload["order"]["refundable_amount_usd"], 0.0);

    assert_eq!(*upstream_hits.lock().expect("mutex should lock"), 0);
    gateway_handle.abort();
    upstream_handle.abort();
}
