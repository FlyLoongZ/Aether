use super::{InMemoryWalletRepository, WalletReadSeed};
use crate::repository::wallet::{
    AdminWalletListQuery, CompareAndSwapPaymentOrderStripeClientSecretInput,
    CreatePlanPurchaseOrderInput, CreatePlanPurchaseOrderOutcome, CreateWalletRechargeOrderInput,
    CreateWalletRechargeOrderOutcome, CreateWalletRefundRequestInput,
    CreateWalletRefundRequestOutcome, FailWalletRechargeCheckoutInput, StoredAdminPaymentOrder,
    StoredAdminWalletRefund, StoredWalletSnapshot, UpdateWalletRechargeCheckoutInput,
    WalletLookupKey, WalletMutationOutcome, WalletReadRepository, WalletWriteRepository,
};
use crate::DataLayerError;
use serde_json::json;
use std::sync::Arc;

fn sample_wallet() -> StoredWalletSnapshot {
    StoredWalletSnapshot::new(
        "wallet-1".to_string(),
        Some("user-1".to_string()),
        Some("key-1".to_string()),
        10.0,
        2.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        0.0,
        0.0,
        0.0,
        0.0,
        100,
    )
    .expect("wallet should build")
}

#[tokio::test]
async fn compensation_delete_preserves_wallet_with_persisted_balance_in_memory() {
    let wallet = StoredWalletSnapshot::new(
        "funded-wallet".to_string(),
        Some("funded-user".to_string()),
        None,
        1.0,
        0.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        1.0,
        0.0,
        0.0,
        1.0,
        100,
    )
    .expect("wallet should build");
    let repository = InMemoryWalletRepository::seed(vec![wallet]);

    assert!(!repository
        .delete_wallet_if_unreferenced("funded-wallet", WalletLookupKey::UserId("funded-user"))
        .await
        .expect("funded wallet must not be deleted"));
    assert!(repository
        .find(WalletLookupKey::UserId("funded-user"))
        .await
        .expect("wallet lookup should succeed")
        .is_some());
}

#[tokio::test]
async fn provisional_recharge_cleanup_preserves_wallet_changed_after_initial_check_in_memory() {
    let wallet = StoredWalletSnapshot::new(
        "provisional-recharge-wallet".to_string(),
        Some("provisional-recharge-user".to_string()),
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
        100,
    )
    .expect("wallet should build");
    let repository = InMemoryWalletRepository::seed(vec![wallet]);
    repository.with_wallets_mut(|wallets| {
        let wallet = wallets
            .get_mut("provisional-recharge-wallet")
            .expect("wallet should be seeded");
        wallet.balance = 2.0;
        wallet.total_recharged = 2.0;
        wallet.updated_at_unix_secs = 200;
    });

    // This is the same cleanup helper used when a recharge order loses a
    // uniqueness race after creating a provisional wallet. A wallet that
    // acquired funds must survive the compensation attempt.
    repository.remove_created_wallet_if_unreferenced("provisional-recharge-wallet");
    let retained = repository
        .find(WalletLookupKey::UserId("provisional-recharge-user"))
        .await
        .expect("wallet lookup should succeed")
        .expect("changed wallet should remain");
    assert_eq!(retained.balance, 2.0);
    assert_eq!(retained.total_recharged, 2.0);
}

#[tokio::test]
async fn snapshot_compensation_deletes_matching_funded_wallet_and_preserves_changed_one() {
    let wallet = StoredWalletSnapshot::new(
        "import-funded-wallet".to_string(),
        Some("import-funded-user".to_string()),
        None,
        12.5,
        3.25,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        12.5,
        0.0,
        0.0,
        0.0,
        4242,
    )
    .expect("wallet should build");
    let repository = InMemoryWalletRepository::seed(vec![wallet.clone()]);
    assert!(repository
        .delete_wallet_if_snapshot_matches_and_unreferenced(
            &wallet,
            WalletLookupKey::UserId("import-funded-user"),
        )
        .await
        .expect("matching snapshot should delete"));
    assert!(repository
        .find(WalletLookupKey::WalletId("import-funded-wallet"))
        .await
        .expect("wallet lookup should succeed")
        .is_none());

    let repository = InMemoryWalletRepository::seed(vec![wallet.clone()]);
    repository.with_wallets_mut(|wallets| {
        wallets
            .get_mut("import-funded-wallet")
            .expect("wallet should be seeded")
            .balance = 99.0;
    });
    assert!(!repository
        .delete_wallet_if_snapshot_matches_and_unreferenced(
            &wallet,
            WalletLookupKey::UserId("import-funded-user"),
        )
        .await
        .expect("changed snapshot should be retained"));
    assert!(repository
        .find(WalletLookupKey::WalletId("import-funded-wallet"))
        .await
        .expect("wallet lookup should succeed")
        .is_some());
}

#[tokio::test]
async fn snapshot_restore_is_compare_and_swap_in_memory() {
    let before = StoredWalletSnapshot::new(
        "existing-wallet".to_string(),
        Some("existing-user".to_string()),
        None,
        4.0,
        2.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        7.0,
        3.0,
        0.0,
        0.0,
        100,
    )
    .expect("wallet should build");
    let mut after = before.clone();
    after.balance = 25.0;
    after.gift_balance = 5.0;
    after.total_recharged = 28.0;
    after.updated_at_unix_secs = 200;

    let repository = InMemoryWalletRepository::seed(vec![after.clone()]);
    assert!(repository
        .restore_wallet_if_snapshot_matches(
            &before,
            &after,
            WalletLookupKey::UserId("existing-user"),
        )
        .await
        .expect("matching post-state should restore"));
    assert_eq!(
        repository
            .find(WalletLookupKey::UserId("existing-user"))
            .await
            .expect("wallet lookup should succeed"),
        Some(before.clone())
    );

    let repository = InMemoryWalletRepository::seed(vec![after.clone()]);
    repository.with_wallets_mut(|wallets| {
        let wallet = wallets
            .get_mut("existing-wallet")
            .expect("wallet should be seeded");
        wallet.balance = 99.0;
        wallet.updated_at_unix_secs = 300;
    });
    assert!(!repository
        .restore_wallet_if_snapshot_matches(
            &before,
            &after,
            WalletLookupKey::UserId("existing-user"),
        )
        .await
        .expect("changed post-state should fail closed"));
    let retained = repository
        .find(WalletLookupKey::UserId("existing-user"))
        .await
        .expect("wallet lookup should succeed")
        .expect("changed wallet should remain");
    assert_eq!(retained.balance, 99.0);
    assert_eq!(retained.updated_at_unix_secs, 300);
}

#[tokio::test]
async fn updates_auth_wallet_limit_mode_and_snapshot_in_memory() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);

    let limit_updated = repository
        .update_auth_user_wallet_limit_mode("user-1", "unlimited")
        .await
        .expect("limit mode update should succeed")
        .expect("wallet should update");
    assert_eq!(limit_updated.limit_mode, "unlimited");

    let snapshot_updated = repository
        .update_auth_api_key_wallet_snapshot(
            "key-1",
            20.0,
            4.0,
            "finite",
            "USD",
            "active",
            30.0,
            5.0,
            1.0,
            2.0,
            Some(777),
        )
        .await
        .expect("snapshot update should succeed")
        .expect("wallet should update");
    assert_eq!(snapshot_updated.balance, 20.0);
    assert_eq!(snapshot_updated.gift_balance, 4.0);
    assert_eq!(snapshot_updated.total_recharged, 30.0);
    assert_eq!(snapshot_updated.total_consumed, 5.0);
    assert_eq!(snapshot_updated.total_refunded, 1.0);
    assert_eq!(snapshot_updated.total_adjusted, 2.0);
    assert_eq!(snapshot_updated.updated_at_unix_secs, 777);

    assert!(repository
        .update_auth_user_wallet_limit_mode("missing-user", "finite")
        .await
        .expect("missing limit mode update should succeed")
        .is_none());

    let user_wallet = repository
        .initialize_auth_user_wallet("user-2", 7.0, false)
        .await
        .expect("user wallet init should succeed")
        .expect("user wallet should initialize");
    assert_eq!(user_wallet.user_id.as_deref(), Some("user-2"));
    assert_eq!(user_wallet.gift_balance, 7.0);
    assert_eq!(user_wallet.total_adjusted, 7.0);

    let api_key_wallet = repository
        .initialize_auth_api_key_wallet("key-2", 7.0, true)
        .await
        .expect("api key wallet init should succeed")
        .expect("api key wallet should initialize");
    assert_eq!(api_key_wallet.api_key_id.as_deref(), Some("key-2"));
    assert_eq!(api_key_wallet.limit_mode, "unlimited");
    assert_eq!(api_key_wallet.gift_balance, 0.0);
}

fn sample_payment_order(id: &str, user_id: Option<&str>, status: &str) -> StoredAdminPaymentOrder {
    StoredAdminPaymentOrder {
        id: id.to_string(),
        order_no: format!("order-no-{id}"),
        wallet_id: "wallet-1".to_string(),
        user_id: user_id.map(str::to_string),
        amount_usd: 10.0,
        pay_amount: None,
        pay_currency: None,
        exchange_rate: None,
        refunded_amount_usd: 0.0,
        refundable_amount_usd: 10.0,
        payment_method: "stripe".to_string(),
        payment_provider: Some("stripe".to_string()),
        order_kind: "wallet_recharge".to_string(),
        gateway_order_id: None,
        gateway_response: None,
        status: status.to_string(),
        created_at_unix_ms: 100,
        paid_at_unix_secs: None,
        credited_at_unix_secs: None,
        expires_at_unix_secs: None,
    }
}

fn stripe_secret_cas_input(
    order: &StoredAdminPaymentOrder,
    expected_gateway_response: serde_json::Value,
    expected_ciphertext: &str,
    replacement_ciphertext: &str,
) -> CompareAndSwapPaymentOrderStripeClientSecretInput {
    CompareAndSwapPaymentOrderStripeClientSecretInput {
        order_id: order.id.clone(),
        order_no: order.order_no.clone(),
        wallet_id: order.wallet_id.clone(),
        user_id: order.user_id.clone(),
        payment_method: order.payment_method.clone(),
        payment_provider: order.payment_provider.clone(),
        order_kind: order.order_kind.clone(),
        gateway_order_id: order.gateway_order_id.clone(),
        expected_status: order.status.clone(),
        expected_expires_at_unix_secs: order.expires_at_unix_secs,
        expected_gateway_response,
        expected_client_secret_encrypted: expected_ciphertext.to_string(),
        replacement_client_secret_encrypted: replacement_ciphertext.to_string(),
    }
}

#[tokio::test]
async fn stripe_secret_cas_is_exact_and_never_overwrites_a_newer_value_in_memory() {
    let legacy = "gAAAAABlegacy";
    let replacement = concat!(
        "aether-payment-order-stripe-client-secret-v2:",
        "aether-runtime-secret-v1:gAAAAABreplacement"
    );
    let mut order = sample_payment_order("stripe-cas-order", Some("user-1"), "pending");
    order.gateway_order_id = Some("pi-cas".to_string());
    order.expires_at_unix_secs = Some(4_102_444_800);
    order.gateway_response = Some(json!({
        "gateway": "stripe",
        "publishable_key": "pk_test_public",
        "_stripe_client_secret_encrypted": legacy,
    }));
    let observed = order
        .gateway_response
        .clone()
        .expect("fixture response should exist");
    let repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        payment_orders: vec![order.clone()],
        ..WalletReadSeed::default()
    });
    let input = stripe_secret_cas_input(&order, observed.clone(), legacy, replacement);

    let mut stale_json = input.clone();
    stale_json.expected_gateway_response["publishable_key"] = json!("pk_test_changed");
    assert!(!repository
        .compare_and_swap_payment_order_stripe_client_secret(stale_json)
        .await
        .expect("stale JSON should be a normal CAS miss"));

    let mut stale_ciphertext = input.clone();
    stale_ciphertext.expected_client_secret_encrypted = "gAAAAABother".to_string();
    assert!(!repository
        .compare_and_swap_payment_order_stripe_client_secret(stale_ciphertext)
        .await
        .expect("stale ciphertext should be a normal CAS miss"));

    let mut foreign_identity = input.clone();
    foreign_identity.order_no = "order-no-foreign".to_string();
    assert!(!repository
        .compare_and_swap_payment_order_stripe_client_secret(foreign_identity)
        .await
        .expect("foreign identity should be a normal CAS miss"));

    assert!(repository
        .compare_and_swap_payment_order_stripe_client_secret(input.clone())
        .await
        .expect("exact CAS should succeed"));
    assert!(!repository
        .compare_and_swap_payment_order_stripe_client_secret(input)
        .await
        .expect("an old migration must not overwrite the new value"));

    let stored = repository
        .find_admin_payment_order(&order.id)
        .await
        .expect("stored order should be readable")
        .expect("stored order should remain");
    let response = stored
        .gateway_response
        .expect("stored response should remain");
    assert_eq!(
        response["_stripe_client_secret_encrypted"].as_str(),
        Some(replacement)
    );
    assert_eq!(response["publishable_key"], "pk_test_public");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_stripe_secret_migrations_have_exactly_one_winner_in_memory() {
    let legacy = "gAAAAABlegacy-race";
    let mut order = sample_payment_order("stripe-cas-race", Some("user-1"), "pending");
    order.expires_at_unix_secs = Some(4_102_444_800);
    order.gateway_response = Some(json!({
        "gateway": "stripe",
        "_stripe_client_secret_encrypted": legacy,
    }));
    let observed = order
        .gateway_response
        .clone()
        .expect("fixture response should exist");
    let repository = Arc::new(InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        payment_orders: vec![order.clone()],
        ..WalletReadSeed::default()
    }));
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for suffix in ["winner-a", "winner-b"] {
        let repository = Arc::clone(&repository);
        let barrier = Arc::clone(&barrier);
        let input = stripe_secret_cas_input(
            &order,
            observed.clone(),
            legacy,
            &format!(
                "aether-payment-order-stripe-client-secret-v2:aether-runtime-secret-v1:gAAAAAB{suffix}"
            ),
        );
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository
                .compare_and_swap_payment_order_stripe_client_secret(input)
                .await
        }));
    }
    barrier.wait().await;

    let mut winners = 0;
    for task in tasks {
        if task
            .await
            .expect("migration task should join")
            .expect("migration should not error")
        {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
}

fn sample_refund(id: &str, user_id: Option<&str>, status: &str) -> StoredAdminWalletRefund {
    StoredAdminWalletRefund {
        id: id.to_string(),
        refund_no: format!("refund-no-{id}"),
        wallet_id: "wallet-1".to_string(),
        user_id: user_id.map(str::to_string),
        payment_order_id: None,
        source_type: "wallet_balance".to_string(),
        source_id: None,
        refund_mode: "offline_payout".to_string(),
        amount_usd: 3.0,
        status: status.to_string(),
        reason: None,
        failure_reason: None,
        gateway_refund_id: None,
        payout_method: None,
        payout_reference: None,
        payout_proof: None,
        requested_by: None,
        approved_by: None,
        processed_by: None,
        created_at_unix_ms: 100,
        updated_at_unix_secs: 100,
        processed_at_unix_secs: None,
        completed_at_unix_secs: None,
    }
}

#[tokio::test]
async fn finds_wallet_by_owner() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
    let wallet = repository
        .find(WalletLookupKey::UserId("user-1"))
        .await
        .expect("lookup should succeed")
        .expect("wallet should exist");
    assert_eq!(wallet.id, "wallet-1");
}

#[tokio::test]
async fn lists_admin_wallets_with_filters_and_pagination() {
    let repository = InMemoryWalletRepository::seed(vec![
        sample_wallet(),
        StoredWalletSnapshot::new(
            "wallet-2".to_string(),
            Some("user-2".to_string()),
            None,
            3.0,
            1.0,
            "finite".to_string(),
            "USD".to_string(),
            "inactive".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            90,
        )
        .expect("wallet should build"),
        StoredWalletSnapshot::new(
            "wallet-3".to_string(),
            None,
            Some("key-3".to_string()),
            5.0,
            0.0,
            "unlimited".to_string(),
            "USD".to_string(),
            "active".to_string(),
            0.0,
            0.0,
            0.0,
            0.0,
            110,
        )
        .expect("wallet should build"),
    ]);

    let page = repository
        .list_admin_wallets(&AdminWalletListQuery {
            user_id: None,
            status: Some("active".to_string()),
            owner_type: Some("api_key".to_string()),
            limit: 1,
            offset: 0,
        })
        .await
        .expect("list should succeed");

    assert_eq!(page.total, 2);
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].id, "wallet-3");
    assert_eq!(page.items[0].updated_at_unix_secs, Some(110));

    let query = AdminWalletListQuery {
        user_id: Some("user-2".to_string()),
        owner_type: Some("user".to_string()),
        limit: 1,
        ..Default::default()
    };
    let selected = repository.list_admin_wallets(&query).await.unwrap();
    assert_eq!(selected.total, 1);
    assert_eq!(selected.items[0].id, "wallet-2");
    let missing = repository
        .list_admin_wallets(&AdminWalletListQuery {
            user_id: Some("missing-user".to_string()),
            ..query.clone()
        })
        .await
        .unwrap();
    assert_eq!(missing.total, 0);
    assert!(missing.items.is_empty());
    let beyond_page = repository
        .list_admin_wallets(&AdminWalletListQuery { offset: 1, ..query })
        .await
        .unwrap();
    assert_eq!(beyond_page.total, 1);
    assert!(beyond_page.items.is_empty());
}

#[tokio::test]
async fn daily_usage_queries_default_to_empty_in_memory() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
    let today = repository
        .find_wallet_today_usage("wallet-1", "Asia/Shanghai")
        .await
        .expect("lookup should succeed");
    let history = repository
        .list_wallet_daily_usage_history("wallet-1", "Asia/Shanghai", 20)
        .await
        .expect("history should succeed");

    assert!(today.is_none());
    assert_eq!(history.total, 0);
    assert!(history.items.is_empty());
}

#[tokio::test]
async fn deletes_only_untouched_provisional_auth_wallets_in_memory() {
    let repository = InMemoryWalletRepository::seed(Vec::new());
    let provisional_wallet = repository
        .initialize_auth_user_wallet("provisional-user", 10.0, false)
        .await
        .expect("wallet initialization should succeed")
        .expect("provisional wallet should exist");

    assert!(repository
        .delete_provisional_auth_user_wallet(&provisional_wallet.id, "provisional-user")
        .await
        .expect("provisional cleanup should succeed"));
    assert!(repository
        .find(WalletLookupKey::UserId("provisional-user"))
        .await
        .expect("wallet lookup should succeed")
        .is_none());
}

#[tokio::test]
async fn provisional_cleanup_keeps_wallet_with_financial_activity_in_memory() {
    let wallet = StoredWalletSnapshot::new(
        "active-wallet".to_string(),
        Some("active-user".to_string()),
        None,
        0.0,
        10.0,
        "finite".to_string(),
        "USD".to_string(),
        "active".to_string(),
        0.0,
        1.0,
        0.0,
        10.0,
        1,
    )
    .expect("wallet should build");
    let repository = InMemoryWalletRepository::seed([wallet]);

    assert!(!repository
        .delete_provisional_auth_user_wallet("active-wallet", "active-user")
        .await
        .expect("provisional cleanup should succeed"));
    assert!(repository
        .find(WalletLookupKey::UserId("active-user"))
        .await
        .expect("wallet lookup should succeed")
        .is_some());
}

#[tokio::test]
async fn lifetime_plan_purchase_blocks_duplicate_pending_order_in_memory() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
    let snapshot = json!({
        "id": "first-plan",
        "duration_unit": "month",
        "duration_value": 1,
        "max_active_per_user": 1,
        "purchase_limit_scope": "lifetime",
        "entitlements": [
            {
                "type": "wallet_credit",
                "amount_usd": 1.0,
                "balance_bucket": "gift"
            }
        ]
    });

    let first = repository
        .create_plan_purchase_order(CreatePlanPurchaseOrderInput {
            preferred_wallet_id: None,
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            pay_amount: 7.2,
            pay_currency: "CNY".to_string(),
            exchange_rate: 7.2,
            payment_method: "alipay".to_string(),
            payment_provider: Some("epay".to_string()),
            payment_channel: Some("alipay".to_string()),
            gateway_order_id: "gateway-first-plan-1".to_string(),
            gateway_response: json!({ "checkout": true }),
            order_no: "order-first-plan-1".to_string(),
            product_id: "first-plan".to_string(),
            product_snapshot: snapshot.clone(),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await
        .expect("first plan purchase should resolve");
    assert!(matches!(first, CreatePlanPurchaseOrderOutcome::Created(_)));

    let duplicate = repository
        .create_plan_purchase_order(CreatePlanPurchaseOrderInput {
            preferred_wallet_id: None,
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            pay_amount: 7.2,
            pay_currency: "CNY".to_string(),
            exchange_rate: 7.2,
            payment_method: "alipay".to_string(),
            payment_provider: Some("epay".to_string()),
            payment_channel: Some("alipay".to_string()),
            gateway_order_id: "gateway-first-plan-2".to_string(),
            gateway_response: json!({ "checkout": true }),
            order_no: "order-first-plan-2".to_string(),
            product_id: "first-plan".to_string(),
            product_snapshot: snapshot,
            expires_at_unix_secs: 4_102_444_800,
        })
        .await
        .expect("duplicate plan purchase should resolve");
    assert!(matches!(
        duplicate,
        CreatePlanPurchaseOrderOutcome::ActivePlanLimitReached
    ));

    let unlimited_snapshot = json!({
        "id": "unlimited-plan",
        "duration_unit": "month",
        "duration_value": 1,
        "max_active_per_user": 1,
        "purchase_limit_scope": "unlimited",
        "entitlements": [
            {
                "type": "wallet_credit",
                "amount_usd": 1.0,
                "balance_bucket": "gift"
            }
        ]
    });
    for index in 1..=2 {
        let order = repository
            .create_plan_purchase_order(CreatePlanPurchaseOrderInput {
                preferred_wallet_id: None,
                user_id: "user-1".to_string(),
                amount_usd: 1.0,
                pay_amount: 7.2,
                pay_currency: "CNY".to_string(),
                exchange_rate: 7.2,
                payment_method: "alipay".to_string(),
                payment_provider: Some("epay".to_string()),
                payment_channel: Some("alipay".to_string()),
                gateway_order_id: format!("gateway-unlimited-plan-{index}"),
                gateway_response: json!({ "checkout": true }),
                order_no: format!("order-unlimited-plan-{index}"),
                product_id: "unlimited-plan".to_string(),
                product_snapshot: unlimited_snapshot.clone(),
                expires_at_unix_secs: 4_102_444_800,
            })
            .await
            .expect("unlimited plan purchase should resolve");
        assert!(matches!(order, CreatePlanPurchaseOrderOutcome::Created(_)));
    }
}

#[tokio::test]
async fn plan_purchase_rejects_malformed_wallet_credit_in_memory() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
    let result = repository
        .create_plan_purchase_order(CreatePlanPurchaseOrderInput {
            preferred_wallet_id: None,
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            pay_amount: 1.0,
            pay_currency: "USD".to_string(),
            exchange_rate: 1.0,
            payment_method: "stripe".to_string(),
            payment_provider: Some("stripe".to_string()),
            payment_channel: Some("card".to_string()),
            gateway_order_id: "gateway-invalid-wallet-credit".to_string(),
            gateway_response: json!({ "checkout": true }),
            order_no: "order-invalid-wallet-credit".to_string(),
            product_id: "invalid-wallet-credit-plan".to_string(),
            product_snapshot: json!({
                "id": "invalid-wallet-credit-plan",
                "purchase_limit_scope": "unlimited",
                "entitlements": [{
                    "type": "wallet_credit",
                    "amount_usd": 1.0,
                    "balance_bucket": "unknown"
                }]
            }),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await;
    assert!(matches!(result, Err(DataLayerError::InvalidInput(_))));
    assert!(repository
        .payment_orders_by_id
        .read()
        .expect("wallet repo lock")
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_order_uniqueness_is_atomic_in_memory() {
    let repository = Arc::new(InMemoryWalletRepository::seed(Vec::new()));
    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let mut tasks = Vec::new();
    for index in 0..2 {
        let repository = Arc::clone(&repository);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            repository
                .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
                    preferred_wallet_id: Some(format!("wallet-concurrent-{index}")),
                    user_id: format!("user-concurrent-{index}"),
                    amount_usd: 1.0,
                    pay_amount: Some(1.0),
                    pay_currency: Some("USD".to_string()),
                    exchange_rate: Some(1.0),
                    payment_method: " EPAY ".to_string(),
                    payment_provider: None,
                    payment_channel: None,
                    gateway_order_id: "shared-memory-gateway-id".to_string(),
                    gateway_response: json!({ "checkout": true }),
                    order_no: format!("order-concurrent-{index}"),
                    expires_at_unix_secs: 4_102_444_800,
                })
                .await
        }));
    }
    barrier.wait().await;

    let mut created = 0;
    let mut rejected = 0;
    for task in tasks {
        match task.await.expect("order task should join") {
            Ok(CreateWalletRechargeOrderOutcome::Created(order)) => {
                assert_eq!(order.payment_method, "epay");
                created += 1;
            }
            Err(DataLayerError::InvalidInput(_)) => rejected += 1,
            other => panic!("unexpected concurrent order result: {other:?}"),
        }
    }
    assert_eq!((created, rejected), (1, 1));
    assert_eq!(
        repository
            .payment_orders_by_id
            .read()
            .expect("wallet repo lock")
            .len(),
        1
    );
    assert_eq!(
        repository
            .wallets_by_id
            .read()
            .expect("wallet repo lock")
            .len(),
        1,
        "the rejected order must not leave a provisional wallet behind"
    );
    let orders = repository
        .payment_orders_by_id
        .read()
        .expect("wallet repo lock");
    let wallets = repository.wallets_by_id.read().expect("wallet repo lock");
    let stored_order = orders.values().next().expect("winning order should remain");
    let stored_wallet = wallets
        .get(&stored_order.wallet_id)
        .expect("winning order must not reference a removed wallet");
    assert_eq!(stored_wallet.user_id, stored_order.user_id);
}

#[tokio::test]
async fn recharge_order_conflict_removes_unreferenced_provisional_wallet_in_memory() {
    let mut existing = sample_payment_order(
        "existing-recharge-order",
        Some("recharge-conflict-user"),
        "pending",
    );
    existing.order_no = "recharge-conflict-order-no".to_string();
    existing.wallet_id = "wallet-from-old-read-model".to_string();
    existing.payment_method = "epay".to_string();
    existing.gateway_order_id = Some("gateway-from-old-read-model".to_string());
    existing.gateway_response = Some(json!({
        "order_kind": "wallet_recharge",
        "integration_status": "checkout_pending"
    }));
    let repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        payment_orders: vec![existing.clone()],
        ..WalletReadSeed::default()
    });

    let outcome = repository
        .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
            preferred_wallet_id: Some("wallet-provisional-conflict".to_string()),
            user_id: "recharge-conflict-user".to_string(),
            amount_usd: 10.0,
            pay_amount: None,
            pay_currency: None,
            exchange_rate: None,
            payment_method: "epay".to_string(),
            payment_provider: Some("epay".to_string()),
            payment_channel: None,
            gateway_order_id: "gateway-retry".to_string(),
            gateway_response: json!({ "integration_status": "checkout_pending" }),
            order_no: "recharge-conflict-order-no".to_string(),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await
        .expect("existing recharge order should be returned");

    assert!(matches!(
        outcome,
        CreateWalletRechargeOrderOutcome::Existing(order)
            if order.id == "existing-recharge-order"
    ));
    assert!(repository
        .find(WalletLookupKey::UserId("recharge-conflict-user"))
        .await
        .expect("wallet lookup should succeed")
        .is_none());
    assert_eq!(
        repository
            .payment_orders_by_id
            .read()
            .expect("wallet repo lock")
            .len(),
        1
    );
}

#[tokio::test]
async fn recharge_rejects_preferred_wallet_id_owned_by_another_user_in_memory() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);

    let result = repository
        .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
            preferred_wallet_id: Some("wallet-1".to_string()),
            user_id: "different-owner".to_string(),
            amount_usd: 5.0,
            pay_amount: Some(5.0),
            pay_currency: Some("USD".to_string()),
            exchange_rate: Some(1.0),
            payment_method: "stripe".to_string(),
            payment_provider: Some("stripe".to_string()),
            payment_channel: Some("card".to_string()),
            gateway_order_id: "gateway-wallet-id-collision".to_string(),
            gateway_response: json!({ "checkout": true }),
            order_no: "order-wallet-id-collision".to_string(),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await;

    assert!(matches!(
        result,
        Err(DataLayerError::InvalidInput(message))
            if message.contains("wallet identifier already belongs")
    ));
    let original = repository
        .find(WalletLookupKey::UserId("user-1"))
        .await
        .expect("original wallet lookup should succeed")
        .expect("original wallet should remain present");
    assert_eq!(original.id, "wallet-1");
    assert!(repository
        .find(WalletLookupKey::UserId("different-owner"))
        .await
        .expect("new owner lookup should succeed")
        .is_none());
    assert!(repository
        .payment_orders_by_id
        .read()
        .expect("wallet repo lock")
        .is_empty());
}

#[tokio::test]
async fn recharge_checkout_update_preserves_order_kind_in_memory() {
    let repository = InMemoryWalletRepository::seed(Vec::new());
    let created = repository
        .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
            preferred_wallet_id: None,
            user_id: "user-recharge-kind".to_string(),
            amount_usd: 3.0,
            pay_amount: Some(3.0),
            pay_currency: Some("USD".to_string()),
            exchange_rate: Some(1.0),
            payment_method: "epay".to_string(),
            payment_provider: Some("epay".to_string()),
            payment_channel: Some("alipay".to_string()),
            gateway_order_id: "placeholder-order-kind".to_string(),
            gateway_response: json!({
                "gateway": "epay",
                "integration_status": "checkout_pending"
            }),
            order_no: "order-recharge-kind".to_string(),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await
        .expect("recharge order should be created");
    let CreateWalletRechargeOrderOutcome::Created(order) = created else {
        panic!("expected a newly created recharge order");
    };

    let updated = repository
        .update_wallet_recharge_checkout(UpdateWalletRechargeCheckoutInput {
            order_id: order.id.clone(),
            gateway_order_id: "provider-order-kind".to_string(),
            gateway_response: json!({
                "gateway": "epay",
                "payment_url": "https://pay.example.test/order"
            }),
        })
        .await
        .expect("checkout update should succeed");
    assert!(matches!(updated, WalletMutationOutcome::Applied(_)));

    let replay = repository
        .find_wallet_recharge_order_by_order_no("user-recharge-kind", "order-recharge-kind")
        .await
        .expect("recharge lookup should succeed")
        .expect("updated order should remain discoverable");
    assert_eq!(
        replay.gateway_order_id.as_deref(),
        Some("provider-order-kind")
    );
    assert_eq!(
        replay
            .gateway_response
            .as_ref()
            .and_then(|value| value.get("order_kind"))
            .and_then(serde_json::Value::as_str),
        Some("wallet_recharge")
    );
}

#[tokio::test]
async fn recharge_checkout_update_rejects_expired_order_in_memory() {
    let repository = InMemoryWalletRepository::seed(Vec::new());
    let created = repository
        .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
            preferred_wallet_id: None,
            user_id: "user-expired-checkout".to_string(),
            amount_usd: 3.0,
            pay_amount: Some(3.0),
            pay_currency: Some("USD".to_string()),
            exchange_rate: Some(1.0),
            payment_method: "epay".to_string(),
            payment_provider: Some("epay".to_string()),
            payment_channel: Some("alipay".to_string()),
            gateway_order_id: "order-expired-checkout".to_string(),
            gateway_response: serde_json::json!({
                "order_kind": "wallet_recharge",
                "integration_status": "checkout_pending"
            }),
            order_no: "order-expired-checkout".to_string(),
            expires_at_unix_secs: 1,
        })
        .await
        .expect("expired recharge order should be creatable for regression setup");
    let CreateWalletRechargeOrderOutcome::Created(order) = created else {
        panic!("expected a newly created recharge order");
    };

    let result = repository
        .update_wallet_recharge_checkout(UpdateWalletRechargeCheckoutInput {
            order_id: order.id.clone(),
            gateway_order_id: "provider-expired-checkout".to_string(),
            gateway_response: serde_json::json!({
                "order_kind": "wallet_recharge",
                "payment_url": "https://pay.example.test/expired"
            }),
        })
        .await
        .expect("expired checkout update should resolve");
    assert!(matches!(result, WalletMutationOutcome::Invalid(_)));

    let replay = repository
        .find_wallet_recharge_order_by_order_no("user-expired-checkout", "order-expired-checkout")
        .await
        .expect("expired recharge lookup should succeed")
        .expect("expired recharge order should remain stored");
    assert_eq!(
        replay.gateway_order_id.as_deref(),
        Some("order-expired-checkout")
    );
}

#[tokio::test]
async fn failed_recharge_without_channel_can_be_reclaimed_in_memory() {
    let repository = InMemoryWalletRepository::seed(Vec::new());
    let first = repository
        .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
            preferred_wallet_id: None,
            user_id: "user-reclaim-no-channel".to_string(),
            amount_usd: 3.0,
            pay_amount: None,
            pay_currency: None,
            exchange_rate: None,
            payment_method: "stripe".to_string(),
            payment_provider: Some("stripe".to_string()),
            payment_channel: None,
            gateway_order_id: "order-reclaim-no-channel".to_string(),
            gateway_response: json!({
                "gateway": "stripe",
                "order_kind": "wallet_recharge",
                "integration_status": "checkout_pending",
                "checkout_claim_token": "first-claim"
            }),
            order_no: "order-reclaim-no-channel".to_string(),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await
        .expect("initial recharge should be created");
    let CreateWalletRechargeOrderOutcome::Created(first) = first else {
        panic!("expected initial recharge order");
    };

    let failed = repository
        .fail_wallet_recharge_checkout(FailWalletRechargeCheckoutInput {
            order_id: first.id.clone(),
            claim_token: "first-claim".to_string(),
            reason: "provider unavailable".to_string(),
            provider_request_may_have_succeeded: false,
        })
        .await
        .expect("checkout failure should resolve");
    assert!(matches!(failed, WalletMutationOutcome::Applied(_)));

    let retry = repository
        .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
            preferred_wallet_id: None,
            user_id: "user-reclaim-no-channel".to_string(),
            amount_usd: 3.0,
            pay_amount: None,
            pay_currency: None,
            exchange_rate: None,
            payment_method: "stripe".to_string(),
            payment_provider: Some("stripe".to_string()),
            payment_channel: None,
            gateway_order_id: "order-reclaim-no-channel".to_string(),
            gateway_response: json!({
                "gateway": "stripe",
                "order_kind": "wallet_recharge",
                "integration_status": "checkout_pending",
                "checkout_claim_token": "retry-claim"
            }),
            order_no: "order-reclaim-no-channel".to_string(),
            expires_at_unix_secs: 4_102_444_800,
        })
        .await
        .expect("failed placeholder should be reclaimable");

    let CreateWalletRechargeOrderOutcome::Created(reclaimed) = retry else {
        panic!("expected failed placeholder to be reclaimed");
    };
    assert_eq!(reclaimed.id, first.id);
    assert_eq!(
        reclaimed
            .gateway_response
            .as_ref()
            .and_then(|value| value.get("checkout_claim_token"))
            .and_then(serde_json::Value::as_str),
        Some("retry-claim")
    );
    assert_eq!(reclaimed.status, "pending");
}

#[tokio::test]
async fn recharge_order_rejects_non_finite_numeric_fields_in_memory() {
    let invalid_inputs = [
        (f64::NAN, Some(1.0), Some(1.0), 4_102_444_800),
        (1.0, Some(f64::INFINITY), Some(1.0), 4_102_444_800),
        (1.0, Some(1.0), Some(0.0), 4_102_444_800),
        (1.0, Some(1.0), Some(1.0), i64::MAX as u64 + 1),
    ];
    for (index, (amount_usd, pay_amount, exchange_rate, expires_at)) in
        invalid_inputs.into_iter().enumerate()
    {
        let repository = InMemoryWalletRepository::seed(Vec::new());
        let result = repository
            .create_wallet_recharge_order(CreateWalletRechargeOrderInput {
                preferred_wallet_id: None,
                user_id: format!("invalid-recharge-user-{index}"),
                amount_usd,
                pay_amount,
                pay_currency: Some("USD".to_string()),
                exchange_rate,
                payment_method: "stripe".to_string(),
                payment_provider: Some("stripe".to_string()),
                payment_channel: Some("card".to_string()),
                gateway_order_id: format!("invalid-recharge-gateway-{index}"),
                gateway_response: json!({"payment_url": "https://pay.example.test"}),
                order_no: format!("invalid-recharge-order-{index}"),
                expires_at_unix_secs: expires_at,
            })
            .await;
        assert!(matches!(result, Err(DataLayerError::InvalidInput(_))));
        assert!(repository
            .find(WalletLookupKey::UserId(&format!(
                "invalid-recharge-user-{index}"
            )))
            .await
            .expect("wallet lookup should succeed")
            .is_none());
    }
}

#[tokio::test]
async fn refund_creation_is_idempotent_and_rejects_corrupt_wallet_balance_in_memory() {
    let repository = InMemoryWalletRepository::seed(vec![sample_wallet()]);
    let first = repository
        .create_wallet_refund_request(CreateWalletRefundRequestInput {
            wallet_id: "wallet-1".to_string(),
            user_id: "user-1".to_string(),
            amount_usd: 4.0,
            payment_order_id: None,
            source_type: None,
            source_id: None,
            refund_mode: None,
            reason: Some("first request".to_string()),
            idempotency_key: Some("memory-refund-idempotency".to_string()),
            refund_no: "memory-refund-1".to_string(),
        })
        .await
        .expect("first refund should resolve");
    let CreateWalletRefundRequestOutcome::Created(first) = first else {
        panic!("expected first refund to be created");
    };
    let replay = repository
        .create_wallet_refund_request(CreateWalletRefundRequestInput {
            wallet_id: "wallet-1".to_string(),
            user_id: "user-1".to_string(),
            amount_usd: 9.0,
            payment_order_id: None,
            source_type: None,
            source_id: None,
            refund_mode: None,
            reason: Some("replayed request".to_string()),
            idempotency_key: Some("memory-refund-idempotency".to_string()),
            refund_no: "memory-refund-2".to_string(),
        })
        .await
        .expect("refund replay should resolve");
    assert!(matches!(
        replay,
        CreateWalletRefundRequestOutcome::Duplicate(refund) if refund.id == first.id
    ));
    assert_eq!(
        repository
            .refunds_by_id
            .read()
            .expect("wallet repo lock")
            .len(),
        1
    );

    repository.with_wallets_mut(|wallets| {
        wallets
            .get_mut("wallet-1")
            .expect("sample wallet should exist")
            .balance = f64::NAN;
    });
    let corrupt = repository
        .create_wallet_refund_request(CreateWalletRefundRequestInput {
            wallet_id: "wallet-1".to_string(),
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            payment_order_id: None,
            source_type: None,
            source_id: None,
            refund_mode: None,
            reason: None,
            idempotency_key: Some("memory-refund-corrupt".to_string()),
            refund_no: "memory-refund-corrupt".to_string(),
        })
        .await
        .expect("corrupt wallet refund should resolve");
    assert!(matches!(
        corrupt,
        CreateWalletRefundRequestOutcome::InvalidInput(_)
    ));
}

#[tokio::test]
async fn seeded_refund_idempotency_replays_only_explicit_mappings_in_memory() {
    let seeded = sample_refund("refund-seeded-idempotency", Some("user-1"), "approved");
    let mapped_repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        wallets: vec![sample_wallet()],
        refunds: vec![seeded.clone()],
        refund_idempotency: vec![(
            "user-1".to_string(),
            "seeded-refund-key".to_string(),
            seeded.id.clone(),
        )],
        ..WalletReadSeed::default()
    });
    let replay = mapped_repository
        .create_wallet_refund_request(CreateWalletRefundRequestInput {
            wallet_id: "wallet-1".to_string(),
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            payment_order_id: None,
            source_type: None,
            source_id: None,
            refund_mode: None,
            reason: Some("seeded replay".to_string()),
            idempotency_key: Some("seeded-refund-key".to_string()),
            refund_no: "seeded-refund-replay".to_string(),
        })
        .await
        .expect("seeded refund replay should resolve");
    assert!(matches!(
        replay,
        CreateWalletRefundRequestOutcome::Duplicate(refund) if refund.id == seeded.id
    ));
    assert_eq!(
        mapped_repository
            .refunds_by_id
            .read()
            .expect("wallet repo lock")
            .len(),
        1
    );

    let unmapped_repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        wallets: vec![sample_wallet()],
        refunds: vec![seeded],
        ..WalletReadSeed::default()
    });
    let created = unmapped_repository
        .create_wallet_refund_request(CreateWalletRefundRequestInput {
            wallet_id: "wallet-1".to_string(),
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            payment_order_id: None,
            source_type: None,
            source_id: None,
            refund_mode: None,
            reason: Some("unmapped seed".to_string()),
            idempotency_key: Some("seeded-refund-key".to_string()),
            refund_no: "seeded-refund-unmapped".to_string(),
        })
        .await
        .expect("unmapped seeded refund request should resolve");
    assert!(matches!(
        created,
        CreateWalletRefundRequestOutcome::Created(_)
    ));
    assert_eq!(
        unmapped_repository
            .refunds_by_id
            .read()
            .expect("wallet repo lock")
            .len(),
        2
    );
}

#[tokio::test]
async fn counts_pending_user_refunds_and_payment_orders() {
    let repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        wallets: vec![sample_wallet()],
        payment_orders: vec![
            sample_payment_order("order-1", Some("user-1"), "pending"),
            sample_payment_order("order-2", Some("user-1"), "paid"),
            sample_payment_order("order-3", Some("user-1"), "credited"),
            sample_payment_order("order-4", Some("user-2"), "pending"),
        ],
        payment_callbacks: Vec::new(),
        wallet_transactions: Vec::new(),
        refunds: vec![
            sample_refund("refund-1", Some("user-1"), "pending_approval"),
            sample_refund("refund-2", Some("user-1"), "processing"),
            sample_refund("refund-3", Some("user-1"), "completed"),
            sample_refund("refund-4", Some("user-2"), "approved"),
        ],
        refund_idempotency: Vec::new(),
        redeem_batches: Vec::new(),
        redeem_codes: Vec::new(),
    });

    assert_eq!(
        repository
            .count_pending_payment_orders_by_user_id("user-1")
            .await
            .expect("payment order count should succeed"),
        2
    );
    assert_eq!(
        repository
            .count_pending_refunds_by_user_id("user-1")
            .await
            .expect("refund count should succeed"),
        2
    );
}

#[tokio::test]
async fn refund_reservation_rejects_invalid_active_amounts_in_memory() {
    for (label, amount) in [
        ("negative", -100.0),
        ("zero", 0.0),
        ("infinite", f64::INFINITY),
        ("nan", f64::NAN),
    ] {
        let mut invalid = sample_refund(
            &format!("refund-invalid-{label}"),
            Some("user-1"),
            "pending_approval",
        );
        invalid.amount_usd = amount;
        let repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
            wallets: vec![sample_wallet()],
            refunds: vec![invalid],
            ..WalletReadSeed::default()
        });
        let outcome = repository
            .create_wallet_refund_request(CreateWalletRefundRequestInput {
                wallet_id: "wallet-1".to_string(),
                user_id: "user-1".to_string(),
                amount_usd: 1.0,
                payment_order_id: None,
                source_type: None,
                source_id: None,
                refund_mode: None,
                reason: Some(format!("invalid reservation: {label}")),
                idempotency_key: Some(format!("reservation-invalid-{label}")),
                refund_no: format!("reservation-invalid-{label}"),
            })
            .await
            .expect("reservation request should resolve");
        assert!(
            matches!(outcome, CreateWalletRefundRequestOutcome::InvalidInput(_)),
            "active {label} reservation must fail closed: {outcome:?}"
        );
    }

    // Completed refunds do not reserve balance and remain ignored.
    let mut completed = sample_refund("refund-completed", Some("user-1"), "completed");
    completed.amount_usd = 100.0;
    let repository = InMemoryWalletRepository::seed_read_model(WalletReadSeed {
        wallets: vec![sample_wallet()],
        refunds: vec![completed],
        ..WalletReadSeed::default()
    });
    let outcome = repository
        .create_wallet_refund_request(CreateWalletRefundRequestInput {
            wallet_id: "wallet-1".to_string(),
            user_id: "user-1".to_string(),
            amount_usd: 1.0,
            payment_order_id: None,
            source_type: None,
            source_id: None,
            refund_mode: None,
            reason: Some("completed reservation is ignored".to_string()),
            idempotency_key: Some("reservation-completed-ignored".to_string()),
            refund_no: "reservation-completed-ignored".to_string(),
        })
        .await
        .expect("completed reservation should not block request");
    assert!(matches!(
        outcome,
        CreateWalletRefundRequestOutcome::Created(_)
    ));
}
