use super::*;

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_full_http_capture_round_trips_for_direct_and_batch_writes() {
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url: std::env::var("AETHER_TEST_DATABASE_URL").unwrap(),
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .unwrap();
    let repository = SqlxUsageReadRepository::new(factory.connect_lazy().unwrap());
    crate::run_migrations(repository.pool()).await.unwrap();

    for write_mode in 0..3 {
        use aether_data_contracts::repository::usage::{
            usage_json_heap_estimate, UsageCaptureMemoryBudget,
        };
        let batch = write_mode != 0;
        let budget = Arc::new(UsageCaptureMemoryBudget::new(4 * 1024 * 1024));
        let retain_capture = |usage: &mut UpsertUsageRecord| {
            let bytes = [
                usage.request_body.as_ref(),
                usage.provider_request_body.as_ref(),
                usage.response_body.as_ref(),
                usage.client_response_body.as_ref(),
            ]
            .into_iter()
            .flatten()
            .map(|body| std::mem::size_of::<serde_json::Value>() + usage_json_heap_estimate(body))
            .sum();
            assert!(usage.capture_retention.reserve(Arc::clone(&budget), bytes));
            bytes
        };
        let request_id = format!("req-full-capture-{}", uuid::Uuid::new_v4().simple());
        let now_unix_secs = Utc::now().timestamp() as u64;
        let mut pending = fast_clear_usage_record(
            &request_id,
            "full-capture-test",
            now_unix_secs,
            false,
            UsageBodyCaptureState::Inline,
            None,
        );
        pending.request_headers =
            Some(json!({"content-type": "application/json", "authorization": "Bearer private"}));
        pending.request_body =
            Some(json!({"messages": [{"role": "user", "content": "original request"}]}));
        pending.request_body_state = Some(UsageBodyCaptureState::Inline);
        pending.provider_request_body = Some(json!({"input": "provider request"}));
        pending.response_body = Some(json!("pending response"));
        pending.response_body_state = Some(UsageBodyCaptureState::Inline);
        pending.client_response_body = Some(json!("pending client response"));
        pending.client_response_body_state = Some(UsageBodyCaptureState::Inline);
        let pending_bytes = retain_capture(&mut pending);
        if batch {
            let records = if write_mode == 2 {
                vec![pending.clone(), pending.clone()]
            } else {
                vec![pending.clone()]
            };
            repository.upsert_pending_many(records).await.unwrap();
        } else {
            repository.upsert(pending.clone()).await.unwrap();
        }
        assert_eq!(budget.retained_bytes(), pending_bytes);
        for (field, expected) in [
            (UsageBodyField::RequestBody, pending.request_body.as_ref()),
            (
                UsageBodyField::ProviderRequestBody,
                pending.provider_request_body.as_ref(),
            ),
            (UsageBodyField::ResponseBody, pending.response_body.as_ref()),
            (
                UsageBodyField::ClientResponseBody,
                pending.client_response_body.as_ref(),
            ),
        ] {
            assert_eq!(
                repository
                    .resolve_body_ref(&usage_body_ref(&request_id, field))
                    .await
                    .unwrap()
                    .as_ref(),
                expected,
                "batch={batch}, field={field:?}"
            );
        }

        let mut terminal = fast_clear_usage_record(
            &request_id,
            "full-capture-test",
            now_unix_secs,
            true,
            UsageBodyCaptureState::None,
            None,
        );
        terminal.provider_request_body_state = None;
        terminal.response_headers =
            Some(json!({"content-type": "text/event-stream", "set-cookie": "private"}));
        terminal.response_body = Some(json!(format!(
            "data: {}\n\ndata: [DONE]\n\n",
            "streamed text".repeat(8192)
        )));
        terminal.response_body_state = Some(UsageBodyCaptureState::Inline);
        terminal.client_response_body = Some(json!({"output": "final response"}));
        terminal.client_response_body_state = Some(UsageBodyCaptureState::Inline);
        let terminal_bytes = retain_capture(&mut terminal);
        repository.upsert(terminal.clone()).await.unwrap();
        assert_eq!(budget.retained_bytes(), pending_bytes + terminal_bytes);
        assert_eq!(budget.downgraded_total(), 0);

        let stored = repository
            .find_by_request_id_shallow(&request_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.request_headers,
            Some(json!({"content-type": "application/json", "authorization": "Bearer private"}))
        );
        assert_eq!(
            stored.response_headers,
            Some(json!({"content-type": "text/event-stream", "set-cookie": "private"}))
        );
        for (field, expected) in [
            (UsageBodyField::RequestBody, pending.request_body.as_ref()),
            (
                UsageBodyField::ProviderRequestBody,
                pending.provider_request_body.as_ref(),
            ),
            (
                UsageBodyField::ResponseBody,
                terminal.response_body.as_ref(),
            ),
            (
                UsageBodyField::ClientResponseBody,
                terminal.client_response_body.as_ref(),
            ),
        ] {
            assert_eq!(
                stored.body_state(field),
                Some(UsageBodyCaptureState::Reference)
            );
            assert_eq!(
                stored.body_ref(field),
                Some(usage_body_ref(&request_id, field).as_str())
            );
            assert_eq!(
                repository
                    .resolve_body_ref(stored.body_ref(field).unwrap())
                    .await
                    .unwrap()
                    .as_ref(),
                expected,
                "batch={batch}, field={field:?}"
            );
        }
        let legacy_content_present: bool = sqlx::query_scalar("SELECT request_body IS NOT NULL OR request_headers IS NOT NULL OR response_body IS NOT NULL FROM usage WHERE request_id = $1")
            .bind(&request_id).fetch_one(repository.pool()).await.unwrap();
        assert!(!legacy_content_present);
        sqlx::query("DELETE FROM usage WHERE request_id = $1")
            .bind(&request_id)
            .execute(repository.pool())
            .await
            .unwrap();
        drop(pending);
        drop(terminal);
        assert_eq!(budget.retained_bytes(), 0);
    }
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_stale_terminal_event_is_a_full_transaction_noop() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let request_id = format!("req-stale-terminal-{suffix}");
    let provider_name = format!("stale-provider-{suffix}");
    let now_unix_secs = Utc::now().timestamp().max(2) as u64;
    let mut newer = fast_clear_usage_record(
        &request_id,
        &provider_name,
        now_unix_secs,
        true,
        UsageBodyCaptureState::None,
        None,
    );
    newer.candidate_id = Some("candidate-new".to_string());
    newer.route_kind = Some("route-new".to_string());
    newer.total_cost_usd = Some(0.5);
    newer.actual_total_cost_usd = Some(0.4);
    repository
        .upsert(newer)
        .await
        .expect("newer terminal usage should upsert");

    let counter_rows_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM usage_counter_deltas WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("counter rows should count");
    let routing_before = sqlx::query(
        "SELECT candidate_id, route_kind FROM usage_routing_snapshots WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("routing snapshot should load");
    let routing_before = (
        routing_before
            .try_get::<Option<String>, _>("candidate_id")
            .unwrap(),
        routing_before
            .try_get::<Option<String>, _>("route_kind")
            .unwrap(),
    );
    let settlement_before = sqlx::query(
        "SELECT billing_status, billing_total_cost_usd::DOUBLE PRECISION AS billing_total_cost_usd FROM usage_settlement_snapshots WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("settlement snapshot should load");
    let settlement_before = (
        settlement_before
            .try_get::<String, _>("billing_status")
            .unwrap(),
        settlement_before
            .try_get::<Option<f64>, _>("billing_total_cost_usd")
            .unwrap(),
    );

    let mut stale = fast_clear_usage_record(
        &request_id,
        &provider_name,
        now_unix_secs - 2,
        true,
        UsageBodyCaptureState::None,
        None,
    );
    stale.status = "failed".to_string();
    stale.billing_status = "void".to_string();
    stale.status_code = Some(503);
    stale.total_cost_usd = Some(99.0);
    stale.actual_total_cost_usd = Some(98.0);
    stale.candidate_id = Some("candidate-stale".to_string());
    stale.route_kind = Some("route-stale".to_string());
    let stored = repository
        .upsert(stale)
        .await
        .expect("stale terminal usage should be ignored");

    assert_eq!(stored.status, "completed");
    assert_eq!(stored.billing_status, "pending");
    assert_eq!(stored.status_code, Some(200));
    assert_eq!(stored.total_cost_usd, 0.5);
    assert_eq!(stored.routing_candidate_id(), Some("candidate-new"));
    assert_eq!(stored.routing_route_kind(), Some("route-new"));

    let counter_rows_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM usage_counter_deltas WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("counter rows should count");
    let routing_after = sqlx::query(
        "SELECT candidate_id, route_kind FROM usage_routing_snapshots WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("routing snapshot should load");
    let routing_after = (
        routing_after
            .try_get::<Option<String>, _>("candidate_id")
            .unwrap(),
        routing_after
            .try_get::<Option<String>, _>("route_kind")
            .unwrap(),
    );
    let settlement_after = sqlx::query(
        "SELECT billing_status, billing_total_cost_usd::DOUBLE PRECISION AS billing_total_cost_usd FROM usage_settlement_snapshots WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("settlement snapshot should load");
    let settlement_after = (
        settlement_after
            .try_get::<String, _>("billing_status")
            .unwrap(),
        settlement_after
            .try_get::<Option<f64>, _>("billing_total_cost_usd")
            .unwrap(),
    );
    assert_eq!(counter_rows_after, counter_rows_before);
    assert_eq!(routing_after, routing_before);
    assert_eq!(settlement_after, settlement_before);
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_pending_batch_persists_auxiliary_state_and_preserves_terminal_conflicts() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let rich_request_id = format!("req-pending-batch-rich-{suffix}");
    let simple_request_id = format!("req-pending-batch-simple-{suffix}");
    let terminal_request_id = format!("req-pending-batch-terminal-{suffix}");
    let streaming_request_id = format!("req-pending-batch-streaming-{suffix}");
    let provider_name = format!("pending-batch-provider-{suffix}");
    let user_id = uuid::Uuid::new_v4().to_string();
    let api_key_id = uuid::Uuid::new_v4().to_string();
    let provider_id = uuid::Uuid::new_v4().to_string();
    let provider_endpoint_id = uuid::Uuid::new_v4().to_string();
    let provider_key_id = uuid::Uuid::new_v4().to_string();
    let now_unix_secs = Utc::now().timestamp().max(0) as u64;

    let mut terminal = fast_clear_usage_record(
        &terminal_request_id,
        &provider_name,
        now_unix_secs,
        true,
        UsageBodyCaptureState::None,
        None,
    );
    terminal.candidate_id = Some("terminal-candidate".to_string());
    terminal.request_metadata = Some(json!({"terminal": true}));
    repository
        .upsert(terminal)
        .await
        .expect("terminal conflict seed should persist");
    repository
        .upsert(first_byte_usage_record(
            &streaming_request_id,
            &provider_name,
            now_unix_secs,
            Some(json!({"streaming": true})),
        ))
        .await
        .expect("streaming conflict seed should persist");

    let mut rich = fast_clear_usage_record(
        &rich_request_id,
        &provider_name,
        now_unix_secs,
        false,
        UsageBodyCaptureState::Inline,
        None,
    );
    rich.user_id = Some(user_id);
    rich.api_key_id = Some(api_key_id);
    rich.provider_id = Some(provider_id);
    rich.provider_endpoint_id = Some(provider_endpoint_id);
    rich.provider_api_key_id = Some(provider_key_id.clone());
    rich.request_headers = Some(json!({"x-request": "request-value"}));
    rich.request_body = Some(json!({"messages": [{"role": "user", "content": "hello"}]}));
    rich.request_body_state = Some(UsageBodyCaptureState::Inline);
    rich.provider_request_headers = Some(json!({"x-provider": "provider-value"}));
    rich.provider_request_body = Some(json!({"model": "gpt-5", "input": "hello"}));
    rich.provider_request_body_state = Some(UsageBodyCaptureState::Inline);
    rich.response_headers = Some(json!({"x-upstream": "upstream-value"}));
    rich.response_body = Some(json!({"phase": "pending"}));
    rich.response_body_state = Some(UsageBodyCaptureState::Inline);
    rich.client_response_headers = Some(json!({"x-client": "client-value"}));
    rich.client_response_body = Some(json!({"phase": "pending-client"}));
    rich.client_response_body_state = Some(UsageBodyCaptureState::Inline);
    rich.candidate_id = Some("rich-candidate".to_string());
    rich.candidate_index = Some(7);
    rich.key_name = Some("rich-key".to_string());
    rich.planner_kind = Some("rich-planner".to_string());
    rich.route_family = Some("rich-family".to_string());
    rich.route_kind = Some("rich-route".to_string());
    rich.execution_path = Some("rich-path".to_string());
    rich.local_execution_runtime_miss_reason = Some("rich-miss".to_string());
    rich.request_metadata = Some(json!({
        "trace_id": "rich-trace",
        "rate_multiplier": 0.75,
        "billing_snapshot_schema_version": "billing-v1",
        "settlement_snapshot": {"schema_version": "settlement-v1", "status": "pending"}
    }));

    let simple = fast_clear_usage_record(
        &simple_request_id,
        &provider_name,
        now_unix_secs,
        false,
        UsageBodyCaptureState::Inline,
        None,
    );
    let mut late_pending = fast_clear_usage_record(
        &terminal_request_id,
        "late-pending-provider",
        now_unix_secs + 1,
        false,
        UsageBodyCaptureState::Inline,
        None,
    );
    late_pending.candidate_id = Some("late-pending-candidate".to_string());
    late_pending.request_metadata = Some(json!({"late_pending": true}));
    let mut late_streaming_pending = fast_clear_usage_record(
        &streaming_request_id,
        "late-streaming-pending-provider",
        now_unix_secs + 1,
        false,
        UsageBodyCaptureState::Inline,
        None,
    );
    late_streaming_pending.status_code = None;
    late_streaming_pending.first_byte_time_ms = None;

    repository
        .upsert_pending_many(vec![rich, simple, late_pending, late_streaming_pending])
        .await
        .expect("pending usage batch should persist");

    let base_rows = sqlx::query(
        "SELECT request_id, status, billing_status, provider_name, status_code, first_byte_time_ms, request_metadata FROM \"usage\" WHERE request_id = ANY($1)",
    )
    .bind(vec![
        rich_request_id.clone(),
        simple_request_id.clone(),
        terminal_request_id.clone(),
        streaming_request_id.clone(),
    ])
    .fetch_all(repository.pool())
    .await
    .expect("pending batch base rows should be readable");
    assert_eq!(base_rows.len(), 4);
    let rich_base = base_rows
        .iter()
        .find(|row| row.try_get::<String, _>("request_id").unwrap() == rich_request_id)
        .expect("rich base row should exist");
    assert_eq!(rich_base.try_get::<String, _>("status").unwrap(), "pending");
    assert_eq!(
        rich_base.try_get::<String, _>("billing_status").unwrap(),
        "pending"
    );
    assert_eq!(
        rich_base
            .try_get::<serde_json::Value, _>("request_metadata")
            .unwrap()
            .get("trace_id"),
        Some(&json!("rich-trace"))
    );
    let terminal_base = base_rows
        .iter()
        .find(|row| row.try_get::<String, _>("request_id").unwrap() == terminal_request_id)
        .expect("terminal base row should exist");
    assert_eq!(
        terminal_base.try_get::<String, _>("status").unwrap(),
        "completed"
    );
    assert_eq!(
        terminal_base.try_get::<String, _>("provider_name").unwrap(),
        provider_name,
        "late pending must not replace terminal provider state"
    );
    let streaming_base = base_rows
        .iter()
        .find(|row| row.try_get::<String, _>("request_id").unwrap() == streaming_request_id)
        .expect("streaming base row should exist");
    assert_eq!(
        streaming_base.try_get::<String, _>("status").unwrap(),
        "streaming"
    );
    assert_eq!(
        streaming_base
            .try_get::<Option<i32>, _>("status_code")
            .unwrap(),
        Some(200),
        "late pending must not clear the streaming response status"
    );
    assert_eq!(
        streaming_base
            .try_get::<Option<i32>, _>("first_byte_time_ms")
            .unwrap(),
        Some(12),
        "late pending must not clear the first-byte observation"
    );

    let http_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM usage_http_audits WHERE request_id = $1",
    )
    .bind(&rich_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("HTTP audit count should be readable");
    assert_eq!(http_count, 1);
    let blob_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::BIGINT FROM usage_body_blobs WHERE request_id = $1",
    )
    .bind(&rich_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("body blob count should be readable");
    assert_eq!(blob_count, 4);
    let captured = repository
        .find_by_request_id_shallow(&rich_request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        captured.request_headers,
        Some(json!({"x-request": "request-value"}))
    );
    assert_eq!(
        repository
            .resolve_body_ref(captured.body_ref(UsageBodyField::RequestBody).unwrap())
            .await
            .unwrap(),
        Some(json!({"messages": [{"role": "user", "content": "hello"}]}))
    );

    let routing = sqlx::query(
        "SELECT candidate_id, candidate_index, selected_provider_api_key_id FROM usage_routing_snapshots WHERE request_id = $1",
    )
    .bind(&rich_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("rich routing snapshot should exist");
    assert_eq!(
        routing
            .try_get::<Option<String>, _>("candidate_id")
            .unwrap()
            .as_deref(),
        Some("rich-candidate")
    );
    assert_eq!(
        routing
            .try_get::<Option<i32>, _>("candidate_index")
            .unwrap(),
        Some(7)
    );
    assert_eq!(
        routing
            .try_get::<Option<String>, _>("selected_provider_api_key_id")
            .unwrap()
            .as_deref(),
        Some(provider_key_id.as_str())
    );

    let settlement = sqlx::query(
        "SELECT billing_status, CAST(rate_multiplier AS DOUBLE PRECISION) AS rate_multiplier, settlement_snapshot FROM usage_settlement_snapshots WHERE request_id = $1",
    )
    .bind(&rich_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("rich settlement snapshot should exist");
    assert_eq!(
        settlement.try_get::<String, _>("billing_status").unwrap(),
        "pending"
    );
    assert_eq!(
        settlement
            .try_get::<Option<f64>, _>("rate_multiplier")
            .unwrap(),
        Some(0.75)
    );
    assert!(settlement
        .try_get::<Option<serde_json::Value>, _>("settlement_snapshot")
        .unwrap()
        .is_some());

    let counter = sqlx::query(
        "SELECT request_count_delta, target_id, candidate_last_used_at_unix_secs, usage_created_at_unix_secs FROM usage_counter_deltas WHERE request_id = $1 AND kind = 'provider_api_key' AND processed_at IS NULL",
    )
    .bind(&rich_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("pending provider counter delta should exist");
    assert_eq!(counter.try_get::<i64, _>("request_count_delta").unwrap(), 1);
    assert_eq!(
        counter.try_get::<String, _>("target_id").unwrap(),
        provider_key_id
    );
    assert_eq!(
        counter
            .try_get::<Option<i64>, _>("candidate_last_used_at_unix_secs")
            .unwrap(),
        counter
            .try_get::<Option<i64>, _>("usage_created_at_unix_secs")
            .unwrap()
    );

    let request_ids = vec![
        rich_request_id,
        simple_request_id,
        terminal_request_id,
        streaming_request_id,
    ];
    sqlx::query("DELETE FROM usage_counter_deltas WHERE request_id = ANY($1)")
        .bind(&request_ids)
        .execute(repository.pool())
        .await
        .expect("pending batch counter deltas should be removed");
    sqlx::query("DELETE FROM \"usage\" WHERE request_id = ANY($1)")
        .bind(&request_ids)
        .execute(repository.pool())
        .await
        .expect("pending batch usage rows should be removed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_pending_batch_and_terminal_upserts_count_each_provider_request_once() {
    const REQUESTS: usize = 32;

    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 16,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let provider_name = format!("pending-terminal-race-provider-{suffix}");
    let provider_key_id = uuid::Uuid::new_v4().to_string();
    let now_unix_secs = Utc::now().timestamp().max(0) as u64;
    let request_ids = (0..REQUESTS)
        .map(|index| format!("req-pending-terminal-race-{index}-{suffix}"))
        .collect::<Vec<_>>();
    let pending_rows = request_ids
        .iter()
        .map(|request_id| {
            let mut row = fast_clear_usage_record(
                request_id,
                &provider_name,
                now_unix_secs,
                false,
                UsageBodyCaptureState::Inline,
                None,
            );
            row.provider_api_key_id = Some(provider_key_id.clone());
            row
        })
        .collect::<Vec<_>>();
    let terminal_rows = request_ids
        .iter()
        .map(|request_id| {
            let mut row = fast_clear_usage_record(
                request_id,
                &provider_name,
                now_unix_secs + 1,
                true,
                UsageBodyCaptureState::None,
                None,
            );
            row.provider_api_key_id = Some(provider_key_id.clone());
            row
        })
        .collect::<Vec<_>>();

    let start = Arc::new(tokio::sync::Barrier::new(3));
    let pending_repository = repository.clone();
    let pending_start = Arc::clone(&start);
    let pending = tokio::spawn(async move {
        pending_start.wait().await;
        pending_repository.upsert_pending_many(pending_rows).await
    });
    let terminal_repository = repository.clone();
    let terminal_start = Arc::clone(&start);
    let terminal = tokio::spawn(async move {
        terminal_start.wait().await;
        let mut writes = tokio::task::JoinSet::new();
        for row in terminal_rows {
            let repository = terminal_repository.clone();
            writes.spawn(async move { repository.upsert(row).await });
        }
        while let Some(result) = writes.join_next().await {
            result.expect("terminal upsert task should not panic")?;
        }
        Ok::<(), aether_data_contracts::DataLayerError>(())
    });
    start.wait().await;
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        pending
            .await
            .expect("pending batch task should not panic")
            .expect("pending batch should commit");
        terminal
            .await
            .expect("terminal tasks should not panic")
            .expect("terminal upserts should commit");
    })
    .await
    .expect("stable request locks must avoid deadlock");

    let rows = sqlx::query(
        r#"SELECT request_id, COALESCE(SUM(request_count_delta), 0)::BIGINT AS request_count
           FROM usage_counter_deltas
           WHERE request_id = ANY($1) AND kind = 'provider_api_key' AND target_id = $2
           GROUP BY request_id"#,
    )
    .bind(&request_ids)
    .bind(&provider_key_id)
    .fetch_all(repository.pool())
    .await
    .expect("provider request contributions should be readable");
    assert_eq!(rows.len(), REQUESTS);
    for row in rows {
        assert_eq!(row.try_get::<i64, _>("request_count").unwrap(), 1);
    }

    sqlx::query("DELETE FROM usage_counter_deltas WHERE request_id = ANY($1)")
        .bind(&request_ids)
        .execute(repository.pool())
        .await
        .expect("race counter deltas should be removed");
    sqlx::query("DELETE FROM \"usage\" WHERE request_id = ANY($1)")
        .bind(&request_ids)
        .execute(repository.pool())
        .await
        .expect("race usage rows should be removed");
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_first_byte_fast_path_is_atomic_and_preserves_terminal_state() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let missing_request_id = format!("req-first-byte-missing-{suffix}");
    let existing_request_id = format!("req-first-byte-existing-{suffix}");
    let metadata_fill_request_id = format!("req-first-byte-metadata-fill-{suffix}");
    let provider_name = format!("first-byte-fast-{suffix}");
    let missing_provider_key_id = uuid::Uuid::new_v4().to_string();
    let now_unix_secs = Utc::now().timestamp().max(0) as u64;

    let mut missing_first_byte = first_byte_usage_record(
        &missing_request_id,
        &provider_name,
        now_unix_secs,
        Some(json!({
            "trace_id": "missing-row-trace",
            "upstream_is_stream": false
        })),
    );
    missing_first_byte.provider_api_key_id = Some(missing_provider_key_id.clone());
    repository
        .upsert_first_byte(missing_first_byte)
        .await
        .expect("first-byte fast path should insert a missing row");
    let missing = sqlx::query(
        "SELECT status, billing_status, first_byte_time_ms, upstream_is_stream, request_metadata FROM \"usage\" WHERE request_id = $1",
    )
    .bind(&missing_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("inserted first-byte row should be readable");
    assert_eq!(
        missing
            .try_get::<String, _>("status")
            .expect("status should decode"),
        "streaming"
    );
    assert_eq!(
        missing
            .try_get::<String, _>("billing_status")
            .expect("billing status should decode"),
        "pending"
    );
    assert_eq!(
        missing
            .try_get::<Option<i32>, _>("first_byte_time_ms")
            .expect("first-byte time should decode"),
        Some(12)
    );
    assert!(!missing
        .try_get::<bool, _>("upstream_is_stream")
        .expect("upstream stream mode should decode"));
    assert_eq!(
        missing
            .try_get::<Option<serde_json::Value>, _>("request_metadata")
            .expect("metadata should decode"),
        Some(json!({
            "trace_id": "missing-row-trace",
            "upstream_is_stream": false
        }))
    );
    let missing_provider_request_count = sqlx::query_scalar::<_, i64>(
        r#"SELECT COALESCE(SUM(request_count_delta), 0)::BIGINT
           FROM usage_counter_deltas
           WHERE request_id = $1 AND kind = 'provider_api_key' AND target_id = $2"#,
    )
    .bind(&missing_request_id)
    .bind(&missing_provider_key_id)
    .fetch_one(repository.pool())
    .await
    .expect("missing first-byte provider contribution should be readable");
    assert_eq!(
        missing_provider_request_count, 1,
        "a first-byte insert must create the initial provider request contribution"
    );

    let mut pending = first_byte_usage_record(
        &existing_request_id,
        &provider_name,
        now_unix_secs,
        Some(json!({"trace_id": "pending-trace"})),
    );
    pending.status = "pending".to_string();
    pending.status_code = None;
    pending.response_time_ms = None;
    pending.first_byte_time_ms = None;
    repository
        .upsert(pending)
        .await
        .expect("pending usage should be inserted");

    repository
        .upsert_first_byte(first_byte_usage_record(
            &existing_request_id,
            &provider_name,
            now_unix_secs + 1,
            Some(json!({"trace_id": "incoming-first-byte-trace"})),
        ))
        .await
        .expect("first-byte fast path should advance pending usage");
    let streaming = sqlx::query(
        "SELECT status, first_byte_time_ms, request_metadata FROM \"usage\" WHERE request_id = $1",
    )
    .bind(&existing_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("streaming usage should be readable");
    assert_eq!(
        streaming
            .try_get::<String, _>("status")
            .expect("status should decode"),
        "streaming"
    );
    assert_eq!(
        streaming
            .try_get::<Option<serde_json::Value>, _>("request_metadata")
            .expect("metadata should decode"),
        Some(json!({"trace_id": "pending-trace"})),
        "the single-row fast path must not replace metadata already captured by pending usage"
    );

    let mut replay = first_byte_usage_record(
        &existing_request_id,
        &provider_name,
        now_unix_secs + 2,
        Some(json!({"trace_id": "replayed-first-byte-trace"})),
    );
    replay.first_byte_time_ms = Some(3);
    repository
        .upsert_first_byte(replay)
        .await
        .expect("replayed first-byte write should succeed");
    let replayed_first_byte_time = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT first_byte_time_ms FROM \"usage\" WHERE request_id = $1",
    )
    .bind(&existing_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("replayed first-byte time should be readable");
    assert_eq!(
        replayed_first_byte_time,
        Some(12),
        "a replay must preserve the first non-zero TTFB"
    );

    let mut metadata_pending = first_byte_usage_record(
        &metadata_fill_request_id,
        &provider_name,
        now_unix_secs,
        None,
    );
    metadata_pending.status = "pending".to_string();
    metadata_pending.status_code = None;
    metadata_pending.response_time_ms = None;
    metadata_pending.first_byte_time_ms = None;
    repository
        .upsert(metadata_pending)
        .await
        .expect("metadata-fill pending usage should be inserted");
    repository
        .upsert_first_byte(first_byte_usage_record(
            &metadata_fill_request_id,
            &provider_name,
            now_unix_secs + 1,
            Some(json!({"trace_id": "filled-first-byte-trace"})),
        ))
        .await
        .expect("first-byte metadata should fill a missing pending snapshot");
    let filled_metadata = sqlx::query_scalar::<_, Option<serde_json::Value>>(
        "SELECT request_metadata FROM \"usage\" WHERE request_id = $1",
    )
    .bind(&metadata_fill_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("filled first-byte metadata should be readable");
    assert_eq!(
        filled_metadata,
        Some(json!({"trace_id": "filled-first-byte-trace"}))
    );

    let mut terminal = fast_clear_usage_record(
        &existing_request_id,
        &provider_name,
        now_unix_secs + 3,
        true,
        UsageBodyCaptureState::None,
        None,
    );
    terminal.is_stream = Some(true);
    terminal.first_byte_time_ms = Some(44);
    terminal.request_metadata = Some(json!({"trace_id": "terminal-trace"}));
    repository
        .upsert(terminal)
        .await
        .expect("terminal usage should be persisted");

    let mut late_first_byte = first_byte_usage_record(
        &existing_request_id,
        &provider_name,
        now_unix_secs + 4,
        Some(json!({"trace_id": "late-first-byte-trace"})),
    );
    late_first_byte.first_byte_time_ms = Some(3);
    repository
        .upsert_first_byte(late_first_byte)
        .await
        .expect("late first-byte write should be ignored without failing");
    let terminal = sqlx::query(
        "SELECT status, billing_status, first_byte_time_ms, request_metadata, finalized_at FROM \"usage\" WHERE request_id = $1",
    )
    .bind(&existing_request_id)
    .fetch_one(repository.pool())
    .await
    .expect("terminal usage should be readable");
    assert_eq!(
        terminal
            .try_get::<String, _>("status")
            .expect("status should decode"),
        "completed"
    );
    assert_eq!(
        terminal
            .try_get::<Option<i32>, _>("first_byte_time_ms")
            .expect("first-byte time should decode"),
        Some(44)
    );
    assert_eq!(
        terminal
            .try_get::<Option<serde_json::Value>, _>("request_metadata")
            .expect("metadata should decode"),
        Some(json!({"trace_id": "terminal-trace"}))
    );
    assert!(terminal
        .try_get::<Option<chrono::DateTime<Utc>>, _>("finalized_at")
        .expect("finalized timestamp should decode")
        .is_some());

    sqlx::query("DELETE FROM \"usage\" WHERE request_id = ANY($1)")
        .bind(vec![
            missing_request_id.clone(),
            existing_request_id,
            metadata_fill_request_id,
        ])
        .execute(repository.pool())
        .await
        .expect("first-byte test rows should be removed");
    sqlx::query("DELETE FROM usage_counter_deltas WHERE request_id = $1")
        .bind(missing_request_id)
        .execute(repository.pool())
        .await
        .expect("first-byte provider delta should be removed");
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_first_byte_reads_provider_contribution_after_waiting_for_canonical_lock() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 4,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let request_id = format!("req-first-byte-lock-snapshot-{suffix}");
    let provider_name = format!("first-byte-lock-snapshot-{suffix}");
    let provider_key_id = uuid::Uuid::new_v4().to_string();
    let now_unix_secs = Utc::now().timestamp().max(0) as u64;
    let mut pending = first_byte_usage_record(
        &request_id,
        &provider_name,
        now_unix_secs,
        Some(json!({"phase": "pending"})),
    );
    pending.status = "pending".to_string();
    pending.status_code = None;
    pending.response_time_ms = None;
    pending.first_byte_time_ms = None;
    pending.provider_api_key_id = None;
    repository
        .upsert(pending)
        .await
        .expect("pending row without a provider key should be inserted");

    let mut canonical = repository
        .pool()
        .begin()
        .await
        .expect("canonical transaction should begin");
    sqlx::query(super::super::LOCK_USAGE_REQUEST_ID_SQL)
        .bind(&request_id)
        .execute(&mut *canonical)
        .await
        .expect("canonical writer should hold the request advisory lock");
    sqlx::query("UPDATE \"usage\" SET provider_api_key_id = $2 WHERE request_id = $1")
        .bind(&request_id)
        .bind(&provider_key_id)
        .execute(&mut *canonical)
        .await
        .expect("canonical writer should attach the provider key");
    sqlx::query(
        r#"INSERT INTO usage_counter_deltas
           (id, request_id, kind, target_id, request_count_delta)
           VALUES ($1, $2, 'provider_api_key', $3, 1)"#,
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(&request_id)
    .bind(&provider_key_id)
    .execute(&mut *canonical)
    .await
    .expect("canonical writer should enqueue the provider contribution");

    let mut first_byte = first_byte_usage_record(
        &request_id,
        &provider_name,
        now_unix_secs + 1,
        Some(json!({"phase": "first-byte"})),
    );
    first_byte.provider_api_key_id = Some(provider_key_id.clone());
    let first_byte_repository = repository.clone();
    let first_byte_task =
        tokio::spawn(async move { first_byte_repository.upsert_first_byte(first_byte).await });

    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                r#"SELECT EXISTS (
                     SELECT 1
                     FROM pg_locks
                     WHERE locktype = 'advisory'
                       AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                       AND NOT granted
                   )"#,
            )
            .fetch_one(repository.pool())
            .await
            .expect("advisory lock wait should be observable");
            if waiting {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-byte writer should block behind the canonical lock");

    canonical
        .commit()
        .await
        .expect("canonical provider contribution should commit");
    tokio::time::timeout(std::time::Duration::from_secs(5), first_byte_task)
        .await
        .expect("first-byte writer should resume after canonical commit")
        .expect("first-byte task should not panic")
        .expect("first-byte write should succeed");

    let provider_request_count = sqlx::query_scalar::<_, i64>(
        r#"SELECT COALESCE(SUM(request_count_delta), 0)::BIGINT
           FROM usage_counter_deltas
           WHERE request_id = $1 AND kind = 'provider_api_key' AND target_id = $2"#,
    )
    .bind(&request_id)
    .bind(&provider_key_id)
    .fetch_one(repository.pool())
    .await
    .expect("provider contribution should be readable");
    assert_eq!(
        provider_request_count, 1,
        "the post-lock fresh snapshot must prevent a duplicate provider contribution"
    );

    sqlx::query("DELETE FROM \"usage\" WHERE request_id = $1")
        .bind(&request_id)
        .execute(repository.pool())
        .await
        .expect("lock snapshot usage row should be removed");
    sqlx::query("DELETE FROM usage_counter_deltas WHERE request_id = $1")
        .bind(request_id)
        .execute(repository.pool())
        .await
        .expect("lock snapshot counter rows should be removed");
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_first_byte_batch_preserves_duplicate_order_and_terminal_guards() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let provider_name = format!("first-byte-batch-{suffix}");
    let request_a = format!("req-first-byte-batch-a-{suffix}");
    let request_b = format!("req-first-byte-batch-b-{suffix}");
    let request_missing = format!("req-first-byte-batch-missing-{suffix}");
    let request_terminal = format!("req-first-byte-batch-terminal-{suffix}");
    let missing_provider_key_id = uuid::Uuid::new_v4().to_string();
    let now_unix_secs = Utc::now().timestamp().max(0) as u64;

    let mut pending_a = first_byte_usage_record(
        &request_a,
        &provider_name,
        now_unix_secs,
        Some(json!({"trace_id": "seed-a"})),
    );
    pending_a.status = "pending".to_string();
    pending_a.first_byte_time_ms = None;
    pending_a.status_code = None;
    pending_a.response_time_ms = None;
    pending_a.has_format_conversion = Some(true);

    let mut pending_b = first_byte_usage_record(&request_b, &provider_name, now_unix_secs, None);
    pending_b.status = "pending".to_string();
    pending_b.first_byte_time_ms = None;
    pending_b.status_code = None;
    pending_b.response_time_ms = None;
    pending_b.has_format_conversion = Some(false);

    let mut terminal = fast_clear_usage_record(
        &request_terminal,
        &provider_name,
        now_unix_secs,
        true,
        UsageBodyCaptureState::None,
        None,
    );
    terminal.is_stream = Some(true);
    terminal.first_byte_time_ms = Some(44);
    terminal.request_metadata = Some(json!({"trace_id": "terminal"}));

    repository
        .upsert(pending_a)
        .await
        .expect("pending A should be inserted");
    repository
        .upsert(pending_b)
        .await
        .expect("pending B should be inserted");
    repository
        .upsert(terminal)
        .await
        .expect("terminal row should be inserted");

    let mut first_a = first_byte_usage_record(
        &request_a,
        &provider_name,
        now_unix_secs + 1,
        Some(json!({"trace_id": "incoming-a"})),
    );
    first_a.first_byte_time_ms = Some(30);
    first_a.has_format_conversion = None;
    let mut replay_a = first_a.clone();
    replay_a.first_byte_time_ms = Some(7);
    replay_a.response_time_ms = Some(99);

    let mut first_b = first_byte_usage_record(
        &request_b,
        &provider_name,
        now_unix_secs + 1,
        Some(json!({"trace_id": "incoming-b"})),
    );
    first_b.has_format_conversion = Some(true);

    let mut late_terminal = first_byte_usage_record(
        &request_terminal,
        &provider_name,
        now_unix_secs + 2,
        Some(json!({"trace_id": "late"})),
    );
    late_terminal.first_byte_time_ms = Some(3);
    late_terminal.has_format_conversion = None;
    let mut first_missing = first_byte_usage_record(
        &request_missing,
        &provider_name,
        now_unix_secs + 1,
        Some(json!({"trace_id": "incoming-missing"})),
    );
    first_missing.provider_api_key_id = Some(missing_provider_key_id.clone());

    repository
        .upsert_first_byte_many(vec![
            first_a,
            first_b,
            late_terminal,
            replay_a,
            first_missing,
        ])
        .await
        .expect("first-byte batch should commit");

    let rows = sqlx::query(
        "SELECT request_id, status, response_time_ms, first_byte_time_ms, has_format_conversion, request_metadata, finalized_at FROM \"usage\" WHERE request_id = ANY($1) ORDER BY request_id",
    )
    .bind(vec![
        request_a.clone(),
        request_b.clone(),
        request_missing.clone(),
        request_terminal.clone(),
    ])
    .fetch_all(repository.pool())
    .await
    .expect("batch rows should be readable");
    assert_eq!(rows.len(), 4);

    let row_a = rows
        .iter()
        .find(|row| row.try_get::<String, _>("request_id").unwrap() == request_a)
        .expect("row A should exist");
    assert_eq!(row_a.try_get::<String, _>("status").unwrap(), "streaming");
    assert_eq!(
        row_a
            .try_get::<Option<i32>, _>("first_byte_time_ms")
            .unwrap(),
        Some(30),
        "duplicate request IDs must keep the first non-zero TTFB in caller order"
    );
    assert_eq!(
        row_a.try_get::<Option<i32>, _>("response_time_ms").unwrap(),
        Some(99),
        "later duplicate fields must still merge with single-row semantics"
    );
    assert_eq!(
        row_a
            .try_get::<Option<bool>, _>("has_format_conversion")
            .unwrap(),
        Some(true),
        "a None conversion flag must not overwrite an existing true value"
    );
    assert_eq!(
        row_a
            .try_get::<Option<serde_json::Value>, _>("request_metadata")
            .unwrap(),
        Some(json!({"trace_id": "seed-a"})),
        "existing metadata remains authoritative"
    );

    let row_b = rows
        .iter()
        .find(|row| row.try_get::<String, _>("request_id").unwrap() == request_b)
        .expect("row B should exist");
    assert_eq!(row_b.try_get::<String, _>("status").unwrap(), "streaming");
    assert_eq!(
        row_b
            .try_get::<Option<bool>, _>("has_format_conversion")
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        row_b
            .try_get::<Option<serde_json::Value>, _>("request_metadata")
            .unwrap(),
        Some(json!({"trace_id": "incoming-b"}))
    );

    let row_terminal = rows
        .iter()
        .find(|row| row.try_get::<String, _>("request_id").unwrap() == request_terminal)
        .expect("terminal row should exist");
    assert_eq!(
        row_terminal.try_get::<String, _>("status").unwrap(),
        "completed",
        "a late first-byte batch must not regress a terminal row"
    );
    assert_eq!(
        row_terminal
            .try_get::<Option<i32>, _>("first_byte_time_ms")
            .unwrap(),
        Some(44)
    );
    assert!(row_terminal
        .try_get::<Option<chrono::DateTime<Utc>>, _>("finalized_at")
        .unwrap()
        .is_some());

    let missing_provider_request_count = sqlx::query_scalar::<_, i64>(
        r#"SELECT COALESCE(SUM(request_count_delta), 0)::BIGINT
           FROM usage_counter_deltas
           WHERE request_id = $1 AND kind = 'provider_api_key' AND target_id = $2"#,
    )
    .bind(&request_missing)
    .bind(&missing_provider_key_id)
    .fetch_one(repository.pool())
    .await
    .expect("batch first-byte provider contribution should be readable");
    assert_eq!(missing_provider_request_count, 1);

    sqlx::query("DELETE FROM \"usage\" WHERE request_id = ANY($1)")
        .bind(vec![
            request_a,
            request_b,
            request_missing.clone(),
            request_terminal,
        ])
        .execute(repository.pool())
        .await
        .expect("batch test rows should be removed");
    sqlx::query("DELETE FROM usage_counter_deltas WHERE request_id = $1")
        .bind(request_missing)
        .execute(repository.pool())
        .await
        .expect("batch first-byte provider delta should be removed");
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_terminal_none_capture_clears_fast_from_detail_and_lightweight_lists() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    crate::run_migrations(repository.pool())
        .await
        .expect("test database migrations should succeed");

    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let request_id = format!("req-fast-clear-{suffix}");
    let provider_name = format!("fast-clear-{suffix}");
    let now_unix_secs = Utc::now().timestamp().max(0) as u64;

    let mut pending_record = fast_clear_usage_record(
        &request_id,
        &provider_name,
        now_unix_secs,
        false,
        UsageBodyCaptureState::Inline,
        Some("priority"),
    );
    pending_record.candidate_id = Some("candidate-a".to_string());
    pending_record.candidate_index = Some(1);
    pending_record.key_name = Some("key-a".to_string());
    pending_record.planner_kind = Some("planner-a".to_string());
    pending_record.route_family = Some("route-family-a".to_string());
    pending_record.route_kind = Some("route-kind-a".to_string());
    pending_record.execution_path = Some("path-a".to_string());
    pending_record.local_execution_runtime_miss_reason = Some("miss-a".to_string());
    pending_record.request_metadata = Some(json!({
        "trace_id": "trace-a",
        "provider_service_tier": "priority",
        "provider_actual_service_tier": "priority",
        "billing_snapshot": {
            "schema_version": "2.0",
            "status": "complete",
            "resolved_variables": {
                "input_price_per_1m": 30.0,
                "output_price_per_1m": 150.0
            }
        },
        "settlement_snapshot": {
            "schema_version": "2.0",
            "pricing_snapshot": {
                "pricing_source": "processing_tier",
                "service_tier": "priority"
            },
            "billing_plan_snapshot": {
                "rule_id": "fast-rule",
                "rule_version": "1"
            }
        },
        "billing_dimensions": {"service_tier": "priority"},
        "rate_multiplier": 2.0,
        "input_price_per_1m": 30.0,
        "output_price_per_1m": 150.0
    }));
    let pending = repository
        .upsert(pending_record)
        .await
        .expect("pending usage should persist");
    assert_eq!(pending.provider_service_tier().as_deref(), Some("priority"));

    let mut terminal_record = fast_clear_usage_record(
        &request_id,
        &provider_name,
        now_unix_secs,
        true,
        UsageBodyCaptureState::None,
        None,
    );
    terminal_record.provider_id = Some("final-provider-id".to_string());
    terminal_record.provider_endpoint_id = Some("final-endpoint-id".to_string());
    terminal_record.provider_api_key_id = Some("final-key-id".to_string());
    terminal_record.target_model = None;
    let terminal = repository
        .upsert(terminal_record)
        .await
        .expect("terminal usage should persist");
    assert_eq!(terminal.provider_service_tier(), None);

    let stored = repository
        .find_by_request_id(&request_id)
        .await
        .expect("detail lookup should succeed")
        .expect("usage should exist");
    assert_eq!(
        stored.provider_request_body_state,
        Some(UsageBodyCaptureState::None)
    );
    assert_eq!(stored.provider_service_tier(), None);
    assert_eq!(stored.provider_actual_service_tier(), None);
    let stored_metadata = stored
        .request_metadata
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .expect("terminal audit metadata should remain");
    assert_eq!(
        stored_metadata
            .get("trace_id")
            .and_then(serde_json::Value::as_str),
        Some("trace-a")
    );
    for stale_key in [
        "provider_actual_service_tier",
        "billing_snapshot",
        "settlement_snapshot",
        "billing_dimensions",
        "rate_multiplier",
        "input_price_per_1m",
        "output_price_per_1m",
        "candidate_id",
    ] {
        assert!(
            stored_metadata.get(stale_key).is_none(),
            "terminal metadata retained stale key {stale_key}"
        );
    }
    assert_eq!(stored.settlement_rate_multiplier(), None);
    assert_eq!(stored.settlement_input_price_per_1m(), None);
    assert_eq!(stored.settlement_output_price_per_1m(), None);

    let settlement_row = sqlx::query(
        "SELECT settlement_snapshot, billing_dimensions, CAST(rate_multiplier AS DOUBLE PRECISION) AS rate_multiplier, CAST(input_price_per_1m AS DOUBLE PRECISION) AS input_price_per_1m, CAST(output_price_per_1m AS DOUBLE PRECISION) AS output_price_per_1m FROM usage_settlement_snapshots WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("settlement snapshot row should be readable");
    assert!(settlement_row
        .try_get::<Option<serde_json::Value>, _>("settlement_snapshot")
        .expect("settlement snapshot should decode")
        .is_none());
    assert!(settlement_row
        .try_get::<Option<serde_json::Value>, _>("billing_dimensions")
        .expect("billing dimensions should decode")
        .is_none());
    assert!(settlement_row
        .try_get::<Option<f64>, _>("rate_multiplier")
        .expect("rate multiplier should decode")
        .is_none());
    assert!(settlement_row
        .try_get::<Option<f64>, _>("input_price_per_1m")
        .expect("input price should decode")
        .is_none());
    assert!(settlement_row
        .try_get::<Option<f64>, _>("output_price_per_1m")
        .expect("output price should decode")
        .is_none());

    let physical_metadata = sqlx::query_scalar::<_, Option<serde_json::Value>>(
        "SELECT request_metadata FROM \"usage\" WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("physical metadata should be readable")
    .expect("clear tombstone should be stored");
    assert!(physical_metadata.get("provider_service_tier").is_none());

    let physical_body =
        sqlx::query("SELECT provider_request_body FROM \"usage\" WHERE request_id = $1")
            .bind(&request_id)
            .fetch_one(repository.pool())
            .await
            .expect("physical body columns should be readable");
    assert!(physical_body
        .try_get::<Option<serde_json::Value>, _>("provider_request_body")
        .expect("provider body column should decode")
        .is_none());

    let physical_http = sqlx::query(
        "SELECT provider_request_body_ref, provider_request_body_state FROM usage_http_audits WHERE request_id = $1",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("HTTP audit row should be readable");
    assert!(physical_http
        .try_get::<Option<String>, _>("provider_request_body_ref")
        .expect("provider body ref should decode")
        .is_none());
    assert_eq!(
        physical_http
            .try_get::<Option<String>, _>("provider_request_body_state")
            .expect("provider body state should decode")
            .as_deref(),
        Some("none")
    );
    let physical_blob_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM usage_body_blobs WHERE request_id = $1 AND body_field = 'provider_request_body'",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("provider body blob count should be readable");
    assert_eq!(physical_blob_count, 0);

    // A late pending candidate must not resurrect the terminal capture, metadata, or detached
    // blob after the final state has been written.
    let mut late_record = fast_clear_usage_record(
        &request_id,
        "late-provider",
        now_unix_secs + 2,
        false,
        UsageBodyCaptureState::Inline,
        Some("priority"),
    );
    late_record.model = "late-model".to_string();
    late_record.target_model = Some("late-target".to_string());
    late_record.provider_id = Some("late-provider-id".to_string());
    late_record.provider_endpoint_id = Some("late-endpoint-id".to_string());
    late_record.provider_api_key_id = Some("late-key-id".to_string());
    late_record.endpoint_api_format = Some("late:format".to_string());
    late_record.candidate_id = Some("late-candidate".to_string());
    let late = repository
        .upsert(late_record)
        .await
        .expect("late pending usage should be accepted without regressing capture");
    assert_eq!(late.provider_service_tier(), None);
    assert_eq!(late.provider_name, provider_name);
    assert_eq!(late.model, "gpt-5");
    assert_eq!(late.target_model, None);
    assert_eq!(late.provider_id.as_deref(), Some("final-provider-id"));
    assert_eq!(
        late.provider_endpoint_id.as_deref(),
        Some("final-endpoint-id")
    );
    assert_eq!(late.provider_api_key_id.as_deref(), Some("final-key-id"));
    assert_eq!(late.endpoint_api_format.as_deref(), Some("openai:chat"));
    assert_eq!(late.candidate_id, None);
    let late_blob_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM usage_body_blobs WHERE request_id = $1 AND body_field = 'provider_request_body'",
    )
    .bind(&request_id)
    .fetch_one(repository.pool())
    .await
    .expect("late provider body blob count should be readable");
    assert_eq!(late_blob_count, 0);

    let list_item = repository
        .list_usage_audits(&UsageAuditListQuery {
            provider_name: Some(provider_name.clone()),
            limit: Some(10),
            newest_first: true,
            ..UsageAuditListQuery::default()
        })
        .await
        .expect("usage list should succeed")
        .into_iter()
        .find(|item| item.request_id == request_id)
        .expect("usage list should contain the test row");
    assert_eq!(list_item.provider_request_body_state, None);
    assert_eq!(list_item.provider_service_tier(), None);
    assert_eq!(list_item.target_model, None);
    assert_eq!(list_item.candidate_id, None);
    assert_eq!(list_item.candidate_index, None);
    assert_eq!(list_item.key_name, None);
    assert_eq!(list_item.planner_kind, None);
    assert_eq!(list_item.route_family, None);
    assert_eq!(list_item.route_kind, None);
    assert_eq!(list_item.execution_path, None);
    assert_eq!(list_item.local_execution_runtime_miss_reason, None);

    let recent_item = repository
        .list_recent_usage_audits(None, 20)
        .await
        .expect("recent usage list should succeed")
        .into_iter()
        .find(|item| item.request_id == request_id)
        .expect("recent usage list should contain the test row");
    assert_eq!(recent_item.provider_request_body_state, None);
    assert_eq!(recent_item.provider_service_tier(), None);

    sqlx::query("DELETE FROM \"usage\" WHERE request_id = $1")
        .bind(&request_id)
        .execute(repository.pool())
        .await
        .expect("test usage should be removed");

    for (index, state, incoming_tier, expected_tier) in [
        ("disabled", UsageBodyCaptureState::Disabled, None, None),
        ("truncated", UsageBodyCaptureState::Truncated, None, None),
        (
            "unavailable",
            UsageBodyCaptureState::Unavailable,
            None,
            None,
        ),
        (
            "none-stale-metadata",
            UsageBodyCaptureState::None,
            Some("priority"),
            None,
        ),
        (
            "disabled-preserved",
            UsageBodyCaptureState::Disabled,
            Some("priority"),
            Some("priority"),
        ),
        (
            "truncated-preserved",
            UsageBodyCaptureState::Truncated,
            Some("priority"),
            Some("priority"),
        ),
    ] {
        let request_id = format!("req-fast-clear-{suffix}-{index}");
        let provider_name = format!("fast-clear-{suffix}-{index}");
        repository
            .upsert(fast_clear_usage_record(
                &request_id,
                &provider_name,
                now_unix_secs,
                false,
                UsageBodyCaptureState::Inline,
                Some("priority"),
            ))
            .await
            .expect("typed-state pending usage should persist");
        let terminal = repository
            .upsert(fast_clear_usage_record(
                &request_id,
                &provider_name,
                now_unix_secs + 1,
                true,
                state,
                incoming_tier,
            ))
            .await
            .expect("typed-state terminal usage should persist");
        assert_eq!(
            terminal.provider_service_tier().as_deref(),
            expected_tier,
            "state={state:?} should use only same-source metadata"
        );
        let list_item = repository
            .list_usage_audits(&UsageAuditListQuery {
                provider_name: Some(provider_name.clone()),
                limit: Some(10),
                newest_first: true,
                ..UsageAuditListQuery::default()
            })
            .await
            .expect("typed-state list should succeed")
            .into_iter()
            .find(|item| item.request_id == request_id)
            .expect("typed-state row should be listed");
        assert_eq!(list_item.provider_service_tier().as_deref(), expected_tier);
        sqlx::query("DELETE FROM \"usage\" WHERE request_id = $1")
            .bind(&request_id)
            .execute(repository.pool())
            .await
            .expect("typed-state test usage should be removed");
    }
}

#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_dashboard_daily_breakdown_uses_canonical_covering_read_path() {
    let database_url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");
    let repository =
        SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"));
    let until = Utc::now().timestamp().max(0) as u64;
    crate::run_migrations(repository.pool()).await.unwrap();
    let request_id = format!("daily-breakdown-{}", uuid::Uuid::new_v4().simple());
    let provider_name = format!("daily-provider-{}", uuid::Uuid::new_v4().simple());
    repository
        .upsert(fast_clear_usage_record(
            &request_id,
            &provider_name,
            until.saturating_sub(60),
            true,
            UsageBodyCaptureState::None,
            None,
        ))
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let rows = repository
        .list_dashboard_daily_breakdown(&UsageDashboardDailyBreakdownQuery {
            created_from_unix_secs: until.saturating_sub(7 * 24 * 60 * 60),
            created_until_unix_secs: until,
            tz_offset_minutes: 480,
            user_id: None,
        })
        .await
        .expect("daily breakdown should succeed");
    eprintln!(
        "daily_breakdown={:?} rows={}",
        started.elapsed(),
        rows.len()
    );
    let seeded = rows
        .iter()
        .find(|row| row.provider == provider_name)
        .unwrap();
    assert_eq!(seeded.requests, 1);
    assert_eq!(seeded.total_tokens, 2);
    sqlx::query("DELETE FROM \"usage\" WHERE request_id = $1")
        .bind(&request_id)
        .execute(repository.pool())
        .await
        .unwrap();
}

/// Execute the metadata projection from both production list queries. A temporary table
/// keeps this independent of migrations and avoids writing any business records.
#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL"]
async fn live_usage_list_metadata_preserves_boolean_chat_pii_redaction() {
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url: std::env::var("AETHER_TEST_DATABASE_URL")
            .expect("AETHER_TEST_DATABASE_URL must point at the test database"),
        min_connections: 1,
        max_connections: 1,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("pool factory should build");
    let pool = factory.connect_lazy().expect("lazy pool should build");
    let mut conn = pool.acquire().await.expect("connection should acquire");
    sqlx::query("CREATE TEMP TABLE usage_list_metadata_probe (request_metadata jsonb)")
        .execute(&mut *conn)
        .await
        .expect("metadata probe table should build");

    for (path, prefix) in [
        ("list", super::super::LIST_USAGE_AUDITS_PREFIX),
        ("recent", super::super::LIST_RECENT_USAGE_AUDITS_PREFIX),
    ] {
        // Extract the entire outer CASE, including its gate, instead of duplicating the
        // production expression. Omitting the marker from either branch must fail.
        let projection = prefix
            .split_once("NULL::jsonb AS client_response_body,\n")
            .and_then(|(_, tail)| tail.split_once("  END AS request_metadata,"))
            .map(|(projection, _)| projection)
            .expect("list metadata projection should be present");
        let sql = format!(
            "SELECT {projection} END AS request_metadata \
             FROM usage_list_metadata_probe AS \"usage\""
        );

        // Test the marker alone (the outer CASE gate), and alongside another retained
        // key (the inner value check). A missing marker must not become an explicit false.
        for user_agent in [None, Some("metadata-probe")] {
            for (label, marker, expected_marker) in [
                ("true", Some(json!(true)), Some(true)),
                ("false", Some(json!(false)), Some(false)),
                ("string true", Some(json!("true")), None),
                ("string false", Some(json!("false")), None),
                ("number", Some(json!(1)), None),
                ("null", Some(json!(null)), None),
                ("object", Some(json!({})), None),
                ("array", Some(json!([true])), None),
                ("missing", None, None),
            ] {
                let mut metadata = json!({"private_probe": "must not appear in list metadata"});
                let mut expected = serde_json::Map::new();
                if let Some(marker) = marker {
                    metadata["chat_pii_redaction"] = marker;
                }
                if let Some(marker) = expected_marker {
                    expected.insert("chat_pii_redaction".into(), json!(marker));
                }
                if let Some(user_agent) = user_agent {
                    metadata["user_agent"] = json!(user_agent);
                    expected.insert("user_agent".into(), json!(user_agent));
                }
                sqlx::query("TRUNCATE usage_list_metadata_probe")
                    .execute(&mut *conn)
                    .await
                    .expect("probe table should reset");
                sqlx::query("INSERT INTO usage_list_metadata_probe VALUES ($1)")
                    .bind(metadata)
                    .execute(&mut *conn)
                    .await
                    .expect("metadata probe should insert");
                let projected: Option<serde_json::Value> = sqlx::query_scalar(&sql)
                    .fetch_one(&mut *conn)
                    .await
                    .expect("production list metadata projection should execute");
                let expected = (!expected.is_empty()).then(|| json!(expected));
                assert_eq!(
                    projected, expected,
                    "{path} projection changed {label} marker with user_agent={user_agent:?}"
                );
            }
        }
    }

    drop(conn);
    pool.close().await;
}

/// Live 回归测试：时间线条聚合 SQL 必须能在真实 PostgreSQL 上执行，并按窗口偏移正确分桶。
///
/// 用会话级临时表 + 同名临时视图替换 `usage_billing_facts`（`pg_temp` 在 search_path 中最靠前），
/// 因此**不写入任何业务表、也不需要建库权限**，只验证聚合 SQL 本身的语法、参数绑定、
/// 分桶数学与过滤条件；临时对象随连接断开自动消失。
///
/// 运行：`AETHER_TEST_DATABASE_URL=... cargo test -p aether-data-postgres --lib -- --ignored live_usage_health_timeline`
#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL"]
async fn live_usage_health_timeline_buckets_by_window_offset() {
    use aether_data_contracts::repository::usage::StoredUsageHealthTimelineRow;

    let url = std::env::var("AETHER_TEST_DATABASE_URL")
        .expect("AETHER_TEST_DATABASE_URL must point at the test database");
    // 与仓库其它 live 测试保持一致：走 PostgresPoolFactory，显式关闭 SSL。
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url: url,
        min_connections: 1,
        max_connections: 2,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("pool factory should build");
    let pool = factory.connect_lazy().expect("lazy pool should build");
    // 临时对象只在同一条连接上可见，后续所有语句都必须复用这个连接。
    let mut conn = pool.acquire().await.expect("connection should acquire");

    let from = 1_800_000_000u64;
    let until = from + 3600;
    let segments = 6u32;

    let setup = [
        r#"CREATE TEMP TABLE usage_rows (
             created_at timestamptz NOT NULL,
             model text NOT NULL,
             provider_name text NOT NULL,
             api_format text,
             status text NOT NULL,
             status_code int,
             error_message text,
             response_time_ms int,
             first_byte_time_ms int,
             output_tokens bigint NOT NULL DEFAULT 0
           )"#,
        "CREATE TEMP VIEW usage_billing_facts AS SELECT * FROM usage_rows",
    ];
    for statement in setup {
        sqlx::query(statement)
            .execute(&mut *conn)
            .await
            .expect("probe schema should build");
    }

    // 事件构造：窗口 [from, until)，宽度 600 秒。
    // 前 5 条用于验证分段归属，后面几条验证窗口与状态过滤。
    // 测试用的事件表，位置含义由下面的解构语句给出；拆成结构体会让这张表更难横向对比。
    #[allow(clippy::type_complexity)]
    let events: Vec<(
        i64,
        &str,
        &str,
        &str,
        Option<i32>,
        Option<&str>,
        i32,
        i32,
        i64,
    )> = vec![
        (0, "m1", "p1", "completed", Some(200), None, 100, 50, 10),
        (599, "m1", "p1", "completed", Some(200), None, 200, 60, 20),
        (600, "m1", "p1", "completed", Some(200), None, 300, 70, 30),
        (1500, "m1", "p1", "completed", Some(200), None, 400, 80, 40),
        (3599, "m1", "p1", "completed", Some(200), None, 500, 90, 50),
        // 窗口右端为开区间：正好落在 until 的事件不计入。
        (
            3600,
            "m1",
            "p1",
            "completed",
            Some(200),
            None,
            999,
            999,
            999,
        ),
        // 窗口左端之前的事件不计入。
        (-1, "m1", "p1", "completed", Some(200), None, 999, 999, 999),
        // 失败事件计入失败数。
        (
            300,
            "m1",
            "p1",
            "failed",
            Some(500),
            Some("boom"),
            60,
            20,
            0,
        ),
        // 用户取消（499）按 exclude_status_codes 排除。
        (300, "m1", "p1", "completed", Some(499), None, 60, 20, 0),
        // 进行中的请求被排除。
        (300, "m1", "p1", "pending", None, None, 0, 0, 0),
        // 归属未定的提供商被排除。
        (300, "m1", "unknown", "completed", Some(200), None, 0, 0, 0),
        // error_message 为空串（非 NULL）按 breakdown 口径计为失败。
        (300, "m1", "p1", "completed", Some(200), Some(""), 60, 20, 0),
        // 其他模型不属于本次分组。
        (100, "m2", "p1", "completed", Some(200), None, 1, 1, 1),
    ];
    for (
        offset,
        model,
        provider,
        status,
        status_code,
        error_message,
        latency,
        first_byte,
        tokens,
    ) in events
    {
        sqlx::query(
            r#"INSERT INTO usage_rows
               (created_at, model, provider_name, api_format, status, status_code, error_message,
                response_time_ms, first_byte_time_ms, output_tokens)
               VALUES (to_timestamp($1), $2, $3, 'openai:chat', $4, $5, $6, $7, $8, $9)"#,
        )
        .bind(from as f64 + offset as f64)
        .bind(model)
        .bind(provider)
        .bind(status)
        .bind(status_code)
        .bind(error_message)
        .bind(latency)
        .bind(first_byte)
        .bind(tokens)
        .execute(&mut *conn)
        .await
        .expect("probe row should insert");
    }

    // pg_temp 在 search_path 中最靠前，未限定的 usage_billing_facts 会解析到上面的临时视图。
    sqlx::query("SET search_path TO pg_temp, public")
        .execute(&mut *conn)
        .await
        .expect("search_path should switch");

    let query = UsageHealthTimelineQuery {
        created_from_unix_secs: from,
        created_until_unix_secs: until,
        group_by: UsageHealthTimelineGroupBy::Model,
        group_values: vec!["m1".to_string()],
        provider_name: None,
        model: None,
        api_format: None,
        segments,
        exclude_status_codes: vec![499],
    };
    let mut builder = build_usage_health_timeline_query(&query).expect("query should build");
    let rows = builder
        .build()
        .fetch_all(&mut *conn)
        .await
        .expect("health timeline SQL should execute against PostgreSQL");
    let buckets = rows
        .into_iter()
        .map(|row| {
            use sqlx::Row;
            StoredUsageHealthTimelineRow {
                group_key: row.get("group_key"),
                segment_idx: row.get::<i64, _>("segment_idx") as u32,
                request_count: row.get::<i64, _>("request_count") as u64,
                success_count: row.get::<i64, _>("success_count") as u64,
                response_time_sum_ms: row.get("response_time_sum_ms"),
                response_time_samples: row.get::<i64, _>("response_time_samples") as u64,
                first_byte_sum_ms: row.get("first_byte_sum_ms"),
                first_byte_samples: row.get::<i64, _>("first_byte_samples") as u64,
                output_tokens: row.get::<i64, _>("output_tokens") as u64,
            }
        })
        .collect::<Vec<_>>();

    let by_segment = buckets
        .iter()
        .map(|row| (row.segment_idx, row))
        .collect::<std::collections::BTreeMap<_, _>>();

    // 分段归属：0 / 599 落在第 0 段，600 落在第 1 段，1500 落在第 2 段，3599 落在第 5 段；
    // 第 3、4 段没有事件。窗口外、499、pending、unknown 提供商、其他模型全部被排除。
    assert_eq!(
        by_segment.keys().copied().collect::<Vec<_>>(),
        vec![0, 1, 2, 5],
        "分段归属不符合窗口偏移（实际 {buckets:?}）"
    );
    assert_eq!(by_segment[&0].request_count, 4);
    assert_eq!(by_segment[&0].success_count, 2);
    assert_eq!(by_segment[&1].request_count, 1);
    assert_eq!(by_segment[&1].success_count, 1);
    assert_eq!(by_segment[&2].request_count, 1);
    assert_eq!(by_segment[&5].request_count, 1);

    let total: u64 = buckets.iter().map(|row| row.request_count).sum();
    assert_eq!(total, 7, "窗口内符合条件的请求数应为 7");
    let success: u64 = buckets.iter().map(|row| row.success_count).sum();
    assert_eq!(
        success, 5,
        "成功数应与 status/status_code/error_message 判定一致"
    );

    // 指标聚合：第 0 段的耗时 = 100 + 200 + 60 + 60。
    assert_eq!(by_segment[&0].response_time_sum_ms, 420.0);
    assert_eq!(by_segment[&0].response_time_samples, 4);
    assert_eq!(by_segment[&0].first_byte_sum_ms, 150.0);
    // 所有分段都必须落在 [0, segments) 内。
    assert!(buckets.iter().all(|row| row.segment_idx < segments));

    // 按提供商维度聚合时，同一批请求落在 p1 下，且总数一致。
    let provider_query = UsageHealthTimelineQuery {
        created_from_unix_secs: from,
        created_until_unix_secs: until,
        group_by: UsageHealthTimelineGroupBy::Provider,
        group_values: vec!["p1".to_string()],
        provider_name: None,
        model: None,
        api_format: None,
        segments,
        exclude_status_codes: vec![499],
    };
    let mut provider_builder =
        build_usage_health_timeline_query(&provider_query).expect("query should build");
    let provider_rows = provider_builder
        .build()
        .fetch_all(&mut *conn)
        .await
        .expect("provider dimension SQL should execute");
    let provider_total: i64 = provider_rows
        .iter()
        .map(|row| {
            use sqlx::Row;
            row.get::<i64, _>("request_count")
        })
        .sum();
    assert_eq!(
        provider_total, 8,
        "提供商维度还包含 m2 的一条请求（分组过滤只作用于提供的 group_values）"
    );

    sqlx::query("SET search_path TO public")
        .execute(&mut *conn)
        .await
        .ok();
    drop(conn);
    pool.close().await;
}

/// Live 对账测试：时间线条各分段计数之和必须等于卡片头部（breakdown）的请求数。
///
/// 这是用户可见缺陷的核心契约——同一张卡片上的"265 次请求"与 History 状态条必须同源。
/// 写入的行用唯一的 request_id 前缀，结束后原地删除。
///
/// 运行：`AETHER_TEST_DATABASE_URL=... cargo nextest run -p aether-data-postgres --run-ignored ignored-only -E 'test(live_usage_health_timeline_totals_match_breakdown)'`
#[tokio::test]
#[ignore = "requires AETHER_TEST_DATABASE_URL and PostgreSQL migrations"]
async fn live_usage_health_timeline_totals_match_breakdown() {
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url: std::env::var("AETHER_TEST_DATABASE_URL")
            .expect("AETHER_TEST_DATABASE_URL must point at the test database"),
        min_connections: 1,
        max_connections: 4,
        acquire_timeout_ms: 10_000,
        idle_timeout_ms: 30_000,
        max_lifetime_ms: 60_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("pool factory should build");
    let repository = SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool"));
    crate::run_migrations(repository.pool())
        .await
        .expect("migrations should apply");

    let now_unix_secs = Utc::now().timestamp() as u64;
    let since_unix_secs = now_unix_secs - 6 * 3600;
    let segments = 60u32;
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let model = format!("health-live-{run_id}");
    let provider = format!("health-live-provider-{run_id}");
    let total_rows = 265usize;

    let mut request_ids = Vec::with_capacity(total_rows);
    for index in 0..total_rows {
        let request_id = format!("req-health-live-{run_id}-{index}");
        let mut record = fast_clear_usage_record(
            &request_id,
            &provider,
            now_unix_secs,
            true,
            UsageBodyCaptureState::Inline,
            None,
        );
        record.model = model.clone();
        // 每 20 条造一个失败，用来验证成功/失败判定在两条查询路径上完全一致。
        if index % 20 == 0 {
            record.status = "failed".to_string();
            record.status_code = Some(500);
            record.error_message = Some("live timeline probe failure".to_string());
        }
        repository.upsert(record).await.expect("usage should write");
        request_ids.push(request_id);
    }

    // 把本次写入的行等距铺满整个 6 小时窗口（只动本次 run 的 request_id）。
    let step_secs = (6.0 * 3600.0) / total_rows as f64;
    sqlx::query(
        r#"UPDATE "usage" AS u
              SET created_at = TO_TIMESTAMP($1::double precision)
                             + ((s.row_number - 1) * $3::double precision * INTERVAL '1 second')
             FROM (
                   SELECT request_id, row_number() OVER (ORDER BY request_id) AS row_number
                     FROM "usage"
                    WHERE request_id = ANY($2)
                  ) AS s
            WHERE u.request_id = s.request_id"#,
    )
    .bind(since_unix_secs as f64)
    .bind(&request_ids)
    .bind(step_secs)
    .execute(repository.pool())
    .await
    .expect("probe rows should spread across the window");

    let buckets = repository
        .aggregate_usage_health_timeline(&UsageHealthTimelineQuery {
            created_from_unix_secs: since_unix_secs,
            created_until_unix_secs: now_unix_secs,
            group_by: UsageHealthTimelineGroupBy::Model,
            group_values: vec![model.clone()],
            provider_name: None,
            model: None,
            api_format: None,
            segments,
            exclude_status_codes: vec![499],
        })
        .await
        .expect("health timeline aggregation should succeed");

    let breakdown = repository
        .summarize_usage_breakdown(&UsageBreakdownSummaryQuery {
            created_from_unix_secs: since_unix_secs,
            created_until_unix_secs: now_unix_secs,
            user_id: None,
            provider_name: None,
            model: Some(model.clone()),
            api_format: None,
            exclude_status_codes: vec![499],
            group_by: UsageBreakdownGroupBy::Model,
        })
        .await
        .expect("breakdown should succeed");

    let headline = breakdown
        .iter()
        .find(|row| row.group_key == model)
        .expect("headline row for the probe model");
    let timeline_total: u64 = buckets.iter().map(|row| row.request_count).sum();
    let timeline_success: u64 = buckets.iter().map(|row| row.success_count).sum();

    // 核心契约：状态条与头部数字必须同源。这正是用户报告的"265 次请求却大片灰条"。
    assert_eq!(
        headline.request_count, total_rows as u64,
        "头部应看到窗口内的全部请求"
    );
    assert_eq!(
        timeline_total, headline.request_count,
        "时间线条各段请求数之和必须等于头部请求数"
    );
    assert_eq!(
        timeline_success, headline.success_count,
        "时间线条各段成功数之和必须等于头部成功数"
    );
    assert!(
        buckets.len() >= segments as usize - 1,
        "265 次等距请求应铺满几乎全部 {segments} 段，实际只有 {} 段",
        buckets.len()
    );
    let failure_segments = buckets
        .iter()
        .filter(|row| row.success_count < row.request_count)
        .count();
    assert!(failure_segments > 0, "构造的失败请求必须体现在状态条上");

    // 清理：只删除本次 run 写入的行，并确认没有残留。
    sqlx::query("DELETE FROM \"usage\" WHERE request_id = ANY($1)")
        .bind(&request_ids)
        .execute(repository.pool())
        .await
        .expect("probe rows should clean up");
    let remaining: i64 =
        sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM \"usage\" WHERE request_id = ANY($1)")
            .bind(&request_ids)
            .fetch_one(repository.pool())
            .await
            .expect("cleanup verification query");
    assert_eq!(remaining, 0, "探针数据必须清理干净，不能留在测试库里");
}
