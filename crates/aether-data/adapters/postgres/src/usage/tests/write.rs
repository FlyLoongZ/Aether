use super::*;

#[tokio::test]
async fn pending_batch_is_opt_in_and_rejects_non_pending_before_connecting() {
    let repository = lazy_usage_repository();
    assert!(UsageWriteRepository::supports_pending_usage_batch(
        &repository
    ));

    let mut streaming = fast_clear_usage_record(
        "req-invalid-pending-batch",
        "provider-invalid-pending-batch",
        1_700_000_000,
        false,
        UsageBodyCaptureState::Inline,
        None,
    );
    streaming.status = "streaming".to_string();
    let err = repository
        .upsert_pending_many(vec![streaming])
        .await
        .expect_err("non-pending lifecycle rows must be rejected before database access");
    assert!(err
        .to_string()
        .contains("pending usage batch requires pending status"));
}

#[tokio::test]
async fn validates_upsert_before_hitting_database() {
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url: "postgres://localhost/aether".to_string(),
        min_connections: 1,
        max_connections: 4,
        acquire_timeout_ms: 1_000,
        idle_timeout_ms: 5_000,
        max_lifetime_ms: 30_000,
        statement_cache_capacity: 64,
        require_ssl: false,
    })
    .expect("factory should build");

    let pool = factory.connect_lazy().expect("pool should build");
    let repository = SqlxUsageReadRepository::new(pool);
    let result = repository
        .upsert(UpsertUsageRecord {
            capture_retention: Default::default(),
            request_id: "".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: None,
            provider_endpoint_id: None,
            provider_api_key_id: None,
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(false),
            is_stream: Some(false),
            input_tokens: Some(10),
            output_tokens: Some(20),
            total_tokens: Some(30),
            cache_creation_input_tokens: None,
            cache_creation_ephemeral_5m_input_tokens: None,
            cache_creation_ephemeral_1h_input_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_cost_usd: None,
            cache_read_cost_usd: None,
            output_price_per_1m: None,
            total_cost_usd: None,
            actual_total_cost_usd: None,
            status_code: Some(200),
            error_message: None,
            error_category: None,
            response_time_ms: Some(100),
            first_byte_time_ms: None,
            status: "completed".to_string(),
            billing_status: "pending".to_string(),
            request_headers: None,
            request_body: None,
            request_body_ref: None,
            provider_request_headers: None,
            provider_request_body: None,
            provider_request_body_ref: None,
            response_headers: None,
            response_body: None,
            response_body_ref: None,
            client_response_headers: None,
            client_response_body: None,
            client_response_body_ref: None,
            request_body_state: None,
            provider_request_body_state: None,
            response_body_state: None,
            client_response_body_state: None,
            candidate_id: None,
            candidate_index: None,
            key_name: None,
            planner_kind: None,
            route_family: None,
            route_kind: None,
            execution_path: None,
            local_execution_runtime_miss_reason: None,
            request_metadata: None,
            finalized_at_unix_secs: None,
            created_at_unix_ms: Some(100),
            updated_at_unix_secs: 101,
        })
        .await;

    assert!(result.is_err());
}

#[test]
fn usage_sql_serializes_request_id_upserts_before_reading_previous_usage() {
    assert!(super::super::LOCK_USAGE_REQUEST_ID_SQL.contains("pg_advisory_xact_lock"));
    assert!(super::super::LOCK_USAGE_REQUEST_ID_SQL.contains("hashtext($1)::BIGINT"));
    assert!(include_str!("../mod.rs")
        .contains("lock_usage_request_id_in_tx(tx, &usage.request_id).await?;"));
}

#[test]
fn usage_sql_moves_shared_counter_updates_behind_outbox() {
    let source = include_str!("../mod.rs");
    assert!(super::super::INSERT_USAGE_COUNTER_DELTA_SQL.contains("usage_counter_deltas"));
    assert!(super::super::CLAIM_USAGE_COUNTER_DELTAS_SQL.contains("FOR UPDATE SKIP LOCKED"));
    assert!(super::super::MARK_USAGE_COUNTER_DELTAS_PROCESSED_SQL.contains("processed_at = NOW()"));
    assert!(super::super::TRY_LOCK_USAGE_COUNTER_FLUSH_SQL.contains("pg_try_advisory_xact_lock"));
    assert!(source.contains("enqueue_api_key_usage_delta_in_tx("));
    assert!(source.contains("enqueue_provider_api_key_usage_delta_in_tx("));
    assert!(source.contains("enqueue_model_usage_delta_in_tx("));
    assert!(source.contains("apply_provider_api_key_main_usage_delta_in_tx("));
    assert!(source.contains("USAGE_COUNTER_KIND_PROVIDER_MONTHLY"));
    assert!(source.contains("apply_provider_monthly_usage_delta_in_tx("));
}

#[test]
fn stale_pending_cleanup_index_matches_the_batch_filter_and_order() {
    let migration = include_str!(
        "../../../migrations/20260720000000_add_usage_stale_pending_cleanup_index.sql"
    );
    assert!(migration.starts_with("-- no-transaction"));
    assert!(migration.contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS"));
    assert!(migration.contains("idx_usage_stale_pending_created_request"));
    assert!(migration.contains("ON public.usage USING btree (created_at, request_id)"));

    let active_predicate = "status IN ('pending', 'streaming')";
    assert!(migration.contains(active_predicate));
    assert!(SELECT_STALE_PENDING_USAGE_BATCH_SQL.contains(active_predicate));
    assert!(SELECT_STALE_PENDING_USAGE_BATCH_SQL
        .contains("ORDER BY usage.created_at ASC, usage.request_id ASC"));
}

#[test]
fn usage_sql_insert_values_aligns_request_metadata_and_timestamps() {
    let sql = normalize_newlines(super::super::UPSERT_SQL);
    assert!(sql.contains("\n  $48::jsonb,\n  $49::jsonb,\n  CASE"));
    assert!(sql.contains("WHEN $50 IS NULL THEN NULL"));
    assert!(sql.contains("TO_TIMESTAMP($51::double precision)"));
}

#[test]
fn usage_sql_upsert_materializes_upstream_stream_mode() {
    assert!(super::super::UPSERT_SQL.contains("upstream_is_stream,"));
    assert!(super::super::UPSERT_SQL.contains("$49::jsonb->>'upstream_is_stream'"));
    assert!(super::super::UPSERT_SQL.contains("COALESCE($21, FALSE)"));
    assert!(super::super::UPSERT_SQL.contains("upstream_is_stream = CASE"));
}

#[test]
fn usage_sql_upsert_returning_includes_routing_placeholders() {
    assert!(super::super::UPSERT_SQL.contains("NULL::varchar AS http_request_body_state"));
    assert!(super::super::UPSERT_SQL.contains("NULL::varchar AS http_client_response_body_state"));
    assert!(super::super::UPSERT_SQL.contains("NULL::varchar AS routing_candidate_id"));
    assert!(super::super::UPSERT_SQL.contains("NULL::varchar AS routing_planner_kind"));
    assert!(super::super::UPSERT_SQL.contains("NULL::varchar AS routing_execution_path"));
    assert!(super::super::UPSERT_SQL
        .contains("NULL::varchar AS settlement_billing_snapshot_schema_version"));
    assert!(super::super::UPSERT_SQL
        .contains("NULL::double precision AS settlement_input_price_per_1m"));
    assert!(super::super::UPSERT_SQL.contains("input_output_total_tokens"));
    assert!(super::super::UPSERT_SQL.contains("input_context_tokens"));
}

#[test]
fn usage_sql_writes_usage_settlement_pricing_snapshots() {
    assert!(super::super::UPSERT_USAGE_SETTLEMENT_PRICING_SNAPSHOT_SQL
        .contains("INSERT INTO usage_settlement_snapshots"));
    assert!(super::super::UPSERT_USAGE_SETTLEMENT_PRICING_SNAPSHOT_SQL
        .contains("billing_snapshot_schema_version"));
    assert!(
        super::super::UPSERT_USAGE_SETTLEMENT_PRICING_SNAPSHOT_SQL.contains("price_per_request")
    );
}

#[test]
fn usage_sql_upsert_recovers_missing_provider_links_after_billing_finalizes() {
    for assignment in [
        "provider_id = CASE WHEN (\"usage\".billing_status = 'pending' AND $57) OR (\"usage\".billing_status <> 'pending' AND \"usage\".provider_id IS NULL AND (\"usage\".provider_endpoint_id IS NULL OR \"usage\".provider_endpoint_id = EXCLUDED.provider_endpoint_id) AND (\"usage\".provider_api_key_id IS NULL OR \"usage\".provider_api_key_id = EXCLUDED.provider_api_key_id)) THEN COALESCE(EXCLUDED.provider_id, \"usage\".provider_id) ELSE \"usage\".provider_id END",
        "provider_endpoint_id = CASE WHEN (\"usage\".billing_status = 'pending' AND $57) OR (\"usage\".billing_status <> 'pending' AND \"usage\".provider_endpoint_id IS NULL AND (\"usage\".provider_id IS NULL OR \"usage\".provider_id = EXCLUDED.provider_id) AND (\"usage\".provider_api_key_id IS NULL OR \"usage\".provider_api_key_id = EXCLUDED.provider_api_key_id)) THEN COALESCE(EXCLUDED.provider_endpoint_id, \"usage\".provider_endpoint_id) ELSE \"usage\".provider_endpoint_id END",
        "provider_api_key_id = CASE WHEN (\"usage\".billing_status = 'pending' AND $57) OR (\"usage\".billing_status <> 'pending' AND \"usage\".provider_api_key_id IS NULL AND (\"usage\".provider_id IS NULL OR \"usage\".provider_id = EXCLUDED.provider_id) AND (\"usage\".provider_endpoint_id IS NULL OR \"usage\".provider_endpoint_id = EXCLUDED.provider_endpoint_id)) THEN COALESCE(EXCLUDED.provider_api_key_id, \"usage\".provider_api_key_id) ELSE \"usage\".provider_api_key_id END",
    ] {
        assert!(
            super::super::UPSERT_SQL.contains(assignment),
            "missing provider link recovery assignment: {assignment}"
        );
    }
}

#[test]
fn usage_sql_updates_usage_mirror_columns_from_terminal_events_only() {
    for field in [
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "input_output_total_tokens",
        "input_context_tokens",
        "cache_creation_input_tokens",
        "cache_creation_input_tokens_5m",
        "cache_creation_input_tokens_1h",
        "cache_read_input_tokens",
        "cache_creation_cost_usd",
        "cache_read_cost_usd",
        "total_cost_usd",
        "actual_total_cost_usd",
    ] {
        let assignment = format!(
            "{field} = CASE WHEN \"usage\".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST(\"usage\".{field}, EXCLUDED.{field}) ELSE \"usage\".{field} END"
        );
        assert!(
            super::super::UPSERT_SQL.contains(assignment.as_str()),
            "missing terminal mirror assignment: {assignment}"
        );
        assert!(
            !super::super::UPSERT_SQL
                .contains(format!("{field} = CASE WHEN EXCLUDED.status IN").as_str()),
            "terminal mirror assignment must keep pending billing guard: {field}"
        );
    }
}

#[test]
fn usage_sql_binds_usage_mirror_values_for_terminal_upserts() {
    let source = include_str!("../mod.rs");
    for binding in [
        ".bind(usage.input_tokens.map(to_i32).transpose()?)",
        ".bind(usage.output_tokens.map(to_i32).transpose()?)",
        ".bind(usage.total_tokens.map(to_i32).transpose()?)",
        ".bind(usage.cache_creation_input_tokens.map(to_i32).transpose()?)",
        ".bind(usage.cache_read_input_tokens.map(to_i32).transpose()?)",
        ".bind(usage.cache_creation_cost_usd)",
        ".bind(usage.cache_read_cost_usd)",
        ".bind(usage.total_cost_usd)",
        ".bind(usage.actual_total_cost_usd)",
    ] {
        assert!(
            source.contains(binding),
            "missing usage upsert bind: {binding}"
        );
    }
}

#[test]
fn usage_sql_clears_legacy_output_price_column_on_upsert() {
    assert!(super::super::UPSERT_SQL.contains("output_price_per_1m = NULL"));
    assert!(include_str!("../mod.rs").contains(".bind(None::<f64>)"));
}

#[test]
fn usage_sql_clears_legacy_header_columns_on_upsert() {
    assert!(super::super::UPSERT_SQL.contains("request_headers = NULL"));
    assert!(super::super::UPSERT_SQL.contains("provider_request_headers = NULL"));
    assert!(super::super::UPSERT_SQL.contains("response_headers = NULL"));
    assert!(super::super::UPSERT_SQL.contains("client_response_headers = NULL"));
}

#[test]
fn usage_sql_terminal_snapshots_replace_sparse_routing_and_settlement_facts() {
    assert!(super::super::UPSERT_SQL.contains(
        "target_model = CASE WHEN \"usage\".billing_status = 'pending' AND $57 THEN CASE WHEN EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN EXCLUDED.target_model"
    ));
    let routing = super::super::UPSERT_USAGE_ROUTING_SNAPSHOT_SQL;
    assert!(routing.contains("candidate_id = CASE WHEN $14 THEN EXCLUDED.candidate_id"));
    assert!(
        routing.contains("selected_provider_id = CASE WHEN $14 THEN EXCLUDED.selected_provider_id")
    );
    let settlement = super::super::UPSERT_USAGE_SETTLEMENT_PRICING_SNAPSHOT_SQL;
    assert!(settlement
        .contains("settlement_snapshot = CASE WHEN $30 THEN EXCLUDED.settlement_snapshot"));
    assert!(
        settlement.contains("billing_dimensions = CASE WHEN $30 THEN EXCLUDED.billing_dimensions")
    );
    assert!(settlement.contains("rate_multiplier = CASE WHEN $30 THEN EXCLUDED.rate_multiplier"));
    assert!(include_str!("../mod.rs").contains(".bind(replace_existing)"));
}

#[test]
fn usage_sql_clears_stale_failure_fields_for_non_failed_status_updates() {
    assert!(super::super::UPSERT_SQL.contains(
            "WHEN EXCLUDED.status IN ('pending', 'streaming', 'completed', 'cancelled') AND EXCLUDED.status_code IS NULL THEN NULL"
        ));
    assert!(super::super::UPSERT_SQL.contains(
            "WHEN EXCLUDED.status IN ('pending', 'streaming', 'completed', 'cancelled') THEN EXCLUDED.error_message"
        ));
    assert!(super::super::UPSERT_SQL.contains(
            "WHEN EXCLUDED.status IN ('pending', 'streaming', 'completed', 'cancelled') THEN EXCLUDED.error_category"
        ));
}

#[test]
fn stale_cleanup_failed_candidate_sql_orders_by_effective_timestamp() {
    let sql = super::super::SELECT_LATEST_FAILED_CANDIDATE_FOR_STALE_REQUESTS_SQL;
    assert!(sql.contains("COALESCE(finished_at, started_at, created_at) DESC"));
    assert!(!sql.contains("error_message"));
    assert!(!sql.contains("finished_at DESC NULLS LAST"));
    assert!(!sql.contains("started_at DESC NULLS LAST"));
}

#[test]
fn usage_sql_does_not_allow_streaming_to_regress_back_to_pending() {
    assert!(super::super::UPSERT_SQL.contains(
            "WHEN \"usage\".status = 'streaming' AND EXCLUDED.status = 'pending' THEN \"usage\".status_code"
        ));
    assert!(super::super::UPSERT_SQL.contains(
            "WHEN \"usage\".status = 'streaming' AND EXCLUDED.status = 'streaming' AND EXCLUDED.status_code IS NULL THEN \"usage\".status_code"
        ));
    assert!(super::super::UPSERT_SQL.contains(
            "WHEN \"usage\".status = 'streaming' AND EXCLUDED.status = 'pending' THEN \"usage\".error_message"
        ));
    assert!(super::super::UPSERT_SQL.contains(
        "WHEN \"usage\".status = 'streaming' AND EXCLUDED.status = 'pending' THEN \"usage\".status"
    ));
}

#[test]
fn first_byte_upsert_sql_is_single_row_guarded_and_preserves_existing_metadata() {
    let sql = normalize_newlines(super::super::UPSERT_FIRST_BYTE_SQL);
    let revision_guard = r#"AND EXCLUDED.updated_at_unix_secs >= COALESCE(
    NULLIF("usage".updated_at_unix_secs, 0),
    CAST(EXTRACT(EPOCH FROM "usage".created_at) AS BIGINT)
  )"#;
    assert_eq!(sql.matches("INSERT INTO").count(), 1);
    assert!(!sql.contains("usage_http_audits"));
    assert!(!sql.contains("usage_routing_snapshots"));
    assert!(!sql.contains("usage_settlement_snapshots"));
    assert!(!sql.contains("usage_counter_deltas"));
    assert!(sql.contains("'streaming'"));
    assert!(sql.contains("'pending'"));
    assert!(sql.contains("WHERE \"usage\".billing_status = 'pending'"));
    assert!(sql.contains("\"usage\".status IN ('pending', 'streaming')"));
    assert!(sql.contains("\"usage\".finalized_at IS NULL"));
    assert!(sql.contains(revision_guard));
    assert!(normalize_newlines(include_str!("../mod.rs")).contains(revision_guard));
    assert!(sql.contains("$22::jsonb->>'upstream_is_stream'"));
    assert!(sql.contains("\"usage\".upstream_is_stream"));

    let update_clause = sql
        .split_once("DO UPDATE SET")
        .map(|(_, update)| update)
        .expect("first-byte SQL should have an update clause");
    assert!(update_clause.contains(
        "request_metadata = COALESCE(\"usage\".request_metadata, EXCLUDED.request_metadata)"
    ));
    assert!(update_clause.contains(
        "WHEN \"usage\".first_byte_time_ms IS NOT NULL AND \"usage\".first_byte_time_ms <> 0"
    ));
    assert!(sql.contains("RETURNING\n  request_id,\n  provider_api_key_id"));
}

#[test]
fn first_byte_batch_partition_preserves_duplicate_input_order_and_fields() {
    let mut first_a = first_byte_usage_record(
        "req-first-byte-partition-a",
        "provider-first-byte-partition",
        100,
        Some(json!({"sequence": "a-first"})),
    );
    first_a.first_byte_time_ms = Some(30);
    first_a.response_time_ms = Some(31);

    let mut unique = first_byte_usage_record(
        "req-first-byte-partition-unique",
        "provider-first-byte-partition",
        101,
        Some(json!({"sequence": "unique"})),
    );
    unique.first_byte_time_ms = Some(40);
    unique.response_time_ms = Some(41);

    let mut replay_a = first_a.clone();
    replay_a.first_byte_time_ms = Some(7);
    replay_a.response_time_ms = Some(99);
    replay_a.request_metadata = Some(json!({"sequence": "a-replay"}));

    let mut first_c = first_byte_usage_record(
        "req-first-byte-partition-c",
        "provider-first-byte-partition",
        102,
        Some(json!({"sequence": "c-first"})),
    );
    first_c.first_byte_time_ms = Some(50);
    first_c.response_time_ms = Some(51);
    let mut replay_c = first_c.clone();
    replay_c.first_byte_time_ms = Some(5);
    replay_c.response_time_ms = Some(52);
    replay_c.request_metadata = Some(json!({"sequence": "c-replay"}));

    let (batch, fallback) = super::super::partition_first_byte_usages(vec![
        first_a, unique, replay_a, first_c, replay_c,
    ])
    .expect("valid first-byte rows should partition");

    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].usage.request_id, "req-first-byte-partition-unique");
    assert_eq!(batch[0].first_byte_time_ms, Some(40));
    assert_eq!(batch[0].response_time_ms, Some(41));
    let duplicate_fields = fallback
        .iter()
        .map(|(sequence, usage)| {
            (
                *sequence,
                usage.request_id.as_str(),
                usage.first_byte_time_ms,
                usage.response_time_ms,
                usage.request_metadata.clone(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        duplicate_fields,
        vec![
            (
                0,
                "req-first-byte-partition-a",
                Some(30),
                Some(31),
                Some(json!({"sequence": "a-first"})),
            ),
            (
                2,
                "req-first-byte-partition-a",
                Some(7),
                Some(99),
                Some(json!({"sequence": "a-replay"})),
            ),
            (
                3,
                "req-first-byte-partition-c",
                Some(50),
                Some(51),
                Some(json!({"sequence": "c-first"})),
            ),
            (
                4,
                "req-first-byte-partition-c",
                Some(5),
                Some(52),
                Some(json!({"sequence": "c-replay"})),
            ),
        ]
    );
}

#[test]
fn first_byte_provider_counter_transitions_cover_key_changes_and_same_key_noops() {
    let usage = |request_id: &str, key_id: Option<&str>, created_at_unix_secs: u64| {
        super::super::InsertedPendingUsage {
            request_id: request_id.to_string(),
            provider_api_key_id: key_id.map(ToOwned::to_owned),
            created_at_unix_secs,
        }
    };

    let missing_after = usage("req-missing", Some("key-new"), 10);
    let missing = super::super::first_byte_provider_contribution_transitions(None, &missing_after);
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].key_id, "key-new");
    assert_eq!(missing[0].delta.request_count, 1);
    assert_eq!(missing[0].delta.candidate_last_used_at_unix_secs, Some(10));
    assert_eq!(missing[0].delta.usage_created_at_unix_secs, Some(10));

    let no_key_before = usage("req-no-key", None, 11);
    let no_key_after = usage("req-no-key", Some("key-added"), 11);
    let no_key = super::super::first_byte_provider_contribution_transitions(
        Some(&no_key_before),
        &no_key_after,
    );
    assert_eq!(no_key.len(), 1);
    assert_eq!(no_key[0].key_id, "key-added");
    assert_eq!(no_key[0].delta.request_count, 1);

    let changed_before = usage("req-changed", Some("key-a"), 12);
    let changed_after = usage("req-changed", Some("key-b"), 12);
    let changed = super::super::first_byte_provider_contribution_transitions(
        Some(&changed_before),
        &changed_after,
    );
    assert_eq!(changed.len(), 2);
    assert_eq!(changed[0].key_id, "key-a");
    assert_eq!(changed[0].delta.request_count, -1);
    assert_eq!(changed[0].delta.removed_last_used_at_unix_secs, Some(12));
    assert_eq!(changed[1].key_id, "key-b");
    assert_eq!(changed[1].delta.request_count, 1);
    assert_eq!(changed[1].delta.candidate_last_used_at_unix_secs, Some(12));

    let same_before = usage("req-same", Some("key-same"), 13);
    let same_after = usage("req-same", Some("key-same"), 13);
    assert!(super::super::first_byte_provider_contribution_transitions(
        Some(&same_before),
        &same_after,
    )
    .is_empty());

    let newer_after = usage("req-same", Some("key-same"), 14);
    let newer = super::super::first_byte_provider_contribution_transitions(
        Some(&same_before),
        &newer_after,
    );
    assert_eq!(newer.len(), 1);
    assert_eq!(newer[0].key_id, "key-same");
    assert_eq!(newer[0].delta.request_count, 0);
    assert_eq!(newer[0].delta.candidate_last_used_at_unix_secs, Some(14));
}

#[test]
fn first_byte_provider_counter_batch_prepares_all_columns_before_query_building() {
    let input = super::super::UsageCounterDeltaInsert {
        request_id: "  req-counter-prepared  ",
        kind: "provider_api_key",
        target_id: "  key-counter-prepared  ",
        target_node_generation: None,
        request_count_delta: 1,
        total_requests_delta: 2,
        success_count_delta: 3,
        error_count_delta: 4,
        dns_failures_delta: 5,
        stream_errors_delta: 6,
        total_tokens_delta: 7,
        total_cost_usd_delta: f64::NAN,
        total_response_time_ms_delta: 8,
        last_used_at_unix_secs: Some(9),
        last_used_ip: Some("  127.0.0.1  "),
        candidate_last_used_at_unix_secs: Some(10),
        removed_last_used_at_unix_secs: Some(11),
        usage_created_at_unix_secs: Some(12),
    };
    let prepared = super::super::prepare_usage_counter_delta_insert(input)
        .expect("counter row should prepare")
        .expect("non-empty counter row should remain");

    assert!(!prepared.id.is_empty());
    assert_eq!(prepared.request_id, "req-counter-prepared");
    assert_eq!(prepared.kind, "provider_api_key");
    assert_eq!(prepared.target_id, "key-counter-prepared");
    assert_eq!(prepared.request_count_delta, 1);
    assert_eq!(prepared.total_requests_delta, 2);
    assert_eq!(prepared.success_count_delta, 3);
    assert_eq!(prepared.error_count_delta, 4);
    assert_eq!(prepared.dns_failures_delta, 5);
    assert_eq!(prepared.stream_errors_delta, 6);
    assert_eq!(prepared.total_tokens_delta, 7);
    assert_eq!(prepared.total_cost_usd_delta, 0.0);
    assert_eq!(prepared.total_response_time_ms_delta, 8);
    assert_eq!(prepared.last_used_at_unix_secs, Some(9));
    assert_eq!(prepared.last_used_ip.as_deref(), Some("127.0.0.1"));
    assert_eq!(prepared.candidate_last_used_at_unix_secs, Some(10));
    assert_eq!(prepared.removed_last_used_at_unix_secs, Some(11));
    assert_eq!(prepared.usage_created_at_unix_secs, Some(12));
    const { assert!(super::super::USAGE_COUNTER_DELTA_INSERT_BATCH_SIZE >= 1_024) };
    assert!(
        super::super::USAGE_COUNTER_DELTA_INSERT_BATCH_SIZE
            * super::super::USAGE_COUNTER_DELTA_INSERT_BINDS_PER_ROW
            <= u16::MAX as usize
    );
    assert!(
        (super::super::USAGE_COUNTER_DELTA_INSERT_BATCH_SIZE + 1)
            * super::super::USAGE_COUNTER_DELTA_INSERT_BINDS_PER_ROW
            > u16::MAX as usize
    );

    let out_of_range = super::super::UsageCounterDeltaInsert {
        request_id: "req-counter-out-of-range",
        kind: "provider_api_key",
        target_id: "key-counter-out-of-range",
        target_node_generation: None,
        request_count_delta: 0,
        total_requests_delta: 0,
        success_count_delta: 0,
        error_count_delta: 0,
        dns_failures_delta: 0,
        stream_errors_delta: 0,
        total_tokens_delta: 0,
        total_cost_usd_delta: 0.0,
        total_response_time_ms_delta: 0,
        last_used_at_unix_secs: None,
        last_used_ip: None,
        candidate_last_used_at_unix_secs: Some(u64::MAX),
        removed_last_used_at_unix_secs: None,
        usage_created_at_unix_secs: None,
    };
    assert!(super::super::prepare_usage_counter_delta_insert(out_of_range).is_err());
}

#[test]
fn first_byte_contribution_read_uses_a_fresh_statement_after_canonical_locks() {
    assert!(super::super::LOCK_USAGE_REQUEST_IDS_SQL.contains("pg_advisory_xact_lock"));
    assert!(super::super::FIND_USAGE_PROVIDER_CONTRIBUTIONS_SQL.contains("FROM \"usage\" AS u"));
    assert!(!super::super::FIND_USAGE_PROVIDER_CONTRIBUTIONS_SQL.contains("pg_advisory_xact_lock"));
    assert!(!super::super::FIND_USAGE_PROVIDER_CONTRIBUTIONS_SQL.contains("MATERIALIZED"));

    let source = include_str!("../mod.rs");
    let implementation = source
        .split("async fn lock_and_find_usage_provider_contributions_in_tx")
        .nth(1)
        .and_then(|tail| {
            tail.split("fn pending_provider_api_key_contribution")
                .next()
        })
        .expect("lock-and-find implementation should be present");
    let lock_position = implementation
        .find("lock_usage_request_ids_in_tx(tx, request_ids).await?")
        .expect("canonical lock statement should run first");
    let read_position = implementation
        .find("sqlx::query(FIND_USAGE_PROVIDER_CONTRIBUTIONS_SQL)")
        .expect("provider contributions should be read in a later statement");
    assert!(lock_position < read_position);
}

#[test]
fn first_byte_batch_writes_provider_counter_transitions_in_bulk() {
    let source = include_str!("../mod.rs");
    let implementation = source
        .split("pub async fn upsert_first_byte_many")
        .nth(1)
        .and_then(|tail| tail.split("async fn execute_first_byte_batch").next())
        .expect("first-byte batch implementation should be present");
    assert!(implementation.contains("prepare_first_byte_provider_contribution_transitions"));
    assert!(implementation.contains("insert_usage_counter_deltas_batch_in_tx"));
    assert!(!implementation.contains("enqueue_first_byte_provider_contribution_transition_in_tx"));
    const { assert!(super::super::USAGE_COUNTER_DELTA_INSERT_BATCH_SIZE > 512 * 2) };
}

#[test]
fn usage_batch_advisory_locks_share_the_canonical_key_and_stable_order() {
    assert!(super::super::LOCK_USAGE_REQUEST_ID_SQL.contains("hashtext($1)::BIGINT"));
    assert!(super::super::LOCK_USAGE_REQUEST_IDS_SQL
        .contains("SELECT DISTINCT hashtext(request_id)::BIGINT AS lock_id"));
    assert!(super::super::LOCK_USAGE_REQUEST_IDS_SQL.contains("UNNEST($1::TEXT[])"));
    assert!(super::super::LOCK_USAGE_REQUEST_IDS_SQL.contains("ORDER BY lock_id"));
}

#[test]
fn usage_sql_recovers_void_failures_before_upsert_and_settlement() {
    assert!(super::super::RESET_STALE_VOID_USAGE_SQL.contains("UPDATE \"usage\""));
    assert!(super::super::RESET_STALE_VOID_USAGE_SQL.contains("billing_status = 'pending'"));
    assert!(super::super::RESET_STALE_VOID_USAGE_SQL.contains("finalized_at = NULL"));
    assert!(super::super::RESET_STALE_VOID_USAGE_SQL.contains("status IN ('failed', 'cancelled')"));
    assert!(super::super::RESET_STALE_VOID_USAGE_SETTLEMENT_SNAPSHOT_SQL
        .contains("UPDATE usage_settlement_snapshots"));
    assert!(super::super::RESET_STALE_VOID_USAGE_SETTLEMENT_SNAPSHOT_SQL
        .contains("billing_status = 'pending'"));
    assert!(super::super::RESET_STALE_VOID_USAGE_SETTLEMENT_SNAPSHOT_SQL
        .contains("finalized_at = NULL"));
}

#[tokio::test(flavor = "current_thread")]
async fn usage_body_decode_does_not_block_the_async_runtime_thread() {
    let runtime_thread = std::thread::current().id();
    let payload = json!({"message": "background decoding"});
    let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
        .expect("body should serialize");
    let encoding = storage.encoding;
    let bytes = storage.payload.expect("body should be detached");

    let decoded = super::super::decode_usage_body_in_background(move || {
        assert_ne!(std::thread::current().id(), runtime_thread);
        decode_container(encoding, &bytes)
            .map(Some)
            .map_err(|error| {
                aether_data_contracts::DataLayerError::UnexpectedValue(format!(
                    "failed to parse usage json: {error}"
                ))
            })
    })
    .await
    .expect("body should decode");

    assert_eq!(
        container_body(
            &decoded.expect("body should decode"),
            UsageBodyField::RequestBody
        ),
        Some(payload)
    );
}

#[test]
fn explicit_none_capture_replaces_stale_request_body_facts_even_with_a_residual_body() {
    let stale_body = json!({
        "reasoning_effort": "xhigh",
        "service_tier": "priority"
    });

    assert!(request_body_capture_replaces_derived_facts(
        Some(&stale_body),
        Some(UsageBodyCaptureState::None),
    ));
    assert!(request_body_capture_replaces_derived_facts(
        Some(&stale_body),
        None,
    ));
    assert!(request_body_capture_replaces_derived_facts(
        Some(&stale_body),
        Some(UsageBodyCaptureState::Disabled),
    ));
    assert!(request_body_capture_replaces_derived_facts(
        None,
        Some(UsageBodyCaptureState::Truncated),
    ));
    assert!(!request_body_capture_replaces_derived_facts(None, None,));
}

#[test]
fn terminal_capture_rejects_late_non_terminal_updates() {
    assert!(usage_capture_update_allowed(None, "pending"));
    assert!(usage_capture_update_allowed(
        Some(("pending", "pending")),
        "streaming",
    ));
    assert!(usage_capture_update_allowed(
        Some(("failed", "pending")),
        "completed",
    ));
    assert!(!usage_capture_update_allowed(
        Some(("completed", "pending")),
        "pending",
    ));
    assert!(!usage_capture_update_allowed(
        Some(("failed", "pending")),
        "streaming",
    ));
    assert!(!usage_capture_update_allowed(
        Some(("streaming", "pending")),
        "pending",
    ));
    assert!(!usage_capture_update_allowed(
        Some(("completed", "settled")),
        "completed",
    ));
}
