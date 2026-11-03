use super::*;

#[test]
fn usage_sql_reads_split_capture_tables_for_single_record_fetches() {
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
    ] {
        assert!(sql.contains("LEFT JOIN usage_http_headers"));
        assert!(sql.contains("usage_http_headers.client_request AS request_headers"));
        assert!(sql.contains("usage_http_headers.client_response AS client_response_headers"));
        // Bodies are attached by a dedicated single-row query; the accounting
        // join never needed the payload columns.
        assert!(!sql.contains("LEFT JOIN usage_http_bodies"));
    }
}

#[test]
fn usage_sql_single_record_fetches_preserve_full_request_metadata() {
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
    ] {
        assert!(
            sql.contains("\n  \"usage\".request_metadata,\n"),
            "single-record usage detail queries must preserve the full request metadata, \
             including WebSocket and Live session summaries"
        );
        assert!(
            !sql.contains("END AS request_metadata"),
            "bounded list metadata projection must not leak into detail queries"
        );
    }
}

#[test]
fn usage_sql_reads_routing_snapshots_for_single_record_fetches() {
    assert!(super::super::FIND_BY_REQUEST_ID_SQL.contains("LEFT JOIN usage_routing_snapshots"));
    assert!(super::super::FIND_BY_ID_SQL.contains("LEFT JOIN usage_routing_snapshots"));
    assert!(super::super::FIND_BY_REQUEST_ID_SQL.contains("routing_candidate_id"));
    assert!(super::super::FIND_BY_REQUEST_ID_SQL.contains("routing_candidate_index"));
    assert!(super::super::FIND_BY_ID_SQL.contains("routing_local_execution_runtime_miss_reason"));
}

#[test]
fn usage_sql_uses_json_null_placeholders_for_usage_payload_columns() {
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX.contains("NULL::jsonb AS request_headers"));
    // Bounded list queries never load captured bodies: the bytes live in their
    // own table and only detail reads ask for them.
    assert!(!super::super::LIST_USAGE_AUDITS_PREFIX.contains("usage_http_bodies"));
    assert!(!super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("usage_http_bodies"));
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX.contains("usage_routing_snapshots.candidate_id"));
    assert!(
        super::super::LIST_USAGE_AUDITS_PREFIX.contains("usage_routing_snapshots.candidate_index")
    );
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX.contains("request_metadata->>'candidate_index'"));
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX.contains("LEFT JOIN usage_routing_snapshots"));
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX.contains("LEFT JOIN usage_settlement_snapshots"));
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX
        .contains("settlement_billing_snapshot_schema_version"));
    assert!(
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("NULL::jsonb AS request_headers")
    );
    assert!(!super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("usage_http_bodies"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("usage_routing_snapshots.candidate_index"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("request_metadata->>'candidate_index'"));
    assert!(
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("LEFT JOIN usage_routing_snapshots")
    );
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("usage_routing_snapshots.execution_path"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("LEFT JOIN usage_settlement_snapshots"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("settlement_price_per_request"));
    for sql in [
        super::super::LIST_USAGE_AUDITS_PREFIX,
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX,
    ] {
        assert!(sql.contains("jsonb_strip_nulls(jsonb_build_object("));
        assert!(!sql.contains("jsonb_typeof(\"usage\".provider_request_body::jsonb)"));
        assert!(!sql.contains("\"usage\".provider_request_body->"));
        assert!(!sql.contains("jsonb_typeof(\"usage\".provider_request_body)"));
        assert!(sql.contains("'client_ip'"));
        assert!(sql.contains("request_metadata->>'client_ip'"));
        assert!(sql.contains("'user_agent'"));
        assert!(sql.contains("request_metadata->>'user_agent'"));
        assert!(sql.contains("request_metadata->>'requested_reasoning_effort'"));
        assert!(sql.contains("request_metadata->>'provider_reasoning_effort'"));
        assert!(sql.contains("request_metadata->>'provider_service_tier'"));
        assert!(sql.contains("request_metadata->>'provider_actual_service_tier'"));
        assert!(sql.contains("request_metadata->>'provider_response_model'"));
        assert!(sql.contains("'provider_response_model'"));
        assert!(sql.contains("request_metadata->>'websocket_mode'"));
        assert!(sql.contains("'websocket_mode'"));
        assert!(sql.contains("AS client_family"));
        assert!(sql.contains("request_metadata->'client_session_affinity'->>'client_family'"));
        assert!(sql.contains("request_metadata->>'client_family'"));
        assert!(sql.contains("CAST(\"usage\".input_tokens AS INTEGER) AS input_tokens"));
        assert!(sql.contains(
            "usage_settlement_snapshots.billing_input_tokens AS settlement_billing_input_tokens"
        ));
        assert!(sql.contains("usage_settlement_snapshots.billing_cache_creation_1h_tokens"));
        assert!(sql.contains(
            "CAST(usage_settlement_snapshots.billing_total_cost_usd AS DOUBLE PRECISION)"
        ));
    }
    assert!(!super::super::LIST_USAGE_AUDITS_PREFIX.contains("NULL::json AS "));
    assert!(!super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("NULL::json AS "));
}

#[test]
fn usage_sql_list_queries_project_bounded_live_and_realtime_metadata() {
    for sql in [
        super::super::LIST_USAGE_AUDITS_PREFIX,
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX,
    ] {
        assert!(sql.contains("'websocket_transport'"));
        assert!(
            sql.contains("NULLIF(BTRIM(\"usage\".request_metadata->>'websocket_transport'), '')")
        );

        for key in ["usage_available", "usage_pricing_available"] {
            assert!(sql.contains(format!("'{key}'").as_str()));
            assert!(sql.contains(
                format!("WHEN (\"usage\".request_metadata->>'{key}') IN ('true', 'false')")
                    .as_str()
            ));
            assert!(sql.contains(
                format!("THEN (\"usage\".request_metadata->>'{key}')::boolean").as_str()
            ));
        }

        for key in ["live_session", "realtime_session"] {
            assert!(sql.contains(format!("'{key}'").as_str()));
            assert!(sql.contains(
                format!("WHEN jsonb_typeof(\"usage\".request_metadata->'{key}') = 'object'")
                    .as_str()
            ));
            assert!(sql.contains(format!("THEN \"usage\".request_metadata->'{key}'").as_str()));
        }

        assert!(sql.contains("END AS request_metadata"));
        assert!(!sql.contains("\n  \"usage\".request_metadata,\n"));
    }
}

#[test]
fn usage_sql_websocket_filter_compares_json_metadata_without_boolean_casts() {
    let mut builder = QueryBuilder::<Postgres>::new("SELECT * FROM usage");
    let mut has_where = false;
    push_postgres_usage_websocket_filter(&mut builder, &mut has_where, Some(true));

    assert!(has_where);
    assert!(builder
        .sql()
        .as_str()
        .contains("LOWER(COALESCE(\"usage\".request_metadata->>'websocket_mode', 'false'))"));
    assert!(!builder.sql().as_str().contains("::boolean"));
}

#[test]
fn usage_sql_casts_json_payload_bind_parameters_explicitly() {
    for placeholder in [41, 42, 43, 44, 45] {
        assert!(
            super::super::UPSERT_SQL.contains(format!("${placeholder}::jsonb").as_str()),
            "missing ::jsonb cast for placeholder ${placeholder}"
        );
    }
}

#[test]
fn usage_sql_settlement_pricing_snapshot_billing_values_use_authoritative_incoming_values() {
    let sql = super::super::UPSERT_USAGE_SETTLEMENT_PRICING_SNAPSHOT_SQL;
    for field in [
        "billing_input_tokens",
        "billing_effective_input_tokens",
        "billing_output_tokens",
        "billing_cache_creation_tokens",
        "billing_cache_creation_5m_tokens",
        "billing_cache_creation_1h_tokens",
        "billing_cache_read_tokens",
        "billing_total_input_context",
        "billing_cache_creation_cost_usd",
        "billing_cache_read_cost_usd",
        "billing_total_cost_usd",
        "billing_actual_total_cost_usd",
    ] {
        let assignment = format!(
            "{field} = CASE WHEN $30 THEN EXCLUDED.{field} ELSE COALESCE(EXCLUDED.{field}, usage_settlement_snapshots.{field}) END"
        );
        assert!(
            sql.contains(assignment.as_str()),
            "missing authoritative billing snapshot assignment: {assignment}"
        );
        assert!(
            !sql.contains(format!("{field} = GREATEST(").as_str()),
            "billing snapshot field should not use max-only conflict resolution: {field}"
        );
    }
}

#[test]
fn usage_sql_capture_guard_covers_metadata_and_the_capture_tables() {
    // Bodies are not columns of `usage` any more: the accounting row only guards
    // its metadata, and the capture tables own the bytes.
    assert!(super::super::UPSERT_SQL
        .contains("request_metadata = CASE WHEN \"usage\".billing_status = 'pending' AND $49"));
    for body in [
        "request_body",
        "provider_request_body",
        "response_body",
        "client_response_body",
    ] {
        assert!(
            !super::super::UPSERT_SQL.contains(&format!("{body} = CASE")),
            "{body} must not be written on the accounting row"
        );
    }
    let capture = include_str!("../http_capture.rs");
    for column in [
        "client_request",
        "provider_request",
        "provider_response",
        "client_response",
    ] {
        assert!(
            capture.contains(format!("{column},").as_str()),
            "missing body column: {column}"
        );
        assert!(
            capture.contains(format!("{column}_encoding").as_str()),
            "missing encoding column: {column}"
        );
    }
    let source = include_str!("../mod.rs");
    assert!(source.contains(".bind(capture_update_allowed)"));
    assert!(source.contains("if capture_update_allowed {"));
    // The capture write must be fed from the prepared row: the accounting row
    // leaves `upsert` sanitized to None, and reading its capture fields in the
    // sync is exactly how a full capture was lost while the metadata ledger
    // still said the sides were captured.
    assert!(!source.contains("client_request_headers: usage.request_headers.as_ref()"));
    assert!(!source.contains("client_request_body: usage.request_body.as_ref()"));
}

#[test]
fn managed_capture_preparation_hands_bodies_to_storage_without_reparsing() {
    use aether_data_contracts::repository::usage::{
        sanitize_usage_for_persistence, CapturedBody, UsageCaptureMemoryBudget,
    };

    let mut usage = fast_clear_usage_record(
        "req-managed-capture",
        "managed-capture",
        100,
        true,
        Some(CapturedBody::json(json!({"model": "gpt-5"}))),
        Some("priority"),
    );
    let bodies = [
        CapturedBody::json(
            json!({"messages": [{"role": "user", "content": "request".repeat(4096)}]}),
        ),
        CapturedBody::json(
            json!({"input": "provider request".repeat(4096), "service_tier": "priority"}),
        ),
        CapturedBody::json(json!({"output": "provider response".repeat(4096)})),
        CapturedBody::json(json!({"output": "client response".repeat(4096)})),
    ];
    usage.request_body = Some(bodies[0].clone());
    usage.provider_request_body = Some(bodies[1].clone());
    usage.response_body = Some(bodies[2].clone());
    usage.client_response_body = Some(bodies[3].clone());
    usage.request_headers = Some(json!({"content-type": "application/json"}));
    usage.cache_read_input_tokens = Some(0);
    usage.total_cost_usd = Some(0.25);
    usage.actual_total_cost_usd = Some(0.125);
    let expected_accounting = sanitize_usage_for_persistence(usage.clone());
    let bytes: usize = bodies
        .iter()
        .map(CapturedBody::retained_bytes_estimate)
        .sum();
    let budget = Arc::new(UsageCaptureMemoryBudget::new(bytes));
    assert!(usage.capture_retention.reserve(Arc::clone(&budget), bytes));

    let (accounting, prepared) = prepare_usage_for_persistence(usage);
    let prepared = prepared.expect("managed capture should prepare without cloning bodies");
    assert_eq!(budget.retained_bytes(), 0);
    assert_eq!(budget.downgraded_total(), 0);
    assert_eq!(accounting, expected_accounting);
    // The prepared row keeps the exact bytes and their encoding: nothing is
    // re-serialized on the way to the database.
    assert_eq!(
        prepared.client_request_body.bytes.as_deref(),
        bodies[0].bytes()
    );
    assert_eq!(
        prepared.client_request_body.encoding.as_deref(),
        Some("json")
    );
    assert_eq!(
        prepared.client_response_body.bytes.as_deref(),
        bodies[3].bytes(),
    );
    assert_eq!(
        prepared.client_response_body.encoding.as_deref(),
        Some("json")
    );
    assert!(!prepared.client_request_body.withdrawn);
    assert!(!prepared.client_response_body.withdrawn);
}

/// The terminal `upsert` writes the capture tables from the prepared row, not
/// from the accounting row: preparation sanitizes the accounting row to drop
/// every capture field, so a sync that reads payloads off it would store
/// nothing while the metadata ledger still claims the sides were captured.
#[test]
fn terminal_preparation_carries_every_captured_side_for_storage() {
    let mut usage =
        fast_clear_usage_record("req-all-sides", "all-sides", 100, true, None, None);
    usage.request_headers = Some(json!({"content-type": "application/json"}));
    usage.provider_request_headers = Some(json!({"authorization": "Bearer upstream"}));
    usage.response_headers = Some(json!({"content-type": "text/event-stream"}));
    usage.client_response_headers = Some(json!({"x-marker": "client"}));
    usage.request_body = Some(CapturedBody::json(json!({"messages": []})));
    usage.provider_request_body = Some(CapturedBody::json(json!({"input": "upstream"})));
    usage.response_body = Some(CapturedBody::from_bytes(
        b"data: chunk\n\n".to_vec(),
        BodyEncoding::Sse,
        true,
    ));
    usage.client_response_body = Some(CapturedBody::json(json!({"output": "client"})));

    let (_, prepared) = prepare_usage_for_persistence(usage);
    let prepared = prepared.expect("usage should prepare");

    // Every side survives preparation with its exact bytes and encoding, so the
    // capture-table write can persist the whole exchange.
    assert_eq!(
        prepared.client_request_headers.as_deref(),
        Some(r#"{"content-type":"application/json"}"#)
    );
    assert_eq!(
        prepared.provider_request_headers.as_deref(),
        Some(r#"{"authorization":"Bearer upstream"}"#)
    );
    assert_eq!(
        prepared.provider_response_headers.as_deref(),
        Some(r#"{"content-type":"text/event-stream"}"#)
    );
    assert_eq!(
        prepared.client_response_headers.as_deref(),
        Some(r#"{"x-marker":"client"}"#)
    );
    assert_eq!(
        prepared.client_request_body.bytes.as_deref(),
        Some(br#"{"messages":[]}"#.as_slice())
    );
    assert_eq!(
        prepared.client_request_body.encoding.as_deref(),
        Some("json")
    );
    assert_eq!(
        prepared.provider_request_body.bytes.as_deref(),
        Some(br#"{"input":"upstream"}"#.as_slice())
    );
    assert_eq!(
        prepared.client_response_body.bytes.as_deref(),
        Some(br#"{"output":"client"}"#.as_slice())
    );
    let response = &prepared.provider_response_body;
    assert_eq!(response.bytes.as_deref(), Some(b"data: chunk\n\n".as_slice()));
    assert_eq!(response.encoding.as_deref(), Some("sse"));
    assert!(response.truncated);
    for side in [
        &prepared.client_request_body,
        &prepared.client_response_body,
        &prepared.provider_request_body,
        response,
    ] {
        assert!(!side.withdrawn, "captured side must not be withdrawn");
    }
}

/// A side set to `Disabled` or `Unavailable` withdraws the bytes an earlier
/// write stored; a side the write says nothing about keeps them.
#[test]
fn terminal_preparation_marks_disabled_sides_as_withdrawals() {
    let mut usage = fast_clear_usage_record("req-withdraw", "withdraw", 100, true, None, None);
    usage.request_body = Some(CapturedBody::Disabled);
    usage.provider_request_body = Some(CapturedBody::Unavailable);
    usage.response_body = Some(CapturedBody::json(json!({"output": "kept"})));

    let (_, prepared) = prepare_usage_for_persistence(usage);
    let prepared = prepared.expect("usage should prepare");

    assert!(prepared.client_request_body.withdrawn);
    assert!(prepared.client_request_body.bytes.is_none());
    assert!(prepared.provider_request_body.withdrawn);
    assert!(prepared.provider_request_body.bytes.is_none());
    // Sides the write leaves silent keep whatever was stored before.
    assert!(!prepared.client_response_body.withdrawn);
    assert!(!prepared.provider_response_body.withdrawn);
    assert_eq!(
        prepared.provider_response_body.bytes.as_deref(),
        Some(br#"{"output":"kept"}"#.as_slice())
    );
}

#[test]
fn disabled_capture_keeps_previously_derived_facts() {
    use aether_data_contracts::repository::usage::CapturedBody;

    let mut usage = fast_clear_usage_record(
        "req-disabled-residual",
        "disabled-residual",
        100,
        true,
        Some(CapturedBody::Disabled),
        Some("priority"),
    );
    usage.request_metadata = Some(json!({
        "trace_id": "trace-1",
        "provider_service_tier": "priority",
        "provider_reasoning_effort": "high",
        "provider_cache_ttl_minutes": 30,
        "provider_request_body_ref": "usage://request/req-disabled-residual/provider_request_body"
    }));

    let (_, prepared) = prepare_usage_for_persistence(usage);
    let prepared = prepared.expect("usage should prepare");
    // A disabled capture cannot replace the facts derived from a body it never
    // saw, so whatever the event carried stays.
    assert!(!prepared.replace_provider_request_body_facts);
    assert_eq!(
        prepared.request_metadata_value,
        Some(json!({
            "body_capture": {"provider_request": {"state": "disabled"}},
            "trace_id": "trace-1",
            "provider_service_tier": "priority",
            "provider_reasoning_effort": "high",
            "provider_cache_ttl_minutes": 30
        }))
    );
}

#[test]
fn prepare_request_metadata_for_body_storage_strips_retired_body_ref_keys() {
    use aether_data_contracts::repository::usage::CapturedBody;

    let request_body = CapturedBody::json(json!({"message": "inline"}));
    let metadata = prepare_request_metadata_for_body_storage(
        Some(json!({
            "trace_id": "trace-1",
            "request_body_ref": "blob://old-request",
            "provider_request_body_ref": "blob://old-provider"
        })),
        [
            (UsageBodyField::RequestBody, Some(&request_body)),
            (UsageBodyField::ProviderRequestBody, None),
        ],
    )
    .expect("metadata should be present");

    assert_eq!(
        metadata,
        json!({
            "trace_id": "trace-1",
            "provider_request_body_ref": "blob://old-provider"
        })
    );
}

#[test]
fn usage_routing_snapshot_from_usage_only_activates_for_routing_metadata() {
    let snapshot = usage_routing_snapshot_from_usage(
        &UpsertUsageRecord {
            capture_retention: Default::default(),
            request_id: "req-123".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: Some("provider-1".to_string()),
            provider_endpoint_id: Some("endpoint-1".to_string()),
            provider_api_key_id: Some("provider-key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(false),
            is_stream: Some(false),
            input_tokens: Some(1),
            output_tokens: Some(2),
            total_tokens: Some(3),
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
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
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
            updated_at_unix_secs: 100,
        },
        Some(&json!({
            "candidate_id": "cand-1",
            "key_name": "primary",
            "planner_kind": "claude_cli_sync",
            "route_family": "claude",
            "route_kind": "cli",
            "execution_path": "local_execution_runtime_miss",
            "local_execution_runtime_miss_reason": "all_candidates_skipped"
        })),
    );

    assert_eq!(
        snapshot,
        UsageRoutingSnapshot {
            candidate_id: Some("cand-1".to_string()),
            candidate_index: None,
            key_name: Some("primary".to_string()),
            planner_kind: Some("claude_cli_sync".to_string()),
            route_family: Some("claude".to_string()),
            route_kind: Some("cli".to_string()),
            execution_path: Some("local_execution_runtime_miss".to_string()),
            local_execution_runtime_miss_reason: Some("all_candidates_skipped".to_string()),
            selected_provider_id: Some("provider-1".to_string()),
            selected_endpoint_id: Some("endpoint-1".to_string()),
            selected_provider_api_key_id: Some("provider-key-1".to_string()),
            has_format_conversion: Some(false),
        }
    );

    let empty_snapshot = usage_routing_snapshot_from_usage(
        &UpsertUsageRecord {
            capture_retention: Default::default(),
            request_id: "req-124".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: Some("provider-2".to_string()),
            provider_endpoint_id: Some("endpoint-2".to_string()),
            provider_api_key_id: Some("provider-key-2".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(true),
            is_stream: Some(false),
            input_tokens: Some(1),
            output_tokens: Some(2),
            total_tokens: Some(3),
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
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
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
            updated_at_unix_secs: 100,
        },
        Some(&json!({"trace_id": "trace-1"})),
    );

    assert_eq!(empty_snapshot, UsageRoutingSnapshot::default());
}

#[test]
fn usage_routing_snapshot_from_usage_prefers_typed_routing_fields_without_metadata() {
    let snapshot = usage_routing_snapshot_from_usage(
        &UpsertUsageRecord {
            capture_retention: Default::default(),
            request_id: "req-typed-routing-1".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: Some("provider-1".to_string()),
            provider_endpoint_id: Some("endpoint-1".to_string()),
            provider_api_key_id: Some("provider-key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(true),
            is_stream: Some(false),
            input_tokens: Some(1),
            output_tokens: Some(2),
            total_tokens: Some(3),
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
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
            candidate_id: Some("cand-typed".to_string()),
            candidate_index: Some(2),
            key_name: Some("primary".to_string()),
            planner_kind: Some("claude_cli_sync".to_string()),
            route_family: Some("claude".to_string()),
            route_kind: Some("cli".to_string()),
            execution_path: Some("local_execution_runtime_miss".to_string()),
            local_execution_runtime_miss_reason: Some("all_candidates_skipped".to_string()),
            request_metadata: Some(json!({
                "trace_id": "trace-1"
            })),
            finalized_at_unix_secs: None,
            created_at_unix_ms: Some(100),
            updated_at_unix_secs: 100,
        },
        None,
    );

    assert_eq!(
        snapshot,
        UsageRoutingSnapshot {
            candidate_id: Some("cand-typed".to_string()),
            candidate_index: Some(2),
            key_name: Some("primary".to_string()),
            planner_kind: Some("claude_cli_sync".to_string()),
            route_family: Some("claude".to_string()),
            route_kind: Some("cli".to_string()),
            execution_path: Some("local_execution_runtime_miss".to_string()),
            local_execution_runtime_miss_reason: Some("all_candidates_skipped".to_string()),
            selected_provider_id: Some("provider-1".to_string()),
            selected_endpoint_id: Some("endpoint-1".to_string()),
            selected_provider_api_key_id: Some("provider-key-1".to_string()),
            has_format_conversion: Some(true),
        }
    );
}

#[test]
fn attach_usage_routing_snapshot_metadata_adds_missing_keys_without_overwriting_existing_values() {
    let metadata = attach_usage_routing_snapshot_metadata(
        Some(json!({
            "candidate_id": "cand-existing",
            "route_kind": "cli"
        })),
        &UsageRoutingSnapshot {
            candidate_id: Some("cand-1".to_string()),
            candidate_index: Some(2),
            key_name: Some("primary".to_string()),
            planner_kind: Some("claude_cli_sync".to_string()),
            route_family: Some("claude".to_string()),
            route_kind: Some("chat".to_string()),
            execution_path: Some("local_execution_runtime_miss".to_string()),
            local_execution_runtime_miss_reason: Some("all_candidates_skipped".to_string()),
            selected_provider_id: None,
            selected_endpoint_id: None,
            selected_provider_api_key_id: None,
            has_format_conversion: None,
        },
    )
    .expect("metadata should remain");

    assert_eq!(
        metadata,
        json!({
            "candidate_id": "cand-existing",
            "key_name": "primary",
            "planner_kind": "claude_cli_sync",
            "route_family": "claude",
            "route_kind": "cli",
            "execution_path": "local_execution_runtime_miss",
            "local_execution_runtime_miss_reason": "all_candidates_skipped"
        })
    );
}

#[test]
fn usage_settlement_pricing_snapshot_from_usage_extracts_typed_billing_fields() {
    let snapshot = usage_settlement_pricing_snapshot_from_usage(
        &UpsertUsageRecord {
            capture_retention: Default::default(),
            request_id: "req-125".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            target_model: None,
            provider_id: Some("provider-1".to_string()),
            provider_endpoint_id: Some("endpoint-1".to_string()),
            provider_api_key_id: Some("provider-key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai:chat".to_string()),
            api_family: Some("openai".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            provider_api_family: Some("openai".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(false),
            is_stream: Some(false),
            input_tokens: Some(1),
            output_tokens: Some(2),
            total_tokens: Some(3),
            cache_creation_input_tokens: None,
            cache_creation_ephemeral_5m_input_tokens: None,
            cache_creation_ephemeral_1h_input_tokens: None,
            cache_read_input_tokens: None,
            cache_creation_cost_usd: None,
            cache_read_cost_usd: None,
            output_price_per_1m: Some(15.0),
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
            provider_request_headers: None,
            provider_request_body: None,
            response_headers: None,
            response_body: None,
            client_response_headers: None,
            client_response_body: None,
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
            updated_at_unix_secs: 100,
        },
        Some(&json!({
            "rate_multiplier": 0.5,
            "is_free_tier": false,
            "billing_snapshot": {
                "schema_version": "2.0",
                "status": "complete",
                "resolved_variables": {
                    "input_price_per_1m": 3.0,
                    "output_price_per_1m": 15.0,
                    "cache_creation_price_per_1m": 3.75,
                    "cache_read_price_per_1m": 0.30,
                    "price_per_request": 0.02
                }
            }
        })),
    )
    .expect("snapshot should build");

    assert_eq!(
        snapshot,
        UsageSettlementPricingSnapshot {
            billing_status: Some("pending".to_string()),
            billing_snapshot_schema_version: Some("2.0".to_string()),
            billing_snapshot_status: Some("complete".to_string()),
            billing_input_tokens: Some(1),
            billing_effective_input_tokens: Some(1),
            billing_output_tokens: Some(2),
            billing_total_input_context: Some(1),
            rate_multiplier: Some(0.5),
            is_free_tier: Some(false),
            input_price_per_1m: Some(3.0),
            output_price_per_1m: Some(15.0),
            cache_creation_price_per_1m: Some(3.75),
            cache_read_price_per_1m: Some(0.30),
            price_per_request: Some(0.02),
            ..UsageSettlementPricingSnapshot::default()
        }
    );
}

#[test]
fn usage_settlement_pricing_snapshot_with_billing_status_only_is_still_persisted() {
    let snapshot = UsageSettlementPricingSnapshot {
        billing_status: Some("pending".to_string()),
        ..UsageSettlementPricingSnapshot::default()
    };

    assert!(snapshot.any_present());
}

#[test]
fn attach_usage_settlement_pricing_snapshot_metadata_adds_missing_values_without_overwriting() {
    let metadata = attach_usage_settlement_pricing_snapshot_metadata(
        Some(json!({
            "rate_multiplier": 1.0,
            "billing_snapshot_status": "complete"
        })),
        &UsageSettlementPricingSnapshot {
            billing_status: None,
            billing_snapshot_schema_version: Some("2.0".to_string()),
            billing_snapshot_status: Some("incomplete".to_string()),
            rate_multiplier: Some(0.5),
            is_free_tier: Some(false),
            input_price_per_1m: Some(3.0),
            output_price_per_1m: Some(15.0),
            cache_creation_price_per_1m: Some(3.75),
            cache_read_price_per_1m: Some(0.30),
            price_per_request: Some(0.02),
            ..UsageSettlementPricingSnapshot::default()
        },
    )
    .expect("metadata should remain");

    assert_eq!(
        metadata,
        json!({
            "rate_multiplier": 1.0,
            "billing_snapshot_status": "complete",
            "billing_snapshot_schema_version": "2.0",
            "is_free_tier": false,
            "input_price_per_1m": 3.0,
            "output_price_per_1m": 15.0,
            "cache_creation_price_per_1m": 3.75,
            "cache_read_price_per_1m": 0.30,
            "price_per_request": 0.02
        })
    );
}
