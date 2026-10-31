use super::*;

#[test]
fn usage_sql_reads_http_audits_for_single_record_fetches() {
    assert!(super::super::FIND_BY_REQUEST_ID_SQL.contains("LEFT JOIN usage_http_audits"));
    assert!(super::super::FIND_BY_ID_SQL.contains("LEFT JOIN usage_http_audits"));
    // The body reference is derived from the row identity, so no query reads a stored one.
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
    ] {
        assert!(!sql.contains("http_request_body_ref"));
        assert!(!sql.contains("http_client_response_body_ref"));
    }
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
    ] {
        for field in [
            "request_body",
            "provider_request_body",
            "response_body",
            "client_response_body",
        ] {
            assert!(sql.contains(&format!(
                "usage_http_audits.{field}_state AS http_{field}_state"
            )));
        }
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
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX.contains("NULL::jsonb AS provider_request_body"));
    assert!(
        super::super::LIST_USAGE_AUDITS_PREFIX.contains("NULL::varchar AS http_request_body_state")
    );
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX
        .contains("NULL::varchar AS http_client_response_body_state"));
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
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("NULL::jsonb AS provider_request_body"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("NULL::varchar AS http_request_body_state"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("NULL::varchar AS http_client_response_body_state"));
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
    for placeholder in [41, 42, 43, 44, 45, 46, 47, 48, 49] {
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
fn usage_sql_detached_body_flags_clear_body_columns() {
    for flag in ["$53", "$54", "$55", "$56"] {
        assert!(
            super::super::UPSERT_SQL.contains(format!("WHEN {flag} THEN NULL").as_str()),
            "missing detached body clear flag: {flag}"
        );
    }
}

#[test]
fn usage_sql_capture_guard_covers_bodies_metadata_and_audit_body_states() {
    for assignment in [
        "request_body = CASE WHEN \"usage\".billing_status = 'pending' AND $57",
        "provider_request_body = CASE WHEN \"usage\".billing_status = 'pending' AND $57",
        "response_body = CASE WHEN \"usage\".billing_status = 'pending' AND $57",
        "client_response_body = CASE WHEN \"usage\".billing_status = 'pending' AND $57",
        "request_metadata = CASE WHEN \"usage\".billing_status = 'pending' AND $57",
    ] {
        assert!(
            super::super::UPSERT_SQL.contains(assignment),
            "missing guard: {assignment}"
        );
    }
    // The audit row keeps the capture states but no stored reference.
    assert!(!super::super::UPSERT_USAGE_HTTP_AUDIT_SQL.contains("body_ref"));
    for assignment in [
        "request_body_state = COALESCE(",
        "provider_request_body_state = COALESCE(",
        "response_body_state = COALESCE(",
        "client_response_body_state = COALESCE(",
    ] {
        assert!(
            super::super::UPSERT_USAGE_HTTP_AUDIT_SQL.contains(assignment),
            "missing HTTP body state assignment: {assignment}"
        );
    }
    let source = include_str!("../mod.rs");
    assert!(source.contains(".bind(capture_update_allowed)"));
    assert!(source.contains("if capture_update_allowed {"));
}

/// The body one field of a prepared pair stores.
fn stored_body(storage: &UsageBodyPairStorage, field: UsageBodyField) -> serde_json::Value {
    let payload = storage
        .payload
        .as_deref()
        .expect("pair should store a payload");
    let container =
        decode_container(storage.encoding, payload).expect("pair payload should decode");
    container_body(&container, field).expect("field should be present")
}

#[test]
fn prepare_usage_body_pair_frames_small_payloads() {
    let payload = json!({"message": "hello"});
    let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
        .expect("storage should serialize");

    assert_eq!(stored_body(&storage, UsageBodyField::RequestBody), payload);
}

#[tokio::test]
async fn usage_body_decode_preserves_storage_decode_errors() {
    let error = super::super::decode_usage_body_in_background(|| {
        serde_json::from_slice::<serde_json::Value>(b"not json")
            .map(Some)
            .map_err(|error| {
                aether_data_contracts::DataLayerError::UnexpectedValue(format!(
                    "failed to parse usage json: {error}"
                ))
            })
    })
    .await
    .expect_err("corrupt bodies should fail");

    assert!(error.to_string().contains("failed to parse usage json:"));
}

#[test]
fn prepare_usage_body_pair_stores_an_uncompressed_container() {
    let payload = json!({
        "content": "x".repeat(64 * 1024)
    });
    let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
        .expect("storage should serialize");

    // Preparing a record only serializes; compression happens afterwards, outside
    // the bounded preparation step and outside the write transaction.
    assert_eq!(storage.encoding, ENCODING_RAW_CONTAINER);
    assert_eq!(stored_body(&storage, UsageBodyField::RequestBody), payload);
}

#[test]
fn compress_usage_body_pair_turns_the_container_into_a_frame() {
    let payload = json!({
        "content": "x".repeat(64 * 1024)
    });
    let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
        .expect("storage should serialize");

    let compressed = compress_usage_body_pair(storage);
    assert_eq!(compressed.encoding, ENCODING_ZSTD_CONTAINER);
    assert_eq!(
        stored_body(&compressed, UsageBodyField::RequestBody),
        payload
    );
    assert!(compressed.payload.as_deref().unwrap().len() < 64 * 1024);
}

#[test]
fn prepare_usage_body_pair_keeps_json_shapes_readable() {
    // A body that is literally JSON `null` is the one shape that cannot survive:
    // `null` is how the container spells a half that was not captured.
    for payload in [
        json!(false),
        json!(42),
        json!(["quoted\"text", "line\nbreak", "\u{4e2d}\u{6587}", null]),
        json!({
            "content": "escaped\n\"\\value".repeat(32 * 1024),
            "nested": {"values": [true, null, 1.25, -7]}
        }),
    ] {
        let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
            .expect("body should serialize");
        assert_eq!(stored_body(&storage, UsageBodyField::RequestBody), payload);
    }

    let storage = prepare_usage_body_pair(
        UsageBodyField::RequestBody,
        Some(&serde_json::Value::Null),
        None,
    )
    .expect("body should serialize");
    assert!(!storage.holds(UsageBodyField::RequestBody));
}

#[test]
fn managed_capture_preparation_moves_bodies_without_a_second_reservation() {
    use aether_data_contracts::repository::usage::{
        sanitize_usage_for_persistence, usage_json_heap_estimate, UsageCaptureMemoryBudget,
    };

    let mut usage = fast_clear_usage_record(
        "req-managed-capture",
        "managed-capture",
        100,
        true,
        UsageBodyCaptureState::Inline,
        Some("priority"),
    );
    let bodies = [
        json!({"messages": [{"role": "user", "content": "request".repeat(4096)}]}),
        json!({"input": "provider request".repeat(4096), "service_tier": "priority"}),
        json!({"output": "provider response".repeat(4096)}),
        json!({"output": "client response".repeat(4096)}),
    ];
    usage.request_body = Some(bodies[0].clone());
    usage.provider_request_body = Some(bodies[1].clone());
    usage.response_body = Some(bodies[2].clone());
    usage.client_response_body = Some(bodies[3].clone());
    usage.request_body_state = Some(UsageBodyCaptureState::Inline);
    usage.response_body_state = Some(UsageBodyCaptureState::Inline);
    usage.client_response_body_state = Some(UsageBodyCaptureState::Inline);
    usage.request_headers = Some(json!({"content-type": "application/json"}));
    usage.cache_read_input_tokens = Some(0);
    usage.total_cost_usd = Some(0.25);
    usage.actual_total_cost_usd = Some(0.125);
    let expected_accounting = sanitize_usage_for_persistence(usage.clone());
    let bodies = [
        usage.request_body.clone(),
        usage.provider_request_body.clone(),
        usage.response_body.clone(),
        usage.client_response_body.clone(),
    ];
    let bytes = bodies
        .iter()
        .flatten()
        .map(|body| std::mem::size_of::<serde_json::Value>() + usage_json_heap_estimate(body))
        .sum();
    let budget = Arc::new(UsageCaptureMemoryBudget::new(bytes));
    assert!(usage.capture_retention.reserve(Arc::clone(&budget), bytes));

    let (accounting, prepared) = prepare_usage_for_persistence(usage);
    let prepared = prepared.expect("managed capture should prepare without cloning bodies");
    assert_eq!(budget.retained_bytes(), 0);
    assert_eq!(budget.downgraded_total(), 0);
    assert_eq!(accounting, expected_accounting);
    for (field, expected) in [
        (UsageBodyField::RequestBody, bodies[0].as_ref()),
        (UsageBodyField::ProviderRequestBody, bodies[1].as_ref()),
        (UsageBodyField::ResponseBody, bodies[2].as_ref()),
        (UsageBodyField::ClientResponseBody, bodies[3].as_ref()),
    ] {
        let pair = if matches!(
            field,
            UsageBodyField::RequestBody | UsageBodyField::ProviderRequestBody
        ) {
            &prepared.bodies.request
        } else {
            &prepared.bodies.response
        };
        let payload = pair
            .payload
            .as_deref()
            .expect("pair should store a payload");
        let container =
            decode_container(pair.encoding, payload).expect("pair payload should decode");
        assert_eq!(container_body(&container, field), expected.cloned());
    }
}

#[test]
fn usage_body_capture_state_for_storage_marks_detached_bodies_as_reference() {
    let payload = json!({"message": "hello"});
    let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
        .expect("storage should serialize");

    assert!(storage.holds(UsageBodyField::RequestBody));
    assert_eq!(
        usage_body_capture_state_for_storage(
            Some(UsageBodyCaptureState::Inline),
            &storage,
            UsageBodyField::RequestBody,
            None,
        ),
        Some(UsageBodyCaptureState::Reference)
    );
    assert_eq!(
        usage_body_capture_state_for_storage(None, &storage, UsageBodyField::RequestBody, None),
        Some(UsageBodyCaptureState::Reference)
    );
}

#[test]
fn usage_body_capture_state_for_storage_preserves_unavailable_states() {
    let payload = json!({"message": "hello"});
    let storage = prepare_usage_body_pair(UsageBodyField::RequestBody, Some(&payload), None)
        .expect("storage should serialize");

    assert_eq!(
        usage_body_capture_state_for_storage(
            Some(UsageBodyCaptureState::Disabled),
            &storage,
            UsageBodyField::RequestBody,
            Some("usage://request/req-1/request_body"),
        ),
        Some(UsageBodyCaptureState::Disabled)
    );
    assert_eq!(
        usage_body_capture_state_for_storage(
            Some(UsageBodyCaptureState::Unavailable),
            &storage,
            UsageBodyField::RequestBody,
            Some("usage://request/req-1/request_body"),
        ),
        Some(UsageBodyCaptureState::Unavailable)
    );
}

#[test]
fn explicit_none_capture_drops_residual_body_ref_and_incoming_fast_metadata_before_storage() {
    let mut usage = fast_clear_usage_record(
        "req-none-residual",
        "none-residual",
        100,
        true,
        UsageBodyCaptureState::None,
        Some("priority"),
    );
    usage.provider_request_body = Some(json!({
        "model": "gpt-5",
        "service_tier": "priority"
    }));
    usage.provider_request_body_ref =
        Some("usage://request/req-none-residual/provider_request_body".to_string());
    usage.request_metadata = Some(json!({
        "trace_id": "trace-1",
        "provider_service_tier": "priority",
        "provider_reasoning_effort": "high",
        "provider_cache_ttl_minutes": 30,
        "provider_request_body_ref": "usage://request/req-none-residual/provider_request_body"
    }));

    let (_, prepared) = prepare_usage_for_persistence(usage);
    let prepared = prepared.expect("usage should prepare");
    assert!(prepared.clear_provider_request_body);
    assert!(!prepared
        .bodies
        .request
        .holds(UsageBodyField::ProviderRequestBody));
    assert_eq!(
        prepared.request_metadata_value,
        Some(json!({"trace_id": "trace-1"}))
    );
}

#[test]
fn request_body_fact_clear_keeps_unrelated_metadata_and_emits_an_empty_tombstone() {
    let previous = json!({
        "trace_id": "trace-1",
        "requested_reasoning_effort": "xhigh",
        "provider_reasoning_effort": "max",
        "provider_service_tier": "priority",
        "provider_cache_ttl_minutes": 30,
        "provider_actual_service_tier": "default"
    });

    assert_eq!(
        clear_previous_request_body_facts(Some(&previous), true, true),
        json!({
            "trace_id": "trace-1",
            "provider_actual_service_tier": "default"
        })
    );
    assert_eq!(
        clear_previous_request_body_facts(
            Some(&json!({"provider_service_tier": "priority"})),
            false,
            true,
        ),
        json!({})
    );
}

#[test]
fn prepare_request_metadata_for_body_storage_strips_body_ref_compatibility_keys() {
    let detached = prepare_usage_body_pair(
        UsageBodyField::RequestBody,
        Some(&json!({
            "content": "x".repeat(64 * 1024)
        })),
        None,
    )
    .expect("detached storage should build");
    let inline = prepare_usage_body_pair(
        UsageBodyField::RequestBody,
        None,
        Some(&json!({"message": "inline"})),
    )
    .expect("inline body");

    let metadata = prepare_request_metadata_for_body_storage(
        Some(json!({
            "trace_id": "trace-1",
            "request_body_ref": "blob://old-request",
            "provider_request_body_ref": "blob://old-provider"
        })),
        [
            (
                UsageBodyField::RequestBody,
                &detached,
                Some(&json!({"request": true})),
                Some("usage://request/req-123/request_body"),
            ),
            (
                UsageBodyField::ProviderRequestBody,
                &inline,
                Some(&json!({"provider": true})),
                None,
            ),
        ],
    )
    .expect("metadata should be present");

    assert_eq!(
        metadata,
        json!({
            "trace_id": "trace-1"
        })
    );
}

#[test]
fn usage_http_audit_capture_mode_prefers_refs_over_inline_legacy() {
    assert_eq!(
        usage_http_audit_capture_mode(
            [
                Some("usage://request/req-123/request_body"),
                None,
                None,
                None
            ],
            [Some(&json!({"request": true})), None, None, None]
        ),
        "ref_backed"
    );
    assert_eq!(
        usage_http_audit_capture_mode(
            [None, None, None, None],
            [Some(&json!({"request": true})), None, None, None]
        ),
        "inline_legacy"
    );
    assert_eq!(
        usage_http_audit_capture_mode([None, None, None, None], [None, None, None, None]),
        "none"
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
