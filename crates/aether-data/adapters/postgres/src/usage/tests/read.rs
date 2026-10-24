use super::*;

#[test]
fn usage_sql_does_not_require_updated_at_column() {
    assert!(!super::super::FIND_BY_REQUEST_ID_SQL.contains("COALESCE(updated_at, created_at)"));
    assert!(!super::super::LIST_USAGE_AUDITS_PREFIX.contains("COALESCE(updated_at, created_at)"));
    assert!(!super::super::UPSERT_SQL.contains("\n  updated_at\n"));
    assert!(!super::super::UPSERT_SQL.contains("updated_at = CASE"));
}

#[test]
fn usage_sql_preserves_nonzero_lifecycle_updated_revision() {
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
        super::super::LIST_USAGE_AUDITS_PREFIX,
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX,
    ] {
        assert!(sql
            .contains("COALESCE(\n    NULLIF(\"usage\".updated_at_unix_secs, 0),\n    GREATEST("));
        assert!(
            !sql.contains("GREATEST(\n    COALESCE(NULLIF(\"usage\".updated_at_unix_secs, 0), 0),")
        );
    }
}

#[test]
fn usage_sql_rebuilds_provider_key_window_usage_into_status_snapshot() {
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL
            .contains("UPDATE provider_api_keys AS keys")
    );
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL
            .contains("usage_billing_facts")
    );
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL
            .contains("provider_type', ''))) = 'codex'")
    );
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL
            .contains("'{quota,windows}'")
    );
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL.contains("'{usage}'")
    );
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL
            .contains("WHEN '5h' THEN 300")
    );
    assert!(
        super::super::REBUILD_PROVIDER_API_KEY_CODEX_WINDOW_USAGE_STATS_SQL
            .contains("WHEN 'weekly' THEN 10080")
    );
}

#[test]
fn usage_sql_rebuild_matches_online_api_key_usage_semantics() {
    let sql = normalize_newlines(super::super::REBUILD_API_KEY_USAGE_STATS_SQL);
    assert!(sql.contains("COUNT(*)::BIGINT"));
    assert!(sql.contains("GREATEST(\n        COALESCE(total_tokens, 0),"));
    assert!(!sql.contains("COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)"));
    assert!(sql.contains("AND BTRIM(api_key_id) <> ''"));
    assert!(sql.contains("AND status NOT IN ('pending', 'streaming')"));
}

#[test]
fn usage_sql_rebuild_matches_online_provider_key_usage_semantics() {
    let sql = normalize_newlines(super::super::REBUILD_PROVIDER_API_KEY_USAGE_STATS_SQL);
    assert!(sql.contains("COUNT(*)::BIGINT"));
    assert!(sql.contains("NULLIF(BTRIM(error_message), '') IS NULL"));
    assert!(sql.contains("GREATEST(\n          COALESCE(total_tokens, 0),"));
    assert!(!sql.contains("COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)"));
    assert!(sql.contains("AND BTRIM(provider_api_key_id) <> ''"));
    assert!(sql.contains("WHEN status NOT IN ('pending', 'streaming')"));
    assert!(sql.contains("WHEN status IN ('pending', 'streaming') THEN 0"));
}

#[test]
fn usage_sql_supports_recent_usage_audits_query() {
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX.contains("FROM \"usage\""));
}

#[test]
fn usage_sql_cache_affinity_interval_query_coalesces_nullable_model_values() {
    assert!(include_str!("../mod.rs").contains("COALESCE(\"usage\".model, '') AS model"));
}

#[test]
fn usage_sql_cache_affinity_interval_query_casts_interval_minutes_to_double_precision() {
    assert!(include_str!("../mod.rs").contains("AS DOUBLE PRECISION) AS interval_minutes"));
}

#[test]
fn usage_sql_provider_aggregation_excludes_unknown_provider_labels() {
    let source = normalize_newlines(include_str!("../mod.rs"));
    assert!(source.contains("const USAGE_PROVIDER_IDENTITY_FILTER_SQL"));
    assert!(source.contains("const USAGE_PROVIDER_IDENTITY_SOURCE_SQL"));
    assert!(source.contains(r#"BTRIM(COALESCE("usage".provider_id, '')) <> ''"#));
    assert!(source.contains(r#"BTRIM(COALESCE("usage".provider_name, '')) <> ''"#));
    assert!(source.contains("LEFT JOIN providers AS provider_by_id"));
    assert!(source.contains("provider_by_id.id = BTRIM(\"usage\".provider_id)"));
    assert!(source.contains("COALESCE(\n      provider_by_id.id,\n      CASE"));
    assert!(
        source.contains(
            "ELSE BTRIM(\"usage\".provider_id)\n      END,\n      CASE\n        WHEN BTRIM(COALESCE(\"usage\".provider_name, ''))"
        )
    );
    assert!(source.contains("COALESCE(\n      provider_by_id.name,"));
    assert!(!source.contains("provider_by_name.name = BTRIM(\"usage\".provider_name)"));
    assert!(source.contains("{provider_identity_source_expr} AS provider_identity_source"));
    assert!(source.contains(r#"secondary_name_expr: "provider_identity_source""#));
    assert!(source
        .contains("COUNT(*) FILTER (WHERE secondary_name = 'provider_id') > 0 THEN 'provider_id'"));
    assert!(source.contains(
        "if matches!(query.group_by, UsageAuditAggregationGroupBy::Provider) {\n            return self.aggregate_usage_audits_raw(query).await;"
    ));
    assert!(source.contains("exclude_reserved_provider_labels"));
    assert!(source.contains(
        r#"lower(BTRIM(COALESCE(provider_name, ''))) NOT IN ('unknown', 'unknow', 'pending')"#
    ));
    assert!(source.contains("MAX(display_name)"));
    assert!(!source.contains("COALESCE(MAX(NULLIF(display_name, 'Unknown')), 'Unknown')"));
}

#[test]
fn usage_sql_canonical_openai_cache_case_preserves_effective_and_total_tokens() {
    let upstream_input_tokens = 166_103_i64;
    let effective_input_tokens = 1_495_i64;
    let cache_creation_tokens = 0_i64;
    let cache_read_tokens = 164_608_i64;
    let output_tokens = 94_i64;
    let total_input_context = 166_103_i64;
    let total_tokens = 166_197_i64;

    assert_eq!(
        effective_input_tokens + cache_creation_tokens + cache_read_tokens,
        total_input_context
    );
    assert_eq!(total_input_context, upstream_input_tokens);
    assert_eq!(
        effective_input_tokens + cache_creation_tokens + cache_read_tokens + output_tokens,
        total_tokens
    );

    let source = include_str!("../mod.rs");
    assert!(source.contains("\"usage\".effective_input_tokens"));
    assert!(source.contains("\"usage\".total_input_context"));
    assert!(!source.contains("split_part(lower("));

    let aggregate_audit_summary = source
        .split("async fn summarize_usage_audits_from_daily_aggregates")
        .nth(1)
        .and_then(|tail| tail.split("async fn summarize_usage_audits_raw").next())
        .expect("daily aggregate usage audit summary should be present");
    assert_eq!(
        aggregate_audit_summary
            .matches("WHEN effective_input_tokens = 0 AND total_input_context = 0")
            .count(),
        1,
        "the shared daily aggregate query should define the legacy token fallback once"
    );
    assert_eq!(
        aggregate_audit_summary
            .matches("+ output_tokens + cache_creation_tokens + cache_read_tokens")
            .count(),
        1,
        "the shared daily aggregate query should define canonical total tokens once"
    );
    assert!(!aggregate_audit_summary.contains("SUM(input_tokens + output_tokens)"));

    let raw_audit_summary = source
        .split("async fn summarize_usage_audits_raw")
        .nth(1)
        .and_then(|tail| tail.split("pub async fn summarize_usage_audits").next())
        .expect("raw usage audit summary should be present");
    assert!(raw_audit_summary.contains("SUM(GREATEST(COALESCE(\"usage\".total_tokens, 0), 0))"));
}

#[test]
fn usage_openai_total_input_context_includes_cache_creation_tokens() {
    let effective_input_tokens =
        usage_effective_input_tokens(Some(1_000), Some(100), Some(400), "openai");
    assert_eq!(effective_input_tokens, Some(500));
    assert_eq!(
        usage_total_input_context(
            Some(1_000),
            effective_input_tokens,
            Some(100),
            Some(400),
            "openai",
        ),
        Some(1_000)
    );
}

#[test]
fn usage_billing_facts_projects_upstream_stream_mode() {
    let migration = include_str!(
        "../../../migrations/20260505130000_project_upstream_stream_in_usage_billing_facts.sql"
    );

    assert!(migration.contains("AS upstream_is_stream"));
    assert!(migration.contains("COALESCE(usage_rows.upstream_is_stream"));
    assert!(migration.contains("COALESCE(usage_rows.is_stream, FALSE)"));
    assert!(migration.contains("ADD COLUMN IF NOT EXISTS upstream_is_stream boolean"));
    assert!(
        !migration.contains("request_metadata->>'upstream_is_stream'"),
        "migration should avoid backfilling historical usage rows from request metadata"
    );
}

#[test]
fn usage_billing_facts_total_tokens_uses_canonical_effective_input() {
    let migration =
        include_str!("../../../migrations/20260716000000_fix_usage_billing_facts_total_tokens.sql");
    let bootstrap =
        include_str!("../../../../../runtime/schema/bootstrap/postgres/100_usage_capture.sql");

    for sql in [migration, bootstrap] {
        assert!(sql.contains("WHEN settlement.billing_effective_input_tokens IS NOT NULL"));
        assert!(sql.contains("GREATEST(settlement.billing_effective_input_tokens, 0)"));
        assert!(sql.contains("WHEN settlement.billing_total_input_context IS NOT NULL"));
        assert!(sql.contains("NULLIF(GREATEST(COALESCE(usage_rows.total_tokens, 0), 0), 0)"));
        assert!(!sql.contains("THEN COALESCE(settlement.billing_input_tokens, 0)"));
    }
}

#[test]
fn usage_sql_reads_settlement_snapshots_for_single_record_fetches() {
    assert!(super::super::FIND_BY_REQUEST_ID_SQL.contains("LEFT JOIN usage_settlement_snapshots"));
    assert!(super::super::FIND_BY_ID_SQL.contains("LEFT JOIN usage_settlement_snapshots"));
    assert!(
        super::super::FIND_BY_REQUEST_ID_SQL.contains("settlement_billing_snapshot_schema_version")
    );
    assert!(super::super::FIND_BY_ID_SQL.contains("settlement_price_per_request"));
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
        super::super::LIST_USAGE_AUDITS_PREFIX,
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX,
    ] {
        assert!(sql.contains("CAST(\"usage\".input_tokens AS INTEGER) AS input_tokens"));
        assert!(sql.contains(
            "usage_settlement_snapshots.billing_input_tokens AS settlement_billing_input_tokens"
        ));
        assert!(sql.contains(
            "WHEN usage_settlement_snapshots.billing_effective_input_tokens IS NOT NULL"
        ));
        assert!(
            sql.contains("WHEN usage_settlement_snapshots.billing_total_input_context IS NOT NULL")
        );
        assert!(sql.contains("NULLIF(GREATEST(COALESCE(\"usage\".total_tokens, 0), 0), 0)"));
        assert!(!sql.contains("THEN COALESCE(usage_settlement_snapshots.billing_input_tokens, 0)"));
        assert!(sql.contains("usage_settlement_snapshots.billing_cache_creation_5m_tokens"));
        assert!(sql.contains(
            "CAST(usage_settlement_snapshots.billing_total_cost_usd AS DOUBLE PRECISION)"
        ));
    }
}

#[test]
fn usage_sql_qualifies_shared_usage_columns_for_single_record_fetches() {
    for sql in [
        super::super::FIND_BY_REQUEST_ID_SQL,
        super::super::FIND_BY_ID_SQL,
    ] {
        assert!(sql.contains("\"usage\".request_id"));
        assert!(
                sql.contains(
                    "COALESCE(usage_settlement_snapshots.billing_status, \"usage\".billing_status) AS billing_status"
                )
            );
        assert!(sql
            .contains("CAST(usage_settlement_snapshots.output_price_per_1m AS DOUBLE PRECISION)"));
        assert!(sql.contains("EXTRACT(EPOCH FROM \"usage\".created_at)"));
        assert!(sql.contains("usage_settlement_snapshots.finalized_at"));
        assert!(!sql.contains("CAST(EXTRACT(EPOCH FROM created_at) AS BIGINT)"));
        assert!(!sql.contains("CAST(output_price_per_1m AS DOUBLE PRECISION)"));
    }

    assert!(super::super::FIND_BY_REQUEST_ID_SQL.contains("WHERE \"usage\".request_id = $1"));
    assert!(super::super::FIND_BY_ID_SQL.contains("WHERE \"usage\".id = $1"));
}

#[test]
fn usage_sql_list_queries_project_boolean_chat_pii_redaction() {
    for sql in [
        super::super::LIST_USAGE_AUDITS_PREFIX,
        super::super::LIST_RECENT_USAGE_AUDITS_PREFIX,
    ] {
        assert!(sql.contains(
            "OR jsonb_typeof(\"usage\".request_metadata->'chat_pii_redaction') = 'boolean'"
        ));
        assert!(sql.contains(
            "WHEN jsonb_typeof(\"usage\".request_metadata->'chat_pii_redaction') = 'boolean'"
        ));
        assert!(sql.contains("THEN \"usage\".request_metadata->'chat_pii_redaction'"));
        assert!(
            !sql.contains("request_metadata->>'chat_pii_redaction'"),
            "list queries must not coerce a string redaction marker into a boolean"
        );
    }
}

#[test]
fn usage_sql_admin_record_filters_are_pushed_into_postgres_queries() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("push_postgres_usage_client_family_filter"));
    assert!(source.contains("request_metadata->'client_session_affinity'->>'client_family'"));
    assert!(source.contains("request_metadata->>'client_family'"));
    assert!(source.contains("exclude_unknown_model_or_provider"));
    assert!(source.contains("NOT IN ('unknown', 'unknow')"));
    assert_eq!(
        source
            .matches("push_postgres_usage_websocket_filter(")
            .count(),
        5,
        "the WebSocket filter must cover list/count and keyword list/count"
    );
}

#[test]
fn usage_sql_keyword_search_error_filter_keeps_where_state_before_keywords() {
    let source = include_str!("../mod.rs");
    let function = source
        .split("pub async fn list_usage_audits_by_keyword_search")
        .nth(1)
        .and_then(|tail| tail.split("pub async fn count_usage_audits").next())
        .expect("keyword search function should be present");
    let error_filter = function
        .split("if query.error_only")
        .nth(1)
        .and_then(|tail| tail.split("for (index, keyword)").next())
        .expect("error filter should precede keyword loop");

    assert!(error_filter.contains("has_where = true;"));
}

#[test]
fn usage_sql_reads_list_output_price_from_settlement_snapshots_before_legacy_usage_column() {
    assert!(super::super::LIST_USAGE_AUDITS_PREFIX
        .contains("CAST(usage_settlement_snapshots.output_price_per_1m AS DOUBLE PRECISION)"));
    assert!(super::super::LIST_RECENT_USAGE_AUDITS_PREFIX
        .contains("CAST(usage_settlement_snapshots.output_price_per_1m AS DOUBLE PRECISION)"));
}
