use super::*;

#[test]
fn api_key_leaderboard_user_filter_keeps_aggregates_with_historical_identity_evidence() {
    let source = include_str!("../mod.rs");
    let aggregate_path = source
        .split("async fn summarize_usage_leaderboard_from_daily_aggregates")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub async fn summarize_usage_leaderboard")
                .next()
        })
        .expect("daily aggregate leaderboard path should be present");

    assert!(aggregate_path.contains("api_keys.user_id = "));
    assert!(aggregate_path.contains("api_keys.id IS NULL"));
    assert!(aggregate_path.contains("stats_daily_api_key.api_key_id IN ("));
    assert!(aggregate_path.contains("FROM usage AS identity_usage"));
    assert!(aggregate_path.contains("identity_usage.user_id = "));
    assert!(aggregate_path.contains("GROUP BY identity_usage.api_key_id"));
}

#[test]
fn dashboard_daily_aggregate_split_keeps_partial_days_raw() {
    let start_utc = Utc
        .with_ymd_and_hms(2026, 4, 20, 13, 15, 0)
        .single()
        .unwrap();
    let end_utc = Utc
        .with_ymd_and_hms(2026, 4, 23, 4, 45, 0)
        .single()
        .unwrap();
    let cutoff_utc = Utc.with_ymd_and_hms(2026, 4, 23, 0, 0, 0).single().unwrap();

    let split = split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc);

    assert_eq!(
        split,
        AggregateRangeSplit {
            raw_leading: Some((
                Utc.with_ymd_and_hms(2026, 4, 20, 13, 15, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 21, 0, 0, 0).single().unwrap(),
            )),
            aggregate: Some((
                Utc.with_ymd_and_hms(2026, 4, 21, 0, 0, 0).single().unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 23, 0, 0, 0).single().unwrap(),
            )),
            raw_trailing: Some((
                Utc.with_ymd_and_hms(2026, 4, 23, 0, 0, 0).single().unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 23, 4, 45, 0)
                    .single()
                    .unwrap(),
            )),
        }
    );
}

#[test]
fn dashboard_hourly_aggregate_split_keeps_partial_hours_raw() {
    let start_utc = Utc
        .with_ymd_and_hms(2026, 4, 20, 10, 15, 0)
        .single()
        .unwrap();
    let end_utc = Utc
        .with_ymd_and_hms(2026, 4, 20, 15, 30, 0)
        .single()
        .unwrap();
    let cutoff_utc = Utc
        .with_ymd_and_hms(2026, 4, 20, 15, 0, 0)
        .single()
        .unwrap();

    let split = split_dashboard_hourly_aggregate_range(start_utc, end_utc, cutoff_utc);

    assert_eq!(
        split,
        AggregateRangeSplit {
            raw_leading: Some((
                Utc.with_ymd_and_hms(2026, 4, 20, 10, 15, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 20, 11, 0, 0)
                    .single()
                    .unwrap(),
            )),
            aggregate: Some((
                Utc.with_ymd_and_hms(2026, 4, 20, 11, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 20, 15, 0, 0)
                    .single()
                    .unwrap(),
            )),
            raw_trailing: Some((
                Utc.with_ymd_and_hms(2026, 4, 20, 15, 0, 0)
                    .single()
                    .unwrap(),
                Utc.with_ymd_and_hms(2026, 4, 20, 15, 30, 0)
                    .single()
                    .unwrap(),
            )),
        }
    );
}

#[test]
fn usage_sql_summarizes_tokens_by_api_key_ids_in_database() {
    let sql = super::super::SUMMARIZE_TOTAL_TOKENS_BY_API_KEY_IDS_SQL;
    assert!(sql.contains("GROUP BY api_key_id"));
    assert!(sql.contains("ANY($1::TEXT[])"));
    assert!(sql.contains("GREATEST(COALESCE(total_tokens, 0), 0)"));
    assert!(sql.contains(")::BIGINT AS total_tokens"));
    assert!(!sql.contains("COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)"));

    let source = include_str!("../mod.rs");
    let implementation = source
        .split("pub async fn summarize_total_tokens_by_api_key_ids")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub async fn summarize_usage_totals_by_user_ids")
                .next()
        })
        .expect("API-key total-token summary implementation should be present");
    assert!(implementation.contains("SUM(GREATEST(COALESCE(total_tokens, 0), 0))"));
    assert!(implementation.contains(")::BIGINT AS total_tokens"));
    assert!(!implementation.contains("COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)"));
}

#[test]
fn usage_sql_summarizes_usage_by_provider_api_key_ids_in_database() {
    assert!(super::super::SUMMARIZE_USAGE_BY_PROVIDER_API_KEY_IDS_SQL
        .contains("FROM provider_api_keys"));
    assert!(super::super::SUMMARIZE_USAGE_BY_PROVIDER_API_KEY_IDS_SQL
        .contains("COALESCE(request_count, 0) > 0"));
    assert!(super::super::SUMMARIZE_USAGE_BY_PROVIDER_API_KEY_IDS_SQL.contains("ANY($1::TEXT[])"));
}

#[test]
fn usage_sql_exposes_counter_outbox_health() {
    assert!(super::super::READ_USAGE_COUNTER_HEALTH_SQL.contains("pending_rows"));
    assert!(
        super::super::READ_USAGE_COUNTER_HEALTH_SQL.contains("oldest_pending_created_at_unix_secs")
    );
    assert!(super::super::READ_USAGE_COUNTER_HEALTH_SQL.contains("latest_processed_at_unix_secs"));
    assert!(super::super::READ_PENDING_USAGE_COUNTER_DELTAS_BY_KIND_SQL.contains("GROUP BY kind"));
}

#[test]
fn usage_counter_pending_health_does_not_scan_processed_history() {
    let sql = super::super::READ_USAGE_COUNTER_PENDING_HEALTH_SQL;
    assert!(sql.contains("COUNT(*)::BIGINT AS pending_rows"));
    assert!(sql.contains("MIN(created_at)"));
    assert!(sql.contains("WHERE processed_at IS NULL"));
    assert!(sql.contains("GROUP BY kind"));
    assert!(!sql.contains("processed_at IS NOT NULL"));
}

#[test]
fn usage_sql_summarize_usage_audits_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_audits_from_daily_aggregates"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("cache_creation_ephemeral_5m_tokens"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_cache_hit_summary_supports_global_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_cache_hit_summary_from_daily_aggregates"));
    assert!(source.contains("summarize_usage_cache_hit_summary_from_hourly_aggregates"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_hourly"));
    assert!(
        source.contains("split_dashboard_hourly_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_cache_affinity_hit_summary_supports_global_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_cache_affinity_hit_summary_from_daily_aggregates"));
    assert!(source.contains("summarize_usage_cache_affinity_hit_summary_from_hourly_aggregates"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_hourly"));
    assert!(source.contains("completed_total_requests"));
    assert!(
        source.contains("split_dashboard_hourly_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_settled_cost_supports_user_and_global_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_settled_cost_from_daily_aggregates"));
    assert!(source.contains("summarize_usage_settled_cost_from_hourly_aggregates"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("FROM stats_hourly"));
    assert!(source.contains("FROM stats_hourly_user"));
    assert!(source.contains("settled_total_cost"));
    assert!(
        source.contains("split_dashboard_hourly_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_error_distribution_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_error_distribution_from_daily_aggregates"));
    assert!(source.contains("FROM stats_daily_error"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_performance_percentiles_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_performance_percentiles_from_daily_aggregates"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("p50_response_time_ms"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_cost_savings_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_cost_savings_from_daily_aggregates"));
    assert!(source.contains("FROM stats_daily_cost_savings"));
    assert!(source.contains("FROM stats_daily_cost_savings_model_provider"));
    assert!(source.contains("FROM stats_user_daily_cost_savings"));
    assert!(source.contains("FROM stats_user_daily_cost_savings_model_provider"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_time_series_supports_global_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_time_series_from_daily_aggregates"));
    assert!(source.contains("summarize_usage_time_series_from_hourly_aggregates"));
    assert!(source.contains("FROM stats_hourly"));
    assert!(
        source.contains("split_dashboard_hourly_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_usage_daily_heatmap_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_daily_heatmap_from_daily_aggregates"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("total_requests::BIGINT AS total_requests"));
    assert!(source.contains("total_cost::DOUBLE PRECISION AS total_cost"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_daily_cutoff_falls_back_to_imported_stats_daily() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("FROM stats_summary"));
    assert!(source.contains("SELECT MAX(date) AS latest_date"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("FROM stats_daily_api_key"));
    assert!(source.contains("value + chrono::Duration::days(1)"));
}

#[test]
fn usage_sql_dashboard_daily_breakdown_falls_back_to_daily_totals() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("list_dashboard_daily_breakdown_from_daily_totals"));
    assert!(source.contains("'aggregate'::TEXT AS model"));
    assert!(source.contains("FROM stats_daily"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("detailed_dates.contains(&item.date)"));
    assert!(source.contains("query.tz_offset_minutes != 0"));
    assert!(source.contains("return self.list_dashboard_daily_breakdown_raw(query).await;"));
    assert!(!source.contains("aggregate_dates.insert(item.date.clone())"));
}

#[test]
fn usage_sql_dashboard_daily_breakdown_keeps_all_local_day_model_provider_rows() {
    let source = include_str!("../mod.rs");
    let raw_breakdown = source
        .split("async fn list_dashboard_daily_breakdown_raw")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub async fn list_dashboard_daily_breakdown")
                .next()
        })
        .expect("raw daily breakdown function should be present");
    assert!(raw_breakdown.contains("GROUP BY date, \"usage\".model, \"usage\".provider_name"));
    assert!(raw_breakdown.contains("ORDER BY date ASC, total_cost_usd DESC"));
    assert!(raw_breakdown.contains("(\"usage\".created_at AT TIME ZONE 'UTC')"));
    assert!(raw_breakdown.contains("SUM(\"usage\".total_tokens)"));
    assert!(
        !raw_breakdown.contains("split_part(lower("),
        "raw daily breakdown should aggregate the canonical total_tokens projection"
    );
    assert!(!raw_breakdown.contains("date_trunc('day', \"usage\".created_at +"));
    assert!(!source.contains("if aggregate_dates.insert(item.date.clone())"));
}

#[test]
fn usage_sql_summarize_usage_leaderboard_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("summarize_usage_leaderboard_from_daily_aggregates"));
    assert!(source.contains("FROM stats_daily_model"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("FROM stats_daily_api_key"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_aggregate_reads_use_materialized_total_tokens_only_when_available() {
    fn select_projections_before_table<'a>(source: &'a str, table: &str) -> Vec<&'a str> {
        let marker = format!("FROM {table}\n");
        let mut remaining = source;
        let mut projections = Vec::new();
        while let Some((before_table, after_table)) = remaining.split_once(marker.as_str()) {
            projections.push(
                before_table
                    .rsplit_once("SELECT")
                    .map(|(_, projection)| projection)
                    .unwrap_or_else(|| panic!("SELECT projection for {table} should be present")),
            );
            remaining = after_table;
        }
        projections
    }

    let source = normalize_newlines(include_str!("../mod.rs"));
    let breakdown = source
        .split("async fn summarize_usage_breakdown_from_daily_aggregates")
        .nth(1)
        .and_then(|tail| tail.split("async fn summarize_usage_breakdown_raw").next())
        .expect("daily aggregate usage breakdown query should be present");
    for table in [
        "stats_user_daily_model",
        "stats_user_daily_provider",
        "stats_user_daily_api_format",
    ] {
        assert!(breakdown.contains(table));
    }
    assert!(breakdown.contains("COALESCE(SUM(total_tokens), 0)::BIGINT AS total_tokens"));
    assert!(!breakdown.contains(
        "SUM(effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens)"
    ));

    let leaderboard = source
        .split("async fn summarize_usage_leaderboard_from_daily_aggregates")
        .nth(1)
        .and_then(|tail| {
            tail.split("pub async fn summarize_usage_leaderboard")
                .next()
        })
        .expect("daily aggregate usage leaderboard queries should be present");
    for (table, expected_queries) in [
        ("stats_user_daily_provider", 1),
        ("stats_user_daily_model", 2),
    ] {
        let projections = select_projections_before_table(leaderboard, table);
        assert_eq!(
            projections.len(),
            expected_queries,
            "unexpected {table} query count"
        );
        assert!(projections.iter().all(|projection| projection
            .contains("COALESCE(SUM(total_tokens), 0)::BIGINT AS total_tokens")));
    }

    for (table, component_expression) in [
        (
            "stats_user_daily",
            "SUM(effective_input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens)",
        ),
        (
            "stats_daily_model",
            "SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens)",
        ),
        ("stats_daily_api_key", "stats_daily_api_key.input_tokens"),
    ] {
        let projections = select_projections_before_table(leaderboard, table);
        assert_eq!(projections.len(), 1, "unexpected {table} query count");
        assert!(projections[0].contains(component_expression));
        assert!(!projections[0].contains("SUM(total_tokens)"));
    }
}

#[test]
fn usage_sql_aggregate_usage_audits_supports_daily_model_and_provider_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("aggregate_usage_audits_from_daily_aggregates"));
    assert!(source.contains("stats_user_daily_model"));
    assert!(source.contains("stats_user_daily_provider"));
    assert!(source.contains("stats_user_daily_api_format"));
    assert!(source.contains("absorb_usage_audit_aggregation_rows"));
    assert!(
        source.contains("split_dashboard_daily_aggregate_range(start_utc, end_utc, cutoff_utc)")
    );
}

#[test]
fn usage_sql_summarize_total_tokens_by_api_key_ids_supports_daily_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("FROM stats_daily_api_key"));
    assert!(source.contains("read_stats_daily_cutoff_date().await?"));
}

#[test]
fn dashboard_aggregate_schema_mismatch_detector_matches_legacy_schema_failures() {
    assert!(super::super::dashboard_aggregate_schema_mismatch_message(
        "postgres error: error occurred while decoding column \"cutoff_date\": \
         mismatched types; Rust type `chrono::DateTime<Utc>` (as SQL type `TIMESTAMPTZ`) \
         is not compatible with SQL type `INT8`"
    ));
    assert!(super::super::dashboard_aggregate_schema_mismatch_message(
        "postgres error: db error: ERROR: relation \"stats_daily_model_provider\" does not exist"
    ));
    assert!(super::super::dashboard_aggregate_schema_mismatch_message(
        "postgres error: db error: ERROR: column \"effective_input_tokens\" does not exist"
    ));
    assert!(!super::super::dashboard_aggregate_schema_mismatch_message(
        "postgres error: db error: ERROR: permission denied for relation stats_daily"
    ));
}

#[test]
fn dashboard_aggregate_reads_fallback_to_raw_on_schema_mismatch() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("dashboard_should_fallback_to_raw_on_aggregate_error"));
    assert!(
        source.contains("Err(err) if dashboard_should_fallback_to_raw_on_aggregate_error(&err)")
    );
    assert!(source.contains("return self.list_dashboard_daily_breakdown_raw(query).await;"));
}

#[test]
fn usage_sql_summarize_usage_totals_by_user_ids_supports_user_summary_aggregates() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("FROM stats_user_summary"));
    assert!(source.contains("all_time_input_tokens"));
    assert!(source.contains("FROM stats_user_daily"));
    assert!(source.contains("date < $2"));
    assert!(source.contains("summary_user_ids.contains(&user_id)"));
}

#[test]
fn usage_sql_raw_aggregates_use_canonical_billing_facts() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("FROM usage_billing_facts AS \"usage\""));
    assert!(super::super::REBUILD_API_KEY_USAGE_STATS_SQL
        .contains("FROM usage_billing_facts AS \"usage\""));
    assert!(super::super::REBUILD_PROVIDER_API_KEY_USAGE_STATS_SQL
        .contains("FROM usage_billing_facts AS \"usage\""));
    assert!(super::super::SUMMARIZE_TOTAL_TOKENS_BY_API_KEY_IDS_SQL
        .contains("FROM usage_billing_facts AS \"usage\""));
    assert!(super::super::SUMMARIZE_USAGE_TOTALS_BY_USER_IDS_SQL
        .contains("FROM usage_billing_facts AS \"usage\""));
    assert!(!source.contains("apply_provider_api_key_codex_window_usage_delta_in_tx"));
}

#[test]
fn usage_sql_raw_token_aggregates_do_not_renormalize_canonical_billing_facts() {
    let source = include_str!("../mod.rs");
    for (start, end, label) in [
        (
            "async fn summarize_usage_audits_raw",
            "pub async fn summarize_usage_audits",
            "usage audit summary",
        ),
        (
            "async fn summarize_usage_cache_affinity_hit_summary_raw",
            "async fn summarize_usage_cache_affinity_hit_summary_from_daily_aggregates",
            "cache affinity",
        ),
        (
            "async fn summarize_usage_breakdown_raw",
            "pub async fn summarize_usage_breakdown",
            "usage breakdown",
        ),
        (
            "async fn summarize_usage_leaderboard_raw",
            "async fn summarize_usage_leaderboard_from_daily_aggregates",
            "usage leaderboard",
        ),
        (
            "async fn aggregate_usage_audits_raw",
            "pub async fn aggregate_usage_audits",
            "usage audit aggregation",
        ),
        (
            "async fn summarize_usage_daily_heatmap_raw_from_range",
            "async fn summarize_usage_daily_heatmap_from_daily_aggregates",
            "usage daily heatmap",
        ),
    ] {
        let body = source
            .split(start)
            .nth(1)
            .and_then(|tail| tail.split(end).next())
            .unwrap_or_else(|| panic!("{label} raw query should be present"));
        assert!(body.contains("FROM usage_billing_facts"));
        assert!(
            !body.contains("split_part(lower("),
            "{label} must not normalize canonical billing facts by API format"
        );
        assert!(
            !body.contains("input_tokens - cache_"),
            "{label} must not subtract cache tokens from canonical input tokens"
        );
    }

    let cache_affinity = source
        .split("async fn summarize_usage_cache_affinity_hit_summary_raw")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn summarize_usage_cache_affinity_hit_summary_from_daily_aggregates")
                .next()
        })
        .expect("cache affinity raw query should be present");
    assert!(cache_affinity.contains("\"usage\".input_tokens"));
    assert!(!cache_affinity.contains("\"usage\".effective_input_tokens"));
    assert!(cache_affinity.contains("\"usage\".total_input_context"));

    let audit_summary = source
        .split("async fn summarize_usage_audits_raw")
        .nth(1)
        .and_then(|tail| tail.split("pub async fn summarize_usage_audits").next())
        .expect("usage audit summary raw query should be present");
    assert!(audit_summary.contains("SUM(GREATEST(COALESCE(\"usage\".cache_creation_input_tokens"));
    assert!(!audit_summary.contains("WHEN COALESCE(\"usage\".cache_creation_input_tokens, 0) = 0"));

    for (start, end, label) in [
        (
            "async fn summarize_usage_breakdown_raw",
            "pub async fn summarize_usage_breakdown",
            "usage breakdown",
        ),
        (
            "async fn aggregate_usage_audits_raw",
            "pub async fn aggregate_usage_audits",
            "usage audit aggregation",
        ),
    ] {
        let body = source
            .split(start)
            .nth(1)
            .and_then(|tail| tail.split(end).next())
            .unwrap_or_else(|| panic!("{label} raw query should be present"));
        assert!(body.contains("\"usage\".effective_input_tokens"));
        assert!(body.contains("\"usage\".total_input_context"));
    }

    let leaderboard = source
        .split("async fn summarize_usage_leaderboard_raw")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn summarize_usage_leaderboard_from_daily_aggregates")
                .next()
        })
        .expect("usage leaderboard raw query should be present");
    assert!(leaderboard.contains("SUM(GREATEST(COALESCE(\"usage\".total_tokens"));

    let daily_heatmap = source
        .split("async fn summarize_usage_daily_heatmap_raw_from_range")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn summarize_usage_daily_heatmap_from_daily_aggregates")
                .next()
        })
        .expect("usage daily heatmap raw query should be present");
    assert!(daily_heatmap.contains("SUM(GREATEST(COALESCE(\"usage\".total_tokens"));
    assert!(!daily_heatmap.contains("\"usage\".input_tokens + \"usage\".output_tokens"));
}

#[test]
fn dashboard_covering_index_keeps_canonical_billing_reads_heap_light() {
    let migration = include_str!(
        "../../../migrations/20260715130000_add_usage_settlement_dashboard_covering_index.sql"
    );
    assert!(migration.starts_with("-- no-transaction"));
    assert!(migration.contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS"));
    assert!(migration.contains("idx_usage_settlement_dashboard_cover"));
    for column in [
        "billing_input_tokens",
        "billing_effective_input_tokens",
        "billing_output_tokens",
        "billing_cache_creation_tokens",
        "billing_cache_read_tokens",
        "billing_total_input_context",
        "billing_cache_creation_cost_usd",
        "billing_cache_read_cost_usd",
        "billing_total_cost_usd",
        "billing_actual_total_cost_usd",
        "input_price_per_1m",
    ] {
        assert!(
            migration.contains(column),
            "missing covering column {column}"
        );
    }
}

#[test]
fn dashboard_combined_summary_scans_each_raw_window_once() {
    let source = include_str!("../mod.rs");
    let combined = source
        .split("async fn summarize_dashboard_stats_raw")
        .nth(1)
        .and_then(|tail| tail.split("async fn summarize_dashboard_usage_raw").next())
        .expect("combined raw summary function should be present");
    assert_eq!(combined.matches("FROM usage_billing_facts").count(), 1);
    assert!(combined.contains("dashboard_eligible"));
    assert!(combined.contains("savings_estimated_full_cost_usd"));
    assert!(combined.contains("FILTER (WHERE \"usage\".dashboard_eligible)"));
    for canonical_column in [
        "effective_input_tokens",
        "total_tokens",
        "cache_creation_input_tokens",
        "total_input_context",
    ] {
        assert!(
            combined.contains(&format!("SUM(\"usage\".{canonical_column})")),
            "dashboard summary should aggregate the canonical {canonical_column} projection"
        );
    }
    assert!(
        !combined.contains("split_part(lower("),
        "dashboard summary should not recompute canonical token fields for every row"
    );
}

#[test]
fn usage_sql_provider_performance_reads_upstream_stream_from_billing_facts() {
    let source = include_str!("../mod.rs");
    assert!(source.contains("\"usage\".upstream_is_stream"));
    assert!(
        !source.contains("usage_base.request_metadata->>'upstream_is_stream'"),
        "provider performance queries should not rejoin public.usage to resolve upstream stream mode"
    );
    assert!(
        !source.contains("LEFT JOIN public.usage AS usage_base"),
        "provider performance queries should stay on usage_billing_facts to avoid an extra usage scan"
    );
}

#[test]
fn usage_provider_performance_combines_summary_and_ranking_without_timeline() {
    let source = include_str!("../mod.rs");
    let grouped_query = source
        .split("async fn summarize_usage_provider_performance_groups(")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn summarize_usage_provider_performance_timeline")
                .next()
        })
        .expect("provider performance grouped query should be present");
    let implementation = source
        .split("pub async fn summarize_usage_provider_performance(")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn summarize_usage_cost_savings_raw_from_range")
                .next()
        })
        .expect("provider performance implementation should be present");

    assert_eq!(grouped_query.matches("FROM usage_billing_facts").count(), 1);
    assert!(grouped_query.contains("GROUP BY GROUPING SETS ((provider_id), ())"));
    assert!(grouped_query.contains("GROUPING(provider_id)::INTEGER AS is_summary"));
    assert!(implementation.contains("if !query.include_timeline"));
    assert!(implementation.contains("summarize_usage_provider_performance_groups(query, true)"));
    assert!(implementation.contains("summary: summary.unwrap_or_default()"));
    assert!(implementation.contains("timeline: Vec::new()"));
}

#[test]
fn usage_provider_performance_parallelizes_timeline_query_chains() {
    let source = include_str!("../mod.rs");
    let implementation = source
        .split("pub async fn summarize_usage_provider_performance(")
        .nth(1)
        .and_then(|tail| {
            tail.split("async fn summarize_usage_cost_savings_raw_from_range")
                .next()
        })
        .expect("provider performance implementation should be present");

    assert!(implementation.contains("let summary_future ="));
    assert!(implementation.contains("let providers_and_timeline_future = async"));
    assert!(implementation.contains("try_join(summary_future, providers_and_timeline_future)"));
    assert!(implementation.contains("let timeline = if query.include_timeline"));
    assert!(
        implementation
            .find("summarize_usage_provider_performance_groups(query, false)")
            .expect("provider ranking query should be present")
            < implementation
                .find("summarize_usage_provider_performance_timeline")
                .expect("provider timeline query should be present"),
        "timeline must keep running after provider ranking supplies its provider IDs"
    );
}

#[test]
fn provider_breakdown_labels_resolve_catalog_name_before_recorded_name_and_id() {
    let sql = super::super::analytics::ANALYTICS_PROVIDER_LABEL_SQL;
    let catalog = sql
        .find("provider_catalog.name")
        .expect("provider catalog name");
    let recorded = sql
        .find("page.provider_name")
        .expect("recorded provider name snapshot");
    let raw_id = sql
        .find("page.group_id::text")
        .expect("raw provider id fallback");
    // 成本分析“提供商”列的解析顺序：目录名称 → 使用记录里的名称快照 → 原始 provider_id。
    assert!(catalog < recorded && recorded < raw_id);
    // 历史占位值不能被当成提供商名称展示。
    assert!(sql.contains("'unknown', 'unknow', 'pending'"));
}

/// 回归测试：健康监控时间线条必须走数据库侧的全窗口分段聚合，而不是"最新 N 条事件"样本。
///
/// 这里断言生成的 SQL 结构：数据源、过滤条件、成功判定必须与
/// `summarize_usage_breakdown_raw` 一致，且**不允许出现 LIMIT**——一旦有人把时间线
/// 改回带 LIMIT 的样本查询，高流量模型/提供商的早期时段就会再次被判成"无请求"。
#[test]
fn usage_health_timeline_sql_buckets_full_window_without_sample_limit() {
    let query = UsageHealthTimelineQuery {
        created_from_unix_secs: 1_800_000_000,
        created_until_unix_secs: 1_800_021_600,
        group_by: UsageHealthTimelineGroupBy::Model,
        group_values: vec!["deepseek-v4.1-flash".to_string()],
        provider_name: None,
        model: None,
        api_format: None,
        segments: 60,
        exclude_status_codes: vec![499],
    };
    let mut builder = build_usage_health_timeline_query(&query).expect("query should build");
    let sql = builder.build().sql().as_str().to_string();

    // 与卡片头部统计同源：同一张 billing 事实视图。
    assert!(sql.contains("FROM usage_billing_facts AS \"usage\""));
    // 过滤条件与 summarize_usage_breakdown_raw 逐字对齐，保证两处数字可对账。
    assert!(sql.contains("\"usage\".status NOT IN ('pending', 'streaming')"));
    assert!(sql.contains("\"usage\".provider_name NOT IN ('unknown', 'pending')"));
    assert!(sql.contains("\"usage\".status <> 'failed'"));
    assert!(sql.contains("(\"usage\".status_code IS NULL OR \"usage\".status_code < 400)"));
    assert!(sql.contains("\"usage\".error_message IS NULL"));
    // 分桶在库内完成，按 (分组键, 分段序号) 聚合。
    assert!(sql.contains("EXTRACT(EPOCH FROM"));
    assert!(sql.contains("AS segment_idx"));
    assert!(sql.contains("GROUP BY group_key, segment_idx"));
    // 窗口右端必须是开区间，与 breakdown 的 created_at < until 一致。
    assert!(sql.contains("\"usage\".created_at < TO_TIMESTAMP("));
    // 关键回归点：不允许出现 LIMIT，否则会退回"截断样本"的错误实现。
    assert!(
        !sql.to_uppercase().contains("LIMIT"),
        "健康时间线聚合不能被 LIMIT 截断：{sql}"
    );
    // 分组键通过绑定参数传入，不做字符串拼接。
    assert!(sql.contains("= ANY("));
    assert!(!sql.contains("deepseek-v4.1-flash"));
}

/// ApiFormat 维度必须排除空格式，与 breakdown 的 filtered_extra_where 一致。
#[test]
fn usage_health_timeline_sql_excludes_null_api_format_for_api_format_dimension() {
    let query = UsageHealthTimelineQuery {
        created_from_unix_secs: 1_800_000_000,
        created_until_unix_secs: 1_800_021_600,
        group_by: UsageHealthTimelineGroupBy::ApiFormat,
        group_values: vec!["openai:chat".to_string()],
        provider_name: Some("codebuddy2api".to_string()),
        model: Some("deepseek-v4.1-flash".to_string()),
        api_format: None,
        segments: 60,
        exclude_status_codes: vec![499],
    };
    let mut builder = build_usage_health_timeline_query(&query).expect("query should build");
    let sql = builder.build().sql().as_str().to_string();

    assert!(sql.contains("AND \"usage\".api_format IS NOT NULL"));
    assert!(sql.contains("\"usage\".provider_name = "));
    assert!(sql.contains("\"usage\".model = "));
}

/// 非法窗口（空分段、空分组键、时间倒挂）不产生查询。
#[test]
fn usage_health_timeline_query_rejects_invalid_windows() {
    let base = UsageHealthTimelineQuery {
        created_from_unix_secs: 1_800_000_000,
        created_until_unix_secs: 1_800_021_600,
        group_by: UsageHealthTimelineGroupBy::Model,
        group_values: vec!["gpt-4.1".to_string()],
        provider_name: None,
        model: None,
        api_format: None,
        segments: 60,
        exclude_status_codes: Vec::new(),
    };
    assert!(build_usage_health_timeline_query(&base).is_some());

    let mut no_segments = base.clone();
    no_segments.segments = 0;
    assert!(build_usage_health_timeline_query(&no_segments).is_none());

    let mut no_groups = base.clone();
    no_groups.group_values.clear();
    assert!(build_usage_health_timeline_query(&no_groups).is_none());

    let mut inverted = base.clone();
    inverted.created_until_unix_secs = inverted.created_from_unix_secs;
    assert!(build_usage_health_timeline_query(&inverted).is_none());
}
