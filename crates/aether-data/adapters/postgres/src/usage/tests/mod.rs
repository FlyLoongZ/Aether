use chrono::{TimeZone, Utc};
use serde_json::json;
use sqlx::{Execute, Postgres, QueryBuilder, Row};
use std::sync::Arc;

use super::{
    attach_usage_routing_snapshot_metadata, attach_usage_settlement_pricing_snapshot_metadata,
    build_usage_health_timeline_query, clear_previous_request_body_facts,
    compress_usage_body_pair, container_body, decode_container,
    prepare_request_metadata_for_body_storage, prepare_usage_body_pair,
    prepare_usage_for_persistence, push_postgres_usage_websocket_filter,
    request_body_capture_replaces_derived_facts, split_dashboard_daily_aggregate_range,
    split_dashboard_hourly_aggregate_range, usage_body_capture_state_for_storage, usage_body_ref,
    usage_capture_update_allowed, usage_effective_input_tokens, usage_http_audit_capture_mode,
    usage_routing_snapshot_from_usage, usage_settlement_pricing_snapshot_from_usage,
    usage_total_input_context, AggregateRangeSplit, SqlxUsageReadRepository, UsageBodyPairStorage,
    UsageRoutingSnapshot, UsageSettlementPricingSnapshot, ENCODING_RAW_CONTAINER,
    ENCODING_ZSTD_CONTAINER, SELECT_STALE_PENDING_USAGE_BATCH_SQL,
};
use crate::{PostgresPoolConfig, PostgresPoolFactory};
use aether_data_contracts::repository::usage::{
    UpsertUsageRecord, UsageAuditListQuery, UsageBodyCaptureState, UsageBodyField,
    UsageBreakdownGroupBy, UsageBreakdownSummaryQuery, UsageDashboardDailyBreakdownQuery,
    UsageHealthTimelineGroupBy, UsageHealthTimelineQuery, UsageWriteRepository,
};

mod body;
mod dashboard;
mod live;
mod read;
mod write;

fn normalize_newlines(value: &str) -> String {
    value.replace("\r\n", "\n")
}

fn fast_clear_usage_record(
    request_id: &str,
    provider_name: &str,
    now_unix_secs: u64,
    terminal: bool,
    terminal_state: UsageBodyCaptureState,
    terminal_service_tier: Option<&str>,
) -> UpsertUsageRecord {
    UpsertUsageRecord {
        capture_retention: Default::default(),
        request_id: request_id.to_string(),
        user_id: None,
        api_key_id: None,
        username: None,
        api_key_name: None,
        provider_name: provider_name.to_string(),
        model: "gpt-5".to_string(),
        target_model: Some("gpt-5".to_string()),
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
        input_tokens: terminal.then_some(1),
        output_tokens: terminal.then_some(1),
        total_tokens: terminal.then_some(2),
        cache_creation_input_tokens: None,
        cache_creation_ephemeral_5m_input_tokens: None,
        cache_creation_ephemeral_1h_input_tokens: None,
        cache_read_input_tokens: None,
        cache_creation_cost_usd: None,
        cache_read_cost_usd: None,
        output_price_per_1m: None,
        total_cost_usd: None,
        actual_total_cost_usd: None,
        status_code: terminal.then_some(200),
        error_message: None,
        error_category: None,
        response_time_ms: terminal.then_some(10),
        first_byte_time_ms: None,
        status: if terminal { "completed" } else { "pending" }.to_string(),
        billing_status: "pending".to_string(),
        request_headers: None,
        request_body: None,
        request_body_ref: None,
        request_body_state: None,
        provider_request_headers: None,
        provider_request_body: (!terminal).then(|| {
            json!({
                "model": "gpt-5",
                "service_tier": "priority"
            })
        }),
        provider_request_body_ref: None,
        provider_request_body_state: Some(if terminal {
            terminal_state
        } else {
            UsageBodyCaptureState::Inline
        }),
        response_headers: None,
        response_body: None,
        response_body_ref: None,
        response_body_state: None,
        client_response_headers: None,
        client_response_body: None,
        client_response_body_ref: None,
        client_response_body_state: None,
        candidate_id: None,
        candidate_index: None,
        key_name: None,
        planner_kind: None,
        route_family: None,
        route_kind: None,
        execution_path: None,
        local_execution_runtime_miss_reason: None,
        request_metadata: if terminal {
            terminal_service_tier.map(|tier| json!({"provider_service_tier": tier}))
        } else {
            Some(json!({"provider_service_tier": "priority"}))
        },
        finalized_at_unix_secs: terminal.then_some(now_unix_secs + 1),
        created_at_unix_ms: Some(now_unix_secs),
        updated_at_unix_secs: now_unix_secs + u64::from(terminal),
    }
}

fn first_byte_usage_record(
    request_id: &str,
    provider_name: &str,
    now_unix_secs: u64,
    request_metadata: Option<serde_json::Value>,
) -> UpsertUsageRecord {
    let mut record = fast_clear_usage_record(
        request_id,
        provider_name,
        now_unix_secs,
        false,
        UsageBodyCaptureState::Inline,
        None,
    );
    record.status = "streaming".to_string();
    record.is_stream = Some(true);
    record.status_code = Some(200);
    record.response_time_ms = Some(14);
    record.first_byte_time_ms = Some(12);
    record.provider_request_body = None;
    record.provider_request_body_state = None;
    record.request_metadata = request_metadata;
    record
}

fn lazy_usage_repository() -> SqlxUsageReadRepository {
    let factory = PostgresPoolFactory::new(PostgresPoolConfig {
        database_url: "postgresql://postgres:postgres@127.0.0.1:1/aether".to_string(),
        min_connections: 0,
        max_connections: 1,
        acquire_timeout_ms: 100,
        idle_timeout_ms: 100,
        max_lifetime_ms: 100,
        statement_cache_capacity: 8,
        require_ssl: false,
    })
    .expect("factory should build");
    SqlxUsageReadRepository::new(factory.connect_lazy().expect("lazy pool should build"))
}
