use std::sync::Arc;
use std::time::Duration;

use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::{DatabaseDriver, SqlDatabaseConfig, SqlPoolConfig};
use aether_data_contracts::repository::usage::{
    UsageCounterHealthSnapshot, UsageCounterPendingHealthSnapshot,
};
use serde_json::json;

use super::{
    admin_usage_aggregate_import_error, database_bounded_auth_load_limit,
    merge_usage_counter_health_snapshots, usage_counter_pending_health_metric_samples_with_timeout,
    usage_queue_health_metric_samples_with_timeout, usage_runtime_metric_samples, AppState,
    MetricKind, MetricSample, METRIC_SNAPSHOT_TTL,
    USAGE_COUNTER_EXACT_HEALTH_METRICS_MAX_STALENESS, USAGE_COUNTER_EXACT_HEALTH_METRICS_TTL,
};
use crate::cache::SchedulerAffinityTarget;
use crate::data::{GatewayDataConfig, GatewayDataState};

#[test]
fn usage_aggregate_import_invalid_input_is_projected_to_a_safe_client_message() {
    let error = admin_usage_aggregate_import_error(
        "postgres table stats_user_daily row secret-user contains column password".to_string(),
    );

    match error {
        super::GatewayError::Client { status, message } => {
            assert_eq!(status, http::StatusCode::BAD_REQUEST);
            assert_eq!(message, "usage aggregate import payload is invalid");
            assert!(!message.contains("secret-user"));
            assert!(!message.contains("password"));
        }
        other => panic!("expected client-safe import error, got {other:?}"),
    }
}

#[test]
fn auth_load_gate_reserves_half_of_foreground_database_pool() {
    assert_eq!(
        database_bounded_auth_load_limit(Some(192), Some(92)),
        Some(46)
    );
    assert_eq!(database_bounded_auth_load_limit(Some(4), Some(92)), Some(4));
    assert_eq!(database_bounded_auth_load_limit(Some(64), Some(1)), Some(1));
    assert_eq!(database_bounded_auth_load_limit(None, Some(92)), None);
    assert_eq!(database_bounded_auth_load_limit(Some(64), None), Some(64));
}

#[tokio::test]
async fn http_connection_budget_is_optional_and_shared_by_app_state_clones() {
    let state = AppState::new().expect("app state should build");
    assert!(state.http_connection_budget.is_none());
    let budget = Arc::new(super::HttpConnectionBudget::new(1));
    let state = state.with_http_connection_budget(Arc::clone(&budget));
    let cloned = state.clone();
    assert!(Arc::ptr_eq(
        cloned.http_connection_budget.as_ref().unwrap(),
        &budget,
    ));

    let connection = budget.try_admit(()).expect("first connection admitted");
    assert!(cloned
        .http_connection_budget
        .as_ref()
        .unwrap()
        .try_admit(())
        .is_err());
    let samples = state.collect_metric_samples().await;
    for (name, expected) in [
        ("gateway_http_connections_limit", 1),
        ("gateway_http_connections_in_flight", 1),
        ("gateway_http_connections_high_watermark", 1),
        ("gateway_http_connections_rejected_total", 1),
        ("gateway_http_connections_accept_errors_total", 0),
    ] {
        assert_eq!(
            samples
                .iter()
                .find(|sample| sample.name == name)
                .unwrap()
                .value,
            expected,
        );
    }
    drop(connection);
    assert_eq!(budget.snapshot().in_flight, 0);
}

#[test]
fn http_connection_metrics_export_all_budget_counters() {
    let samples = super::http_connection_metric_samples(&super::HttpConnectionBudgetSnapshot {
        limit: 4096,
        in_flight: 11,
        high_watermark: 30,
        rejected_total: 7,
        accept_errors_total: 3,
    });
    for (name, kind, value) in [
        ("gateway_http_connections_limit", MetricKind::Gauge, 4096),
        ("gateway_http_connections_in_flight", MetricKind::Gauge, 11),
        (
            "gateway_http_connections_high_watermark",
            MetricKind::Gauge,
            30,
        ),
        (
            "gateway_http_connections_rejected_total",
            MetricKind::Counter,
            7,
        ),
        (
            "gateway_http_connections_accept_errors_total",
            MetricKind::Counter,
            3,
        ),
    ] {
        let matching = samples
            .iter()
            .filter(|sample| sample.name == name)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1);
        assert_eq!((matching[0].kind, matching[0].value), (kind, value));
    }
}

#[test]
fn usage_runtime_metrics_export_queue_payload_limit_and_attempt_counters() {
    let mut snapshot = crate::usage::UsageRuntimeMetricsSnapshot::default();
    snapshot.queue_payload_max_bytes = 1024 * 1024;
    snapshot.queue_payload_downgraded_total = 11;
    snapshot.queue_payload_rejected_total = 3;
    snapshot.enqueue_retry_permanent_failure_total = 2;
    let samples = usage_runtime_metric_samples(&snapshot);
    for (name, kind, value) in [
        (
            "usage_runtime_queue_payload_max_bytes",
            MetricKind::Gauge,
            1024 * 1024,
        ),
        (
            "usage_runtime_queue_payload_downgraded_total",
            MetricKind::Counter,
            11,
        ),
        (
            "usage_runtime_queue_payload_rejected_total",
            MetricKind::Counter,
            3,
        ),
        (
            "usage_runtime_enqueue_retry_permanent_failure_total",
            MetricKind::Counter,
            2,
        ),
    ] {
        let matching = samples
            .iter()
            .filter(|sample| sample.name == name)
            .collect::<Vec<_>>();
        assert_eq!(
            matching.len(),
            1,
            "each payload metric must be emitted once"
        );
        assert_eq!((matching[0].kind, matching[0].value), (kind, value));
    }
}

#[test]
fn usage_runtime_metrics_export_first_byte_batch_counters() {
    let mut snapshot = crate::usage::UsageRuntimeMetricsSnapshot::default();
    snapshot.first_byte_persistence_batch_flush_total = 7;
    snapshot.first_byte_persistence_batch_records_total = 896;
    snapshot.first_byte_persistence_max_batch_size = 128;
    snapshot.first_byte_persistence_batch_failed_total = 2;

    let samples = usage_runtime_metric_samples(&snapshot);
    let value = |name: &str| {
        samples
            .iter()
            .find(|sample| sample.name == name)
            .map(|sample| (sample.kind, sample.value))
            .expect("first-byte batch metric should be exported")
    };

    assert_eq!(
        value("usage_runtime_first_byte_persistence_batch_flush_total"),
        (MetricKind::Counter, 7)
    );
    assert_eq!(
        value("usage_runtime_first_byte_persistence_batch_records_total"),
        (MetricKind::Counter, 896)
    );
    assert_eq!(
        value("usage_runtime_first_byte_persistence_max_batch_size"),
        (MetricKind::Gauge, 128)
    );
    assert_eq!(
        value("usage_runtime_first_byte_persistence_batch_failed_total"),
        (MetricKind::Counter, 2)
    );
}

#[test]
fn usage_runtime_metrics_export_lifecycle_submission_health() {
    let mut snapshot = crate::usage::UsageRuntimeMetricsSnapshot::default();
    snapshot.lifecycle_submission_capacity = 32_768;
    snapshot.lifecycle_submission_workers = 64;
    snapshot.lifecycle_submission_pending = 123;
    snapshot.lifecycle_submission_max_pending = 4_567;
    snapshot.lifecycle_submission_enqueued_total = 20_000;
    snapshot.lifecycle_submission_coalesced_total = 18_000;
    snapshot.lifecycle_submission_overflow_total = 3;
    snapshot.lifecycle_submission_processed_total = 19_877;
    snapshot.ordered_lifecycle_pending = 321;
    snapshot.ordered_lifecycle_max_pending = 8_765;
    snapshot.pending_persistence_capacity = 65_536;
    snapshot.pending_persistence_pending = 222;
    snapshot.pending_persistence_max_pending = 7_654;
    snapshot.pending_persistence_batch_flush_total = 40;
    snapshot.pending_persistence_batch_records_total = 10_000;
    snapshot.pending_persistence_max_batch_size = 512;
    snapshot.pending_persistence_batch_failed_total = 2;
    snapshot.pending_persistence_retried_total = 9;
    snapshot.pending_persistence_overflow_total = 4;

    let samples = usage_runtime_metric_samples(&snapshot);
    let value = |name: &str| {
        samples
            .iter()
            .find(|sample| sample.name == name)
            .map(|sample| (sample.kind, sample.value))
            .expect("lifecycle submission metric should be exported")
    };

    assert_eq!(
        value("usage_runtime_lifecycle_submission_capacity"),
        (MetricKind::Gauge, 32_768)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_workers"),
        (MetricKind::Gauge, 64)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_pending"),
        (MetricKind::Gauge, 123)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_max_pending"),
        (MetricKind::Gauge, 4_567)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_enqueued_total"),
        (MetricKind::Counter, 20_000)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_coalesced_total"),
        (MetricKind::Counter, 18_000)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_overflow_total"),
        (MetricKind::Counter, 3)
    );
    assert_eq!(
        value("usage_runtime_lifecycle_submission_processed_total"),
        (MetricKind::Counter, 19_877)
    );
    assert_eq!(
        value("usage_runtime_ordered_lifecycle_pending"),
        (MetricKind::Gauge, 321)
    );
    assert_eq!(
        value("usage_runtime_ordered_lifecycle_max_pending"),
        (MetricKind::Gauge, 8_765)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_capacity"),
        (MetricKind::Gauge, 65_536)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_pending"),
        (MetricKind::Gauge, 222)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_max_pending"),
        (MetricKind::Gauge, 7_654)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_batch_flush_total"),
        (MetricKind::Counter, 40)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_batch_records_total"),
        (MetricKind::Counter, 10_000)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_max_batch_size"),
        (MetricKind::Gauge, 512)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_batch_failed_total"),
        (MetricKind::Counter, 2)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_retried_total"),
        (MetricKind::Counter, 9)
    );
    assert_eq!(
        value("usage_runtime_pending_persistence_overflow_total"),
        (MetricKind::Counter, 4)
    );
}

#[test]
fn cached_usage_counter_health_keeps_pending_fields_fresh() {
    let mut pending_by_kind = std::collections::BTreeMap::new();
    pending_by_kind.insert("api_key".to_string(), 3);
    let pending = UsageCounterPendingHealthSnapshot {
        pending_rows: 3,
        oldest_pending_created_at_unix_secs: Some(1_050),
        pending_by_kind: pending_by_kind.clone(),
    };
    let exact = UsageCounterHealthSnapshot {
        pending_rows: 99,
        processed_rows: 42,
        oldest_pending_created_at_unix_secs: Some(1),
        latest_processed_at_unix_secs: Some(1_100),
        pending_by_kind: std::collections::BTreeMap::new(),
    };

    assert_eq!(
        merge_usage_counter_health_snapshots(pending, exact),
        UsageCounterHealthSnapshot {
            pending_rows: 3,
            processed_rows: 42,
            oldest_pending_created_at_unix_secs: Some(1_050),
            latest_processed_at_unix_secs: Some(1_100),
            pending_by_kind,
        }
    );
}

#[tokio::test]
async fn admin_usage_counter_health_reuses_recent_exact_snapshot() {
    let state = AppState::new().expect("app state should build");
    *state
        .usage_counter_exact_health_metric_snapshot
        .write()
        .await = Some((
        std::time::Instant::now(),
        UsageCounterHealthSnapshot {
            pending_rows: 99,
            processed_rows: 42,
            oldest_pending_created_at_unix_secs: Some(1),
            latest_processed_at_unix_secs: Some(1_100),
            pending_by_kind: std::collections::BTreeMap::new(),
        },
    ));
    state.mark_usage_counter_exact_health_metric_attempt();

    let snapshot = state
        .read_cached_usage_counter_health()
        .await
        .expect("cached health read should succeed");

    assert_eq!(snapshot.pending_rows, 0);
    assert_eq!(snapshot.oldest_pending_created_at_unix_secs, None);
    assert_eq!(snapshot.processed_rows, 42);
    assert_eq!(snapshot.latest_processed_at_unix_secs, Some(1_100));
}

#[tokio::test]
async fn admin_usage_counter_health_serves_bounded_stale_exact_snapshot_during_backoff() {
    let state = AppState::new().expect("app state should build");
    *state
        .usage_counter_exact_health_metric_snapshot
        .write()
        .await = Some((
        std::time::Instant::now() - USAGE_COUNTER_EXACT_HEALTH_METRICS_TTL - Duration::from_secs(1),
        UsageCounterHealthSnapshot {
            processed_rows: 42,
            latest_processed_at_unix_secs: Some(1_100),
            ..UsageCounterHealthSnapshot::default()
        },
    ));
    state.mark_usage_counter_exact_health_metric_attempt();

    let snapshot = state
        .read_cached_usage_counter_health()
        .await
        .expect("bounded stale health should remain available during retry backoff");

    assert_eq!(snapshot.processed_rows, 42);
    assert_eq!(snapshot.latest_processed_at_unix_secs, Some(1_100));
}

#[tokio::test]
async fn admin_usage_counter_health_rejects_exact_snapshot_past_max_staleness() {
    let state = AppState::new().expect("app state should build");
    *state
        .usage_counter_exact_health_metric_snapshot
        .write()
        .await = Some((
        std::time::Instant::now()
            - USAGE_COUNTER_EXACT_HEALTH_METRICS_MAX_STALENESS
            - Duration::from_secs(1),
        UsageCounterHealthSnapshot {
            processed_rows: 42,
            latest_processed_at_unix_secs: Some(1_100),
            ..UsageCounterHealthSnapshot::default()
        },
    ));
    state.mark_usage_counter_exact_health_metric_attempt();

    let err = state
        .read_cached_usage_counter_health()
        .await
        .expect_err("an over-age exact snapshot must not be returned during retry backoff");

    assert!(matches!(err, aether_data::DataLayerError::TimedOut(_)));
}

#[tokio::test]
async fn admin_usage_counter_health_does_not_report_default_exact_values_after_failed_refresh() {
    let state = AppState::new().expect("app state should build");
    state.mark_usage_counter_exact_health_metric_attempt();

    let err = state
        .read_cached_usage_counter_health()
        .await
        .expect_err("a recent failed refresh must remain unavailable");

    assert!(matches!(err, aether_data::DataLayerError::TimedOut(_)));
}

#[test]
fn stale_or_missing_exact_health_snapshot_uses_short_retry_backoff() {
    let state = AppState::new().expect("app state should build");
    let stale_created_at = std::time::Instant::now() - USAGE_COUNTER_EXACT_HEALTH_METRICS_TTL;
    state.mark_usage_counter_exact_health_metric_attempt();
    assert!(!state.usage_counter_exact_health_metric_refresh_is_due(None));
    assert!(!state.usage_counter_exact_health_metric_refresh_is_due(Some(stale_created_at)));

    *state
        .usage_counter_exact_health_metric_last_attempt
        .lock()
        .expect("last attempt lock should be available") =
        Some(std::time::Instant::now() - Duration::from_secs(6));

    assert!(state.usage_counter_exact_health_metric_refresh_is_due(None));
    assert!(state.usage_counter_exact_health_metric_refresh_is_due(Some(stale_created_at)));
    assert!(
        !state.usage_counter_exact_health_metric_refresh_is_due(Some(std::time::Instant::now()))
    );
}

#[tokio::test]
async fn metric_snapshot_prewarm_failure_allows_immediate_admin_retry() {
    let state = AppState::new().expect("app state should build");

    assert!(
        !state
            .prewarm_metric_snapshot_with_exact_refresh(std::future::ready(Err(
                aether_data::DataLayerError::TimedOut("test exact refresh failure".to_string()),
            )))
            .await
    );
    assert!(state
        .usage_counter_exact_health_metric_last_attempt
        .lock()
        .expect("last attempt lock should be available")
        .is_none());

    state
        .read_cached_usage_counter_health()
        .await
        .expect("admin read should retry immediately after failed prewarm");
}

#[tokio::test]
async fn runtime_pool_can_disable_background_isolation() {
    let config = GatewayDataConfig::from_database_config(
        SqlDatabaseConfig::new(
            DatabaseDriver::Postgres,
            "postgres://localhost/aether",
            SqlPoolConfig {
                min_connections: 4,
                max_connections: 20,
                ..SqlPoolConfig::default()
            },
        )
        .expect("database config should be valid"),
    );

    let state = AppState::new()
        .expect("app state should build")
        .with_data_config_and_background_isolation(config, false)
        .expect("data state should build");

    assert!(!state.background_data_isolated);
}

#[tokio::test]
async fn system_config_reads_use_short_lived_cache_until_app_invalidation() {
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled()
                .with_system_config_values_for_tests([("site_name".to_string(), json!("old"))]),
        );

    assert_eq!(
        state
            .read_system_config_json_value("site_name")
            .await
            .expect("system config read should succeed"),
        Some(json!("old"))
    );

    state
        .data
        .upsert_system_config_value("site_name", &json!("bypassed"), None)
        .await
        .expect("direct data write should succeed");

    assert_eq!(
        state
            .read_system_config_json_value("site_name")
            .await
            .expect("cached system config read should succeed"),
        Some(json!("old"))
    );

    state
        .upsert_system_config_json_value("site_name", &json!("fresh"), None)
        .await
        .expect("app system config write should succeed");

    assert_eq!(
        state
            .read_system_config_json_value("site_name")
            .await
            .expect("refreshed system config read should succeed"),
        Some(json!("fresh"))
    );
}

#[tokio::test]
async fn stale_system_config_reads_return_before_background_refresh_finishes() {
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled()
                .with_system_config_values_for_tests([("site_name".to_string(), json!("old"))]),
        );
    let fresh_ttl = Duration::from_millis(2);
    let max_staleness = Duration::from_millis(500);

    assert_eq!(
        state
            .read_system_config_json_value_with_cache_windows(
                "site_name",
                fresh_ttl,
                max_staleness,
            )
            .await
            .expect("initial system config read should succeed"),
        Some(json!("old"))
    );
    state
        .data
        .upsert_system_config_value("site_name", &json!("new"), None)
        .await
        .expect("direct data write should succeed");
    tokio::time::sleep(Duration::from_millis(10)).await;

    let started = std::time::Instant::now();
    let stale = state
        .read_system_config_json_value_with_cache_windows("site_name", fresh_ttl, max_staleness)
        .await
        .expect("stale system config read should succeed");
    assert_eq!(stale, Some(json!("old")));
    assert!(
        started.elapsed() < Duration::from_millis(100),
        "stale reads must not wait on refresh"
    );

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if state
                .system_config_cache
                .get_with_age("site_name", max_staleness)
                .map(|(value, _)| value)
                == Some(Some(json!("new")))
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background refresh should eventually publish the new value");
}

#[tokio::test]
async fn system_config_reads_reload_synchronously_after_hard_staleness() {
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled()
                .with_system_config_values_for_tests([("site_name".to_string(), json!("old"))]),
        );
    let fresh_ttl = Duration::from_millis(2);
    let max_staleness = Duration::from_millis(10);

    assert_eq!(
        state
            .read_system_config_json_value_with_cache_windows(
                "site_name",
                fresh_ttl,
                max_staleness,
            )
            .await
            .expect("initial system config read should succeed"),
        Some(json!("old"))
    );
    state
        .data
        .upsert_system_config_value("site_name", &json!("new"), None)
        .await
        .expect("direct data write should succeed");
    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(
        state
            .read_system_config_json_value_with_cache_windows(
                "site_name",
                fresh_ttl,
                max_staleness,
            )
            .await
            .expect("hard-stale system config read should succeed"),
        Some(json!("new"))
    );
}

#[tokio::test]
async fn system_config_entry_write_refreshes_cache_and_scheduler_affinity_for_format_conversion() {
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled().with_system_config_values_for_tests([(
                "enable_format_conversion".to_string(),
                json!(false),
            )]),
        );
    let cache_key = "scheduler_affinity:api-key-1:openai:chat:gpt-5";
    let ttl = std::time::Duration::from_secs(300);

    assert_eq!(
        state
            .read_system_config_json_value("enable_format_conversion")
            .await
            .expect("system config read should succeed"),
        Some(json!(false))
    );
    state.remember_scheduler_affinity_target(
        cache_key,
        SchedulerAffinityTarget {
            provider_id: "provider-old".to_string(),
            endpoint_id: "endpoint-old".to_string(),
            key_id: "key-old".to_string(),
        },
        ttl,
        128,
    );
    assert!(state
        .read_scheduler_affinity_target(cache_key, ttl)
        .is_some());

    let initial_epoch = state.scheduler_affinity_epoch();
    state
        .upsert_system_config_entry("enable_format_conversion", &json!(true), None)
        .await
        .expect("admin config write should succeed");

    assert_eq!(
        state
            .read_system_config_json_value("enable_format_conversion")
            .await
            .expect("system config read should use refreshed cache"),
        Some(json!(true))
    );
    assert!(state.scheduler_affinity_epoch() > initial_epoch);
    assert_eq!(state.read_scheduler_affinity_target(cache_key, ttl), None);
}

#[tokio::test]
async fn system_config_write_refreshes_frontdoor_rpm_default_cache() {
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled().with_system_config_values_for_tests([(
                "rate_limit_per_minute".to_string(),
                json!(1),
            )]),
        );

    assert_eq!(
        state
            .frontdoor_user_rpm()
            .current_system_default_limit(&state)
            .await
            .expect("default rpm limit should read"),
        1
    );
    state
        .upsert_system_config_entry("rate_limit_per_minute", &json!(0), None)
        .await
        .expect("rpm system config should update");

    assert_eq!(
        state
            .frontdoor_user_rpm()
            .current_system_default_limit(&state)
            .await
            .expect("default rpm limit should use refreshed value"),
        0
    );
}

#[tokio::test]
async fn replacing_data_state_clears_system_config_cache() {
    let mut state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(
            GatewayDataState::disabled()
                .with_system_config_values_for_tests([("site_name".to_string(), json!("old"))]),
        );

    assert_eq!(
        state
            .read_system_config_json_value("site_name")
            .await
            .expect("system config read should succeed"),
        Some(json!("old"))
    );

    state.replace_data_state(Arc::new(
        GatewayDataState::disabled()
            .with_system_config_values_for_tests([("site_name".to_string(), json!("new"))]),
    ));

    assert_eq!(
        state
            .read_system_config_json_value("site_name")
            .await
            .expect("system config read should reflect replaced data"),
        Some(json!("new"))
    );
}

#[tokio::test]
async fn metric_samples_include_auth_snapshot_load_gate() {
    let state = AppState::new().expect("app state should build");
    assert!(state.prewarm_metric_snapshot().await);
    let samples = state.metric_samples().await;

    assert!(samples.iter().any(|sample| {
        sample.name == "concurrency_available_permits"
            && sample
                .labels
                .iter()
                .any(|label| label.key == "gate" && label.value == "gateway_auth_snapshot_load")
    }));
}

#[tokio::test]
async fn metric_samples_include_request_body_buffer_budget_usage() {
    let state = AppState::new().expect("app state should build");
    let _permit = Arc::clone(&state.request_body_buffer_budget)
        .acquire_many_owned(2)
        .await
        .expect("request body budget should be open");
    assert!(state.prewarm_metric_snapshot().await);
    let samples = state.metric_samples().await;

    assert!(samples.iter().any(|sample| {
        sample.name == "request_body_buffer_in_use_bytes"
            && sample.value
                == u64::try_from(2 * crate::state::REQUEST_BODY_BUFFER_PERMIT_BYTES)
                    .unwrap_or(u64::MAX)
    }));
}

#[tokio::test]
async fn metric_samples_reuse_recent_snapshot() {
    let state = AppState::new().expect("app state should build");
    assert!(state.prewarm_metric_snapshot().await);
    let first = state.metric_samples().await;
    let _permit = Arc::clone(&state.request_body_buffer_budget)
        .acquire_many_owned(2)
        .await
        .expect("request body budget should be open");
    let second = state.metric_samples().await;

    assert_eq!(first, second);
    assert!(state.metric_snapshot.read().await.is_some());
}

#[tokio::test]
async fn metric_snapshot_prewarm_populates_low_frequency_exact_counter_metrics() {
    let state = AppState::new().expect("app state should build");

    assert!(state.prewarm_metric_snapshot().await);
    assert!(state
        .usage_counter_exact_health_metric_snapshot
        .read()
        .await
        .is_some());
    let samples = state.metric_samples().await;
    assert!(samples
        .iter()
        .any(|sample| sample.name == "usage_counter_outbox_processed_rows"));
    assert!(samples
        .iter()
        .any(|sample| sample.name == "usage_counter_outbox_latest_processed_at_unix_secs"));
    assert!(samples
        .iter()
        .any(|sample| sample.name == "usage_counter_exact_health_snapshot_age_seconds"));
}

#[tokio::test]
async fn stale_metric_samples_return_immediately_while_background_refreshes() {
    let state = AppState::new().expect("app state should build");
    let stale_sample = MetricSample::new(
        "stale_metric_snapshot_test",
        "Test-only stale metric snapshot marker.",
        MetricKind::Gauge,
        1,
    );
    let stale_created_at = std::time::Instant::now()
        .checked_sub(METRIC_SNAPSHOT_TTL + Duration::from_millis(1))
        .expect("stale timestamp should be representable");
    *state.metric_snapshot.write().await = Some((stale_created_at, vec![stale_sample.clone()]));

    let returned = tokio::time::timeout(Duration::from_millis(100), state.metric_samples())
        .await
        .expect("stale scrape should return without awaiting refresh I/O");
    assert_eq!(returned, vec![stale_sample]);

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let refreshed =
                state
                    .metric_snapshot
                    .read()
                    .await
                    .as_ref()
                    .is_some_and(|(_, samples)| {
                        samples
                            .iter()
                            .all(|sample| sample.name != "stale_metric_snapshot_test")
                    });
            if refreshed {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background metric refresh should replace the stale snapshot");
}

#[tokio::test]
async fn metric_samples_fail_open_while_initial_snapshot_refresh_is_in_progress() {
    let state = AppState::new().expect("app state should build");
    let _refresh_guard = state.metric_snapshot_refresh.lock().await;

    let samples = tokio::time::timeout(Duration::from_millis(100), state.metric_samples())
        .await
        .expect("contending scrape should not wait for the initial refresh");

    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].name, "service_up");
    assert_eq!(samples[0].value, 1);
    assert!(state.metric_snapshot.read().await.is_none());
}

#[tokio::test]
async fn usage_queue_health_metrics_timeout_fails_open() {
    let samples = tokio::time::timeout(
        Duration::from_millis(100),
        usage_queue_health_metric_samples_with_timeout(
            Duration::from_millis(1),
            std::future::pending::<Result<crate::usage::UsageQueueHealthSnapshot, ()>>(),
        ),
    )
    .await
    .expect("metrics timeout wrapper should remain bounded");

    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].name, "usage_queue_health_unavailable");
    assert_eq!(samples[0].value, 1);
}

#[tokio::test]
async fn usage_counter_health_metrics_timeout_fails_open() {
    let samples = tokio::time::timeout(
        Duration::from_millis(100),
        usage_counter_pending_health_metric_samples_with_timeout(
            Duration::from_millis(1),
            std::future::pending::<Result<UsageCounterPendingHealthSnapshot, ()>>(),
            1_000,
        ),
    )
    .await
    .expect("metrics timeout wrapper should remain bounded");

    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].name, "usage_counter_health_unavailable");
    assert_eq!(samples[0].value, 1);
}

#[test]
fn background_worker_state_uses_isolated_data_pool() {
    let mut state = AppState::new().expect("app state should build");
    let foreground = Arc::new(GatewayDataState::disabled());
    let background = Arc::new(GatewayDataState::disabled());
    state.replace_data_states(foreground, background.clone(), true);

    let worker_state = state.background_worker_state();

    assert!(Arc::ptr_eq(&worker_state.data, &state.background_data));
    assert!(!Arc::ptr_eq(&worker_state.data, &state.data));
    assert!(worker_state.data.has_usage_worker_queue());
    assert!(state.background_data_isolated);
}

#[test]
fn request_candidate_queue_writer_uses_background_data_only_when_isolated() {
    let foreground_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let background_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let foreground = Arc::new(
        GatewayDataState::with_request_candidate_repository_for_tests(foreground_repository),
    );
    let background = Arc::new(
        GatewayDataState::with_request_candidate_repository_for_tests(background_repository),
    );
    let mut state = AppState::new().expect("app state should build");

    state.replace_data_states(foreground, background, true);

    let selected = state.request_candidate_queue_data_state();
    assert!(Arc::ptr_eq(selected, &state.background_data));
    assert!(!Arc::ptr_eq(selected, &state.data));
    let selected_writer = selected
        .request_candidate_writer()
        .expect("isolated background writer should exist");
    let background_writer = state
        .background_data
        .request_candidate_writer()
        .expect("background writer should exist");
    let foreground_writer = state
        .data
        .request_candidate_writer()
        .expect("foreground writer should exist");
    assert!(Arc::ptr_eq(&selected_writer, &background_writer));
    assert!(!Arc::ptr_eq(&selected_writer, &foreground_writer));

    let shared_repository = Arc::new(InMemoryRequestCandidateRepository::default());
    state.replace_data_state(Arc::new(
        GatewayDataState::with_request_candidate_repository_for_tests(shared_repository),
    ));

    let selected = state.request_candidate_queue_data_state();
    assert!(Arc::ptr_eq(selected, &state.data));
    assert!(!state.background_data_isolated);
    let selected_writer = selected
        .request_candidate_writer()
        .expect("shared writer should exist");
    let foreground_writer = state
        .data
        .request_candidate_writer()
        .expect("shared foreground writer should exist");
    assert!(Arc::ptr_eq(&selected_writer, &foreground_writer));
}

#[test]
fn usage_lifecycle_writes_use_background_data_only_when_isolated() {
    let foreground = Arc::new(GatewayDataState::disabled());
    let background = Arc::new(GatewayDataState::disabled());
    let mut state = AppState::new().expect("app state should build");

    state.replace_data_states(foreground, background, true);

    let selected = state.usage_lifecycle_data_state();
    assert!(Arc::ptr_eq(selected, &state.background_data));
    assert!(!Arc::ptr_eq(selected, &state.data));

    state.replace_data_state(Arc::new(GatewayDataState::disabled()));

    let selected = state.usage_lifecycle_data_state();
    assert!(Arc::ptr_eq(selected, &state.data));
    assert!(!state.background_data_isolated);
}

#[test]
fn replacing_shared_data_state_preserves_background_usage_queue() {
    let mut state = AppState::new().expect("app state should build");

    state.replace_data_state(Arc::new(GatewayDataState::disabled()));

    assert!(state.data.has_usage_worker_queue());
    assert!(state.background_data.has_usage_worker_queue());
    assert!(state
        .background_worker_state()
        .data
        .has_usage_worker_queue());
}

#[test]
fn scheduler_affinity_epoch_blocks_stale_rewarm_after_invalidation() {
    let state = AppState::new().expect("app state should build");
    let cache_key = "scheduler_affinity:api-key-1:openai:chat:gpt-5";
    let ttl = std::time::Duration::from_secs(300);
    let first_target = crate::cache::SchedulerAffinityTarget {
        provider_id: "provider-old".to_string(),
        endpoint_id: "endpoint-old".to_string(),
        key_id: "key-old".to_string(),
    };
    let next_target = crate::cache::SchedulerAffinityTarget {
        provider_id: "provider-new".to_string(),
        endpoint_id: "endpoint-new".to_string(),
        key_id: "key-new".to_string(),
    };
    let initial_epoch = state.scheduler_affinity_epoch();

    assert!(state.remember_scheduler_affinity_target_for_epoch(
        cache_key,
        first_target.clone(),
        ttl,
        16,
        Some(initial_epoch),
    ));
    assert_eq!(
        state.read_scheduler_affinity_target(cache_key, ttl),
        Some(first_target)
    );

    let next_epoch = state.invalidate_scheduler_affinity_cache();

    assert!(!state.remember_scheduler_affinity_target_for_epoch(
        cache_key,
        next_target.clone(),
        ttl,
        16,
        Some(initial_epoch),
    ));
    assert_eq!(state.read_scheduler_affinity_target(cache_key, ttl), None);

    assert!(state.remember_scheduler_affinity_target_for_epoch(
        cache_key,
        next_target.clone(),
        ttl,
        16,
        Some(next_epoch),
    ));
    assert_eq!(
        state.read_scheduler_affinity_target(cache_key, ttl),
        Some(next_target)
    );
}
