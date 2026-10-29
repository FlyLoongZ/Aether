use super::*;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn redis_test_connection(url: &str) -> ::redis::aio::MultiplexedConnection {
    // Fixtures have explicit outer deadlines and may inspect large sorted sets.
    let config = ::redis::AsyncConnectionConfig::default().set_response_timeout(None);
    ::redis::Client::open(url)
        .expect("test Redis client")
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .expect("test Redis connection")
}

mod stream_receive {
    include!("redis/stream_receive_tests.rs");
}

mod dead_letter_transfer {
    include!("redis/dead_letter_transfer_tests.rs");
}

mod usage_limit_cleanup {
    include!("redis/usage_limit_cleanup_tests.rs");
}

#[tokio::test]
async fn memory_kv_expires_entries() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    runtime
        .kv_set("hello", "world", Some(Duration::from_millis(5)))
        .await
        .expect("set should succeed");
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(runtime.kv_get("hello").await.expect("get"), None);
    assert!(!runtime.kv_exists("hello").await.expect("exists"));
}

#[tokio::test]
async fn memory_kv_take_consumes_entry_once() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    runtime
        .kv_set("nonce", "payload", Some(Duration::from_secs(60)))
        .await
        .expect("set should succeed");
    assert_eq!(
        runtime.kv_take("nonce").await.expect("take").as_deref(),
        Some("payload")
    );
    assert_eq!(runtime.kv_take("nonce").await.expect("take"), None);
}

#[tokio::test]
async fn runtime_backends_share_atomic_kv_set_if_absent_contract() {
    let memory = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    assert_kv_set_if_absent_contract(&memory).await;

    let Some((_redis, redis_runtime)) = redis_runtime_for_test("kv-set-if-absent").await else {
        return;
    };
    assert_kv_set_if_absent_contract(&redis_runtime).await;
}

#[tokio::test]
async fn memory_rate_limit_rejects_after_limit() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let input = RateLimitInput {
        user_key: "rpm:user:1:1",
        key_key: "rpm:key:1:1",
        bucket: 1,
        user_limit: 1,
        key_limit: 0,
        ttl_seconds: 60,
    };
    assert!(matches!(
        runtime
            .check_and_consume_rate_limit(input)
            .await
            .expect("first"),
        RateLimitCheck::Allowed { .. }
    ));
    assert_eq!(
        runtime
            .rate_limit_count(input.user_key, input.bucket)
            .await
            .expect("count after first"),
        1
    );
    assert_eq!(
        runtime
            .check_and_consume_rate_limit(input)
            .await
            .expect("second"),
        RateLimitCheck::Rejected {
            scope: RateLimitScope::User,
            limit: 1
        }
    );
    assert_eq!(
        runtime
            .rate_limit_count(input.user_key, input.bucket)
            .await
            .expect("count after reject"),
        1
    );
}

#[tokio::test]
async fn memory_rate_limit_keeps_user_limit_atomic_across_api_keys() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let first_key = RateLimitInput {
        user_key: "rpm:user:shared:1",
        key_key: "rpm:key:first:1",
        bucket: 1,
        user_limit: 2,
        key_limit: 10,
        ttl_seconds: 60,
    };
    let second_key = RateLimitInput {
        key_key: "rpm:key:second:1",
        ..first_key
    };

    assert!(matches!(
        runtime
            .check_and_consume_rate_limit(first_key)
            .await
            .expect("first key"),
        RateLimitCheck::Allowed { .. }
    ));
    assert!(matches!(
        runtime
            .check_and_consume_rate_limit(second_key)
            .await
            .expect("second key"),
        RateLimitCheck::Allowed { .. }
    ));
    assert_eq!(
        runtime
            .check_and_consume_rate_limit(first_key)
            .await
            .expect("user limit"),
        RateLimitCheck::Rejected {
            scope: RateLimitScope::User,
            limit: 2,
        }
    );
    assert_eq!(
        runtime
            .rate_limit_count(first_key.user_key, first_key.bucket)
            .await
            .expect("user count"),
        2
    );
    assert_eq!(
        runtime
            .rate_limit_count(first_key.key_key, first_key.bucket)
            .await
            .expect("first key count"),
        1
    );
    assert_eq!(
        runtime
            .rate_limit_count(second_key.key_key, second_key.bucket)
            .await
            .expect("second key count"),
        1
    );
}

#[tokio::test]
async fn memory_usage_limits_check_all_windows_before_consuming() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let rules = [
        UsageLimitRule {
            key: "usage:{user-1}:qps",
            limit: 2,
            window_seconds: 10,
            retention_seconds: 10,
        },
        UsageLimitRule {
            key: "usage:{user-1}:weekly",
            limit: 1,
            window_seconds: 60,
            retention_seconds: 60,
        },
    ];

    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &rules,
                event_id: "request-1",
                now_unix_ms: 100_000,
            })
            .await
            .expect("first request"),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &rules,
                event_id: "request-2",
                now_unix_ms: 105_000,
            })
            .await
            .expect("second request"),
        UsageLimitCheck::Rejected {
            rule_index: 1,
            limit: 1,
            retry_after: 55,
        }
    );

    let qps_only = [rules[0]];
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &qps_only,
                event_id: "request-3",
                now_unix_ms: 105_000,
            })
            .await
            .expect("weekly rejection must not consume qps"),
        UsageLimitCheck::Allowed
    );
}

#[tokio::test]
async fn memory_usage_limits_are_true_sliding_windows_and_idempotent() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let rules = [UsageLimitRule {
        key: "usage:{user-1}:rolling-10",
        limit: 2,
        window_seconds: 10,
        retention_seconds: 10,
    }];
    let consume = |event_id, now_unix_ms| UsageLimitInput {
        rules: &rules,
        event_id,
        now_unix_ms,
    };

    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("request-1", 100_000))
            .await
            .unwrap(),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("request-2", 105_000))
            .await
            .unwrap(),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("request-2", 106_000))
            .await
            .unwrap(),
        UsageLimitCheck::Allowed,
        "replaying the same event must not consume twice"
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("request-3", 109_000))
            .await
            .unwrap(),
        UsageLimitCheck::Rejected {
            rule_index: 0,
            limit: 2,
            retry_after: 1,
        }
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("request-3", 110_000))
            .await
            .unwrap(),
        UsageLimitCheck::Allowed,
        "the event at the exact rolling cutoff must expire"
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("request-4", 111_000))
            .await
            .unwrap(),
        UsageLimitCheck::Rejected {
            rule_index: 0,
            limit: 2,
            retry_after: 4,
        },
        "the first event's expiry must not reset when later events arrive"
    );
}

#[tokio::test]
async fn usage_limit_input_requires_unique_co_located_rule_keys() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let different_tags = [
        UsageLimitRule {
            key: "usage:{user-1}:one",
            limit: 1,
            window_seconds: 1,
            retention_seconds: 1,
        },
        UsageLimitRule {
            key: "usage:{user-2}:two",
            limit: 1,
            window_seconds: 1,
            retention_seconds: 1,
        },
    ];
    assert!(matches!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &different_tags,
                event_id: "request-1",
                now_unix_ms: 100_000,
            })
            .await,
        Err(DataLayerError::InvalidInput(_))
    ));

    let zero_retention = [UsageLimitRule {
        retention_seconds: 0,
        ..different_tags[0]
    }];
    assert!(matches!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &zero_retention,
                event_id: "request-1",
                now_unix_ms: 100_000,
            })
            .await,
        Err(DataLayerError::InvalidInput(message))
            if message.contains("retention_seconds")
    ));

    let duplicate_keys = [different_tags[0], different_tags[0]];
    assert!(matches!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &duplicate_keys,
                event_id: "request-1",
                now_unix_ms: 100_000,
            })
            .await,
        Err(DataLayerError::InvalidInput(_))
    ));
}

#[tokio::test]
async fn memory_usage_limits_apply_the_current_window_when_configuration_changes() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let long_window = [UsageLimitRule {
        key: "usage:{user-1}:changing-window",
        limit: 1,
        window_seconds: 60,
        retention_seconds: 60,
    }];
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &long_window,
                event_id: "request-1",
                now_unix_ms: 100_000,
            })
            .await
            .unwrap(),
        UsageLimitCheck::Allowed
    );

    let short_window = [UsageLimitRule {
        window_seconds: 10,
        retention_seconds: 10,
        ..long_window[0]
    }];
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &short_window,
                event_id: "request-2",
                now_unix_ms: 111_000,
            })
            .await
            .unwrap(),
        UsageLimitCheck::Allowed,
        "the old event is outside the newly configured shorter window"
    );
}

#[tokio::test]
async fn memory_usage_limits_keep_subsecond_events_in_a_one_second_window() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let rules = [UsageLimitRule {
        key: "usage:{user-1}:qps",
        limit: 1,
        window_seconds: 1,
        retention_seconds: 1,
    }];
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &rules,
                event_id: "request-1",
                now_unix_ms: 1_900,
            })
            .await
            .unwrap(),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &rules,
                event_id: "request-2",
                now_unix_ms: 2_100,
            })
            .await
            .unwrap(),
        UsageLimitCheck::Rejected {
            rule_index: 0,
            limit: 1,
            retry_after: 1,
        }
    );
}

#[tokio::test]
async fn memory_usage_limits_do_not_expire_epoch_events_before_the_window_elapses() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let rules = [UsageLimitRule {
        key: "usage:{user-1}:epoch",
        limit: 1,
        window_seconds: 1,
        retention_seconds: 1,
    }];
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &rules,
                event_id: "request-1",
                now_unix_ms: 0,
            })
            .await
            .unwrap(),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules: &rules,
                event_id: "request-2",
                now_unix_ms: 500,
            })
            .await
            .unwrap(),
        UsageLimitCheck::Rejected {
            rule_index: 0,
            limit: 1,
            retry_after: 1,
        }
    );
}

#[test]
fn runtime_memory_usage_limit_capacity_must_be_positive() {
    let mut config = RuntimeStateConfig::memory();
    config.memory.max_usage_limit_windows = 0;
    assert!(matches!(
        config.validate(),
        Err(DataLayerError::InvalidConfiguration(message))
            if message.contains("max_usage_limit_windows")
    ));

    let mut config = RuntimeStateConfig::memory();
    config.memory.max_usage_limit_events = 0;
    assert!(matches!(
        config.validate(),
        Err(DataLayerError::InvalidConfiguration(message))
            if message.contains("max_usage_limit_events")
    ));
}

#[tokio::test]
async fn memory_rate_limit_concurrent_checks_do_not_exceed_limit() {
    let runtime = std::sync::Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let input = RateLimitInput {
        user_key: "rpm:user:concurrent:1",
        key_key: "rpm:key:concurrent:1",
        bucket: 1,
        user_limit: 32,
        key_limit: 64,
        ttl_seconds: 60,
    };
    let mut tasks = Vec::new();
    for _ in 0..128 {
        let runtime = std::sync::Arc::clone(&runtime);
        tasks.push(tokio::spawn(async move {
            runtime
                .check_and_consume_rate_limit(input)
                .await
                .expect("concurrent rate-limit check")
        }));
    }

    let mut allowed = 0;
    let mut rejected = 0;
    for task in tasks {
        match task.await.expect("rate-limit task") {
            RateLimitCheck::Allowed { .. } => allowed += 1,
            RateLimitCheck::Rejected {
                scope: RateLimitScope::User,
                limit: 32,
            } => rejected += 1,
            other => panic!("unexpected rate-limit result: {other:?}"),
        }
    }
    assert_eq!(allowed, 32);
    assert_eq!(rejected, 96);
}

#[tokio::test]
async fn memory_lock_fencing_tokens_increase_after_release() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let first = runtime
        .lock_try_acquire("fencing", "node-a", Duration::from_secs(1))
        .await
        .expect("first acquire")
        .expect("first lease");
    assert!(first.fencing_token > 0);
    assert!(runtime.lock_release(&first).await.expect("first release"));

    let second = runtime
        .lock_try_acquire("fencing", "node-b", Duration::from_secs(1))
        .await
        .expect("second acquire")
        .expect("second lease");
    assert!(second.fencing_token > first.fencing_token);
}

#[tokio::test]
async fn memory_expired_lock_cannot_be_renewed() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let expired = runtime
        .lock_try_acquire("expired-fencing", "node-a", Duration::from_millis(10))
        .await
        .expect("acquire")
        .expect("lease");
    tokio::time::sleep(Duration::from_millis(30)).await;

    assert!(!runtime
        .lock_renew(&expired, Duration::from_secs(1))
        .await
        .expect("expired renew should be rejected"));
    let replacement = runtime
        .lock_try_acquire("expired-fencing", "node-b", Duration::from_secs(1))
        .await
        .expect("replacement acquire")
        .expect("replacement lease");
    assert!(replacement.fencing_token > expired.fencing_token);
}

#[tokio::test]
async fn memory_semaphore_holds_until_permit_drop() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let gate = runtime
        .semaphore("test", 1, RuntimeSemaphoreConfig::default())
        .expect("gate should build");
    let permit = gate.try_acquire().await.expect("first permit");
    assert!(matches!(
        gate.try_acquire().await.expect_err("second rejected"),
        RuntimeSemaphoreError::Saturated { .. }
    ));
    drop(permit);
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(gate.snapshot().await.expect("snapshot").in_flight, 0);
}

#[tokio::test]
async fn keyed_memory_semaphores_share_only_the_same_subject_key() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let first = runtime
        .keyed_semaphore(
            "plan_usage_concurrency",
            "admission:plan_usage_concurrency:user-1",
            1,
            RuntimeSemaphoreConfig::default(),
        )
        .expect("first gate");
    let same_subject = runtime
        .keyed_semaphore(
            "plan_usage_concurrency",
            "admission:plan_usage_concurrency:user-1",
            1,
            RuntimeSemaphoreConfig::default(),
        )
        .expect("same subject gate");
    let other_subject = runtime
        .keyed_semaphore(
            "plan_usage_concurrency",
            "admission:plan_usage_concurrency:user-2",
            1,
            RuntimeSemaphoreConfig::default(),
        )
        .expect("other subject gate");

    let permit = first.try_acquire().await.expect("first permit");
    assert!(matches!(
        same_subject.try_acquire().await,
        Err(RuntimeSemaphoreError::Saturated { limit: 1, .. })
    ));
    assert!(other_subject.try_acquire().await.is_ok());
    drop(permit);
    for _ in 0..20 {
        if same_subject.snapshot().await.expect("snapshot").in_flight == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(same_subject.try_acquire().await.is_ok());
}

#[tokio::test]
async fn memory_keyed_semaphores_isolate_resource_capacity() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let first = runtime
        .keyed_semaphore(
            "provider_key",
            "key-a",
            1,
            RuntimeSemaphoreConfig::default(),
        )
        .expect("first gate should build");
    let second = runtime
        .keyed_semaphore(
            "provider_key",
            "key-b",
            1,
            RuntimeSemaphoreConfig::default(),
        )
        .expect("second gate should build");
    let permit = first.try_acquire().await.expect("first permit");

    assert!(matches!(
        first
            .try_acquire()
            .await
            .expect_err("same key should saturate"),
        RuntimeSemaphoreError::Saturated { .. }
    ));
    let second_permit = second
        .try_acquire()
        .await
        .expect("different key should retain independent capacity");

    drop(second_permit);
    drop(permit);
}

#[tokio::test(flavor = "current_thread")]
async fn memory_semaphore_marks_permit_unhealthy_after_lease_loss() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let gate = runtime
        .semaphore(
            "lease-health",
            1,
            RuntimeSemaphoreConfig {
                lease_ttl_ms: 20,
                renew_interval_ms: 5,
                command_timeout_ms: Some(50),
            },
        )
        .expect("gate should build");
    let permit = gate.try_acquire().await.expect("permit should acquire");
    assert!(aether_runtime::AdmissionPermitHealth::is_healthy(&permit));

    std::thread::sleep(Duration::from_millis(30));
    tokio::time::sleep(Duration::from_millis(10)).await;

    assert!(!aether_runtime::AdmissionPermitHealth::is_healthy(&permit));
}

#[tokio::test]
async fn redis_runtime_reuses_fixed_connections_for_repeated_operations() {
    let Some(redis) = TestRedisServer::start().await else {
        return;
    };
    let runtime = RuntimeState::redis(
        RedisClientConfig {
            url: redis.redis_url.clone(),
            key_prefix: Some(format!("aether-runtime-test-{}", std::process::id())),
        },
        Some(1_000),
    )
    .await
    .expect("runtime should connect");
    let before = runtime
        .redis_diagnostics()
        .await
        .expect("diagnostics")
        .expect("redis diagnostics")
        .total_connections_received
        .expect("total connections");

    for index in 0..200 {
        let key = format!("kv:{index}");
        runtime
            .kv_set(
                &key,
                format!("value-{index}"),
                Some(Duration::from_secs(30)),
            )
            .await
            .expect("set");
        assert_eq!(
            runtime.kv_get(&key).await.expect("get").as_deref(),
            Some(format!("value-{index}").as_str())
        );
    }

    let after = runtime
        .redis_diagnostics()
        .await
        .expect("diagnostics")
        .expect("redis diagnostics")
        .total_connections_received
        .expect("total connections");
    assert_eq!(
        after, before,
        "runtime Redis operations should reuse initialized lanes"
    );
}

#[tokio::test]
async fn redis_runtime_instances_share_ttl_kv_across_reinitialization() {
    let external_redis_url = std::env::var("AETHER_TEST_REDIS_URL")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let redis = if external_redis_url.is_none() {
        TestRedisServer::start().await
    } else {
        None
    };
    let Some(redis_url) =
        external_redis_url.or_else(|| redis.as_ref().map(|redis| redis.redis_url.clone()))
    else {
        return;
    };
    let key_prefix = format!("aether-history-test-{}", std::process::id());
    let runtime_config = || RedisClientConfig {
        url: redis_url.clone(),
        key_prefix: Some(key_prefix.clone()),
    };
    let writer = RuntimeState::redis(runtime_config(), Some(1_000))
        .await
        .expect("writer runtime should connect");
    let reader = RuntimeState::redis(runtime_config(), Some(1_000))
        .await
        .expect("reader runtime should connect");
    let history_key = "ai:responses:history:v1:shared-record";

    writer
        .kv_set(
            history_key,
            "persisted-history",
            Some(Duration::from_secs(30)),
        )
        .await
        .expect("writer should persist history");
    assert_eq!(
        reader
            .kv_get(history_key)
            .await
            .expect("reader get")
            .as_deref(),
        Some("persisted-history")
    );

    drop(writer);
    drop(reader);
    let restarted = RuntimeState::redis(runtime_config(), Some(1_000))
        .await
        .expect("restarted runtime should connect");
    assert_eq!(
        restarted
            .kv_get(history_key)
            .await
            .expect("restarted get")
            .as_deref(),
        Some("persisted-history")
    );
    assert!(matches!(
        restarted
            .kv_ttl_seconds(history_key)
            .await
            .expect("history ttl"),
        Some(1..=30)
    ));
}

#[tokio::test]
async fn redis_lock_fencing_tokens_increase_and_expired_lease_cannot_renew() {
    let Some((_redis, runtime)) = redis_runtime_for_test("lock-fencing").await else {
        return;
    };
    let first = runtime
        .lock_try_acquire("fencing", "node-a", Duration::from_millis(20))
        .await
        .expect("first acquire")
        .expect("first lease");
    tokio::time::sleep(Duration::from_millis(60)).await;
    assert!(!runtime
        .lock_renew(&first, Duration::from_secs(1))
        .await
        .expect("expired renew should be rejected"));

    let second = runtime
        .lock_try_acquire("fencing", "node-b", Duration::from_secs(1))
        .await
        .expect("second acquire")
        .expect("second lease");
    assert!(second.fencing_token > first.fencing_token);
    assert!(runtime.lock_release(&second).await.expect("second release"));

    let third = runtime
        .lock_try_acquire("fencing", "node-c", Duration::from_secs(1))
        .await
        .expect("third acquire")
        .expect("third lease");
    assert!(third.fencing_token > second.fencing_token);
}

#[tokio::test]
async fn redis_blocking_stream_read_does_not_block_fast_lane() {
    let Some(redis) = TestRedisServer::start().await else {
        return;
    };
    let runtime = RuntimeState::redis(
        RedisClientConfig {
            url: redis.redis_url.clone(),
            key_prefix: Some(format!("aether-block-test-{}", std::process::id())),
        },
        Some(1_000),
    )
    .await
    .expect("runtime should connect");
    RuntimeQueueStore::ensure_consumer_group(&runtime, "blocking-stream", "workers", "0-0")
        .await
        .expect("consumer group");

    let blocking_runtime = runtime.clone();
    let blocking = tokio::spawn(async move {
        RuntimeQueueStore::read_group(
            &blocking_runtime,
            "blocking-stream",
            "workers",
            "consumer-a",
            1,
            Some(500),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    runtime
        .kv_set("fast-lane", "ok", Some(Duration::from_secs(30)))
        .await
        .expect("fast lane set should complete while stream read blocks");
    assert_eq!(
        runtime
            .kv_get("fast-lane")
            .await
            .expect("fast lane get")
            .as_deref(),
        Some("ok")
    );
    let _ = blocking.await.expect("blocking task join");
}

#[tokio::test]
async fn redis_concurrent_blocking_stream_reads_do_not_share_single_connection() {
    let Some(redis) = TestRedisServer::start().await else {
        return;
    };
    let runtime = RuntimeState::redis(
        RedisClientConfig {
            url: redis.redis_url.clone(),
            key_prefix: Some(format!("aether-block-pool-test-{}", std::process::id())),
        },
        Some(1_000),
    )
    .await
    .expect("runtime should connect");
    RuntimeQueueStore::ensure_consumer_group(&runtime, "blocking-stream", "workers", "0-0")
        .await
        .expect("consumer group");

    let mut handles = Vec::new();
    for index in 0..4 {
        let blocking_runtime = runtime.clone();
        handles.push(tokio::spawn(async move {
            let consumer = format!("consumer-{index}");
            RuntimeQueueStore::read_group(
                &blocking_runtime,
                "blocking-stream",
                "workers",
                &consumer,
                1,
                Some(600),
            )
            .await
        }));
    }

    for handle in handles {
        let result = handle.await.expect("blocking task join");
        assert!(
            !matches!(result, Err(DataLayerError::TimedOut(_))),
            "concurrent blocking stream reads should not queue behind one connection"
        );
        assert!(result.expect("blocking read should succeed").is_empty());
    }
}

#[tokio::test]
async fn redis_large_stream_batches_preserve_fields_across_read_reclaim_and_ack() {
    let Some(redis) = TestRedisServer::start().await else {
        return;
    };
    for protocol in ["resp2", "resp3"] {
        let runtime = RuntimeState::redis(
            RedisClientConfig {
                url: format!("{}?protocol={protocol}", redis.redis_url),
                key_prefix: Some(format!("large-batch-{protocol}")),
            },
            Some(5_000),
        )
        .await
        .expect("large batch runtime should connect");
        let stream = "usage:large-batch";
        let group = "workers";
        RuntimeQueueStore::ensure_consumer_group(&runtime, stream, group, "0-0")
            .await
            .unwrap();
        let payload = format!(
            "{}\r\n\"escaped\"\\\u{4e2d}\u{6587}",
            "x".repeat(512 * 1024)
        );
        let mut expected = BTreeMap::new();
        for sequence in 0..24 {
            let fields = BTreeMap::from([
                ("payload".to_string(), payload.clone()),
                ("sequence".to_string(), sequence.to_string()),
                ("legacy_marker".to_string(), "preserve exactly".to_string()),
            ]);
            let id = RuntimeQueueStore::append_fields_with_maxlen(&runtime, stream, &fields, None)
                .await
                .unwrap();
            expected.insert(id, sequence.to_string());
        }
        let mut readers = tokio::task::JoinSet::new();
        for index in 0..3 {
            let runtime = runtime.clone();
            readers.spawn(async move {
                RuntimeQueueStore::read_group(
                    &runtime,
                    stream,
                    group,
                    &format!("reader-{index}"),
                    8,
                    Some(1),
                )
                .await
                .unwrap()
            });
        }
        let mut delivered = std::collections::BTreeSet::new();
        while let Some(entries) = readers.join_next().await {
            let entries = entries.unwrap();
            assert_eq!(entries.len(), 8);
            for entry in entries {
                assert_eq!(entry.fields.len(), 3);
                assert_eq!(entry.fields["payload"].as_bytes(), payload.as_bytes());
                assert_eq!(entry.fields["sequence"], expected[&entry.id]);
                assert_eq!(entry.fields["legacy_marker"], "preserve exactly");
                assert!(delivered.insert(entry.id));
            }
        }
        assert_eq!(delivered.len(), 24);
        let stats = RuntimeQueueStore::stats(&runtime, stream, Some(group))
            .await
            .unwrap();
        assert_eq!(stats.group_pending, 24);
        assert_eq!(stats.group_lag, Some(0));

        tokio::time::sleep(Duration::from_millis(20)).await;
        let mut reclaimed = std::collections::BTreeSet::new();
        while reclaimed.len() < 24 {
            let entries = RuntimeQueueStore::claim_stale(
                &runtime,
                stream,
                group,
                "retry-consumer",
                "0-0",
                RuntimeQueueReclaimConfig {
                    min_idle_ms: 1,
                    count: 5,
                },
            )
            .await
            .unwrap();
            assert!(!entries.is_empty());
            assert!(entries.len() <= 5);
            let mut ids = Vec::new();
            for entry in entries {
                assert_eq!(entry.fields.len(), 3);
                assert_eq!(entry.fields["payload"].as_bytes(), payload.as_bytes());
                assert_eq!(entry.fields["sequence"], expected[&entry.id]);
                assert_eq!(entry.fields["legacy_marker"], "preserve exactly");
                assert!(reclaimed.insert(entry.id.clone()));
                ids.push(entry.id);
            }
            assert_eq!(
                RuntimeQueueStore::ack(&runtime, stream, group, &ids)
                    .await
                    .unwrap(),
                ids.len()
            );
            assert_eq!(
                RuntimeQueueStore::delete(&runtime, stream, &ids)
                    .await
                    .unwrap(),
                ids.len()
            );
        }
        assert_eq!(reclaimed, delivered);
        let stats = RuntimeQueueStore::stats(&runtime, stream, Some(group))
            .await
            .unwrap();
        assert_eq!(stats.stream_length, 0);
        assert_eq!(stats.group_pending, 0);
        assert_eq!(stats.group_lag, Some(0));
        eprintln!("verified {protocol}: 24 large records, 3 readers, read/reclaim/ack complete");
    }
}

#[tokio::test]
async fn redis_connection_manager_recovers_after_restart() {
    let Some(mut redis) = TestRedisServer::start().await else {
        return;
    };
    let runtime = RuntimeState::redis(
        RedisClientConfig {
            url: redis.redis_url.clone(),
            key_prefix: Some(format!("aether-restart-test-{}", std::process::id())),
        },
        Some(500),
    )
    .await
    .expect("runtime should connect");
    runtime
        .kv_set("before-restart", "ok", Some(Duration::from_secs(30)))
        .await
        .expect("initial set");

    redis.stop();
    let _ = runtime
        .kv_set("during-restart", "may-fail", Some(Duration::from_secs(30)))
        .await;
    redis.restart().await.expect("redis restart");

    let mut recovered = false;
    for _ in 0..20 {
        if runtime
            .kv_set("after-restart", "ok", Some(Duration::from_secs(30)))
            .await
            .is_ok()
        {
            recovered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        recovered,
        "connection manager should reconnect after restart"
    );
}

#[tokio::test]
async fn runtime_backends_share_kv_score_and_queue_contracts() {
    let memory = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    assert_kv_score_and_queue_contract(&memory).await;

    let Some((_redis, redis_runtime)) = redis_runtime_for_test("shared-contract").await else {
        return;
    };
    assert_kv_score_and_queue_contract(&redis_runtime).await;
}

#[tokio::test]
async fn runtime_backends_share_bounded_score_window_aggregation() {
    let memory = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    assert_bounded_score_window_aggregation(&memory).await;

    let Some((_server, runtime)) = redis_runtime_for_test("score-window").await else {
        return;
    };
    assert_bounded_score_window_aggregation(&runtime).await;
}

async fn assert_bounded_score_window_aggregation(runtime: &RuntimeState) {
    let keys = (0..35)
        .map(|index| format!("window:{index}"))
        .collect::<Vec<_>>();
    for (member, score) in [
        ("expired:999", 99.999),
        ("boundary:7", 100.0),
        ("recent:9007199254740993", 101.0),
        ("nested:prefix:+00012", 102.0),
        ("zero:0", 103.0),
        ("invalid:1.5", 104.0),
        ("invalid:-1", 105.0),
        ("invalid:18446744073709551616", 106.0),
        ("invalid: 12", 107.0),
        ("missing-separator", 108.0),
    ] {
        runtime
            .score_set(&keys[0], member, score)
            .await
            .expect("seed values");
    }
    runtime
        .score_set(&keys[1], "max:18446744073709551615", 100.0)
        .await
        .expect("seed max");
    runtime
        .score_set(&keys[2], "max:18446744073709551615", 100.0)
        .await
        .expect("seed overflow");
    runtime
        .score_set(&keys[2], "additional:2", 100.0)
        .await
        .expect("seed overflow addition");
    for index in 0..SCORE_WINDOW_AGGREGATION_MEMBER_LIMIT {
        runtime
            .score_set(&keys[3], &format!("{index}:3"), 100.0)
            .await
            .expect("seed bounded window");
    }
    runtime
        .score_set(&keys[3], "expired:9999", 0.0)
        .await
        .expect("seed expired sample");
    let stats = runtime
        .score_window_u64_stats_by_min(&keys, 100.0)
        .await
        .expect("aggregate");
    assert_eq!(
        stats.len(),
        keys.len(),
        "pipeline batches preserve key order"
    );
    assert_eq!(
        stats[0],
        Some(ScoreWindowU64Stats {
            sum: 9_007_199_254_741_012,
            positive_count: 3
        })
    );
    assert_eq!(
        stats[1],
        Some(ScoreWindowU64Stats {
            sum: u64::MAX,
            positive_count: 1
        })
    );
    assert_eq!(
        stats[2],
        Some(ScoreWindowU64Stats {
            sum: u64::MAX,
            positive_count: 2
        })
    );
    assert_eq!(
        stats[3],
        Some(ScoreWindowU64Stats {
            sum: 1536,
            positive_count: 512
        })
    );
    assert!(stats[4..]
        .iter()
        .all(|stats| *stats == Some(ScoreWindowU64Stats::default())));

    runtime
        .score_set(&keys[3], "overflowing-window:11", 101.0)
        .await
        .expect("exceed server limit");
    let stats = runtime
        .score_window_u64_stats_by_min(&keys[3..4], 100.0)
        .await
        .expect("bounded fallback");
    assert_eq!(
        stats,
        vec![None],
        "oversized windows require the full exact read"
    );
    let members = runtime
        .score_range_by_min(&keys[3], 100.0)
        .await
        .expect("full window");
    assert_eq!(
        ScoreWindowU64Stats::from_members(members.iter().map(String::as_str)).sum,
        1547
    );
    runtime
        .score_remove(&keys[3], "overflowing-window:11")
        .await
        .expect("remove newest");
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys[3..4], 100.0)
            .await
            .expect("read after remove")[0]
            .unwrap()
            .sum,
        1536
    );
    runtime
        .score_set(&keys[3], "0:3", 99.0)
        .await
        .expect("move sample outside window");
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys[3..4], 100.0)
            .await
            .expect("read changed score")[0]
            .unwrap()
            .sum,
        1533
    );
    assert!(runtime
        .score_window_u64_stats_by_min(&[], 100.0)
        .await
        .expect("empty query")
        .is_empty());
    assert!(runtime
        .score_window_u64_stats_by_min(&keys, f64::NAN)
        .await
        .is_err());
}

#[tokio::test]
async fn redis_score_window_aggregation_reloads_scripts_without_caching_old_cost() {
    let Some((server, runtime)) = redis_runtime_for_test("score-window-reload").await else {
        return;
    };
    let keys = vec!["reload:cost".to_string()];
    runtime
        .score_set(&keys[0], "first:7", 100.0)
        .await
        .expect("first cost");
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys, 100.0)
            .await
            .expect("first aggregate")[0]
            .unwrap()
            .sum,
        7
    );
    let client = ::redis::Client::open(server.redis_url.as_str()).expect("test Redis client");
    let mut connection = client
        .get_multiplexed_async_connection()
        .await
        .expect("test connection");
    ::redis::cmd("SCRIPT")
        .arg("FLUSH")
        .query_async::<()>(&mut connection)
        .await
        .expect("flush scripts");
    runtime
        .score_set(&keys[0], "second:11", 101.0)
        .await
        .expect("new cost");
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys, 100.0)
            .await
            .expect("reload aggregate")[0]
            .unwrap()
            .sum,
        18
    );
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys, 101.0)
            .await
            .expect("changed window")[0]
            .unwrap()
            .sum,
        11
    );
    runtime
        .key_expire(&keys[0], Duration::ZERO)
        .await
        .expect("expire window");
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys, 100.0)
            .await
            .expect("expired aggregate"),
        vec![Some(ScoreWindowU64Stats::default())]
    );
}

#[tokio::test]
async fn redis_score_window_aggregation_observes_completed_concurrent_writes() {
    let Some((_server, runtime)) = redis_runtime_for_test("score-window-concurrent").await else {
        return;
    };
    let writer_runtime = runtime.clone();
    let (written_tx, mut written_rx) = tokio::sync::mpsc::channel(8);
    let writer = tokio::spawn(async move {
        for index in 1..=128_u64 {
            writer_runtime
                .score_set("concurrent:cost", &format!("{index}:2"), 100.0)
                .await
                .expect("concurrent write");
            written_tx.send(index).await.expect("notify reader");
        }
    });
    let keys = vec!["concurrent:cost".to_string()];
    while let Some(written) = written_rx.recv().await {
        let stats = runtime
            .score_window_u64_stats_by_min(&keys, 100.0)
            .await
            .expect("concurrent aggregate")[0]
            .unwrap();
        assert!(
            stats.positive_count >= written,
            "completed writes must not be hidden by a stale aggregate"
        );
        assert_eq!(
            stats.sum,
            stats.positive_count * 2,
            "one script observes one consistent window"
        );
    }
    writer.await.expect("writer task");
    assert_eq!(
        runtime
            .score_window_u64_stats_by_min(&keys, 100.0)
            .await
            .expect("final aggregate")[0]
            .unwrap()
            .sum,
        256
    );
}

#[tokio::test]
async fn runtime_backends_reject_invalid_shared_inputs() {
    let memory = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    assert_invalid_shared_inputs(&memory).await;

    let Some((_redis, redis_runtime)) = redis_runtime_for_test("invalid-contract").await else {
        return;
    };
    assert_invalid_shared_inputs(&redis_runtime).await;
}

#[tokio::test]
async fn runtime_backends_share_atomic_sliding_usage_limit_contract() {
    let memory = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    assert_sliding_usage_limit_contract(&memory).await;
    assert_concurrent_usage_limit_cap(&memory).await;

    let Some((_redis, redis_runtime)) = redis_runtime_for_test("usage-limits").await else {
        return;
    };
    assert_sliding_usage_limit_contract(&redis_runtime).await;
    assert_concurrent_usage_limit_cap(&redis_runtime).await;
}

#[tokio::test]
async fn runtime_backends_share_short_remaining_usage_limit_retention_contract() {
    let memory = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    let redis = redis_runtime_for_test("usage-limit-retention").await;
    let rules = [UsageLimitRule {
        key: "usage:{retention-user}:period-bucket",
        limit: 1,
        window_seconds: 60,
        retention_seconds: 1,
    }];

    assert_usage_limit_retention_seed(&memory, &rules).await;
    if let Some((_, redis_runtime)) = &redis {
        assert_usage_limit_retention_seed(redis_runtime, &rules).await;
    }

    // Redis deliberately adds one second of expiry grace to the requested retention.
    tokio::time::sleep(Duration::from_millis(2_200)).await;

    assert_usage_limit_retention_expired(&memory, &rules).await;
    if let Some((_, redis_runtime)) = &redis {
        assert_usage_limit_retention_expired(redis_runtime, &rules).await;
    }
}

#[tokio::test]
async fn memory_blocking_queue_read_does_not_block_kv_operations() {
    let runtime = RuntimeState::memory(MemoryRuntimeStateConfig::default());
    RuntimeQueueStore::ensure_consumer_group(&runtime, "memory-blocking", "workers", "0-0")
        .await
        .expect("consumer group");

    let blocking_runtime = runtime.clone();
    let blocking = tokio::spawn(async move {
        RuntimeQueueStore::read_group(
            &blocking_runtime,
            "memory-blocking",
            "workers",
            "consumer-a",
            1,
            Some(100),
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;

    runtime
        .kv_set("memory-fast-lane", "ok", Some(Duration::from_millis(100)))
        .await
        .expect("set should complete while memory stream read blocks");
    assert_eq!(
        runtime
            .kv_get("memory-fast-lane")
            .await
            .expect("get")
            .as_deref(),
        Some("ok")
    );
    assert!(blocking
        .await
        .expect("blocking task join")
        .expect("read should complete")
        .is_empty());
}

#[test]
fn redis_stream_timeout_expands_past_blocking_read() {
    assert_eq!(
        redis_stream_command_timeout_for_block(Some(1_000), Some(1_000)),
        Some(2_000)
    );
    assert_eq!(
        redis_stream_command_timeout_for_block(Some(5_000), Some(500)),
        Some(5_000)
    );
    assert_eq!(
        redis_stream_command_timeout_for_block(None, Some(500)),
        None
    );
}

async fn assert_kv_score_and_queue_contract(runtime: &RuntimeState) {
    runtime
        .kv_set("contract:ttl:set", "value", Some(Duration::from_millis(30)))
        .await
        .expect("set with ttl");
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        runtime.kv_get("contract:ttl:set").await.expect("ttl get"),
        None
    );

    runtime
        .kv_set("contract:ttl:expire", "value", None)
        .await
        .expect("set without ttl");
    assert!(runtime
        .key_expire("contract:ttl:expire", Duration::from_millis(30))
        .await
        .expect("expire existing key"));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        runtime
            .kv_get("contract:ttl:expire")
            .await
            .expect("expired get"),
        None
    );

    runtime
        .kv_set("contract:ttl:zero", "value", None)
        .await
        .expect("set zero ttl key");
    assert!(runtime
        .key_expire("contract:ttl:zero", Duration::ZERO)
        .await
        .expect("zero expire existing key"));
    assert_eq!(
        runtime
            .kv_get("contract:ttl:zero")
            .await
            .expect("zero expired get"),
        None
    );

    runtime
        .set_add("contract:set", "member")
        .await
        .expect("set add");
    assert!(runtime
        .key_expire("contract:set", Duration::from_millis(30))
        .await
        .expect("expire set"));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(runtime.set_len("contract:set").await.expect("set len"), 0);

    for (member, score) in [("a", 1.0), ("b", 2.0), ("c", 3.0), ("d", 4.0)] {
        runtime
            .score_set("contract:zset", member, score)
            .await
            .expect("score set");
    }
    assert_eq!(
        runtime
            .score_range_by_min("contract:zset", 0.0)
            .await
            .expect("score range"),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        runtime
            .score_remove_by_rank("contract:zset", 0, -3)
            .await
            .expect("rank trim"),
        2
    );
    assert_eq!(
        runtime
            .score_many(
                "contract:zset",
                &["a".to_string(), "b".to_string(), "c".to_string()]
            )
            .await
            .expect("score many"),
        vec![None, None, Some(3.0)]
    );
    assert!(runtime
        .key_expire("contract:zset", Duration::from_millis(30))
        .await
        .expect("expire zset"));
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        runtime
            .score_len("contract:zset")
            .await
            .expect("expired zset len"),
        0
    );

    RuntimeQueueStore::ensure_consumer_group(runtime, "contract:stream", "workers", "0-0")
        .await
        .expect("consumer group");
    let fields = BTreeMap::from([("payload".to_string(), "one".to_string())]);
    let first = RuntimeQueueStore::append_fields_with_maxlen(
        runtime,
        "contract:stream",
        &fields,
        Some(100),
    )
    .await
    .expect("append first");
    let fields = BTreeMap::from([("payload".to_string(), "two".to_string())]);
    let second = RuntimeQueueStore::append_fields_with_maxlen(
        runtime,
        "contract:stream",
        &fields,
        Some(100),
    )
    .await
    .expect("append second");
    let delivered = RuntimeQueueStore::read_group(
        runtime,
        "contract:stream",
        "workers",
        "consumer-a",
        10,
        None,
    )
    .await
    .expect("read group");
    assert_eq!(
        delivered
            .iter()
            .map(|entry| entry.id.clone())
            .collect::<Vec<_>>(),
        vec![first.clone(), second.clone()]
    );
    let stats = RuntimeQueueStore::stats(runtime, "contract:stream", Some("workers"))
        .await
        .expect("queue stats");
    assert_eq!(stats.stream_length, 2);
    assert_eq!(stats.group_pending, 2);
    assert_eq!(stats.group_lag, Some(0));
    assert!(RuntimeQueueStore::read_group(
        runtime,
        "contract:stream",
        "workers",
        "consumer-a",
        10,
        None
    )
    .await
    .expect("second read")
    .is_empty());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let claimed = RuntimeQueueStore::claim_stale(
        runtime,
        "contract:stream",
        "workers",
        "consumer-b",
        "0-0",
        RuntimeQueueReclaimConfig {
            min_idle_ms: 1,
            count: 10,
        },
    )
    .await
    .expect("claim stale");
    let ids = claimed
        .iter()
        .map(|entry| entry.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![first.clone(), second.clone()]);
    let stats = RuntimeQueueStore::stats(runtime, "contract:stream", Some("workers"))
        .await
        .expect("queue stats after claim");
    assert_eq!(stats.group_pending, 2);
    assert!(stats.oldest_pending_idle_ms.unwrap_or_default() <= 5000);
    assert_eq!(
        RuntimeQueueStore::ack(runtime, "contract:stream", "workers", &ids)
            .await
            .expect("ack"),
        2
    );
    assert_eq!(
        RuntimeQueueStore::delete(runtime, "contract:stream", &ids)
            .await
            .expect("delete"),
        2
    );
    let stats = RuntimeQueueStore::stats(runtime, "contract:stream", Some("workers"))
        .await
        .expect("queue stats after delete");
    assert_eq!(stats.stream_length, 0);
    assert_eq!(stats.group_pending, 0);
    assert_eq!(stats.group_lag, Some(0));
    assert!(RuntimeQueueStore::read_group(
        runtime,
        "contract:stream",
        "workers",
        "consumer-b",
        10,
        None
    )
    .await
    .expect("read after delete")
    .is_empty());
}

async fn assert_invalid_shared_inputs(runtime: &RuntimeState) {
    assert!(matches!(
        runtime
            .kv_set_if_absent("contract:invalid-ttl", "value", Duration::ZERO)
            .await,
        Err(DataLayerError::InvalidInput(_))
    ));
    assert!(matches!(
        runtime
            .score_set("contract:invalid-score", "nan", f64::NAN)
            .await,
        Err(DataLayerError::InvalidInput(_))
    ));
    assert!(matches!(
        RuntimeQueueStore::read_group(runtime, "", "workers", "consumer-a", 1, None).await,
        Err(DataLayerError::InvalidInput(_))
    ));
    assert!(matches!(
        RuntimeQueueStore::claim_stale(
            runtime,
            "contract:stream",
            "workers",
            "consumer-a",
            "0-0",
            RuntimeQueueReclaimConfig {
                min_idle_ms: 0,
                count: 1,
            },
        )
        .await,
        Err(DataLayerError::InvalidInput(_))
    ));
}

async fn assert_kv_set_if_absent_contract(runtime: &RuntimeState) {
    let key = "contract:kv:set-if-absent";
    assert!(runtime
        .kv_set_if_absent(key, "first", Duration::from_millis(30))
        .await
        .expect("first set-if-absent should succeed"));
    assert!(!runtime
        .kv_set_if_absent(key, "second", Duration::from_secs(30))
        .await
        .expect("duplicate set-if-absent should be rejected"));
    assert_eq!(
        runtime
            .kv_get(key)
            .await
            .expect("existing value should be readable")
            .as_deref(),
        Some("first"),
        "a rejected set-if-absent must not replace the existing value"
    );

    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(runtime
        .kv_set_if_absent(key, "after-expiry", Duration::from_secs(30))
        .await
        .expect("expired key should be reusable"));
    assert_eq!(
        runtime
            .kv_get(key)
            .await
            .expect("replacement value should be readable")
            .as_deref(),
        Some("after-expiry")
    );

    let concurrent_key = "contract:kv:set-if-absent:concurrent";
    let mut tasks = Vec::new();
    for index in 0..64 {
        let runtime = runtime.clone();
        tasks.push(tokio::spawn(async move {
            runtime
                .kv_set_if_absent(
                    concurrent_key,
                    format!("candidate-{index}"),
                    Duration::from_secs(30),
                )
                .await
                .expect("concurrent set-if-absent should complete")
        }));
    }
    let mut created = 0;
    for task in tasks {
        if task.await.expect("set-if-absent task should join") {
            created += 1;
        }
    }
    assert_eq!(
        created, 1,
        "exactly one concurrent caller may create the key"
    );
    assert!(runtime
        .kv_get(concurrent_key)
        .await
        .expect("winning value should be readable")
        .is_some());
}

async fn assert_sliding_usage_limit_contract(runtime: &RuntimeState) {
    let rules = [
        UsageLimitRule {
            key: "usage:{shared-user}:short",
            limit: 2,
            window_seconds: 10,
            retention_seconds: 10,
        },
        UsageLimitRule {
            key: "usage:{shared-user}:long",
            limit: 1,
            window_seconds: 60,
            retention_seconds: 60,
        },
    ];
    let consume = |event_id, now_unix_ms, rules| UsageLimitInput {
        rules,
        event_id,
        now_unix_ms,
    };

    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("event-1", 100_000, &rules))
            .await
            .expect("first event"),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("event-1", 101_000, &rules))
            .await
            .expect("idempotent replay"),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("event-2", 105_000, &rules))
            .await
            .expect("long window rejection"),
        UsageLimitCheck::Rejected {
            rule_index: 1,
            limit: 1,
            retry_after: 55,
        }
    );

    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("event-3", 105_000, &rules[..1]))
            .await
            .expect("rejection must leave short rule untouched"),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("event-4", 109_000, &rules[..1]))
            .await
            .expect("short rule rejection"),
        UsageLimitCheck::Rejected {
            rule_index: 0,
            limit: 2,
            retry_after: 1,
        }
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("event-4", 110_000, &rules[..1]))
            .await
            .expect("event at cutoff expires"),
        UsageLimitCheck::Allowed
    );

    let qps = [UsageLimitRule {
        key: "usage:{shared-user}:qps",
        limit: 1,
        window_seconds: 1,
        retention_seconds: 1,
    }];
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("qps-1", 200_900, &qps))
            .await
            .expect("first qps event"),
        UsageLimitCheck::Allowed
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("qps-2", 201_100, &qps))
            .await
            .expect("subsecond qps rejection"),
        UsageLimitCheck::Rejected {
            rule_index: 0,
            limit: 1,
            retry_after: 1,
        }
    );
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("qps-2", 201_900, &qps))
            .await
            .expect("qps event at cutoff"),
        UsageLimitCheck::Allowed
    );

    runtime
        .release_usage_limits(UsageLimitReleaseInput {
            rules: &qps,
            event_id: "qps-2",
        })
        .await
        .expect("release consumed qps event");
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(consume("qps-3", 201_900, &qps))
            .await
            .expect("released qps capacity should be reusable"),
        UsageLimitCheck::Allowed
    );
    runtime
        .release_usage_limits(UsageLimitReleaseInput {
            rules: &qps,
            event_id: "qps-2",
        })
        .await
        .expect("release should be idempotent");
}

async fn assert_usage_limit_retention_seed(runtime: &RuntimeState, rules: &[UsageLimitRule<'_>]) {
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules,
                event_id: "period-event-1",
                now_unix_ms: 100_000,
            })
            .await
            .expect("seed short-retention period bucket"),
        UsageLimitCheck::Allowed
    );
}

async fn assert_usage_limit_retention_expired(
    runtime: &RuntimeState,
    rules: &[UsageLimitRule<'_>],
) {
    assert_eq!(
        runtime
            .check_and_consume_usage_limits(UsageLimitInput {
                rules,
                event_id: "period-event-2",
                now_unix_ms: 102_200,
            })
            .await
            .expect("expired period bucket must be reusable"),
        UsageLimitCheck::Allowed,
        "retention must expire the bucket before its full counting window"
    );
}

async fn assert_concurrent_usage_limit_cap(runtime: &RuntimeState) {
    let mut tasks = Vec::new();
    for index in 0..64 {
        let runtime = runtime.clone();
        tasks.push(tokio::spawn(async move {
            let event_id = format!("parallel-event-{index}");
            let rules = [UsageLimitRule {
                key: "usage:{shared-user}:parallel",
                limit: 8,
                window_seconds: 60,
                retention_seconds: 60,
            }];
            runtime
                .check_and_consume_usage_limits(UsageLimitInput {
                    rules: &rules,
                    event_id: &event_id,
                    now_unix_ms: 300_000,
                })
                .await
                .expect("concurrent usage limit check")
        }));
    }

    let mut allowed = 0;
    let mut rejected = 0;
    for task in tasks {
        match task.await.expect("concurrent usage limit task") {
            UsageLimitCheck::Allowed => allowed += 1,
            UsageLimitCheck::Rejected {
                rule_index: 0,
                limit: 8,
                retry_after: 60,
            } => rejected += 1,
            unexpected => panic!("unexpected usage limit result: {unexpected:?}"),
        }
    }
    assert_eq!(allowed, 8);
    assert_eq!(rejected, 56);
}

async fn redis_runtime_for_test(prefix: &str) -> Option<(TestRedisServer, RuntimeState)> {
    let redis = TestRedisServer::start().await?;
    let runtime = RuntimeState::redis(
        RedisClientConfig {
            url: redis.redis_url.clone(),
            key_prefix: Some(format!(
                "aether-runtime-test-{prefix}-{}",
                std::process::id()
            )),
        },
        Some(1_000),
    )
    .await
    .ok()?;
    Some((redis, runtime))
}

struct TestRedisServer {
    child: Option<Child>,
    binary: String,
    port: u16,
    workdir: PathBuf,
    redis_url: String,
}

impl TestRedisServer {
    async fn start() -> Option<Self> {
        let port = reserve_local_port().ok()?;
        let workdir = std::env::temp_dir().join(format!(
            "aether-runtime-state-redis-{}-{port}",
            std::process::id()
        ));
        std::fs::create_dir_all(&workdir).ok()?;
        let binary = std::env::var("AETHER_REDIS_SERVER_BIN")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "redis-server".to_string());
        let mut server = Self {
            child: None,
            binary,
            port,
            workdir,
            redis_url: format!("redis://127.0.0.1:{port}/0"),
        };
        server.restart().await.ok()?;
        Some(server)
    }

    fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    async fn restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.stop();
        let child = Command::new(&self.binary)
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--port")
            .arg(self.port.to_string())
            .arg("--dir")
            .arg(&self.workdir)
            .arg("--bind")
            .arg("127.0.0.1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        self.child = Some(child);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            if redis_ping(self.port).await.unwrap_or(false) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.stop();
        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out waiting for test redis-server",
        )
        .into())
    }
}

impl Drop for TestRedisServer {
    fn drop(&mut self) {
        self.stop();
        let _ = std::fs::remove_dir_all(&self.workdir);
    }
}

async fn redis_ping(port: u16) -> Result<bool, std::io::Error> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await?;
    stream.write_all(b"*1\r\n$4\r\nPING\r\n").await?;
    let mut buffer = [0_u8; 16];
    let len = stream.read(&mut buffer).await?;
    Ok(buffer[..len].starts_with(b"+PONG"))
}

fn reserve_local_port() -> Result<u16, std::io::Error> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}
