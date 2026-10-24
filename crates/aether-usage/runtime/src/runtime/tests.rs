mod shutdown {
    include!("../runtime_shutdown_tests.rs");
}

mod queue_payload {
    include!("../runtime_queue_payload_tests.rs");
}

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use aether_contracts::{ExecutionPlan, ExecutionTelemetry, RequestBody};
use aether_data_contracts::repository::settlement::{StoredUsageSettlement, UsageSettlementInput};
use aether_data_contracts::repository::usage::{
    StoredRequestUsageAudit, UpsertUsageRecord, UsageBodyCaptureState,
};
use aether_data_contracts::DataLayerError;
use aether_runtime_state::{MemoryRuntimeStateConfig, RuntimeQueueStore, RuntimeState};
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

use super::{
    preserve_provider_response_facts, preserve_request_facts, LifecycleAdmissionPermit,
    LifecycleEventCoalescer, LifecycleSubmissionDispatcher, LifecycleSubmissionItem,
    LifecycleSubmissionPriority, LifecycleTerminalUsageSeed, UsageBillingEventEnricher,
    UsageBodyCapturePolicy, UsageEnqueueRetryDispatcher, UsageRequestRecordLevel,
    UsageRuntimeAccess, UsageWorkerObservation, UsageWorkerSupervisorState,
};
use crate::worker::ManualProxyNodeCounter;
use crate::{
    apply_usage_body_capture_policy_to_event, build_lifecycle_usage_seed,
    build_terminal_usage_context_seed, SyncTerminalUsagePayloadSeed, TerminalUsageContextSeed,
    UsageEvent, UsageEventData, UsageEventType, UsageQueue, UsageRecordWriter, UsageRuntime,
    UsageRuntimeConfig, UsageSettlementWriter,
};

fn terminal_test_plan(request_id: &str) -> ExecutionPlan {
    ExecutionPlan {
        request_id: request_id.to_string(),
        candidate_id: Some(format!("candidate-{request_id}")),
        provider_name: Some("openai".to_string()),
        provider_id: "provider-terminal-test".to_string(),
        endpoint_id: "endpoint-terminal-test".to_string(),
        key_id: "key-terminal-test".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

fn sync_terminal_test_seeds(
    request_id: &str,
) -> (TerminalUsageContextSeed, SyncTerminalUsagePayloadSeed) {
    let plan = terminal_test_plan(request_id);
    (
        build_terminal_usage_context_seed(&plan, None),
        SyncTerminalUsagePayloadSeed {
            report_kind: "sync_completed".to_string(),
            status_code: 200,
            response_time_ms: Some(12),
            first_byte_time_ms: None,
            provider_response_headers: None,
            client_response_headers: None,
            provider_response_full: Some(json!({"id": request_id})),
            provider_response_body_state: None,
            client_response: Some(json!({"id": request_id})),
            client_response_body_state: None,
            standardized_usage: None,
            capture_metadata: None,
        },
    )
}

fn terminal_seed_capture_test_seeds(
    request_id: &str,
    padding_bytes: usize,
) -> (TerminalUsageContextSeed, SyncTerminalUsagePayloadSeed) {
    let (_, mut payload) = sync_terminal_test_seeds(request_id);
    let provider_request = json!({
        "model": "gpt-5.6-sol", "reasoning": {"effort": "medium"},
        "service_tier": "priority", "prompt": "retained request facts"
    });
    let mut plan = terminal_test_plan(request_id);
    plan.model_name = Some("gpt-5.6-sol".to_string());
    plan.body = RequestBody::from_json(provider_request.clone());
    let report_context = json!({
        "original_request_body": {"reasoning": {"effort": "high"}},
        "provider_request_body": provider_request,
    });
    let context = build_terminal_usage_context_seed(&plan, Some(&report_context));
    payload.provider_response_full = Some(json!({
        "id": request_id,
        "output": "x".repeat(padding_bytes),
        "service_tier": "default",
        "usage": {"input_tokens": 100, "output_tokens": 500, "total_tokens": 600}
    }));
    (context, payload)
}

#[tokio::test]
async fn terminal_seed_capture_budget_zero_preserves_sync_facts_and_explicit_zero() {
    for tokens in [0, 100] {
        let (context, mut payload) = terminal_seed_capture_test_seeds("seed-budget-sync", 4096);
        payload.provider_response_full.as_mut().unwrap()["usage"] = json!({
            "input_tokens": tokens, "output_tokens": tokens, "total_tokens": 2 * tokens
        });
        let mut expected = crate::build_terminal_usage_event_from_seed(
            crate::build_sync_terminal_usage_seed(context.clone(), payload.clone()),
        )
        .expect("original terminal event");
        let budget = Arc::new(super::EventCaptureMemoryBudget::new(0));
        let prepared = LifecycleTerminalUsageSeed::Sync {
            context,
            payload,
            capture: None,
        }
        .prepare_capture_memory(Arc::clone(&budget));
        assert!(matches!(
            prepared,
            LifecycleTerminalUsageSeed::Prepared { .. }
        ));
        let event = prepared
            .build("seed-budget-sync")
            .await
            .expect("prepared event");
        super::prepare_event_capture_memory(&mut expected, Arc::clone(&budget));
        assert_eq!(event.event_type, expected.event_type);
        assert_eq!(event.data, expected.data);
        assert_eq!(event.data.input_tokens, Some(tokens));
        assert_eq!(event.data.output_tokens, Some(tokens));
        assert_eq!(event.data.total_tokens, Some(2 * tokens));
        assert!(event.data.request_body.is_none());
        assert!(event.data.provider_request_body.is_none());
        assert!(event.data.response_body.is_none());
        assert!(event.data.client_response_body.is_none());
        assert_eq!(
            event.data.response_body_state,
            Some(UsageBodyCaptureState::Truncated)
        );
        let metadata = event.data.request_metadata.as_ref().expect("billing facts");
        assert_eq!(metadata["requested_reasoning_effort"], "high");
        assert_eq!(metadata["provider_reasoning_effort"], "medium");
        assert_eq!(metadata["provider_service_tier"], "priority");
        assert_eq!(metadata["provider_actual_service_tier"], "default");
        assert_eq!(metadata["provider_cache_ttl_minutes"], 30);
        assert_eq!(budget.retained_bytes(), 0);
    }
}

#[tokio::test]
async fn terminal_seed_capture_budget_zero_preserves_image_estimates_and_dimensions() {
    let (mut context, mut payload) = terminal_seed_capture_test_seeds("seed-budget-image", 0);
    context.client_contract = "openai:image".to_string();
    context.provider_contract = "openai:image".to_string();
    context.request_type = "image".to_string();
    context.provider_request = Some(json!({
        "model": "gpt-image-2", "prompt": "Draw a red kite", "n": 1,
        "size": "1024x1024", "quality": "high", "output_format": "png"
    }));
    payload.provider_response_full = Some(json!({
        "data": [{"b64_json": "x".repeat(4096)}, {"b64_json": "y".repeat(4096)}]
    }));
    let expected = crate::build_terminal_usage_event_from_seed(
        crate::build_sync_terminal_usage_seed(context.clone(), payload.clone()),
    )
    .expect("original image event");
    let budget = Arc::new(super::EventCaptureMemoryBudget::new(0));
    let event = LifecycleTerminalUsageSeed::Sync {
        context,
        payload,
        capture: None,
    }
    .prepare_capture_memory(Arc::clone(&budget))
    .build("seed-budget-image")
    .await
    .expect("image event");
    assert_eq!(event.event_type, UsageEventType::Completed);
    assert_eq!(event.data.input_tokens, expected.data.input_tokens);
    assert!(event.data.input_tokens.unwrap_or_default() > 0);
    assert_eq!(event.data.total_tokens, expected.data.total_tokens);
    let metadata = event
        .data
        .request_metadata
        .as_ref()
        .expect("image dimensions");
    assert_eq!(metadata["dimensions"]["image_count"], 2);
    assert_eq!(metadata["dimensions"]["image_size"], "1024x1024");
    assert_eq!(metadata["dimensions"]["image_quality"], "high");
    assert_eq!(metadata["dimensions"]["image_output_format"], "png");
    assert!(event.data.response_body.is_none());
    assert!(event.data.provider_request_body.is_none());
    assert_eq!(budget.retained_bytes(), 0);
}

#[tokio::test]
async fn terminal_seed_capture_budget_zero_preserves_stream_terminal_evidence() {
    for (failed, cancelled) in [(false, false), (true, false), (false, true)] {
        let (mut context, _) = terminal_seed_capture_test_seeds("seed-budget-stream", 0);
        context.is_stream = true;
        let provider_response_full = Some(json!({
            "chunks": [{
                "type": if failed { "response.failed" } else { "response.completed" },
                "response": {
                    "status": if failed { "failed" } else { "completed" },
                    "service_tier": "default",
                    "usage": {"input_tokens": 100, "output_tokens": 500, "total_tokens": 600},
                    "error": if failed { json!({"message": "provider refused"}) } else { json!(null) }
                }
            }]
        }));
        let payload = crate::StreamTerminalUsagePayloadSeed {
            report_kind: "openai_responses_stream_success".to_string(),
            status_code: if cancelled { 499 } else { 200 },
            response_time_ms: Some(12),
            first_byte_time_ms: Some(3),
            provider_response_headers: None,
            client_response_headers: None,
            provider_response_full,
            provider_response_body_state: Some(UsageBodyCaptureState::Inline),
            client_response: None,
            client_response_body_state: Some(UsageBodyCaptureState::None),
            standardized_usage: None,
            provider_actual_service_tier: Some("default".to_string()),
            observed_stream_finish: Some(true),
            terminal_error_message: None,
            capture_metadata: None,
        };
        let mut expected = crate::build_terminal_usage_event_from_seed(
            crate::build_stream_terminal_usage_seed(context.clone(), payload.clone(), cancelled),
        )
        .expect("original stream event");
        let budget = Arc::new(super::EventCaptureMemoryBudget::new(0));
        let event = LifecycleTerminalUsageSeed::Stream {
            context,
            payload,
            cancelled,
            capture: None,
        }
        .prepare_capture_memory(Arc::clone(&budget))
        .build("seed-budget-stream")
        .await
        .expect("stream event");
        super::prepare_event_capture_memory(&mut expected, Arc::clone(&budget));
        assert_eq!(event.event_type, expected.event_type);
        assert_eq!(event.data, expected.data);
        assert_eq!(event.data.input_tokens, Some(100));
        assert_eq!(event.data.output_tokens, Some(500));
        assert_eq!(event.data.first_byte_time_ms, Some(3));
        assert_eq!(
            event.event_type,
            if cancelled {
                UsageEventType::Cancelled
            } else if failed {
                UsageEventType::Failed
            } else {
                UsageEventType::Completed
            }
        );
        if failed {
            assert_eq!(
                event.data.error_message.as_deref(),
                Some("provider refused")
            );
        }
        assert!(event.data.response_body.is_none());
        assert_eq!(budget.retained_bytes(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_seed_capture_budget_bounds_concurrent_seeds_and_transfers_to_events() {
    const COUNT: usize = 12;
    const LIMIT: usize = 96 * 1024;
    let budget = Arc::new(super::EventCaptureMemoryBudget::new(LIMIT));
    let barrier = Arc::new(tokio::sync::Barrier::new(COUNT));
    let mut tasks = Vec::new();
    for index in 0..COUNT {
        let (context, payload) =
            terminal_seed_capture_test_seeds(&format!("seed-budget-{index}"), 32 * 1024);
        let budget = Arc::clone(&budget);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            LifecycleTerminalUsageSeed::Sync {
                context,
                payload,
                capture: None,
            }
            .prepare_capture_memory(budget)
        }));
    }
    let mut seeds = Vec::new();
    for task in tasks {
        seeds.push(task.await.expect("concurrent seed preparation"));
    }
    let retained = budget.retained_bytes();
    assert!(retained > 0 && retained <= LIMIT);
    assert!(seeds
        .iter()
        .any(|seed| matches!(seed, LifecycleTerminalUsageSeed::Prepared { .. })));
    let mut events = Vec::new();
    for seed in seeds {
        let event = seed.build("seed-budget").await.expect("terminal event");
        assert_eq!(event.data.input_tokens, Some(100));
        assert_eq!(event.data.output_tokens, Some(500));
        events.push(event);
    }
    assert_eq!(
        budget.retained_bytes(),
        retained,
        "ownership transfer must not drop or duplicate reservations"
    );
    drop(events);
    assert_eq!(budget.retained_bytes(), 0);

    let (context, payload) = terminal_seed_capture_test_seeds("seed-budget-dropped", 4096);
    let queued = LifecycleTerminalUsageSeed::Sync {
        context,
        payload,
        capture: None,
    }
    .prepare_capture_memory(Arc::clone(&budget));
    assert!(budget.retained_bytes() > 0);
    drop(queued);
    assert_eq!(
        budget.retained_bytes(),
        0,
        "dropping an unbuilt seed releases its bodies"
    );
}

#[tokio::test]
async fn terminal_seed_capture_budget_cancellation_keeps_running_blocking_build_reserved() {
    let budget = Arc::new(super::EventCaptureMemoryBudget::new(64 * 1024));
    let body = json!({"diagnostic": "x".repeat(4096)});
    let capture = super::TerminalSeedCaptureRetention::try_reserve(
        [Some(&body), None, None, None],
        Arc::clone(&budget),
    );
    let retained = budget.retained_bytes();
    assert!(retained > 0);
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = std::sync::mpsc::channel();
    let building = tokio::spawn(super::build_terminal_usage_event_offthread(
        capture,
        move || {
            let _ = started.send(());
            release_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .expect("release blocking builder");
            Ok(UsageEvent::new(
                UsageEventType::Completed,
                "seed-budget-cancel",
                UsageEventData {
                    response_body: Some(body),
                    ..UsageEventData::default()
                },
            ))
        },
    ));
    timeout(Duration::from_secs(1), started_rx)
        .await
        .expect("builder started")
        .expect("start signal");
    building.abort();
    assert!(building
        .await
        .expect_err("cancelled wrapper")
        .is_cancelled());
    assert_eq!(
        budget.retained_bytes(),
        retained,
        "spawn_blocking survives cancellation of the caller"
    );
    release.send(()).expect("release owned builder");
    timeout(Duration::from_secs(2), async {
        while budget.retained_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached blocking result drops its body and reservation");

    let body = json!({"diagnostic": "x".repeat(4096)});
    let capture = super::TerminalSeedCaptureRetention::try_reserve(
        [Some(&body), None, None, None],
        Arc::clone(&budget),
    );
    let result = super::build_terminal_usage_event_offthread(capture, move || {
        drop(body);
        panic!("forced terminal seed builder panic")
    })
    .await;
    assert!(result.is_err());
    assert_eq!(budget.retained_bytes(), 0);
}

#[tokio::test]
async fn terminal_seed_capture_budget_bounds_admission_backlog_and_preserves_failed_build_progress()
{
    const COUNT: usize = 12;
    for limit in [0, 96 * 1024] {
        let config = UsageRuntimeConfig {
            enabled: true,
            terminal_submission_max_in_flight: 1,
            ..UsageRuntimeConfig::default()
        };
        let runtime = UsageRuntime::new(config).expect("usage runtime");
        let store = CloneQueueConfiguredUsageStore {
            records: Arc::new(Mutex::new(Vec::new())),
            queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
        };
        let budget = Arc::new(super::EventCaptureMemoryBudget::new(limit));
        let held = runtime
            .terminal_submission_state
            .acquire()
            .await
            .expect("hold terminal admission");
        runtime
            .dispatch_terminal_seed(
                &store,
                "seed-budget-build-error".to_string(),
                LifecycleTerminalUsageSeed::Prepared {
                    kind: super::TerminalSeedKind::Sync,
                    result: Err(DataLayerError::UnexpectedValue(
                        "forced prepared builder failure".to_string(),
                    )),
                },
            )
            .await;
        for index in 0..COUNT {
            let request_id = format!("seed-budget-backlog-{index}");
            let (context, payload) = terminal_seed_capture_test_seeds(&request_id, 32 * 1024);
            let seed = LifecycleTerminalUsageSeed::Sync {
                context,
                payload,
                capture: None,
            }
            .prepare_capture_memory(Arc::clone(&budget));
            runtime
                .dispatch_terminal_seed(&store, request_id, seed)
                .await;
        }
        timeout(Duration::from_secs(2), async {
            while runtime.metrics_snapshot().terminal_submission_pending < COUNT + 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all seeds wait behind terminal admission");
        assert!(budget.retained_bytes() <= limit);
        assert_eq!(budget.retained_bytes() > 0, limit > 0);
        assert!(budget.downgraded_total() > 0);
        assert!(
            store.records.lock().unwrap().is_empty(),
            "no persistence before admission"
        );
        drop(held);
        timeout(Duration::from_secs(5), async {
            loop {
                let snapshot = runtime.metrics_snapshot();
                if store.records.lock().unwrap().len() == COUNT
                    && snapshot.terminal_submission_pending == 0
                    && snapshot.lifecycle_submission_pending == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed build releases its permit and every later terminal persists");
        let records = store.records.lock().unwrap();
        for record in records.iter() {
            assert_eq!(record.status, "completed");
            assert_eq!(record.input_tokens, Some(100));
            assert_eq!(record.output_tokens, Some(500));
            assert_eq!(record.total_tokens, Some(600));
            assert!(
                record.response_body.is_none(),
                "default Basic policy still applies at its original position"
            );
        }
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(runtime.metrics_snapshot().terminal_submission_in_flight, 0);
    }
}

struct TestLifecycleSubmissionItem {
    request_id: String,
    priority: LifecycleSubmissionPriority,
    started: Option<Arc<tokio::sync::Notify>>,
    release: Option<Arc<tokio::sync::Notify>>,
    seen: Arc<Mutex<Vec<LifecycleSubmissionPriority>>>,
}

struct PanickingLifecycleSubmissionItem {
    request_id: String,
}

#[async_trait]
impl LifecycleSubmissionItem for PanickingLifecycleSubmissionItem {
    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn priority(&self) -> LifecycleSubmissionPriority {
        LifecycleSubmissionPriority::Pending
    }

    async fn execute(self: Box<Self>, _admission: LifecycleAdmissionPermit) {
        panic!("forced lifecycle submission panic");
    }
}

struct PanickingTerminalExecutionItem {
    request_id: String,
}

#[async_trait]
impl super::TerminalExecutionItem for PanickingTerminalExecutionItem {
    fn request_id(&self) -> &str {
        &self.request_id
    }

    async fn execute(self: Box<Self>) {
        panic!("forced terminal execution panic");
    }
}

struct CompletingTerminalExecutionItem {
    request_id: String,
    completion: tokio::sync::oneshot::Sender<()>,
}

#[async_trait]
impl super::TerminalExecutionItem for CompletingTerminalExecutionItem {
    fn request_id(&self) -> &str {
        &self.request_id
    }

    async fn execute(self: Box<Self>) {
        let _ = self.completion.send(());
    }
}

struct PanickingFirstBytePersistenceItem {
    _marker_guard: super::FirstByteMarkerGuard,
    record: UpsertUsageRecord,
}

#[async_trait]
impl super::FirstBytePersistenceItem for PanickingFirstBytePersistenceItem {
    fn batch_identity(&self) -> Option<usize> {
        panic!("forced first-byte identity panic");
    }

    async fn is_current(&self) -> bool {
        true
    }

    fn build_record(&self) -> Result<UpsertUsageRecord, DataLayerError> {
        Ok(self.record.clone())
    }

    async fn write_batch(&self, _records: Vec<UpsertUsageRecord>) -> Result<(), DataLayerError> {
        unreachable!("identity panic should precede the write")
    }

    async fn complete_success(self: Box<Self>) {
        unreachable!("identity panic should precede completion")
    }

    async fn complete_cancelled(self: Box<Self>) {
        unreachable!("test item is always current")
    }

    async fn enqueue_fallback(self: Box<Self>) {
        unreachable!("identity panic should precede fallback")
    }
}

#[async_trait]
impl LifecycleSubmissionItem for TestLifecycleSubmissionItem {
    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn priority(&self) -> LifecycleSubmissionPriority {
        self.priority
    }

    async fn execute(self: Box<Self>, _admission: LifecycleAdmissionPermit) {
        if let Some(started) = self.started {
            started.notify_one();
        }
        if let Some(release) = self.release {
            release.notified().await;
        }
        self.seen
            .lock()
            .expect("lifecycle test seen lock")
            .push(self.priority);
    }
}

#[derive(Default)]
struct NoRedisUsageStore {
    records: Mutex<Vec<UpsertUsageRecord>>,
    enrichment_failures: AtomicUsize,
    enrichment_calls: AtomicUsize,
    enriched_costs: Option<(f64, f64)>,
}

struct QueueConfiguredUsageStore {
    inner: NoRedisUsageStore,
    queue: Arc<dyn RuntimeQueueStore>,
}

#[derive(Clone)]
struct CloneQueueConfiguredUsageStore {
    records: Arc<Mutex<Vec<UpsertUsageRecord>>>,
    queue: Arc<dyn RuntimeQueueStore>,
}

#[derive(Clone)]
struct BatchingQueueUsageStore {
    records: Arc<Mutex<Vec<UpsertUsageRecord>>>,
    queue: Arc<dyn RuntimeQueueStore>,
}

#[derive(Clone)]
struct OrderedBatchUsageStore {
    records: Arc<Mutex<Vec<UpsertUsageRecord>>>,
    pending_batch_sizes: Arc<Mutex<Vec<usize>>>,
    pending_attempts: Arc<Mutex<BTreeMap<String, usize>>>,
    permanent_pending_failure_request_id: Option<Arc<str>>,
    remaining_pending_failures: Arc<AtomicUsize>,
    remaining_pending_panics: Arc<AtomicUsize>,
    remaining_first_byte_panics: Arc<AtomicUsize>,
    queue: Arc<dyn RuntimeQueueStore>,
}

#[derive(Clone)]
struct BlockingNonBatchPendingStore {
    release_writes: Arc<tokio::sync::Semaphore>,
    writes_in_flight: Arc<AtomicUsize>,
    max_writes_in_flight: Arc<AtomicUsize>,
    writes_completed: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct AdmissionBlockedUsageStore {
    release_writes: Arc<tokio::sync::Semaphore>,
    writes_in_flight: Arc<AtomicUsize>,
    max_writes_in_flight: Arc<AtomicUsize>,
    writes_completed: Arc<AtomicUsize>,
}

struct BlockingNonBatchPendingItem {
    request_id: String,
    record: UpsertUsageRecord,
    store: BlockingNonBatchPendingStore,
    record_gate: Arc<super::UsageWorkerRecordConcurrencyGate>,
    completed: Arc<AtomicUsize>,
    degraded: Arc<AtomicUsize>,
}

struct PanicOnceQueueConfiguredUsageStore {
    inner: CloneQueueConfiguredUsageStore,
    remaining_panics: AtomicUsize,
}

#[derive(Clone)]
struct PanicOncePolicyQueueConfiguredUsageStore {
    inner: CloneQueueConfiguredUsageStore,
    remaining_policy_panics: Arc<AtomicUsize>,
    policy_reads: Arc<AtomicUsize>,
}

struct EnrichmentCountingQueueStore {
    records: Mutex<Vec<UpsertUsageRecord>>,
    queue: Arc<dyn RuntimeQueueStore>,
    enrich_calls: AtomicUsize,
}

#[derive(Clone)]
struct FailingWriteQueueConfiguredUsageStore {
    queue: Arc<dyn RuntimeQueueStore>,
    upsert_attempts: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct QueueOnlyUsageStore {
    queue: Arc<dyn RuntimeQueueStore>,
    upsert_attempts: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct BlockingWriteQueueConfiguredUsageStore {
    records: Arc<Mutex<Vec<UpsertUsageRecord>>>,
    queue: Arc<dyn RuntimeQueueStore>,
    write_started: Arc<tokio::sync::Notify>,
    release_writes: Arc<tokio::sync::Notify>,
    writes_completed: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct BlockingPolicyQueueConfiguredUsageStore {
    queue: Arc<dyn RuntimeQueueStore>,
    policy_started: Arc<tokio::sync::Notify>,
    release_policy: Arc<tokio::sync::Notify>,
    policy_released: Arc<AtomicBool>,
    policy_reads: Arc<AtomicUsize>,
}

impl BlockingPolicyQueueConfiguredUsageStore {
    fn new(queue: Arc<dyn RuntimeQueueStore>) -> Self {
        Self {
            queue,
            policy_started: Arc::new(tokio::sync::Notify::new()),
            release_policy: Arc::new(tokio::sync::Notify::new()),
            policy_released: Arc::new(AtomicBool::new(false)),
            policy_reads: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn release_blocked_policy(&self) {
        self.policy_released.store(true, Ordering::Release);
        self.release_policy.notify_waiters();
    }
}

#[derive(Default)]
struct FailingPolicyUsageStore {
    inner: NoRedisUsageStore,
    policy_reads: AtomicUsize,
}

struct FlakyAppendQueueStore {
    inner: Arc<dyn RuntimeQueueStore>,
    remaining_failures: AtomicUsize,
    append_attempts: AtomicUsize,
    successful_appends: AtomicUsize,
    active_appends: AtomicUsize,
    max_active_appends: AtomicUsize,
    append_delay_ms: u64,
}

impl FlakyAppendQueueStore {
    fn new(inner: Arc<dyn RuntimeQueueStore>, remaining_failures: usize) -> Self {
        Self {
            inner,
            remaining_failures: AtomicUsize::new(remaining_failures),
            append_attempts: AtomicUsize::new(0),
            successful_appends: AtomicUsize::new(0),
            active_appends: AtomicUsize::new(0),
            max_active_appends: AtomicUsize::new(0),
            append_delay_ms: 0,
        }
    }

    fn with_append_delay_ms(mut self, append_delay_ms: u64) -> Self {
        self.append_delay_ms = append_delay_ms;
        self
    }
}

async fn wait_for_enqueue_dispatcher_to_drain(runtime: &UsageRuntime, expected_recovered: u64) {
    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if snapshot.enqueue_retry_recovered_total >= expected_recovered
                && snapshot.enqueue_retry_pending == 0
            {
                return;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("local usage enqueue dispatcher should drain");
}

#[async_trait]
impl UsageRecordWriter for NoRedisUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.records.lock().expect("records lock").push(record);
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for NoRedisUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for NoRedisUsageStore {
    async fn enrich_usage_event(&self, event: &mut UsageEvent) -> Result<(), DataLayerError> {
        self.enrichment_calls.fetch_add(1, Ordering::AcqRel);
        if self
            .enrichment_failures
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            // Real enrichers may update part of the event before a lookup fails.
            event.data.total_cost_usd = Some(999.0);
            return Err(DataLayerError::TimedOut("test billing lookup".to_string()));
        }
        if let Some((listed, actual)) = self.enriched_costs {
            event.data.total_cost_usd = Some(listed);
            event.data.actual_total_cost_usd = Some(actual);
        }
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for NoRedisUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for NoRedisUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        false
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        None
    }
}

#[async_trait]
impl UsageRecordWriter for QueueConfiguredUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.inner.upsert_usage_record(record).await
    }
}

#[async_trait]
impl UsageSettlementWriter for QueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for QueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for QueueConfiguredUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for QueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for CloneQueueConfiguredUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.records.lock().expect("records lock").push(record);
        Ok(None)
    }
}

#[async_trait]
impl UsageRecordWriter for BlockingNonBatchPendingStore {
    async fn upsert_usage_record(
        &self,
        _record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        let active = self.writes_in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_writes_in_flight
            .fetch_max(active, Ordering::AcqRel);
        let permit = self
            .release_writes
            .acquire()
            .await
            .expect("blocking pending store semaphore should remain open");
        permit.forget();
        self.writes_in_flight.fetch_sub(1, Ordering::AcqRel);
        self.writes_completed.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
}

#[async_trait]
impl UsageRecordWriter for AdmissionBlockedUsageStore {
    async fn upsert_usage_record(
        &self,
        _record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        let active = self.writes_in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_writes_in_flight
            .fetch_max(active, Ordering::AcqRel);
        let permit = self
            .release_writes
            .acquire()
            .await
            .expect("admission test semaphore should remain open");
        permit.forget();
        self.writes_in_flight.fetch_sub(1, Ordering::AcqRel);
        self.writes_completed.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for AdmissionBlockedUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for AdmissionBlockedUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for AdmissionBlockedUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        false
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        None
    }
}

#[async_trait]
impl super::PendingPersistenceItem for BlockingNonBatchPendingItem {
    fn request_id(&self) -> &str {
        &self.request_id
    }

    fn batch_identity(&self) -> Option<usize> {
        None
    }

    fn build_record(&self) -> Result<UpsertUsageRecord, DataLayerError> {
        Ok(self.record.clone())
    }

    fn retry_delay(&self, _attempts: u64) -> Duration {
        Duration::from_millis(1)
    }

    async fn write_batch(&self, records: Vec<UpsertUsageRecord>) -> Result<(), DataLayerError> {
        let _permit = self.record_gate.acquire().await;
        self.store.upsert_pending_usage_records(records).await
    }

    async fn complete_success(self: Box<Self>) {
        self.completed.fetch_add(1, Ordering::AcqRel);
    }

    fn complete_degraded(self: Box<Self>) {
        self.degraded.fetch_add(1, Ordering::AcqRel);
    }
}

#[async_trait]
impl UsageRecordWriter for BatchingQueueUsageStore {
    fn supports_first_byte_usage_batch(&self) -> bool {
        true
    }

    fn first_byte_usage_writer_identity(&self) -> Option<usize> {
        Some(Arc::as_ptr(&self.records) as usize)
    }

    fn supports_pending_usage_batch(&self) -> bool {
        true
    }

    fn pending_usage_writer_identity(&self) -> Option<usize> {
        Some(Arc::as_ptr(&self.records) as usize)
    }

    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.records.lock().expect("records lock").push(record);
        Ok(None)
    }

    async fn upsert_first_byte_usage_records(
        &self,
        records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        self.records.lock().expect("records lock").extend(records);
        Ok(())
    }

    async fn upsert_pending_usage_records(
        &self,
        records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        self.records.lock().expect("records lock").extend(records);
        Ok(())
    }
}

#[async_trait]
impl UsageRecordWriter for OrderedBatchUsageStore {
    fn supports_first_byte_usage_batch(&self) -> bool {
        true
    }

    fn first_byte_usage_writer_identity(&self) -> Option<usize> {
        Some(Arc::as_ptr(&self.records) as usize)
    }

    fn supports_pending_usage_batch(&self) -> bool {
        true
    }

    fn pending_usage_writer_identity(&self) -> Option<usize> {
        Some(Arc::as_ptr(&self.records) as usize)
    }

    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.records.lock().expect("records lock").push(record);
        Ok(None)
    }

    async fn upsert_first_byte_usage_records(
        &self,
        records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        if self
            .remaining_first_byte_panics
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining > 0).then(|| remaining - 1)
            })
            .is_ok()
        {
            panic!("forced first-byte batch panic");
        }
        self.records.lock().expect("records lock").extend(records);
        Ok(())
    }

    async fn upsert_pending_usage_records(
        &self,
        records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        self.pending_batch_sizes
            .lock()
            .expect("pending batch sizes lock")
            .push(records.len());
        {
            let mut attempts = self.pending_attempts.lock().expect("pending attempts lock");
            for record in &records {
                *attempts.entry(record.request_id.clone()).or_default() += 1;
            }
        }
        if self
            .permanent_pending_failure_request_id
            .as_deref()
            .is_some_and(|request_id| records.iter().any(|record| record.request_id == request_id))
        {
            return Err(DataLayerError::Postgres(
                "forced permanent pending batch failure".to_string(),
            ));
        }
        if self
            .remaining_pending_panics
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining > 0).then(|| remaining - 1)
            })
            .is_ok()
        {
            panic!("forced pending batch panic");
        }
        if self
            .remaining_pending_failures
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining > 0).then(|| remaining - 1)
            })
            .is_ok()
        {
            return Err(DataLayerError::Postgres(
                "forced pending batch failure".to_string(),
            ));
        }
        self.records.lock().expect("records lock").extend(records);
        Ok(())
    }
}

#[async_trait]
impl UsageSettlementWriter for OrderedBatchUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for OrderedBatchUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for OrderedBatchUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageSettlementWriter for BatchingQueueUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for BatchingQueueUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for BatchingQueueUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageSettlementWriter for CloneQueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for CloneQueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for CloneQueueConfiguredUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for CloneQueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for PanicOncePolicyQueueConfiguredUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.inner.upsert_usage_record(record).await
    }
}

#[async_trait]
impl UsageSettlementWriter for PanicOncePolicyQueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for PanicOncePolicyQueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl UsageRuntimeAccess for PanicOncePolicyQueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.inner.queue))
    }

    async fn body_capture_policy(&self) -> Result<UsageBodyCapturePolicy, DataLayerError> {
        self.policy_reads.fetch_add(1, Ordering::AcqRel);
        if self
            .remaining_policy_panics
            .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                (remaining > 0).then(|| remaining - 1)
            })
            .is_ok()
        {
            panic!("forced body capture policy panic");
        }
        Ok(UsageBodyCapturePolicy::default())
    }
}

#[async_trait]
impl UsageRecordWriter for PanicOnceQueueConfiguredUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        let should_panic = self
            .remaining_panics
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current > 0).then(|| current - 1)
            })
            .is_ok();
        if should_panic {
            panic!("forced usage writer panic");
        }
        self.inner.upsert_usage_record(record).await
    }
}

#[async_trait]
impl UsageSettlementWriter for PanicOnceQueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for PanicOnceQueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for PanicOnceQueueConfiguredUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for PanicOnceQueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.inner.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for EnrichmentCountingQueueStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.records.lock().expect("records lock").push(record);
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for EnrichmentCountingQueueStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for EnrichmentCountingQueueStore {
    async fn enrich_usage_event(&self, event: &mut UsageEvent) -> Result<(), DataLayerError> {
        self.enrich_calls.fetch_add(1, Ordering::AcqRel);
        event.data.total_cost_usd = Some(0.123);
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for EnrichmentCountingQueueStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for EnrichmentCountingQueueStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for FailingWriteQueueConfiguredUsageStore {
    fn supports_first_byte_usage_batch(&self) -> bool {
        true
    }

    fn first_byte_usage_writer_identity(&self) -> Option<usize> {
        Some(Arc::as_ptr(&self.upsert_attempts) as usize)
    }

    async fn upsert_usage_record(
        &self,
        _record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.upsert_attempts.fetch_add(1, Ordering::AcqRel);
        Err(DataLayerError::Postgres(
            "forced direct fallback write failure".to_string(),
        ))
    }

    async fn upsert_first_byte_usage_records(
        &self,
        _records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        self.upsert_attempts.fetch_add(1, Ordering::AcqRel);
        Err(DataLayerError::Postgres(
            "forced batch first-byte write failure".to_string(),
        ))
    }
}

#[async_trait]
impl UsageSettlementWriter for FailingWriteQueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for FailingWriteQueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for FailingWriteQueueConfiguredUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for FailingWriteQueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for QueueOnlyUsageStore {
    async fn upsert_usage_record(
        &self,
        _record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.upsert_attempts.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for QueueOnlyUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for QueueOnlyUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for QueueOnlyUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for QueueOnlyUsageStore {
    fn has_usage_writer(&self) -> bool {
        false
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for BlockingWriteQueueConfiguredUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.write_started.notify_one();
        self.release_writes.notified().await;
        self.records.lock().expect("records lock").push(record);
        self.writes_completed.fetch_add(1, Ordering::AcqRel);
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for BlockingWriteQueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for BlockingWriteQueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for BlockingWriteQueueConfiguredUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl UsageRuntimeAccess for BlockingWriteQueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn supports_first_byte_usage_fast_path(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }
}

#[async_trait]
impl UsageRecordWriter for BlockingPolicyQueueConfiguredUsageStore {
    async fn upsert_usage_record(
        &self,
        _record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for BlockingPolicyQueueConfiguredUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for BlockingPolicyQueueConfiguredUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for BlockingPolicyQueueConfiguredUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl UsageRuntimeAccess for BlockingPolicyQueueConfiguredUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        Some(Arc::clone(&self.queue))
    }

    async fn body_capture_policy(&self) -> Result<UsageBodyCapturePolicy, DataLayerError> {
        self.policy_reads.fetch_add(1, Ordering::AcqRel);
        self.policy_started.notify_one();
        // Latch the gate: Notify is edge-triggered, and later policy reads
        // (or a waiter that subscribed after a single notify) must not hang.
        loop {
            if self.policy_released.load(Ordering::Acquire) {
                break;
            }
            let notified = self.release_policy.notified();
            if self.policy_released.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        Ok(UsageBodyCapturePolicy::default())
    }
}

#[async_trait]
impl UsageRecordWriter for FailingPolicyUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        self.inner.upsert_usage_record(record).await
    }
}

#[async_trait]
impl UsageSettlementWriter for FailingPolicyUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        false
    }

    async fn settle_usage(
        &self,
        _input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, DataLayerError> {
        Ok(None)
    }
}

#[async_trait]
impl UsageBillingEventEnricher for FailingPolicyUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl ManualProxyNodeCounter for FailingPolicyUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }
}

#[async_trait]
impl UsageRuntimeAccess for FailingPolicyUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        false
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        None
    }

    async fn body_capture_policy(&self) -> Result<UsageBodyCapturePolicy, DataLayerError> {
        self.policy_reads.fetch_add(1, Ordering::AcqRel);
        Err(DataLayerError::Postgres(
            "forced policy read failure".to_string(),
        ))
    }
}

#[async_trait]
impl RuntimeQueueStore for FlakyAppendQueueStore {
    async fn ensure_consumer_group(
        &self,
        stream: &str,
        group: &str,
        start_id: &str,
    ) -> Result<(), DataLayerError> {
        self.inner
            .ensure_consumer_group(stream, group, start_id)
            .await
    }

    async fn append_fields_with_maxlen(
        &self,
        stream: &str,
        fields: &BTreeMap<String, String>,
        maxlen: Option<usize>,
    ) -> Result<String, DataLayerError> {
        self.append_attempts.fetch_add(1, Ordering::AcqRel);
        let active = self.active_appends.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active_appends.fetch_max(active, Ordering::AcqRel);
        if self.append_delay_ms > 0 {
            sleep(Duration::from_millis(self.append_delay_ms)).await;
        }
        let failed = self
            .remaining_failures
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current > 0).then(|| current - 1)
            })
            .is_ok();
        let result = if failed {
            Err(DataLayerError::Redis("forced append failure".to_string()))
        } else {
            self.inner
                .append_fields_with_maxlen(stream, fields, maxlen)
                .await
        };
        if result.is_ok() {
            self.successful_appends.fetch_add(1, Ordering::AcqRel);
        }
        self.active_appends.fetch_sub(1, Ordering::AcqRel);
        result
    }

    async fn read_group(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        count: usize,
        block_ms: Option<u64>,
    ) -> Result<Vec<aether_runtime_state::RuntimeQueueEntry>, DataLayerError> {
        self.inner
            .read_group(stream, group, consumer, count, block_ms)
            .await
    }

    async fn claim_stale(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        start_id: &str,
        config: aether_runtime_state::RuntimeQueueReclaimConfig,
    ) -> Result<Vec<aether_runtime_state::RuntimeQueueEntry>, DataLayerError> {
        self.inner
            .claim_stale(stream, group, consumer, start_id, config)
            .await
    }

    async fn ack(
        &self,
        stream: &str,
        group: &str,
        ids: &[String],
    ) -> Result<usize, DataLayerError> {
        self.inner.ack(stream, group, ids).await
    }

    async fn delete(&self, stream: &str, ids: &[String]) -> Result<usize, DataLayerError> {
        self.inner.delete(stream, ids).await
    }

    async fn stats(
        &self,
        stream: &str,
        group: Option<&str>,
    ) -> Result<aether_runtime_state::RuntimeQueueStats, DataLayerError> {
        self.inner.stats(stream, group).await
    }
}

#[derive(Clone, Copy)]
enum DirectTerminalTestEntry {
    PublicDirect,
    OrderedDirect,
    QueueDisabled,
}

async fn invoke_direct_terminal_test_entry(
    entry: DirectTerminalTestEntry,
    runtime: &UsageRuntime,
    store: &NoRedisUsageStore,
    event: UsageEvent,
) -> Option<super::TerminalPersistenceOutcome> {
    match entry {
        DirectTerminalTestEntry::PublicDirect => {
            runtime.record_terminal_event_direct(store, event).await;
            None
        }
        DirectTerminalTestEntry::OrderedDirect => Some(
            runtime
                .persist_ordered_terminal_event(store, event, true)
                .await,
        ),
        DirectTerminalTestEntry::QueueDisabled => {
            Some(runtime.enqueue_or_write_terminal(store, event).await)
        }
    }
}

async fn assert_direct_terminal_enrichment_failure_is_not_persisted(
    entry: DirectTerminalTestEntry,
    request_id: &str,
) {
    let runtime = UsageRuntime::new(UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: false,
        ..UsageRuntimeConfig::default()
    })
    .expect("usage runtime");
    let store = NoRedisUsageStore {
        enrichment_failures: AtomicUsize::new(1),
        enriched_costs: Some((0.456, 0.123)),
        ..NoRedisUsageStore::default()
    };
    let generation = runtime
        .lifecycle_coalescer
        .register(request_id.to_string())
        .await
        .expect("delayed lifecycle generation");
    let event = UsageEvent::new(
        UsageEventType::Completed,
        request_id,
        UsageEventData {
            user_id: Some("user-direct-pricing-retry".to_string()),
            provider_name: "openai".to_string(),
            provider_id: Some("provider-direct-pricing-retry".to_string()),
            model: "gpt-5".to_string(),
            input_tokens: Some(4),
            output_tokens: Some(8),
            total_tokens: Some(12),
            total_cost_usd: Some(0.9),
            actual_total_cost_usd: Some(0.8),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    let outcome = timeout(
        Duration::from_secs(2),
        invoke_direct_terminal_test_entry(entry, &runtime, &store, event.clone()),
    )
    .await
    .expect("failed enrichment should release the terminal turn");
    if let Some(outcome) = outcome {
        assert_eq!(outcome, super::TerminalPersistenceOutcome::Failed);
    }
    assert_eq!(store.enrichment_calls.load(Ordering::Acquire), 1);
    assert!(store.records.lock().expect("records lock").is_empty());
    {
        let coalescer = &runtime.lifecycle_coalescer;
        let entries = coalescer.shards[coalescer.shard_index(request_id)]
            .entries
            .lock()
            .await;
        let marker = entries
            .get(request_id)
            .expect("delayed marker is preserved");
        assert_eq!(marker.generation, generation);
        assert!(marker.terminal_seen_at.is_none());
    }
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
    assert_eq!(snapshot.terminal_submission_in_flight, 0);
    assert_eq!(snapshot.enqueue_retry_scheduled_total, 0);

    let outcome = timeout(
        Duration::from_secs(2),
        invoke_direct_terminal_test_entry(entry, &runtime, &store, event),
    )
    .await
    .expect("a later terminal attempt should be able to persist");
    if let Some(outcome) = outcome {
        assert_eq!(
            outcome,
            super::TerminalPersistenceOutcome::PersistedDirectly
        );
    }
    assert_eq!(store.enrichment_calls.load(Ordering::Acquire), 2);
    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].total_cost_usd, Some(0.456));
        assert_eq!(records[0].actual_total_cost_usd, Some(0.123));
        assert_eq!(records[0].total_tokens, Some(12));
    }
    {
        let coalescer = &runtime.lifecycle_coalescer;
        let entries = coalescer.shards[coalescer.shard_index(request_id)]
            .entries
            .lock()
            .await;
        let marker = entries.get(request_id).expect("successful terminal marker");
        assert!(marker.terminal_seen_at.is_some());
    }
    assert_eq!(runtime.metrics_snapshot().ordered_lifecycle_pending, 0);
    assert_eq!(runtime.metrics_snapshot().terminal_submission_in_flight, 0);
}

#[tokio::test]
async fn direct_terminal_pricing_failure_preserves_lifecycle_and_allows_correct_retry() {
    assert_direct_terminal_enrichment_failure_is_not_persisted(
        DirectTerminalTestEntry::PublicDirect,
        "direct-terminal-pricing-retry",
    )
    .await;
}

#[tokio::test]
async fn ordered_direct_terminal_pricing_failure_returns_failed_before_correct_retry() {
    assert_direct_terminal_enrichment_failure_is_not_persisted(
        DirectTerminalTestEntry::OrderedDirect,
        "ordered-direct-terminal-pricing-retry",
    )
    .await;
}

#[tokio::test]
async fn queue_disabled_terminal_pricing_failure_returns_failed_before_correct_retry() {
    assert_direct_terminal_enrichment_failure_is_not_persisted(
        DirectTerminalTestEntry::QueueDisabled,
        "queue-disabled-terminal-pricing-retry",
    )
    .await;
}

#[tokio::test]
async fn bounded_direct_fallback_pricing_failure_does_not_write_or_report_success() {
    let runtime = UsageRuntime::new(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    })
    .expect("usage runtime");
    let store = NoRedisUsageStore {
        enrichment_failures: AtomicUsize::new(1),
        enriched_costs: Some((0.456, 0.123)),
        ..NoRedisUsageStore::default()
    };
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "bounded-fallback-pricing-retry",
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            total_tokens: Some(12),
            ..UsageEventData::default()
        },
    );
    assert!(
        !runtime
            .try_write_terminal_direct_fallback(&store, &mut event, "test_retry")
            .await
    );
    assert!(store.records.lock().expect("records lock").is_empty());
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.terminal_direct_fallback_failed_total, 1);
    assert_eq!(snapshot.terminal_direct_fallback_succeeded_total, 0);
    assert_eq!(snapshot.terminal_direct_fallback_in_flight, 0);

    assert!(
        runtime
            .try_write_terminal_direct_fallback(&store, &mut event, "test_retry")
            .await
    );
    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].total_cost_usd, Some(0.456));
    assert_eq!(records[0].actual_total_cost_usd, Some(0.123));
    drop(records);
    assert_eq!(
        runtime
            .metrics_snapshot()
            .terminal_direct_fallback_succeeded_total,
        1
    );
}

#[tokio::test]
async fn terminal_usage_without_redis_writes_directly_to_usage_repository() {
    let runtime = UsageRuntime::new(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    })
    .expect("usage runtime should build");
    let store = NoRedisUsageStore::default();
    let event = UsageEvent::new(
        UsageEventType::Completed,
        "req-no-redis-1",
        UsageEventData {
            user_id: Some("user-no-redis-1".to_string()),
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            input_tokens: Some(4),
            output_tokens: Some(8),
            total_tokens: Some(12),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    runtime.record_terminal_event(&store, event).await;

    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_id, "req-no-redis-1");
    assert_eq!(records[0].status, "completed");
    assert_eq!(records[0].total_tokens, Some(12));
}

#[tokio::test]
async fn direct_terminal_usage_bypasses_redis_queue_and_writes_repository() {
    let runtime = UsageRuntime::new(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    })
    .expect("usage runtime should build");
    let store = QueueConfiguredUsageStore {
        inner: NoRedisUsageStore::default(),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let event = UsageEvent::new(
        UsageEventType::Failed,
        "req-direct-terminal-1",
        UsageEventData {
            user_id: Some("user-direct-terminal-1".to_string()),
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            status_code: Some(503),
            error_message: Some("upstream failed".to_string()),
            ..UsageEventData::default()
        },
    );

    runtime.record_terminal_event_direct(&store, event).await;

    let records = store.inner.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_id, "req-direct-terminal-1");
    assert_eq!(records[0].status, "failed");
    assert_eq!(records[0].billing_status, "void");
    assert_eq!(records[0].status_code, Some(503));
}

#[tokio::test]
async fn pending_usage_is_persisted_before_later_lifecycle_events() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        stream_key: "usage:events:test:pending".to_string(),
        consumer_group: "usage_consumers_test_pending".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let plan = ExecutionPlan {
        request_id: "req-lifecycle-queue-pending-1".to_string(),
        candidate_id: Some("cand-lifecycle-queue-pending-1".to_string()),
        provider_name: Some("openai".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    runtime.record_pending(&store, build_lifecycle_usage_seed(&plan, None));

    timeout(Duration::from_secs(1), async {
        while store.records.lock().expect("records lock").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending lifecycle usage should be persisted");

    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].request_id, "req-lifecycle-queue-pending-1");
        assert_eq!(records[0].status, "pending");
    }
    assert!(
        queue
            .read_group("usage-test-consumer")
            .await
            .expect("queue read should succeed")
            .is_empty(),
        "ordered pending persistence should not leave a duplicate queue event"
    );
}

#[tokio::test]
async fn lifecycle_submission_persists_pending_before_first_byte_streaming() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        stream_key: "usage:events:test:ordered-pending-streaming".to_string(),
        consumer_group: "usage_consumers_test_ordered_pending_streaming".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let plan = ExecutionPlan {
        request_id: "req-ordered-pending-streaming".to_string(),
        candidate_id: Some("cand-ordered-pending-streaming".to_string()),
        provider_name: Some("openai".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let seed = build_lifecycle_usage_seed(&plan, None);

    runtime.record_pending(&store, seed.clone());
    runtime.record_stream_started(
        &store,
        &seed,
        200,
        Some(&ExecutionTelemetry {
            ttfb_ms: Some(12),
            elapsed_ms: Some(34),
            upstream_bytes: Some(56),
        }),
    );

    timeout(Duration::from_secs(1), async {
        while store.records.lock().expect("records lock").len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending and streaming lifecycle writes should complete");

    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].status, "pending");
    assert_eq!(records[1].status, "streaming");
    assert_eq!(records[1].first_byte_time_ms, Some(12));
}

#[tokio::test]
async fn duplicate_first_byte_releases_ordered_barrier_for_terminal() {
    const CAPACITY: usize = 8;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_buffer_capacity: CAPACITY,
        ..UsageRuntimeConfig::default()
    };
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-duplicate-first-byte-terminal";
    let seed = build_lifecycle_usage_seed(&terminal_test_plan(request_id), None);
    let telemetry = ExecutionTelemetry {
        ttfb_ms: Some(12),
        elapsed_ms: Some(20),
        upstream_bytes: Some(1),
    };

    runtime.record_stream_started(&store, &seed, 200, Some(&telemetry));
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if store.records.lock().expect("records lock").len() == 1
                && snapshot.lifecycle_submission_pending == 0
                && snapshot.first_byte_persistence_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first first-byte transition should complete");

    runtime.record_stream_started(&store, &seed, 200, Some(&telemetry));
    timeout(
        Duration::from_secs(1),
        runtime.record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        ),
    )
    .await
    .expect("a duplicate first-byte marker must release the terminal barrier");

    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(
            records.len(),
            2,
            "the duplicate first byte must be coalesced"
        );
        assert_eq!(records[0].status, "streaming");
        assert_eq!(records[1].status, "completed");
    }

    // The terminal persistence notification can arrive before the submission
    // dispatcher accounts for its completed task and releases admission.
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if snapshot.lifecycle_submission_pending == 0
                && snapshot.first_byte_persistence_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
                && runtime
                    .lifecycle_submission
                    .state
                    .admission
                    .available_permits()
                    == CAPACITY
            {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("duplicate first-byte submission accounting should drain");

    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.lifecycle_submission_pending, 0);
    assert_eq!(snapshot.first_byte_persistence_pending, 0);
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
    assert_eq!(
        runtime
            .lifecycle_submission
            .state
            .admission
            .available_permits(),
        CAPACITY
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_terminal_holds_ordered_slot_until_persistence_finishes() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(2),
        terminal_submission_max_in_flight: 2,
        enqueue_retry_buffer_capacity: 8,
        ..UsageRuntimeConfig::default()
    };
    let store = AdmissionBlockedUsageStore {
        release_writes: Arc::new(tokio::sync::Semaphore::new(0)),
        writes_in_flight: Arc::new(AtomicUsize::new(0)),
        max_writes_in_flight: Arc::new(AtomicUsize::new(0)),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-direct-terminal-ordering";
    let direct_runtime = runtime.clone();
    let direct_store = store.clone();
    let direct_terminal = tokio::spawn(async move {
        direct_runtime
            .record_terminal_event_direct(
                &direct_store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    request_id,
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    });
    timeout(Duration::from_secs(1), async {
        while store.writes_in_flight.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the direct terminal write should start");

    runtime.record_pending(
        &store,
        build_lifecycle_usage_seed(&terminal_test_plan(request_id), None),
    );
    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().ordered_lifecycle_pending != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the later pending phase should queue behind the direct terminal");
    sleep(Duration::from_millis(25)).await;
    assert_eq!(store.writes_in_flight.load(Ordering::Acquire), 1);
    assert_eq!(store.max_writes_in_flight.load(Ordering::Acquire), 1);
    assert_eq!(store.writes_completed.load(Ordering::Acquire), 0);

    store.release_writes.add_permits(1);
    timeout(Duration::from_secs(1), direct_terminal)
        .await
        .expect("the direct terminal caller should finish after persistence")
        .expect("the direct terminal task should not panic");
    timeout(Duration::from_secs(1), async {
        while store.writes_completed.load(Ordering::Acquire) != 1
            || store.writes_in_flight.load(Ordering::Acquire) != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the queued pending write should start only after terminal persistence");

    store.release_writes.add_permits(1);
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if store.writes_completed.load(Ordering::Acquire) == 2
                && snapshot.ordered_lifecycle_pending == 0
                && snapshot.lifecycle_submission_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the queued pending phase should drain after its write resumes");
    assert_eq!(store.max_writes_in_flight.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_lifecycle_admission_bounds_resident_work_and_backpressures_excess() {
    const CAPACITY: usize = 40;
    const TOTAL: usize = CAPACITY + 8;

    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(CAPACITY),
        terminal_submission_max_in_flight: CAPACITY as u64,
        enqueue_retry_buffer_capacity: CAPACITY,
        ..UsageRuntimeConfig::default()
    };
    let store = AdmissionBlockedUsageStore {
        release_writes: Arc::new(tokio::sync::Semaphore::new(0)),
        writes_in_flight: Arc::new(AtomicUsize::new(0)),
        max_writes_in_flight: Arc::new(AtomicUsize::new(0)),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let mut submissions = Vec::with_capacity(TOTAL);

    for index in 0..TOTAL {
        let runtime = runtime.clone();
        let store = store.clone();
        submissions.push(tokio::spawn(async move {
            runtime
                .submit_terminal_event(
                    &store,
                    UsageEvent::new(
                        UsageEventType::Completed,
                        format!("req-terminal-admission-{index}"),
                        UsageEventData {
                            provider_name: "openai".to_string(),
                            model: "gpt-5".to_string(),
                            status_code: Some(200),
                            ..UsageEventData::default()
                        },
                    ),
                )
                .await;
        }));
    }

    timeout(Duration::from_secs(2), async {
        loop {
            let finished = submissions
                .iter()
                .filter(|submission| submission.is_finished())
                .count();
            if finished == CAPACITY
                && store.writes_in_flight.load(Ordering::Acquire) == CAPACITY
                && runtime
                    .lifecycle_submission
                    .state
                    .admission
                    .available_permits()
                    == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the hard lifecycle bound should backpressure excess terminal calls");

    let saturated = runtime.metrics_snapshot();
    assert_eq!(
        submissions
            .iter()
            .filter(|submission| submission.is_finished())
            .count(),
        CAPACITY,
        "only admitted callers may return while persistence is blocked"
    );
    assert!(saturated.lifecycle_submission_pending <= CAPACITY);
    assert!(saturated.ordered_lifecycle_pending <= CAPACITY);
    assert!(saturated.terminal_submission_pending <= CAPACITY);
    assert!(saturated.terminal_submission_in_flight <= CAPACITY);
    assert_eq!(saturated.ordered_lifecycle_pending, CAPACITY);
    assert_eq!(saturated.terminal_submission_pending, CAPACITY);
    assert_eq!(
        store.max_writes_in_flight.load(Ordering::Acquire),
        CAPACITY,
        "configured workers should still execute independently up to the hard bound"
    );

    store.release_writes.add_permits(TOTAL);
    timeout(Duration::from_secs(2), async {
        for submission in submissions {
            submission
                .await
                .expect("terminal admission task should not panic");
        }
    })
    .await
    .expect("all backpressured terminal callers should recover after persistence resumes");
    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if store.writes_completed.load(Ordering::Acquire) == TOTAL
                && snapshot.lifecycle_submission_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
                && snapshot.terminal_submission_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all admitted lifecycle work should drain after recovery");

    let recovered = runtime.metrics_snapshot();
    assert!(recovered.lifecycle_submission_max_pending <= CAPACITY);
    assert!(recovered.ordered_lifecycle_max_pending <= CAPACITY);
    assert!(recovered.terminal_submission_max_pending <= CAPACITY);
    assert_eq!(recovered.terminal_submission_in_flight, 0);
    assert_eq!(
        runtime
            .lifecycle_submission
            .state
            .admission
            .available_permits(),
        CAPACITY
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_batch_pending_single_writes_are_concurrent_and_globally_bounded() {
    const ITEMS_PER_BATCH: usize =
        super::PENDING_PERSISTENCE_SINGLE_WRITE_CONCURRENCY_PER_BATCH * 2;
    const TOTAL_ITEMS: usize = ITEMS_PER_BATCH * super::PENDING_PERSISTENCE_MAX_BATCH_CONCURRENCY;

    assert_eq!(
        super::PENDING_PERSISTENCE_SINGLE_WRITE_CONCURRENCY_PER_BATCH
            * super::PENDING_PERSISTENCE_MAX_BATCH_CONCURRENCY,
        super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY
    );

    let store = BlockingNonBatchPendingStore {
        release_writes: Arc::new(tokio::sync::Semaphore::new(0)),
        writes_in_flight: Arc::new(AtomicUsize::new(0)),
        max_writes_in_flight: Arc::new(AtomicUsize::new(0)),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    assert!(!UsageRecordWriter::supports_pending_usage_batch(&store));
    let record_gate = Arc::new(super::UsageWorkerRecordConcurrencyGate::new(
        super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY * 2,
    ));
    let completed = Arc::new(AtomicUsize::new(0));
    let degraded = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(super::PendingPersistenceState::new(TOTAL_ITEMS));
    let mut tasks = tokio::task::JoinSet::new();

    for batch_index in 0..super::PENDING_PERSISTENCE_MAX_BATCH_CONCURRENCY {
        let mut batch = Vec::with_capacity(ITEMS_PER_BATCH);
        for item_index in 0..ITEMS_PER_BATCH {
            let request_id = format!("req-blocking-non-batch-{batch_index}-{item_index}");
            let record = super::build_upsert_usage_record_from_event(&UsageEvent::new(
                UsageEventType::Pending,
                request_id.clone(),
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ))
            .expect("pending record should build");
            state.record_dispatched();
            batch.push(super::PendingPersistenceEnvelope {
                item: Box::new(BlockingNonBatchPendingItem {
                    request_id,
                    record,
                    store: store.clone(),
                    record_gate: Arc::clone(&record_gate),
                    completed: Arc::clone(&completed),
                    degraded: Arc::clone(&degraded),
                }),
                _pending_guard: super::PendingPersistencePendingGuard {
                    state: Arc::clone(&state),
                },
            });
        }
        let task_state = Arc::clone(&state);
        tasks.spawn(async move {
            super::process_pending_persistence_batch(batch, task_state).await;
        });
    }

    timeout(Duration::from_secs(2), async {
        while store.max_writes_in_flight.load(Ordering::Acquire)
            < super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("non-batch pending writes should fill the bounded concurrency window");
    sleep(Duration::from_millis(25)).await;

    let max_in_flight = store.max_writes_in_flight.load(Ordering::Acquire);
    assert!(max_in_flight > super::PENDING_PERSISTENCE_MAX_BATCH_CONCURRENCY);
    assert_eq!(
        max_in_flight,
        super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY
    );
    assert_eq!(
        store.writes_in_flight.load(Ordering::Acquire),
        super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY
    );
    assert_eq!(
        record_gate.max_in_flight(),
        super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY
    );

    store.release_writes.add_permits(TOTAL_ITEMS);
    timeout(Duration::from_secs(2), async {
        while let Some(result) = tasks.join_next().await {
            result.expect("pending batch task should complete");
        }
    })
    .await
    .expect("bounded non-batch pending writes should drain after release");

    assert_eq!(store.writes_completed.load(Ordering::Acquire), TOTAL_ITEMS);
    assert_eq!(completed.load(Ordering::Acquire), TOTAL_ITEMS);
    assert_eq!(degraded.load(Ordering::Acquire), 0);
    assert_eq!(state.pending.load(Ordering::Acquire), 0);
    assert_eq!(store.writes_in_flight.load(Ordering::Acquire), 0);
    assert_eq!(record_gate.in_flight(), 0);
}

#[tokio::test]
async fn saturated_pending_persistence_drops_overflow_and_stays_bounded() {
    const RESIDENT_BOUND: usize = 1_024
        + super::PENDING_PERSISTENCE_BATCH_SIZE * super::PENDING_PERSISTENCE_MAX_BATCH_CONCURRENCY
        + 1;
    const TOTAL_ITEMS: usize = RESIDENT_BOUND + 128;

    let config = UsageRuntimeConfig {
        enabled: true,
        enqueue_retry_buffer_capacity: 1_024,
        ..UsageRuntimeConfig::default()
    };
    let tasks = super::UsageBackgroundTasks::default();
    let dispatcher = super::PendingPersistenceDispatcher::spawn(&config, &tasks);
    let store = BlockingNonBatchPendingStore {
        release_writes: Arc::new(tokio::sync::Semaphore::new(0)),
        writes_in_flight: Arc::new(AtomicUsize::new(0)),
        max_writes_in_flight: Arc::new(AtomicUsize::new(0)),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let record_gate = Arc::new(super::UsageWorkerRecordConcurrencyGate::new(
        super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY * 2,
    ));
    let completed = Arc::new(AtomicUsize::new(0));
    let degraded = Arc::new(AtomicUsize::new(0));
    let dispatch_dispatcher = Arc::clone(&dispatcher);
    let dispatch_store = store.clone();
    let dispatch_gate = Arc::clone(&record_gate);
    let dispatch_completed = Arc::clone(&completed);
    let dispatch_degraded = Arc::clone(&degraded);
    let dispatch = tokio::spawn(async move {
        for index in 0..TOTAL_ITEMS {
            let request_id = format!("req-bounded-pending-{index}");
            let record = super::build_upsert_usage_record_from_event(&UsageEvent::new(
                UsageEventType::Pending,
                request_id.clone(),
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ))
            .expect("pending record should build");
            dispatch_dispatcher.dispatch(Box::new(BlockingNonBatchPendingItem {
                request_id,
                record,
                store: dispatch_store.clone(),
                record_gate: Arc::clone(&dispatch_gate),
                completed: Arc::clone(&dispatch_completed),
                degraded: Arc::clone(&dispatch_degraded),
            }));
        }
    });

    timeout(Duration::from_secs(5), dispatch)
        .await
        .expect("overflow dispatch must remain non-blocking")
        .expect("overflow dispatch task should not panic");
    let overflow = dispatcher.state.overflow_total.load(Ordering::Acquire);
    assert!(overflow > 0, "the test must overflow the bounded channel");
    assert_eq!(degraded.load(Ordering::Acquire) as u64, overflow);
    assert!(
        dispatcher.state.max_pending.load(Ordering::Acquire) <= RESIDENT_BOUND,
        "resident pending records exceeded the fixed channel and worker bound"
    );
    store.release_writes.add_permits(TOTAL_ITEMS);
    timeout(Duration::from_secs(5), async {
        while dispatcher.state.pending.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all accepted pending records should drain after recovery");
    let writes_completed = store.writes_completed.load(Ordering::Acquire);
    assert_eq!(completed.load(Ordering::Acquire), writes_completed);
    assert_eq!(writes_completed as u64 + overflow, TOTAL_ITEMS as u64);
    assert!(
        store.max_writes_in_flight.load(Ordering::Acquire)
            <= super::PENDING_PERSISTENCE_SINGLE_WRITE_TARGET_CONCURRENCY,
        "pending persistence exceeded its fixed worker window"
    );
}

#[tokio::test]
async fn ordered_lifecycle_batches_beyond_submission_worker_count() {
    const REQUESTS: usize = 96;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_workers: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 2,
        ..UsageRuntimeConfig::default()
    };
    let store = OrderedBatchUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        pending_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        pending_attempts: Arc::new(Mutex::new(BTreeMap::new())),
        permanent_pending_failure_request_id: None,
        remaining_pending_failures: Arc::new(AtomicUsize::new(0)),
        remaining_pending_panics: Arc::new(AtomicUsize::new(0)),
        remaining_first_byte_panics: Arc::new(AtomicUsize::new(0)),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let mut request_ids = Vec::with_capacity(REQUESTS);

    for index in 0..REQUESTS {
        let request_id = format!("req-ordered-batch-{index}");
        let seed = build_lifecycle_usage_seed(&terminal_test_plan(&request_id), None);
        runtime.record_pending(&store, seed.clone());
        runtime.record_stream_started(
            &store,
            &seed,
            200,
            Some(&ExecutionTelemetry {
                ttfb_ms: Some(10),
                elapsed_ms: Some(10),
                upstream_bytes: Some(1),
            }),
        );
        request_ids.push(request_id);
    }

    timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if store.records.lock().expect("records lock").len() == REQUESTS * 2
                && snapshot.lifecycle_submission_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
                && snapshot.pending_persistence_pending == 0
                && snapshot.first_byte_persistence_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered pending and first-byte batches should drain");

    let snapshot = runtime.metrics_snapshot();
    assert!(
        snapshot.pending_persistence_max_batch_size > 7,
        "pending max batch was {}",
        snapshot.pending_persistence_max_batch_size
    );
    assert!(
        snapshot.first_byte_persistence_max_batch_size > 7,
        "first-byte max batch was {}",
        snapshot.first_byte_persistence_max_batch_size
    );
    assert_eq!(
        snapshot.pending_persistence_batch_records_total,
        REQUESTS as u64
    );
    assert_eq!(
        snapshot.first_byte_persistence_batch_records_total,
        REQUESTS as u64
    );

    let records = store.records.lock().expect("records lock");
    for request_id in request_ids {
        let statuses = records
            .iter()
            .filter(|record| record.request_id == request_id)
            .map(|record| record.status.as_str())
            .collect::<Vec<_>>();
        assert_eq!(statuses, vec!["pending", "streaming"]);
    }
}

#[tokio::test]
async fn ordered_lifecycle_retries_pending_and_degrades_first_byte_before_terminal_barrier() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 2,
        ..UsageRuntimeConfig::default()
    };
    let store = OrderedBatchUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        pending_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        pending_attempts: Arc::new(Mutex::new(BTreeMap::new())),
        permanent_pending_failure_request_id: None,
        remaining_pending_failures: Arc::new(AtomicUsize::new(1)),
        remaining_pending_panics: Arc::new(AtomicUsize::new(1)),
        remaining_first_byte_panics: Arc::new(AtomicUsize::new(1)),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-ordered-retry-barrier";
    let seed = build_lifecycle_usage_seed(&terminal_test_plan(request_id), None);

    runtime.record_pending(&store, seed.clone());
    runtime.record_stream_started(
        &store,
        &seed,
        200,
        Some(&ExecutionTelemetry {
            ttfb_ms: Some(9),
            elapsed_ms: Some(9),
            upstream_bytes: Some(1),
        }),
    );
    runtime
        .record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    let records = store.records.lock().expect("records lock");
    let statuses = records
        .iter()
        .filter(|record| record.request_id == request_id)
        .map(|record| record.status.as_str())
        .collect::<Vec<_>>();
    assert_eq!(statuses, vec!["pending", "completed"]);
    drop(records);

    let snapshot = runtime.metrics_snapshot();
    assert!(snapshot.pending_persistence_batch_failed_total >= 2);
    assert!(snapshot.pending_persistence_retried_total >= 2);
    assert_eq!(snapshot.first_byte_persistence_batch_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_direct_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_fallback_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_pending, 0);
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn permanent_pending_failure_isolated_without_blocking_healthy_or_terminal_writes() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(4),
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let poison_request_id = "req-permanent-pending-failure";
    let healthy_request_id = "req-after-permanent-pending-failure";
    let pending_attempts = Arc::new(Mutex::new(BTreeMap::new()));
    let pending_batch_sizes = Arc::new(Mutex::new(Vec::new()));
    let store = OrderedBatchUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        pending_batch_sizes: Arc::clone(&pending_batch_sizes),
        pending_attempts: Arc::clone(&pending_attempts),
        permanent_pending_failure_request_id: Some(Arc::from(poison_request_id)),
        remaining_pending_failures: Arc::new(AtomicUsize::new(0)),
        remaining_pending_panics: Arc::new(AtomicUsize::new(0)),
        remaining_first_byte_panics: Arc::new(AtomicUsize::new(0)),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    runtime.record_pending(
        &store,
        build_lifecycle_usage_seed(&terminal_test_plan(poison_request_id), None),
    );
    runtime.record_pending(
        &store,
        build_lifecycle_usage_seed(&terminal_test_plan(healthy_request_id), None),
    );

    let poison_runtime = runtime.clone();
    let poison_store = store.clone();
    let poison_terminal = tokio::spawn(async move {
        poison_runtime
            .record_terminal_event_direct(
                &poison_store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    poison_request_id,
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    });
    let healthy_runtime = runtime.clone();
    let healthy_store = store.clone();
    let healthy_terminal = tokio::spawn(async move {
        healthy_runtime
            .record_terminal_event_direct(
                &healthy_store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    healthy_request_id,
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    });

    timeout(Duration::from_secs(2), async {
        poison_terminal
            .await
            .expect("poison terminal task should not panic");
        healthy_terminal
            .await
            .expect("healthy terminal task should not panic");
    })
    .await
    .expect("bounded pending retries must release both terminal barriers");
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if snapshot.lifecycle_submission_pending == 0
                && snapshot.pending_persistence_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending persistence and ordered lifecycle gauges should drain");

    let records = store.records.lock().expect("records lock");
    let poison_statuses = records
        .iter()
        .filter(|record| record.request_id == poison_request_id)
        .map(|record| record.status.as_str())
        .collect::<Vec<_>>();
    let healthy_statuses = records
        .iter()
        .filter(|record| record.request_id == healthy_request_id)
        .map(|record| record.status.as_str())
        .collect::<Vec<_>>();
    assert_eq!(poison_statuses, vec!["completed"]);
    assert_eq!(healthy_statuses, vec!["pending", "completed"]);
    drop(records);

    let attempts = pending_attempts.lock().expect("pending attempts lock");
    assert_eq!(
        attempts.get(poison_request_id).copied(),
        Some(
            (super::PENDING_PERSISTENCE_BATCH_RETRIES_BEFORE_ISOLATION
                + super::PENDING_PERSISTENCE_SINGLE_RETRIES_BEFORE_DEGRADE) as usize
        )
    );
    assert!(
        (1..=3).contains(
            &attempts
                .get(healthy_request_id)
                .copied()
                .expect("healthy pending record should be attempted")
        ),
        "a healthy record may share the failed batch but must need only one isolated write"
    );
    drop(attempts);
    assert_eq!(
        pending_batch_sizes
            .lock()
            .expect("pending batch sizes lock")
            .len(),
        6,
        "batch isolation plus bounded single retries must have fixed write cost"
    );

    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.pending_persistence_batch_failed_total, 5);
    assert_eq!(snapshot.pending_persistence_overflow_total, 1);
    assert!((5..=7).contains(&snapshot.pending_persistence_retried_total));
    assert_eq!(snapshot.pending_persistence_pending, 0);
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
}

#[tokio::test]
async fn permanent_ordered_first_byte_failure_degrades_and_allows_terminal_progress() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let request_id = "req-permanent-ordered-first-byte-failure";
    let remaining_first_byte_panics = Arc::new(AtomicUsize::new(usize::MAX));
    let store = OrderedBatchUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        pending_batch_sizes: Arc::new(Mutex::new(Vec::new())),
        pending_attempts: Arc::new(Mutex::new(BTreeMap::new())),
        permanent_pending_failure_request_id: None,
        remaining_pending_failures: Arc::new(AtomicUsize::new(0)),
        remaining_pending_panics: Arc::new(AtomicUsize::new(0)),
        remaining_first_byte_panics: Arc::clone(&remaining_first_byte_panics),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let seed = build_lifecycle_usage_seed(&terminal_test_plan(request_id), None);

    runtime.record_pending(&store, seed.clone());
    runtime.record_stream_started(
        &store,
        &seed,
        200,
        Some(&ExecutionTelemetry {
            ttfb_ms: Some(9),
            elapsed_ms: Some(12),
            upstream_bytes: Some(1),
        }),
    );
    timeout(
        Duration::from_secs(2),
        runtime.record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        ),
    )
    .await
    .expect("terminal should pass the degraded first-byte barrier");
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if snapshot.first_byte_persistence_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-byte and ordered lifecycle gauges should drain");

    let records = store.records.lock().expect("records lock");
    let statuses = records
        .iter()
        .filter(|record| record.request_id == request_id)
        .map(|record| record.status.as_str())
        .collect::<Vec<_>>();
    assert_eq!(statuses, vec!["pending", "completed"]);
    drop(records);

    assert_eq!(
        remaining_first_byte_panics.load(Ordering::Acquire),
        usize::MAX - 1,
        "an ordered first-byte failure must not spawn an unbounded fallback task"
    );
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.first_byte_persistence_batch_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_direct_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_fallback_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_pending, 0);
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
}

#[tokio::test]
async fn terminal_worker_catches_panic_and_processes_the_next_item() {
    let (sender, receiver) = mpsc::unbounded_channel::<Box<dyn super::TerminalExecutionItem>>();
    let worker = tokio::spawn(super::run_terminal_execution_worker(receiver));
    assert!(
        sender
            .send(Box::new(PanickingTerminalExecutionItem {
                request_id: "req-terminal-worker-direct-panic".to_string(),
            }))
            .is_ok(),
        "the panic item should enter the worker FIFO"
    );

    let (completion, completed) = tokio::sync::oneshot::channel();
    assert!(
        sender
            .send(Box::new(CompletingTerminalExecutionItem {
                request_id: "req-terminal-worker-direct-next".to_string(),
                completion,
            }))
            .is_ok(),
        "the healthy item should enter the same worker FIFO"
    );
    timeout(Duration::from_secs(1), completed)
        .await
        .expect("the worker should continue after the preceding panic")
        .expect("the healthy item should report completion");

    drop(sender);
    timeout(Duration::from_secs(1), worker)
        .await
        .expect("the terminal worker should stop after its sender closes")
        .expect("the terminal worker should exit normally");
}

#[tokio::test]
async fn failed_terminal_persistence_releases_admission_and_allows_later_attempts() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let upsert_attempts = Arc::new(AtomicUsize::new(0));
    let store = FailingWriteQueueConfiguredUsageStore {
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
        upsert_attempts: Arc::clone(&upsert_attempts),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-terminal-persistence-fail-closed";

    runtime
        .submit_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    timeout(Duration::from_secs(1), async {
        while upsert_attempts.load(Ordering::Acquire) != 1
            || runtime.metrics_snapshot().terminal_submission_pending != 0
            || runtime.metrics_snapshot().ordered_lifecycle_pending != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the failed terminal persistence attempt should release admission");

    timeout(
        Duration::from_secs(1),
        runtime.record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        ),
    )
    .await
    .expect("a later terminal attempt must not remain behind a failed write");
    assert_eq!(
        upsert_attempts.load(Ordering::Acquire),
        2,
        "the later direct caller should receive its own bounded write attempt"
    );
    // Direct persistence can finish before the submission worker joins the
    // barrier handoff and accounts for its completed slot.
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            let submission = &runtime.lifecycle_submission.state;
            if snapshot.terminal_submission_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
                && snapshot.lifecycle_submission_pending == 0
                && submission.admission.available_permits() == submission.capacity
            {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("failed terminal submission accounting and admission should drain");
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.terminal_submission_pending, 0);
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
    assert_eq!(snapshot.lifecycle_submission_pending, 0);
}

#[tokio::test]
async fn terminal_worker_panic_releases_admission_without_stopping_the_shard() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let records = Arc::new(Mutex::new(Vec::new()));
    let policy_reads = Arc::new(AtomicUsize::new(0));
    let remaining_policy_panics = Arc::new(AtomicUsize::new(1));
    let store = PanicOncePolicyQueueConfiguredUsageStore {
        inner: CloneQueueConfiguredUsageStore {
            records: Arc::clone(&records),
            queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
        },
        remaining_policy_panics: Arc::clone(&remaining_policy_panics),
        policy_reads: Arc::clone(&policy_reads),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let failed_request_id = "req-terminal-worker-panic-fail-closed";

    runtime
        .submit_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                failed_request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    timeout(Duration::from_secs(1), async {
        while policy_reads.load(Ordering::Acquire) == 0
            || runtime.metrics_snapshot().terminal_submission_pending != 0
            || runtime.metrics_snapshot().ordered_lifecycle_pending != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the first terminal item should panic and release admission");

    timeout(
        Duration::from_secs(1),
        runtime.record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                failed_request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        ),
    )
    .await
    .expect("a later attempt for the panicked request should make progress");

    let healthy_request_id = "req-terminal-worker-panic-healthy";
    runtime
        .submit_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                healthy_request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    runtime
        .record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                healthy_request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    assert_eq!(remaining_policy_panics.load(Ordering::Acquire), 0);
    {
        let records = records.lock().expect("records lock");
        assert_eq!(
            records
                .iter()
                .filter(|record| record.request_id == healthy_request_id)
                .count(),
            2,
            "the same terminal shard should continue processing healthy requests"
        );
        assert!(
            records
                .iter()
                .filter(|record| record.request_id == failed_request_id)
                .count()
                == 1,
            "only the later healthy attempt should persist for the panicked request"
        );
    }
    // The final direct attempt also submits a barrier whose worker may
    // account for completion after the persistence call has returned.
    timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            let submission = &runtime.lifecycle_submission.state;
            if snapshot.terminal_submission_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
                && snapshot.lifecycle_submission_pending == 0
                && submission.admission.available_permits() == submission.capacity
            {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("panicked terminal submission accounting and admission should drain");
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.terminal_submission_pending, 0);
    assert_eq!(snapshot.ordered_lifecycle_pending, 0);
    assert_eq!(snapshot.lifecycle_submission_pending, 0);
}

#[tokio::test]
async fn stale_lifecycle_generation_keeps_the_current_generation_registered() {
    let coalescer = super::LifecycleEventCoalescer::default();
    let request_id = "req-lifecycle-generation";
    let pending_generation = coalescer
        .register(request_id.to_string())
        .await
        .expect("pending generation should register");
    let streaming_generation = coalescer
        .register(request_id.to_string())
        .await
        .expect("streaming generation should register");

    assert!(!coalescer.should_emit(request_id, pending_generation).await);
    assert!(
        coalescer
            .should_emit(request_id, streaming_generation)
            .await
    );
}

#[tokio::test]
async fn lifecycle_submission_panic_releases_admission_and_allows_later_slots() {
    let config = UsageRuntimeConfig {
        enabled: true,
        enqueue_retry_buffer_capacity: 8,
        enqueue_retry_workers: 1,
        worker_record_concurrency_limit: Some(1),
        ..UsageRuntimeConfig::default()
    };
    let tasks = super::UsageBackgroundTasks::default();
    let dispatcher = LifecycleSubmissionDispatcher::spawn(&config, &tasks);
    let request_id = "req-lifecycle-submission-panic";
    dispatcher.dispatch(Box::new(PanickingLifecycleSubmissionItem {
        request_id: request_id.to_string(),
    }));

    timeout(Duration::from_secs(1), async {
        while dispatcher.state.pending.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the panicked lifecycle slot should release its pending gauge");

    let seen = Arc::new(Mutex::new(Vec::new()));
    dispatcher.dispatch(Box::new(TestLifecycleSubmissionItem {
        request_id: request_id.to_string(),
        priority: LifecycleSubmissionPriority::Terminal,
        started: None,
        release: None,
        seen: Arc::clone(&seen),
    }));
    timeout(Duration::from_secs(1), async {
        while dispatcher.state.processed_total.load(Ordering::Acquire) < 2
            || dispatcher.state.pending.load(Ordering::Acquire) != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a later slot should run after the panicked slot releases admission");

    assert_eq!(
        seen.lock().expect("lifecycle test seen lock").as_slice(),
        [LifecycleSubmissionPriority::Terminal]
    );
    assert_eq!(dispatcher.state.enqueued_total.load(Ordering::Acquire), 2);
    assert_eq!(dispatcher.state.processed_total.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn lifecycle_submission_preserves_pending_before_coalesced_first_byte() {
    let config = UsageRuntimeConfig {
        enabled: true,
        enqueue_retry_buffer_capacity: 8,
        enqueue_retry_workers: 1,
        worker_record_concurrency_limit: Some(1),
        ..UsageRuntimeConfig::default()
    };
    let tasks = super::UsageBackgroundTasks::default();
    let dispatcher = LifecycleSubmissionDispatcher::spawn(&config, &tasks);
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));

    dispatcher.dispatch(Box::new(TestLifecycleSubmissionItem {
        request_id: "blocker".to_string(),
        priority: LifecycleSubmissionPriority::Pending,
        started: Some(Arc::clone(&started)),
        release: Some(Arc::clone(&release)),
        seen: Arc::clone(&seen),
    }));
    timeout(Duration::from_secs(1), started.notified())
        .await
        .expect("lifecycle worker should start the blocker");

    dispatcher.dispatch(Box::new(TestLifecycleSubmissionItem {
        request_id: "target".to_string(),
        priority: LifecycleSubmissionPriority::Pending,
        started: None,
        release: None,
        seen: Arc::clone(&seen),
    }));
    dispatcher.dispatch(Box::new(TestLifecycleSubmissionItem {
        request_id: "target".to_string(),
        priority: LifecycleSubmissionPriority::Streaming,
        started: None,
        release: None,
        seen: Arc::clone(&seen),
    }));
    dispatcher.dispatch(Box::new(TestLifecycleSubmissionItem {
        request_id: "target".to_string(),
        priority: LifecycleSubmissionPriority::FirstByte,
        started: None,
        release: None,
        seen: Arc::clone(&seen),
    }));

    assert_eq!(dispatcher.state.pending.load(Ordering::Acquire), 2);
    release.notify_one();
    timeout(Duration::from_secs(1), async {
        loop {
            if seen.lock().expect("lifecycle test seen lock").len() == 3 {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("coalesced lifecycle items should drain");

    let seen = seen.lock().expect("lifecycle test seen lock").clone();
    assert_eq!(
        seen,
        vec![
            LifecycleSubmissionPriority::Pending,
            LifecycleSubmissionPriority::Pending,
            LifecycleSubmissionPriority::FirstByte
        ]
    );
    assert_eq!(dispatcher.state.coalesced_total.load(Ordering::Acquire), 1);
    assert_eq!(dispatcher.state.overflow_total.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn streaming_lifecycle_event_survives_a_superseded_pending_event() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 40,
        stream_key: "usage:events:test:pending-streaming-coalescing".to_string(),
        consumer_group: "usage_consumers_test_pending_streaming_coalescing".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-pending-streaming-coalescing",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5.6-sol".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    sleep(Duration::from_millis(10)).await;
    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Streaming,
                "req-pending-streaming-coalescing",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5.6-sol".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    sleep(Duration::from_millis(90)).await;
    let entries = queue
        .read_group("usage-test-consumer-pending-streaming-coalescing")
        .await
        .expect("queue read should succeed");
    assert_eq!(entries.len(), 1);
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.event_type, UsageEventType::Streaming);
}

#[tokio::test]
async fn pending_lifecycle_enqueue_can_be_delayed() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 30,
        stream_key: "usage:events:test:pending-delay".to_string(),
        consumer_group: "usage_consumers_test_pending_delay".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let event = UsageEvent::new(
        UsageEventType::Pending,
        "req-lifecycle-delay",
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            ..UsageEventData::default()
        },
    );

    runtime.enqueue_or_write_lifecycle(&store, event).await;

    let immediate = queue
        .read_group("usage-test-consumer-delay")
        .await
        .expect("queue read should succeed");
    assert!(
        immediate.is_empty(),
        "lifecycle event should not enqueue before the delay"
    );

    sleep(Duration::from_millis(60)).await;
    let entries = queue
        .read_group("usage-test-consumer-delay")
        .await
        .expect("queue read should succeed");
    assert_eq!(entries.len(), 1);
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.request_id, "req-lifecycle-delay");
    assert_eq!(event.event_type, UsageEventType::Pending);
}

#[tokio::test]
async fn lifecycle_delay_worker_respects_each_item_due_time() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 60,
        stream_key: "usage:events:test:per-item-delay".to_string(),
        consumer_group: "usage_consumers_test_per_item_delay".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-per-item-delay-1",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    sleep(Duration::from_millis(40)).await;
    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-per-item-delay-2",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    sleep(Duration::from_millis(50)).await;
    runtime
        .lifecycle_coalescer
        .cancel("req-per-item-delay-2")
        .await;

    let entries = queue
        .read_group("usage-test-consumer-per-item-delay-first")
        .await
        .expect("queue read should succeed");
    assert_eq!(
        entries.len(),
        1,
        "only the first lifecycle event should be due"
    );
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.request_id, "req-per-item-delay-1");

    let premature_second = queue
        .read_group("usage-test-consumer-per-item-delay-premature-second")
        .await
        .expect("queue read should succeed");
    assert!(
        premature_second.is_empty(),
        "second lifecycle event should not be emitted before its own delay"
    );

    sleep(Duration::from_millis(50)).await;
    let entries = queue
        .read_group("usage-test-consumer-per-item-delay-second")
        .await
        .expect("queue read should succeed");
    assert!(
        entries.is_empty(),
        "second lifecycle event should still be cancellable until its own due time"
    );
}

#[tokio::test]
async fn lifecycle_delay_retains_admission_while_items_reside_in_timer_heap() {
    const CAPACITY: usize = 3;
    const BURST: usize = CAPACITY * 3;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 60_000,
        enqueue_retry_buffer_capacity: CAPACITY,
        ..UsageRuntimeConfig::default()
    };
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let mut accepted = 0;

    for index in 0..BURST {
        let event = UsageEvent::new(
            UsageEventType::Pending,
            format!("req-delay-heap-bound-{index}"),
            UsageEventData {
                provider_name: "openai".to_string(),
                model: "gpt-5".to_string(),
                ..UsageEventData::default()
            },
        );
        if runtime
            .lifecycle_delay
            .schedule(store.clone(), event, index as u64 + 1)
            .await
            .is_ok()
        {
            accepted += 1;
        }
    }
    assert_eq!(accepted, CAPACITY);

    timeout(Duration::from_secs(1), async {
        while runtime
            .lifecycle_delay
            .sender
            .as_ref()
            .expect("delay sender should be enabled")
            .capacity()
            != CAPACITY
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the delay worker should move accepted items into its timer heap");
    assert_eq!(
        runtime.lifecycle_delay.admission.available_permits(),
        0,
        "moving items out of the channel must not release resident-work admission"
    );

    let overflow = UsageEvent::new(
        UsageEventType::Pending,
        "req-delay-heap-overflow",
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            ..UsageEventData::default()
        },
    );
    assert!(
        runtime
            .lifecycle_delay
            .schedule(store, overflow, u64::MAX)
            .await
            .is_err(),
        "timer-heap residents must continue to enforce the hard capacity"
    );
}

#[tokio::test]
async fn older_delayed_generation_does_not_discard_latest_lifecycle_event() {
    let coalescer = LifecycleEventCoalescer::default();
    let request_id = "req-latest-lifecycle-generation";
    let older = coalescer.register(request_id.to_string()).await;
    let latest = coalescer.register(request_id.to_string()).await;

    assert!(
        !coalescer
            .should_emit(request_id, older.expect("older generation"))
            .await,
        "the superseded generation must not be emitted"
    );
    assert!(
        coalescer
            .should_emit(request_id, latest.expect("latest generation"))
            .await,
        "checking an older generation must preserve the latest generation"
    );
}

#[tokio::test]
async fn lifecycle_cleanup_tokens_preserve_newer_first_byte_and_terminal_markers() {
    let coalescer = LifecycleEventCoalescer::default();
    let request_id = "req-lifecycle-cleanup-token";
    let delayed_generation = coalescer
        .register(request_id.to_string())
        .await
        .expect("delayed generation");
    let first_byte_generation = coalescer
        .mark_first_byte(request_id)
        .await
        .expect("first-byte generation");

    coalescer.abandon(request_id, delayed_generation).await;
    assert!(
        coalescer.mark_first_byte(request_id).await.is_none(),
        "abandoning an older delayed item must preserve the first-byte marker"
    );

    coalescer.cancel(request_id).await;
    coalescer
        .rollback_first_byte(request_id, first_byte_generation)
        .await;
    assert!(
        coalescer.register(request_id.to_string()).await.is_none(),
        "rolling back an older first-byte enqueue must preserve the terminal marker"
    );

    let retry_request_id = "req-first-byte-marker-retry";
    let retry_generation = coalescer
        .mark_first_byte(retry_request_id)
        .await
        .expect("retry first-byte generation");
    coalescer
        .rollback_first_byte(retry_request_id, retry_generation)
        .await;
    assert!(
        coalescer.mark_first_byte(retry_request_id).await.is_some(),
        "a rejected first-byte enqueue should be allowed to retry"
    );
}

#[tokio::test]
async fn accepted_first_byte_remains_current_after_coalescer_ttl() {
    let coalescer = LifecycleEventCoalescer::default();
    let request_id = "req-first-byte-persistence-after-ttl";
    let generation = coalescer
        .mark_first_byte(request_id)
        .await
        .expect("first-byte generation");
    {
        let shard = &coalescer.shards[coalescer.shard_index(request_id)];
        let mut entries = shard.entries.lock().await;
        entries
            .get_mut(request_id)
            .expect("first-byte marker")
            .first_byte_seen_at = Some(Instant::now() - Duration::from_secs(31));
    }
    *coalescer.shards[coalescer.shard_index(request_id)]
        .next_compaction_at
        .lock()
        .expect("coalescer compaction deadline") = Some(Instant::now() - Duration::from_secs(1));
    coalescer
        .register("req-trigger-unrelated-compaction".to_string())
        .await
        .expect("unrelated lifecycle generation");

    assert!(
        coalescer
            .first_byte_is_current(request_id, generation)
            .await,
        "the coalescer TTL must not become a persistence deadline"
    );
    coalescer.cancel(request_id).await;
    assert!(
        !coalescer
            .first_byte_is_current(request_id, generation)
            .await,
        "a terminal marker must still cancel the accepted first-byte write"
    );
}

#[tokio::test]
async fn completed_first_byte_marker_expires_after_coalescer_ttl() {
    let coalescer = LifecycleEventCoalescer::default();
    let request_id = "req-first-byte-completed-marker-ttl";
    let generation = coalescer
        .mark_first_byte(request_id)
        .await
        .expect("first-byte generation");
    coalescer.complete_first_byte(request_id, generation).await;
    {
        let shard = &coalescer.shards[coalescer.shard_index(request_id)];
        let mut entries = shard.entries.lock().await;
        entries
            .get_mut(request_id)
            .expect("completed first-byte marker")
            .first_byte_seen_at = Some(Instant::now() - Duration::from_secs(31));
    }

    assert!(
        coalescer.mark_first_byte(request_id).await.is_some(),
        "completed first-byte markers should only deduplicate for the configured TTL"
    );
}

#[tokio::test]
async fn lifecycle_coalescer_hard_bounds_large_burst_without_repeated_compaction() {
    const CAPACITY: usize = 32;
    const BURST: usize = CAPACITY * 4;
    let coalescer = LifecycleEventCoalescer::new(CAPACITY);
    for shard in &coalescer.shards {
        *shard
            .next_compaction_at
            .lock()
            .expect("coalescer compaction deadline") =
            Some(Instant::now() + Duration::from_secs(60));
    }

    let mut accepted = 0;
    for index in 0..BURST {
        if coalescer
            .mark_first_byte(&format!("req-coalescer-burst-{index}"))
            .await
            .is_some()
        {
            accepted += 1;
        }
    }
    assert_eq!(accepted, CAPACITY);
    assert_eq!(coalescer.entry_count.load(Ordering::Acquire), CAPACITY);
    assert_eq!(coalescer.admission.available_permits(), 0);
    assert_eq!(
        coalescer.rejected_total.load(Ordering::Acquire),
        (BURST - CAPACITY) as u64
    );
    assert_eq!(coalescer.compact_total.load(Ordering::Acquire), 0);

    for shard in &coalescer.shards {
        *shard
            .next_compaction_at
            .lock()
            .expect("coalescer compaction deadline") =
            Some(Instant::now() - Duration::from_secs(1));
    }
    let mut compacted_shards = 0_u64;
    let now = Instant::now();
    for (shard_index, shard) in coalescer.shards.iter().enumerate() {
        let mut entries = shard.entries.lock().await;
        compacted_shards += u64::from(!entries.is_empty());
        coalescer.compact_if_due(shard_index, &mut entries, now);
    }

    assert_eq!(
        coalescer.compact_total.load(Ordering::Acquire),
        compacted_shards
    );
    assert_eq!(
        coalescer
            .compact_entries_scanned_total
            .load(Ordering::Acquire),
        CAPACITY as u64
    );
    assert_eq!(
        coalescer.entry_count.load(Ordering::Acquire),
        CAPACITY,
        "in-flight first-byte markers must not be evicted by TTL compaction"
    );
}

#[tokio::test]
async fn terminal_marker_evicts_completed_coalescer_entry_at_capacity() {
    const CAPACITY: usize = 2;
    let coalescer = LifecycleEventCoalescer::new(CAPACITY);
    let first_request_id = "req-coalescer-completed-first";
    let second_request_id = "req-coalescer-completed-second";

    let first_generation = coalescer
        .mark_first_byte(first_request_id)
        .await
        .expect("first completed marker should be admitted");
    coalescer
        .complete_first_byte(first_request_id, first_generation)
        .await;
    let second_generation = coalescer
        .mark_first_byte(second_request_id)
        .await
        .expect("second completed marker should be admitted");
    coalescer
        .complete_first_byte(second_request_id, second_generation)
        .await;
    assert_eq!(coalescer.admission.available_permits(), 0);

    let terminal_request_id = "req-coalescer-terminal-at-capacity";
    coalescer.cancel(terminal_request_id).await;

    let entries = coalescer.shards.iter().fold(0, |count, shard| {
        count
            + shard
                .entries
                .try_lock()
                .expect("coalescer shards should be unlocked after cancellation")
                .len()
    });
    assert_eq!(entries, CAPACITY);
    assert_eq!(coalescer.entry_count.load(Ordering::Acquire), CAPACITY);
    assert_eq!(coalescer.admission.available_permits(), 0);
    assert!(
        coalescer.shards[coalescer.shard_index(terminal_request_id)]
            .entries
            .lock()
            .await
            .contains_key(terminal_request_id),
        "a terminal marker should replace a completed marker when the cap is full"
    );
}

#[tokio::test]
async fn completed_first_byte_markers_do_not_throttle_new_intermediates_at_capacity() {
    const CAPACITY: usize = 2;
    let coalescer = LifecycleEventCoalescer::new(CAPACITY);
    for request_id in ["req-completed-cap-first", "req-completed-cap-second"] {
        let generation = coalescer
            .mark_first_byte(request_id)
            .await
            .expect("completed first-byte marker should be admitted");
        coalescer.complete_first_byte(request_id, generation).await;
    }
    assert_eq!(coalescer.admission.available_permits(), 0);

    assert!(
        coalescer
            .mark_first_byte("req-new-first-byte-at-capacity")
            .await
            .is_some(),
        "a new first byte should replace a completed tombstone"
    );
    assert!(
        coalescer
            .register("req-new-delayed-at-capacity".to_string())
            .await
            .is_some(),
        "a new delayed intermediate should replace the remaining completed tombstone"
    );
    assert_eq!(coalescer.entry_count.load(Ordering::Acquire), CAPACITY);
    assert_eq!(coalescer.admission.available_permits(), 0);
    assert_eq!(coalescer.rejected_total.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn terminal_marker_evicts_delayed_intermediate_at_capacity() {
    const CAPACITY: usize = 2;
    let coalescer = LifecycleEventCoalescer::new(CAPACITY);
    let delayed_request_ids = ["req-delayed-capacity-first", "req-delayed-capacity-second"];
    for request_id in delayed_request_ids {
        coalescer
            .register(request_id.to_string())
            .await
            .expect("delayed intermediate should be admitted");
    }
    assert_eq!(coalescer.admission.available_permits(), 0);

    let terminal_request_id = "req-terminal-replaces-delayed";
    coalescer.cancel(terminal_request_id).await;

    assert!(
        coalescer.shards[coalescer.shard_index(terminal_request_id)]
            .entries
            .lock()
            .await
            .contains_key(terminal_request_id),
        "terminal admission must displace a delayed intermediate when the cap is full"
    );
    let retained_delayed = async {
        let mut retained = 0;
        for request_id in delayed_request_ids {
            if coalescer.shards[coalescer.shard_index(request_id)]
                .entries
                .lock()
                .await
                .contains_key(request_id)
            {
                retained += 1;
            }
        }
        retained
    }
    .await;
    assert_eq!(retained_delayed, CAPACITY - 1);
    assert_eq!(coalescer.entry_count.load(Ordering::Acquire), CAPACITY);
    assert_eq!(coalescer.admission.available_permits(), 0);
}

#[tokio::test]
async fn terminal_marker_does_not_evict_active_terminal_marker() {
    let coalescer = LifecycleEventCoalescer::new(1);
    let active_terminal_request_id = "req-active-terminal-marker";
    coalescer.cancel(active_terminal_request_id).await;

    let rejected_terminal_request_id = "req-terminal-marker-rejected-at-capacity";
    coalescer.cancel(rejected_terminal_request_id).await;

    assert!(
        coalescer.shards[coalescer.shard_index(active_terminal_request_id)]
            .entries
            .lock()
            .await
            .contains_key(active_terminal_request_id),
        "an active terminal marker must retain its TTL protection"
    );
    assert!(
        !coalescer.shards[coalescer.shard_index(rejected_terminal_request_id)]
            .entries
            .lock()
            .await
            .contains_key(rejected_terminal_request_id),
        "a new marker must not replace an active terminal marker"
    );
    assert_eq!(coalescer.entry_count.load(Ordering::Acquire), 1);
    assert_eq!(coalescer.admission.available_permits(), 0);
    assert_eq!(coalescer.rejected_total.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn terminal_event_cancels_delayed_lifecycle_enqueue() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 50,
        stream_key: "usage:events:test:pending-cancel".to_string(),
        consumer_group: "usage_consumers_test_pending_cancel".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-lifecycle-cancel",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    runtime
        .record_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                "req-lifecycle-cancel",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    total_tokens: Some(12),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    sleep(Duration::from_millis(80)).await;
    let entries = queue
        .read_group("usage-test-consumer-cancel")
        .await
        .expect("queue read should succeed");
    assert_eq!(entries.len(), 1);
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.request_id, "req-lifecycle-cancel");
    assert_eq!(event.event_type, UsageEventType::Completed);
}

#[tokio::test]
async fn first_byte_batch_panic_rolls_back_marker_admission() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        enqueue_retry_buffer_capacity: 1,
        ..UsageRuntimeConfig::default()
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-first-byte-marker-panic";
    let generation = runtime
        .lifecycle_coalescer
        .mark_first_byte(request_id)
        .await
        .expect("first-byte marker should be admitted");
    let record = super::build_upsert_usage_record_from_event(&UsageEvent::new(
        UsageEventType::Streaming,
        request_id,
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            first_byte_time_ms: Some(12),
            ..UsageEventData::default()
        },
    ))
    .expect("first-byte record should build");

    runtime
        .first_byte_persistence
        .dispatch(Box::new(PanickingFirstBytePersistenceItem {
            _marker_guard: super::FirstByteMarkerGuard::new(
                Arc::clone(&runtime.lifecycle_coalescer),
                request_id.to_string(),
                generation,
            ),
            record,
        }))
        .await;

    timeout(Duration::from_secs(1), async {
        while runtime
            .first_byte_persistence
            .state
            .pending
            .load(Ordering::Acquire)
            != 0
            || runtime.lifecycle_coalescer.admission.available_permits() != 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("a panicked first-byte batch should release marker admission");
    assert!(!runtime.lifecycle_coalescer.shards
        [runtime.lifecycle_coalescer.shard_index(request_id)]
    .entries
    .lock()
    .await
    .contains_key(request_id));
    assert!(
        runtime
            .lifecycle_coalescer
            .mark_first_byte("req-first-byte-after-panic")
            .await
            .is_some(),
        "new first-byte work should be admitted after panic cleanup"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn non_batch_first_byte_writes_use_configured_bounded_concurrency() {
    const CONCURRENCY: usize = 16;
    const REQUESTS: usize = CONCURRENCY;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(CONCURRENCY),
        enqueue_retry_buffer_capacity: REQUESTS * 2,
        ..UsageRuntimeConfig::default()
    };
    let store = AdmissionBlockedUsageStore {
        release_writes: Arc::new(tokio::sync::Semaphore::new(0)),
        writes_in_flight: Arc::new(AtomicUsize::new(0)),
        max_writes_in_flight: Arc::new(AtomicUsize::new(0)),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    assert!(!UsageRecordWriter::supports_first_byte_usage_batch(&store));
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    for index in 0..REQUESTS {
        let request_id = format!("req-non-batch-first-byte-{index}");
        let seed = build_lifecycle_usage_seed(&terminal_test_plan(&request_id), None);
        runtime.record_stream_started(
            &store,
            &seed,
            200,
            Some(&ExecutionTelemetry {
                ttfb_ms: Some(12),
                elapsed_ms: Some(20),
                upstream_bytes: Some(1),
            }),
        );
    }

    timeout(Duration::from_secs(2), async {
        while store.max_writes_in_flight.load(Ordering::Acquire) != CONCURRENCY {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("non-batch first-byte writes should fill configured concurrency");
    assert_eq!(store.writes_in_flight.load(Ordering::Acquire), CONCURRENCY);
    assert_eq!(
        runtime
            .worker_record_gate
            .as_ref()
            .expect("record gate should be configured")
            .max_in_flight(),
        CONCURRENCY
    );

    store.release_writes.add_permits(REQUESTS);
    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if store.writes_completed.load(Ordering::Acquire) == REQUESTS
                && snapshot.lifecycle_submission_pending == 0
                && snapshot.ordered_lifecycle_pending == 0
                && snapshot.first_byte_persistence_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("non-batch first-byte writes should drain after release");
    assert_eq!(store.writes_in_flight.load(Ordering::Acquire), 0);
    assert_eq!(
        store.max_writes_in_flight.load(Ordering::Acquire),
        CONCURRENCY
    );
}

#[tokio::test]
async fn first_byte_dispatcher_batches_records_for_one_writer() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(4),
        stream_key: "usage:events:test:first-byte-batch".to_string(),
        consumer_group: "usage_consumers_test_first_byte_batch".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = BatchingQueueUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    for index in 0..32 {
        runtime
            .enqueue_or_write_lifecycle(
                &store,
                UsageEvent::new(
                    UsageEventType::Streaming,
                    format!("req-first-byte-batch-{index}"),
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        first_byte_time_ms: Some(12 + index as u64),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    }

    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            if snapshot.first_byte_persistence_pending == 0
                && snapshot.first_byte_persistence_direct_succeeded_total == 32
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-byte batch dispatcher should drain");

    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 32);
    let snapshot = runtime.metrics_snapshot();
    assert!(snapshot.first_byte_persistence_batch_flush_total >= 1);
    assert_eq!(snapshot.first_byte_persistence_batch_records_total, 32);
    assert!(snapshot.first_byte_persistence_max_batch_size >= 2);
    assert_eq!(snapshot.first_byte_persistence_batch_failed_total, 0);
}

#[tokio::test]
async fn stream_started_direct_with_first_byte_bypasses_queue_backlog_and_lifecycle_delay() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 30,
        stream_key: "usage:events:test:stream-direct-delay".to_string(),
        consumer_group: "usage_consumers_test_stream_direct_delay".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let plan = ExecutionPlan {
        request_id: "req-stream-direct-delay".to_string(),
        candidate_id: Some("cand-stream-direct-delay".to_string()),
        provider_name: Some("openai".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/chat/completions".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };
    let lifecycle_seed = build_lifecycle_usage_seed(&plan, None);

    runtime
        .record_stream_started_direct(
            &store,
            &lifecycle_seed,
            200,
            Some(&ExecutionTelemetry {
                ttfb_ms: Some(12),
                elapsed_ms: Some(34),
                upstream_bytes: Some(56),
            }),
        )
        .await;

    timeout(Duration::from_secs(1), async {
        while store.records.lock().expect("records lock").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-byte transition should be persisted directly");
    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].request_id, "req-stream-direct-delay");
        assert_eq!(records[0].status, "streaming");
        assert_eq!(records[0].first_byte_time_ms, Some(12));
    }

    sleep(Duration::from_millis(60)).await;
    let queued = queue
        .read_group("usage-test-consumer-stream-direct-delay-after-wait")
        .await
        .expect("queue read should succeed");
    assert!(
        queued.is_empty(),
        "a successful first-byte fast-path write must bypass the ordinary queue"
    );
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.first_byte_persistence_pending, 0);
    assert_eq!(snapshot.first_byte_persistence_direct_succeeded_total, 1);
    assert_eq!(snapshot.first_byte_persistence_direct_failed_total, 0);
}

#[tokio::test]
async fn stream_started_without_first_byte_keeps_lifecycle_delay() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 30,
        stream_key: "usage:events:test:stream-no-first-byte-delay".to_string(),
        consumer_group: "usage_consumers_test_stream_no_first_byte_delay".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let event = UsageEvent::new(
        UsageEventType::Streaming,
        "req-stream-no-first-byte-delay",
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            status_code: Some(200),
            first_byte_time_ms: None,
            ..UsageEventData::default()
        },
    );

    runtime.enqueue_or_write_lifecycle(&store, event).await;

    let immediate = queue
        .read_group("usage-test-consumer-stream-no-first-byte-immediate")
        .await
        .expect("queue read should succeed");
    assert!(
        immediate.is_empty(),
        "pre-first-byte streaming event should still be coalesced"
    );

    sleep(Duration::from_millis(60)).await;
    let entries = queue
        .read_group("usage-test-consumer-stream-no-first-byte-delayed")
        .await
        .expect("queue read should succeed");
    assert_eq!(entries.len(), 1);
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.request_id, "req-stream-no-first-byte-delay");
    assert_eq!(event.event_type, UsageEventType::Streaming);
    assert_eq!(event.data.first_byte_time_ms, None);
}

#[tokio::test]
async fn first_byte_transition_supersedes_delayed_pending_event() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 40,
        stream_key: "usage:events:test:first-byte-supersedes-pending".to_string(),
        consumer_group: "usage_consumers_test_first_byte_supersedes_pending".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-first-byte-supersedes-pending",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Streaming,
                "req-first-byte-supersedes-pending",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    first_byte_time_ms: Some(12),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    timeout(Duration::from_secs(1), async {
        while store.records.lock().expect("records lock").is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-byte transition should be persisted directly");
    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, "streaming");
        assert_eq!(records[0].first_byte_time_ms, Some(12));
    }

    sleep(Duration::from_millis(80)).await;
    let delayed = queue
        .read_group("usage-test-consumer-first-byte-after-delay")
        .await
        .expect("queue read should succeed");
    assert!(
        delayed.is_empty(),
        "the delayed pending event must not overwrite the first-byte transition"
    );
}

#[tokio::test]
async fn first_byte_transition_uses_queue_when_node_has_no_usage_writer() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 1_000,
        stream_key: "usage:events:test:first-byte-queue-only-node".to_string(),
        consumer_group: "usage_consumers_test_first_byte_queue_only_node".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let upsert_attempts = Arc::new(AtomicUsize::new(0));
    let store = QueueOnlyUsageStore {
        queue: queue_runner,
        upsert_attempts: Arc::clone(&upsert_attempts),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Streaming,
                "req-first-byte-queue-only-node",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    first_byte_time_ms: Some(12),
                    request_metadata: Some(json!({
                        "trace_id": "trace-queue-only-first-byte",
                        "billing_snapshot": {"source": "original-event"}
                    })),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    let entries = timeout(Duration::from_secs(1), async {
        loop {
            let entries = queue
                .read_group("usage-test-consumer-first-byte-queue-only-node")
                .await
                .expect("queue read should succeed");
            if !entries.is_empty() {
                break entries;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first-byte transition should be queued");
    assert_eq!(entries.len(), 1);
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.event_type, UsageEventType::Streaming);
    assert_eq!(
        event.data.request_metadata,
        Some(json!({
            "trace_id": "trace-queue-only-first-byte",
            "billing_snapshot": {"source": "original-event"}
        })),
        "queue-only nodes must preserve the original lifecycle event"
    );
    assert_eq!(upsert_attempts.load(Ordering::Acquire), 0);
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.first_byte_persistence_pending, 0);
    assert_eq!(snapshot.first_byte_persistence_fallback_accepted_total, 1);
    assert_eq!(snapshot.first_byte_persistence_fallback_failed_total, 0);
}

#[tokio::test]
async fn terminal_cancellation_drops_buffered_first_byte_write() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        worker_record_concurrency_limit: Some(1),
        stream_key: "usage:events:test:first-byte-terminal-cancel".to_string(),
        consumer_group: "usage_consumers_test_first_byte_terminal_cancel".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let write_started = Arc::new(tokio::sync::Notify::new());
    let release_writes = Arc::new(tokio::sync::Notify::new());
    let writes_completed = Arc::new(AtomicUsize::new(0));
    let store = BlockingWriteQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
        write_started: Arc::clone(&write_started),
        release_writes: Arc::clone(&release_writes),
        writes_completed: Arc::clone(&writes_completed),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let first_write_started = write_started.notified();

    let streaming_event = |request_id| {
        UsageEvent::new(
            UsageEventType::Streaming,
            request_id,
            UsageEventData {
                provider_name: "openai".to_string(),
                model: "gpt-5".to_string(),
                status_code: Some(200),
                first_byte_time_ms: Some(12),
                ..UsageEventData::default()
            },
        )
    };
    runtime
        .enqueue_or_write_lifecycle(&store, streaming_event("req-first-byte-blocking"))
        .await;
    timeout(Duration::from_secs(1), first_write_started)
        .await
        .expect("first fast-path write should start");
    runtime
        .enqueue_or_write_lifecycle(&store, streaming_event("req-first-byte-cancelled"))
        .await;
    runtime
        .lifecycle_coalescer
        .cancel("req-first-byte-cancelled")
        .await;
    release_writes.notify_one();
    timeout(Duration::from_secs(1), async {
        while writes_completed.load(Ordering::Acquire) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("first fast-path write should finish");
    sleep(Duration::from_millis(50)).await;
    assert_eq!(writes_completed.load(Ordering::Acquire), 1);
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.first_byte_persistence_pending, 0);
    assert_eq!(snapshot.first_byte_persistence_cancelled_total, 1);
}

#[tokio::test]
async fn queued_terminal_keeps_inflight_first_byte_persistence_alive() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        queue_lifecycle_events: true,
        stream_key: "usage:events:test:first-byte-queued-terminal".to_string(),
        consumer_group: "usage_consumers_test_first_byte_queued_terminal".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let write_started = Arc::new(tokio::sync::Notify::new());
    let release_writes = Arc::new(tokio::sync::Notify::new());
    let writes_completed = Arc::new(AtomicUsize::new(0));
    let store = BlockingWriteQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
        write_started: Arc::clone(&write_started),
        release_writes: Arc::clone(&release_writes),
        writes_completed: Arc::clone(&writes_completed),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-first-byte-queued-terminal";

    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Streaming,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    first_byte_time_ms: Some(12),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    timeout(Duration::from_secs(1), write_started.notified())
        .await
        .expect("first-byte write should start");

    runtime
        .enqueue_or_write_terminal(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    release_writes.notify_one();

    timeout(Duration::from_secs(1), async {
        while writes_completed.load(Ordering::Acquire) != 1
            || runtime
                .metrics_snapshot()
                .first_byte_persistence_direct_succeeded_total
                != 1
            || runtime.metrics_snapshot().first_byte_persistence_pending != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued terminal must not cancel the in-flight first-byte write");
    assert_eq!(
        runtime
            .metrics_snapshot()
            .first_byte_persistence_direct_succeeded_total,
        1
    );
    let entries = queue
        .read_group("usage-test-first-byte-queued-terminal")
        .await
        .expect("queued terminal should be readable");
    assert_eq!(entries.len(), 1);
    let event =
        UsageEvent::from_stream_fields(&entries[0].fields).expect("queued terminal should parse");
    assert_eq!(event.event_type, UsageEventType::Completed);
}

#[tokio::test]
async fn first_byte_append_failure_is_retried_locally() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 1_000,
        retry_deferred_lifecycle_events: true,
        stream_key: "usage:events:test:first-byte-retry".to_string(),
        consumer_group: "usage_consumers_test_first_byte_retry".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: 16,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&inner_queue), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let flaky_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(FlakyAppendQueueStore::new(Arc::clone(&inner_queue), 1).with_append_delay_ms(200));
    let upsert_attempts = Arc::new(AtomicUsize::new(0));
    let store = FailingWriteQueueConfiguredUsageStore {
        queue: flaky_queue,
        upsert_attempts: Arc::clone(&upsert_attempts),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_metadata = json!({
        "trace_id": "trace-first-byte-retry",
        "upstream_is_stream": true,
        "billing_snapshot": {"status": "pending", "source": "full-event"},
        "stage_timings_ms": {"planning": 7}
    });

    timeout(
        Duration::from_millis(100),
        runtime.enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Streaming,
                "req-first-byte-retry",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    first_byte_time_ms: Some(12),
                    request_headers: Some(json!({"x-request-id": "full-event"})),
                    request_body: Some(json!({"messages": [{"content": "hello"}]})),
                    request_body_state: Some(UsageBodyCaptureState::Inline),
                    provider_request_headers: Some(json!({"x-provider": "full-event"})),
                    provider_request_body: Some(json!({"model": "gpt-5"})),
                    provider_request_body_state: Some(UsageBodyCaptureState::Inline),
                    response_headers: Some(json!({"content-type": "text/event-stream"})),
                    response_body_state: Some(UsageBodyCaptureState::Unavailable),
                    request_metadata: Some(request_metadata.clone()),
                    ..UsageEventData::default()
                },
            ),
        ),
    )
    .await
    .expect("first-byte submission must not wait for the Redis append");

    wait_for_enqueue_dispatcher_to_drain(&runtime, 1).await;
    assert_eq!(upsert_attempts.load(Ordering::Acquire), 1);
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.first_byte_persistence_batch_failed_total, 1);
    assert_eq!(snapshot.first_byte_persistence_direct_failed_total, 1);
    let entries = queue
        .read_group("usage-test-consumer-first-byte-retry")
        .await
        .expect("queue read should succeed");
    assert_eq!(entries.len(), 1);
    let event = UsageEvent::from_stream_fields(&entries[0].fields)
        .expect("queued usage event should parse");
    assert_eq!(event.event_type, UsageEventType::Streaming);
    assert_eq!(event.data.first_byte_time_ms, Some(12));
    assert_eq!(
        event.data.request_headers,
        Some(json!({"x-request-id": "full-event"}))
    );
    assert_eq!(
        event.data.request_body,
        Some(json!({"messages": [{"content": "hello"}]}))
    );
    assert_eq!(
        event.data.request_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
    assert_eq!(
        event.data.provider_request_headers,
        Some(json!({"x-provider": "full-event"}))
    );
    assert_eq!(
        event.data.provider_request_body,
        Some(json!({"model": "gpt-5"}))
    );
    assert_eq!(
        event.data.provider_request_body_state,
        Some(UsageBodyCaptureState::Inline)
    );
    assert_eq!(
        event.data.response_headers,
        Some(json!({"content-type": "text/event-stream"}))
    );
    assert_eq!(
        event.data.response_body_state,
        Some(UsageBodyCaptureState::Unavailable)
    );
    assert_eq!(event.data.request_metadata, Some(request_metadata));
}

#[tokio::test]
async fn direct_terminal_event_cancels_delayed_lifecycle_enqueue() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 50,
        stream_key: "usage:events:test:direct-terminal-cancel".to_string(),
        consumer_group: "usage_consumers_test_direct_terminal_cancel".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    runtime
        .enqueue_or_write_lifecycle(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-direct-terminal-cancel",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    runtime
        .record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                "req-direct-terminal-cancel",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    total_tokens: Some(12),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    sleep(Duration::from_millis(80)).await;
    let entries = queue
        .read_group("usage-test-consumer-direct-terminal-cancel")
        .await
        .expect("queue read should succeed");
    assert!(
        entries.is_empty(),
        "direct terminal usage should cancel delayed lifecycle queue writes"
    );
    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_id, "req-direct-terminal-cancel");
    assert_eq!(records[0].status, "completed");
}

#[test]
fn spawn_workers_uses_configured_worker_count() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_count: 3,
        stream_key: "usage:events:test:worker-count".to_string(),
        consumer_group: "usage_consumers_test_worker_count".to_string(),
        ..UsageRuntimeConfig::default()
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };

    let handles = runtime.spawn_workers(Arc::new(store));
    assert_eq!(handles.len(), 3);
    for handle in handles {
        handle.abort();
    }
}

#[test]
fn worker_supervisor_state_records_worker_observations() {
    let state = UsageWorkerSupervisorState::default();

    state.record_observation(UsageWorkerObservation {
        worker_index: Some(1),
        entries_read: 2,
        batch_size: 16,
        reclaimed_entries: 1,
        acked_entries: 2,
        dead_lettered_entries: 1,
        process_failures: 1,
        read_failures: 1,
        reclaim_failures: 1,
    });

    assert_eq!(state.read_batches_total.load(Ordering::Acquire), 1);
    assert_eq!(state.read_entries_total.load(Ordering::Acquire), 2);
    assert_eq!(state.reclaimed_entries_total.load(Ordering::Acquire), 1);
    assert_eq!(state.acked_entries_total.load(Ordering::Acquire), 2);
    assert_eq!(state.dead_lettered_entries_total.load(Ordering::Acquire), 1);
    assert_eq!(state.process_failures_total.load(Ordering::Acquire), 1);
    assert_eq!(state.read_failures_total.load(Ordering::Acquire), 1);
    assert_eq!(state.reclaim_failures_total.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn worker_supervisor_scales_up_when_reads_stay_full() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_count: 1,
        worker_autoscale_enabled: true,
        worker_max_count: 4,
        worker_scale_interval_ms: 10,
        worker_idle_scale_down_ticks: 100,
        stream_key: "usage:events:test:worker-autoscale-up".to_string(),
        consumer_group: "usage_consumers_test_worker_autoscale_up".to_string(),
        consumer_batch_size: 1,
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    for index in 0..32 {
        queue
            .enqueue(&UsageEvent::new(
                UsageEventType::Completed,
                format!("req-worker-autoscale-up-{index}"),
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    total_tokens: Some(12),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ))
            .await
            .expect("usage event should enqueue");
    }
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    let supervisor = runtime
        .spawn_worker_supervisor(Arc::new(store))
        .expect("supervisor should spawn");
    for _ in 0..100 {
        let snapshot = runtime.metrics_snapshot();
        if snapshot.worker_desired_count > 1 {
            supervisor.abort();
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    supervisor.abort();

    let snapshot = runtime.metrics_snapshot();
    assert!(
        snapshot.worker_desired_count > 1,
        "usage worker supervisor should scale up after repeated full reads: {snapshot:?}"
    );
}

#[tokio::test]
async fn worker_supervisor_replaces_worker_after_panic() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_count: 1,
        worker_autoscale_enabled: false,
        worker_max_count: 1,
        worker_scale_interval_ms: 10,
        stream_key: "usage:events:test:worker-panic-recovery".to_string(),
        consumer_group: "usage_consumers_test_worker_panic_recovery".to_string(),
        consumer_batch_size: 1,
        consumer_block_ms: 1,
        reclaim_idle_ms: 60_000,
        reclaim_interval_ms: 60_000,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    queue
        .enqueue(&UsageEvent::new(
            UsageEventType::Completed,
            "req-worker-panic-first",
            UsageEventData {
                provider_name: "openai".to_string(),
                model: "gpt-5".to_string(),
                total_tokens: Some(12),
                status_code: Some(200),
                ..UsageEventData::default()
            },
        ))
        .await
        .expect("first usage event should enqueue");

    let records = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(PanicOnceQueueConfiguredUsageStore {
        inner: CloneQueueConfiguredUsageStore {
            records: Arc::clone(&records),
            queue: queue_runner,
        },
        remaining_panics: AtomicUsize::new(1),
    });
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let supervisor = runtime
        .spawn_worker_supervisor(Arc::clone(&store))
        .expect("supervisor should spawn");

    for _ in 0..100 {
        if store.remaining_panics.load(Ordering::Acquire) == 0 {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        store.remaining_panics.load(Ordering::Acquire),
        0,
        "first worker should panic while processing the first event"
    );

    queue
        .enqueue(&UsageEvent::new(
            UsageEventType::Completed,
            "req-worker-panic-second",
            UsageEventData {
                provider_name: "openai".to_string(),
                model: "gpt-5".to_string(),
                total_tokens: Some(24),
                status_code: Some(200),
                ..UsageEventData::default()
            },
        ))
        .await
        .expect("second usage event should enqueue");

    for _ in 0..100 {
        let recorded_second = records
            .lock()
            .expect("records lock")
            .iter()
            .any(|record| record.request_id == "req-worker-panic-second");
        if recorded_second {
            supervisor.abort();
            return;
        }
        sleep(Duration::from_millis(10)).await;
    }
    supervisor.abort();

    let records = records.lock().expect("records lock");
    assert!(
        records
            .iter()
            .any(|record| record.request_id == "req-worker-panic-second"),
        "replacement worker should consume events after the first worker panics: {records:?}"
    );
}

#[tokio::test]
async fn ordered_pending_bypasses_lifecycle_queue_append_failure() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        lifecycle_enqueue_delay_ms: 0,
        retry_deferred_lifecycle_events: false,
        stream_key: "usage:events:test:lifecycle-failure".to_string(),
        consumer_group: "usage_consumers_test_lifecycle_failure".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: 16,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue: Arc<dyn RuntimeQueueStore> = Arc::new(FlakyAppendQueueStore::new(
        Arc::clone(&inner_queue),
        usize::MAX,
    ));
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: flaky_queue,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let plan = ExecutionPlan {
        request_id: "req-lifecycle-queue-failure-1".to_string(),
        candidate_id: Some("cand-lifecycle-queue-failure-1".to_string()),
        provider_name: Some("openai".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    runtime.record_pending(&store, build_lifecycle_usage_seed(&plan, None));
    sleep(Duration::from_millis(50)).await;

    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, "pending");
    drop(records);
    assert_eq!(
        runtime.metrics_snapshot().lifecycle_enqueue_failed_total,
        0,
        "ordered pending persistence should not attempt the lifecycle queue"
    );
}

#[tokio::test]
async fn lifecycle_enqueue_failure_opens_short_circuit() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        retry_deferred_lifecycle_events: false,
        stream_key: "usage:events:test:lifecycle-circuit".to_string(),
        consumer_group: "usage_consumers_test_lifecycle_circuit".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue = Arc::new(FlakyAppendQueueStore::new(
        Arc::clone(&inner_queue),
        usize::MAX,
    ));
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: flaky_queue.clone(),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    for index in 0..10 {
        let event = UsageEvent::new(
            UsageEventType::Pending,
            format!("req-lifecycle-circuit-{index}"),
            UsageEventData {
                provider_name: "openai".to_string(),
                model: "gpt-5".to_string(),
                ..UsageEventData::default()
            },
        );
        runtime.enqueue_lifecycle_event(&store, event).await;
    }

    assert_eq!(
        flaky_queue.append_attempts.load(Ordering::Acquire),
        1,
        "lifecycle enqueue circuit should prevent repeated Redis appends after a failure"
    );
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(
        snapshot.lifecycle_enqueue_failed_total, 1,
        "first append failure should open the lifecycle enqueue circuit"
    );
    assert_eq!(
        snapshot.lifecycle_enqueue_deferred_dropped_total, 10,
        "the failed primary event and circuit-deferred events should be counted as dropped"
    );
    assert_eq!(
        snapshot.enqueue_retry_scheduled_total, 0,
        "default lifecycle overload policy must not schedule local enqueue retries"
    );
    assert!(
        store.records.lock().expect("records lock").is_empty(),
        "lifecycle enqueue circuit must not fall back to direct DB writes"
    );
}

#[tokio::test]
async fn lifecycle_deferred_retry_can_be_enabled() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        retry_deferred_lifecycle_events: true,
        stream_key: "usage:events:test:lifecycle-deferred-retry".to_string(),
        consumer_group: "usage_consumers_test_lifecycle_deferred_retry".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: 16,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue = Arc::new(FlakyAppendQueueStore::new(Arc::clone(&inner_queue), 1));
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: flaky_queue,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    runtime
        .enqueue_lifecycle_event(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-lifecycle-deferred-retry-open",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    runtime
        .enqueue_lifecycle_event(
            &store,
            UsageEvent::new(
                UsageEventType::Pending,
                "req-lifecycle-deferred-retry-scheduled",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.lifecycle_enqueue_deferred_retry_total, 2);
    assert_eq!(snapshot.lifecycle_enqueue_deferred_dropped_total, 0);
    assert_eq!(
        snapshot.enqueue_retry_scheduled_total, 2,
        "the initial append failure and the circuit-deferred event should both retry"
    );
}

#[tokio::test]
async fn lifecycle_retry_buffer_full_drops_without_waiting_for_capacity() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: true,
        enqueue_retry_buffer_capacity: 1,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(runner, config.clone()).expect("usage queue should build");
    let (sender, _receiver) = mpsc::channel(1);
    let dispatcher = UsageEnqueueRetryDispatcher {
        senders: vec![sender],
        metrics: Arc::new(Default::default()),
    };
    let event = |request_id: &str| {
        UsageEvent::new(
            UsageEventType::Streaming,
            request_id,
            UsageEventData {
                provider_name: "openai".to_string(),
                model: "gpt-5".to_string(),
                first_byte_time_ms: Some(12),
                ..UsageEventData::default()
            },
        )
    };

    assert!(dispatcher.schedule(
        queue.clone(),
        event("req-lifecycle-retry-buffer-fill"),
        "lifecycle",
        DataLayerError::Redis("forced failure".to_string()),
    ));
    let started_at = std::time::Instant::now();
    let accepted = dispatcher.schedule(
        queue,
        event("req-lifecycle-retry-buffer-overflow"),
        "lifecycle",
        DataLayerError::Redis("forced failure".to_string()),
    );
    assert!(
        started_at.elapsed() < Duration::from_millis(50),
        "retry submission must remain non-blocking"
    );
    assert!(!accepted);
    assert_eq!(dispatcher.scheduled_total(), 1);
    assert_eq!(dispatcher.pending(), 1);
    assert_eq!(dispatcher.closed_or_unavailable_total(), 1);
}

fn event_capture_budget_retry_event(
    budget: &Arc<crate::event_capture_budget::EventCaptureMemoryBudget>,
) -> UsageEvent {
    let mut event = UsageEvent {
        event_type: UsageEventType::Failed,
        request_id: "event-capture-budget-retry".to_string(),
        timestamp_ms: 123_000,
        data: UsageEventData {
            provider_name: "provider".to_string(),
            model: "model".to_string(),
            input_tokens: Some(100),
            output_tokens: Some(500),
            total_tokens: Some(600),
            cache_read_input_tokens: Some(0),
            status_code: Some(502),
            error_category: Some("upstream_error".to_string()),
            error_message: Some("upstream failed".to_string()),
            response_body: Some(json!({"diagnostic": "x".repeat(1000)})),
            response_body_state: Some(UsageBodyCaptureState::Inline),
            request_metadata: Some(json!({
                "plan_usage_reservation_token": "550e8400-e29b-41d4-a716-446655440000",
                "provider_service_tier": "priority",
                "provider_cache_ttl_minutes": 60
            })),
            ..UsageEventData::default()
        },
    };
    event.data.apply_capture_memory_budget(Arc::clone(budget));
    event
}

#[tokio::test]
async fn event_capture_budget_bounds_blocked_policy_waiters_and_releases_on_cancel_or_basic() {
    for limit in [0, 64 * 1024] {
        let runtime = UsageRuntime::new(UsageRuntimeConfig::default()).expect("runtime");
        let store = BlockingPolicyQueueConfiguredUsageStore::new(Arc::new(RuntimeState::memory(
            MemoryRuntimeStateConfig::default(),
        )));
        let budget = Arc::new(crate::event_capture_budget::EventCaptureMemoryBudget::new(
            limit,
        ));
        let start = || {
            let runtime = runtime.clone();
            let store = store.clone();
            let budget = Arc::clone(&budget);
            tokio::spawn(async move {
                let mut event = UsageEvent::new(
                    UsageEventType::Completed,
                    "event-capture-budget-policy",
                    UsageEventData {
                        input_tokens: Some(100),
                        output_tokens: Some(500),
                        response_body: Some(json!("x".repeat(40 * 1024))),
                        ..UsageEventData::default()
                    },
                );
                runtime
                    .apply_body_capture_policy_with_budget(&store, &mut event, budget)
                    .await;
                event
            })
        };
        let reading = start();
        timeout(Duration::from_secs(2), store.policy_started.notified())
            .await
            .expect("policy read is blocked");
        let retained = budget.retained_bytes();
        assert_eq!(retained == 0, limit == 0);
        assert!(retained <= limit);
        let previous_denials = budget.downgraded_total();
        let waiting = start();
        timeout(Duration::from_secs(2), async {
            while budget.downgraded_total() == previous_denials {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("waiting event must apply budget before acquiring policy mutex");
        assert_eq!(store.policy_reads.load(Ordering::Acquire), 1);
        assert_eq!(budget.retained_bytes(), retained);
        waiting.abort();
        assert!(waiting
            .await
            .expect_err("waiting task aborted")
            .is_cancelled());
        assert_eq!(budget.retained_bytes(), retained);
        reading.abort();
        assert!(reading
            .await
            .expect_err("reading task aborted")
            .is_cancelled());
        assert_eq!(budget.retained_bytes(), 0);

        let completing = start();
        timeout(Duration::from_secs(2), store.policy_started.notified())
            .await
            .expect("replacement policy read starts");
        assert_eq!(budget.retained_bytes(), retained);
        store.release_blocked_policy();
        let event = timeout(Duration::from_secs(2), completing)
            .await
            .expect("Basic policy completes")
            .expect("policy task completion");
        assert!(event.data.response_body.is_none());
        assert_eq!(
            event.data.response_body_state,
            Some(UsageBodyCaptureState::Disabled)
        );
        assert_eq!(event.data.input_tokens, Some(100));
        assert_eq!(event.data.output_tokens, Some(500));
        assert_eq!(
            budget.retained_bytes(),
            0,
            "Basic returns the lease while the event is still alive"
        );
    }
}

#[tokio::test]
async fn event_capture_budget_retry_holds_lease_and_preserves_event_after_recovery() {
    for limit in [0, 64 * 1024] {
        let config = UsageRuntimeConfig {
            enabled: true,
            queue_terminal_events: true,
            consumer_block_ms: 1,
            enqueue_retry_initial_backoff_ms: 1,
            enqueue_retry_max_backoff_ms: 2,
            ..UsageRuntimeConfig::default()
        };
        let inner: Arc<dyn RuntimeQueueStore> =
            Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
        let flaky = Arc::new(FlakyAppendQueueStore::new(inner, usize::MAX));
        let queue = UsageQueue::new(flaky.clone(), config.clone()).expect("retry queue");
        queue.ensure_consumer_group().await.expect("consumer group");
        let budget = Arc::new(crate::event_capture_budget::EventCaptureMemoryBudget::new(
            limit,
        ));
        let event = event_capture_budget_retry_event(&budget);
        let expected_fields = event.to_stream_fields().expect("expected wire event");
        let retained = budget.retained_bytes();
        assert_eq!(retained == 0, limit == 0);
        let (sender, receiver) = mpsc::channel(1);
        let metrics = Arc::new(super::UsageEnqueueDispatcherMetrics::default());
        let dispatcher = UsageEnqueueRetryDispatcher {
            senders: vec![sender],
            metrics: Arc::clone(&metrics),
        };
        assert!(dispatcher.schedule(
            queue.clone(),
            event,
            "terminal",
            DataLayerError::Redis("initial failure".to_string())
        ));
        let worker = tokio::spawn(super::run_usage_enqueue_retry_worker(
            0,
            config,
            receiver,
            Arc::clone(&metrics),
        ));
        timeout(Duration::from_secs(2), async {
            while metrics.retry_failed_total.load(Ordering::Acquire) < 2 {
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("two retry failures");
        assert_eq!(budget.retained_bytes(), retained);
        assert_eq!(dispatcher.pending(), 1);
        flaky.remaining_failures.store(0, Ordering::Release);
        drop(dispatcher);
        timeout(Duration::from_secs(2), worker)
            .await
            .expect("retry recovery")
            .expect("worker completion");
        assert_eq!(budget.retained_bytes(), 0);
        assert_eq!(metrics.pending.load(Ordering::Acquire), 0);
        let entries = queue
            .read_group("capture-budget-consumer")
            .await
            .expect("persisted queue");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].fields, expected_fields,
            "retry must preserve identity, terminal failure, billing, and truncation metadata"
        );
    }
}

#[tokio::test]
async fn event_capture_budget_retry_cancellation_releases_owned_body() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 2,
        ..UsageRuntimeConfig::default()
    };
    let inner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky = Arc::new(FlakyAppendQueueStore::new(inner, usize::MAX));
    let queue = UsageQueue::new(flaky, config.clone()).expect("retry queue");
    let budget = Arc::new(crate::event_capture_budget::EventCaptureMemoryBudget::new(
        64 * 1024,
    ));
    let event = event_capture_budget_retry_event(&budget);
    let (sender, receiver) = mpsc::channel(1);
    let metrics = Arc::new(super::UsageEnqueueDispatcherMetrics::default());
    let dispatcher = UsageEnqueueRetryDispatcher {
        senders: vec![sender],
        metrics: Arc::clone(&metrics),
    };
    assert!(dispatcher.schedule(
        queue,
        event,
        "terminal",
        DataLayerError::Redis("failure".to_string())
    ));
    let worker = tokio::spawn(super::run_usage_enqueue_retry_worker(
        0,
        config,
        receiver,
        Arc::clone(&metrics),
    ));
    timeout(Duration::from_secs(2), async {
        while metrics.retry_failed_total.load(Ordering::Acquire) == 0 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("retry starts");
    assert!(budget.retained_bytes() > 0);
    worker.abort();
    assert!(worker.await.expect_err("worker was aborted").is_cancelled());
    assert_eq!(budget.retained_bytes(), 0);
}

#[tokio::test]
async fn event_capture_budget_retry_rejection_and_queued_drop_release_owned_bodies() {
    let budget = Arc::new(crate::event_capture_budget::EventCaptureMemoryBudget::new(
        64 * 1024,
    ));
    let first = event_capture_budget_retry_event(&budget);
    let retained_once = budget.retained_bytes();
    let second = event_capture_budget_retry_event(&budget);
    assert_eq!(budget.retained_bytes(), retained_once * 2);
    let config = UsageRuntimeConfig::default();
    let runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(runner, config).expect("queue");
    let (sender, receiver) = mpsc::channel(1);
    let dispatcher = UsageEnqueueRetryDispatcher {
        senders: vec![sender],
        metrics: Arc::new(super::UsageEnqueueDispatcherMetrics::default()),
    };
    assert!(dispatcher.schedule(
        queue.clone(),
        first,
        "terminal",
        DataLayerError::Redis("failure".to_string())
    ));
    assert!(!dispatcher.schedule(
        queue,
        second,
        "terminal",
        DataLayerError::Redis("failure".to_string())
    ));
    assert_eq!(budget.retained_bytes(), retained_once);
    drop(receiver);
    assert_eq!(budget.retained_bytes(), 0);
}

#[tokio::test]
async fn disabled_lifecycle_queue_writes_pending_directly() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_lifecycle_events: false,
        stream_key: "usage:events:test:lifecycle-disabled".to_string(),
        consumer_group: "usage_consumers_test_lifecycle_disabled".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: queue_runner,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let plan = ExecutionPlan {
        request_id: "req-lifecycle-disabled-1".to_string(),
        candidate_id: Some("cand-lifecycle-disabled-1".to_string()),
        provider_name: Some("openai".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/responses".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-5"})),
        stream: false,
        client_api_format: "openai:responses".to_string(),
        provider_api_format: "openai:responses".to_string(),
        model_name: Some("gpt-5".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    };

    runtime.record_pending(&store, build_lifecycle_usage_seed(&plan, None));
    sleep(Duration::from_millis(50)).await;

    let records = store.records.lock().expect("records lock");
    let record = records
        .first()
        .expect("disabled lifecycle queue should direct-write pending usage");
    assert_eq!(record.request_id, "req-lifecycle-disabled-1");
    assert_eq!(record.status, "pending");
    assert_eq!(record.billing_status, "pending");
}

#[tokio::test]
async fn queued_terminal_usage_does_not_enrich_or_write_before_enqueue() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        stream_key: "usage:events:test:terminal".to_string(),
        consumer_group: "usage_consumers_test_terminal".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = EnrichmentCountingQueueStore {
        records: Mutex::new(Vec::new()),
        queue: queue_runner,
        enrich_calls: AtomicUsize::new(0),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let event = UsageEvent::new(
        UsageEventType::Completed,
        "req-terminal-queue-1",
        UsageEventData {
            user_id: Some("user-terminal-queue-1".to_string()),
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            input_tokens: Some(4),
            output_tokens: Some(8),
            total_tokens: Some(12),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    runtime.record_terminal_event(&store, event).await;

    assert_eq!(store.enrich_calls.load(Ordering::Acquire), 0);
    assert!(store.records.lock().expect("records lock").is_empty());
    assert_eq!(
        runtime.metrics_snapshot().enqueue_retry_scheduled_total,
        0,
        "healthy terminal enqueue should be durable before returning"
    );
    let entries = queue
        .read_group("usage-test-terminal-consumer")
        .await
        .expect("queue read should succeed");
    let entry = entries.first().expect("terminal event should be queued");
    let queued =
        UsageEvent::from_stream_fields(&entry.fields).expect("queued terminal event should parse");
    assert_eq!(queued.request_id, "req-terminal-queue-1");
    assert_eq!(queued.event_type, UsageEventType::Completed);
    assert_eq!(queued.data.total_cost_usd, None);
}

fn oversized_full_terminal_event(max_bytes: usize) -> (serde_json::Value, UsageEvent) {
    let body = json!({"content": "full-body".repeat(max_bytes / 16)});
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "oversized-full-capture",
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            status_code: Some(200),
            total_tokens: Some(12),
            request_body: Some(body.clone()),
            provider_request_body: Some(body.clone()),
            response_body: Some(body.clone()),
            client_response_body: Some(body.clone()),
            ..UsageEventData::default()
        },
    );
    apply_usage_body_capture_policy_to_event(
        UsageBodyCapturePolicy {
            record_level: UsageRequestRecordLevel::Full,
            ..UsageBodyCapturePolicy::default()
        },
        &mut event,
    );
    (body, event)
}

#[tokio::test]
async fn oversized_full_terminal_capture_is_persisted_without_queue_truncation() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue_runner: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone()).unwrap();
    let store = EnrichmentCountingQueueStore {
        records: Mutex::new(Vec::new()),
        queue: queue_runner,
        enrich_calls: AtomicUsize::new(0),
    };
    let runtime = UsageRuntime::new(config.clone()).unwrap();
    let (body, event) = oversized_full_terminal_event(config.queue_payload_max_bytes);
    assert!(
        event
            .to_bounded_stream_fields(config.queue_payload_max_bytes)
            .unwrap()
            .diagnostics_omitted
    );

    let outcome = runtime.enqueue_or_write_terminal(&store, event).await;

    assert_eq!(
        outcome,
        super::TerminalPersistenceOutcome::PersistedDirectly
    );
    assert_eq!(store.enrich_calls.load(Ordering::Acquire), 1);
    assert_eq!(queue.stats().await.unwrap().stream_length, 0);
    let records = store.records.lock().unwrap();
    assert_eq!(records.len(), 1);
    for captured in [
        &records[0].request_body,
        &records[0].provider_request_body,
        &records[0].response_body,
        &records[0].client_response_body,
    ] {
        assert_eq!(captured.as_ref(), Some(&body));
    }
}

#[tokio::test]
async fn oversized_full_terminal_capture_keeps_bounded_queue_fallback() {
    for unavailable in ["writer", "write_failure", "worker_gate", "fallback_gate"] {
        let config = UsageRuntimeConfig {
            enabled: true,
            queue_terminal_events: true,
            consumer_block_ms: 1,
            worker_record_concurrency_limit: Some(1),
            ..UsageRuntimeConfig::default()
        };
        let queue_runner: Arc<dyn RuntimeQueueStore> =
            Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
        let queue = UsageQueue::new(Arc::clone(&queue_runner), config.clone()).unwrap();
        queue.ensure_consumer_group().await.unwrap();
        let store = FailingWriteQueueConfiguredUsageStore {
            queue: Arc::clone(&queue_runner),
            upsert_attempts: Arc::new(AtomicUsize::new(0)),
        };
        let queue_only = QueueOnlyUsageStore {
            queue: queue_runner,
            upsert_attempts: Arc::clone(&store.upsert_attempts),
        };
        let runtime = UsageRuntime::new(config.clone()).unwrap();
        let worker_permit = (unavailable == "worker_gate").then(|| {
            runtime
                .worker_record_gate
                .as_ref()
                .unwrap()
                .try_acquire()
                .unwrap()
        });
        let fallback_permit = (unavailable == "fallback_gate").then(|| {
            runtime
                .terminal_direct_fallback_state
                .try_acquire()
                .unwrap()
        });
        let (_, event) = oversized_full_terminal_event(config.queue_payload_max_bytes);

        let outcome = if unavailable == "writer" {
            runtime.enqueue_or_write_terminal(&queue_only, event).await
        } else {
            runtime.enqueue_or_write_terminal(&store, event).await
        };

        assert_eq!(
            outcome,
            super::TerminalPersistenceOutcome::Queued,
            "{unavailable}"
        );
        assert_eq!(
            store.upsert_attempts.load(Ordering::Acquire),
            usize::from(unavailable == "write_failure")
        );
        let entries = queue
            .read_group("oversized-capture-consumer")
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].fields["payload"].len() <= config.queue_payload_max_bytes);
        let queued = UsageEvent::from_stream_fields(&entries[0].fields).unwrap();
        assert_eq!(queued.data.total_tokens, Some(12));
        for (body, state) in [
            (&queued.data.request_body, queued.data.request_body_state),
            (
                &queued.data.provider_request_body,
                queued.data.provider_request_body_state,
            ),
            (&queued.data.response_body, queued.data.response_body_state),
            (
                &queued.data.client_response_body,
                queued.data.client_response_body_state,
            ),
        ] {
            assert!(body.is_none());
            assert_eq!(state, Some(UsageBodyCaptureState::Truncated));
        }
        assert_eq!(runtime.metrics_snapshot().terminal_enqueue_failed_total, 0);
        drop((worker_permit, fallback_permit));
    }
}

#[tokio::test]
async fn terminal_enqueue_failure_uses_bounded_direct_database_fallback() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        stream_key: "usage:events:test:terminal-circuit".to_string(),
        consumer_group: "usage_consumers_test_terminal_circuit".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: 16,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue = Arc::new(FlakyAppendQueueStore::new(Arc::clone(&inner_queue), 2));
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: flaky_queue.clone(),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    for index in 0..2 {
        runtime
            .record_terminal_event(
                &store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    format!("req-terminal-circuit-{index}"),
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        total_tokens: Some(12),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    }

    let snapshot = runtime.metrics_snapshot();
    assert_eq!(
        snapshot.terminal_enqueue_failed_total, 1,
        "the first terminal submission should attempt Redis before opening the circuit"
    );
    assert_eq!(
        snapshot.terminal_enqueue_deferred_direct_write_total, 2,
        "the failed primary event and circuit-deferred event should use direct fallback"
    );
    assert_eq!(snapshot.terminal_enqueue_deferred_retry_total, 0);
    assert_eq!(snapshot.terminal_enqueue_deferred_dropped_total, 0);
    assert_eq!(snapshot.terminal_direct_fallback_succeeded_total, 2);
    assert_eq!(snapshot.enqueue_retry_scheduled_total, 0);
    assert_eq!(snapshot.enqueue_retry_recovered_total, 0);
    assert_eq!(snapshot.enqueue_retry_pending, 0);
    assert_eq!(snapshot.enqueue_retry_failed_total, 0);
    assert_eq!(snapshot.enqueue_retry_closed_or_unavailable_total, 0);
    assert_eq!(
        store.records.lock().expect("records lock").len(),
        2,
        "both terminal events should be persisted directly"
    );
    assert_eq!(flaky_queue.successful_appends.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn failed_terminal_persistence_does_not_cancel_buffered_first_byte() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        queue_lifecycle_events: true,
        stream_key: "usage:events:test:terminal-total-failure".to_string(),
        consumer_group: "usage_consumers_test_terminal_total_failure".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(FlakyAppendQueueStore::new(inner_queue, usize::MAX));
    let store = FailingWriteQueueConfiguredUsageStore {
        queue,
        upsert_attempts: Arc::new(AtomicUsize::new(0)),
    };
    let mut runtime = UsageRuntime::new(config).expect("usage runtime should build");
    runtime.enqueue_retry = UsageEnqueueRetryDispatcher::disabled();
    let request_id = "req-terminal-total-failure";
    let generation = runtime
        .lifecycle_coalescer
        .mark_first_byte(request_id)
        .await
        .expect("buffered first-byte generation");

    runtime
        .enqueue_or_write_terminal(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    assert!(
        runtime
            .lifecycle_coalescer
            .first_byte_is_current(request_id, generation)
            .await,
        "terminal loss must not discard the only remaining lifecycle transition"
    );
    assert_eq!(
        runtime
            .metrics_snapshot()
            .terminal_enqueue_deferred_dropped_total,
        1
    );

    let direct_request_id = "req-terminal-direct-total-failure";
    let direct_generation = runtime
        .lifecycle_coalescer
        .mark_first_byte(direct_request_id)
        .await
        .expect("direct-path buffered first-byte generation");
    runtime
        .record_terminal_event_direct(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                direct_request_id,
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    assert!(
        runtime
            .lifecycle_coalescer
            .first_byte_is_current(direct_request_id, direct_generation)
            .await,
        "failed direct terminal persistence must not cancel buffered first-byte state"
    );
}

#[tokio::test]
async fn permanent_redis_and_database_failure_keeps_terminal_retry_tasks_bounded() {
    const TOTAL: usize = 32;
    const RETRY_CAPACITY: u64 = 1;
    const RETRY_WORKERS: u64 = 1;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: TOTAL as u64,
        terminal_enqueue_max_in_flight: 1,
        stream_key: "usage:events:test:terminal-permanent-failure".to_string(),
        consumer_group: "usage_consumers_test_terminal_permanent_failure".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: RETRY_CAPACITY as usize,
        enqueue_retry_workers: RETRY_WORKERS as usize,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue = Arc::new(FlakyAppendQueueStore::new(
        Arc::clone(&inner_queue),
        usize::MAX,
    ));
    let queue: Arc<dyn RuntimeQueueStore> = flaky_queue.clone();
    let store = FailingWriteQueueConfiguredUsageStore {
        queue,
        upsert_attempts: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let mut submissions = tokio::task::JoinSet::new();

    for index in 0..TOTAL {
        let runtime = runtime.clone();
        let store = store.clone();
        submissions.spawn(async move {
            runtime
                .record_terminal_event(
                    &store,
                    UsageEvent::new(
                        UsageEventType::Completed,
                        format!("req-terminal-permanent-failure-{index}"),
                        UsageEventData {
                            provider_name: "openai".to_string(),
                            model: "gpt-5".to_string(),
                            total_tokens: Some(12),
                            status_code: Some(200),
                            ..UsageEventData::default()
                        },
                    ),
                )
                .await;
        });
    }

    let completed_without_waiters = timeout(Duration::from_secs(1), async {
        while let Some(result) = submissions.join_next().await {
            result.expect("terminal submission should not panic");
        }
    })
    .await;
    let saturated_snapshot = runtime.metrics_snapshot();

    flaky_queue.remaining_failures.store(0, Ordering::Release);
    if completed_without_waiters.is_err() {
        submissions.abort_all();
        while submissions.join_next().await.is_some() {}
    }
    wait_for_enqueue_dispatcher_to_drain(
        &runtime,
        saturated_snapshot.enqueue_retry_scheduled_total,
    )
    .await;

    assert!(
        completed_without_waiters.is_ok(),
        "terminal submissions must never wait outside bounded fallback queues"
    );
    assert_eq!(
        saturated_snapshot.terminal_enqueue_deferred_total,
        TOTAL as u64
    );
    assert_eq!(
        saturated_snapshot.terminal_direct_fallback_failed_total,
        TOTAL as u64
    );
    assert_eq!(
        store.upsert_attempts.load(Ordering::Acquire),
        TOTAL,
        "each terminal event should receive one bounded direct write attempt"
    );
    assert_eq!(
        saturated_snapshot.terminal_enqueue_deferred_retry_total
            + saturated_snapshot.terminal_enqueue_deferred_dropped_total,
        TOTAL as u64
    );
    assert_eq!(
        saturated_snapshot.enqueue_retry_scheduled_total,
        saturated_snapshot.terminal_enqueue_deferred_retry_total
    );
    assert!(
        saturated_snapshot.enqueue_retry_pending <= RETRY_CAPACITY + RETRY_WORKERS,
        "retry pending events must stay within active workers plus channel capacity"
    );
    assert!(
        saturated_snapshot.terminal_enqueue_deferred_dropped_total > 0,
        "the test must saturate the bounded retry buffer"
    );
    assert_eq!(
        saturated_snapshot.enqueue_retry_closed_or_unavailable_total,
        saturated_snapshot.terminal_enqueue_deferred_dropped_total
    );
    assert!(saturated_snapshot.terminal_direct_fallback_max_in_flight <= 1);
    assert_eq!(saturated_snapshot.terminal_direct_fallback_in_flight, 0);
    assert_eq!(saturated_snapshot.terminal_enqueue_in_flight, 0);
}

#[tokio::test]
async fn slow_database_fallback_backpressures_excess_at_hard_capacity_and_recovers() {
    const EXCESS_SUBMISSIONS: usize = 16;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: (EXCESS_SUBMISSIONS + 1) as u64,
        terminal_enqueue_max_in_flight: 1,
        stream_key: "usage:events:test:terminal-slow-db-fallback".to_string(),
        consumer_group: "usage_consumers_test_terminal_slow_db_fallback".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: 1,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue = Arc::new(FlakyAppendQueueStore::new(
        Arc::clone(&inner_queue),
        usize::MAX,
    ));
    let queue: Arc<dyn RuntimeQueueStore> = flaky_queue.clone();
    let store = BlockingWriteQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue,
        write_started: Arc::new(tokio::sync::Notify::new()),
        release_writes: Arc::new(tokio::sync::Notify::new()),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let write_started = store.write_started.notified();
    let first_runtime = runtime.clone();
    let first_store = store.clone();
    let mut first_submission = tokio::spawn(async move {
        first_runtime
            .record_terminal_event(
                &first_store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    "req-terminal-slow-db-fallback-first",
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        total_tokens: Some(12),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    });

    let first_write_started = timeout(Duration::from_secs(1), write_started).await;
    let mut excess = tokio::task::JoinSet::new();
    for index in 0..EXCESS_SUBMISSIONS {
        let runtime = runtime.clone();
        let store = store.clone();
        excess.spawn(async move {
            runtime
                .record_terminal_event(
                    &store,
                    UsageEvent::new(
                        UsageEventType::Completed,
                        format!("req-terminal-slow-db-fallback-excess-{index}"),
                        UsageEventData {
                            provider_name: "openai".to_string(),
                            model: "gpt-5".to_string(),
                            total_tokens: Some(12),
                            status_code: Some(200),
                            ..UsageEventData::default()
                        },
                    ),
                )
                .await;
        });
    }
    let excess_backpressured = timeout(Duration::from_millis(50), async {
        while let Some(result) = excess.join_next().await {
            result.expect("excess terminal submission should not panic");
        }
    })
    .await;
    let saturated_snapshot = runtime.metrics_snapshot();

    flaky_queue.remaining_failures.store(0, Ordering::Release);
    sleep(Duration::from_millis(
        super::LIFECYCLE_ENQUEUE_CIRCUIT_OPEN_MS + 25,
    ))
    .await;
    store.release_writes.notify_waiters();
    let first_completed = timeout(Duration::from_secs(1), &mut first_submission).await;
    if first_completed.is_err() {
        first_submission.abort();
        let _ = first_submission.await;
    }
    let excess_recovered = timeout(Duration::from_secs(2), async {
        while let Some(result) = excess.join_next().await {
            result.expect("backpressured terminal submission should not panic");
        }
    })
    .await;
    wait_for_enqueue_dispatcher_to_drain(
        &runtime,
        saturated_snapshot.enqueue_retry_scheduled_total,
    )
    .await;
    let recovered_snapshot = runtime.metrics_snapshot();

    assert!(
        first_write_started.is_ok(),
        "first fallback should reach the writer"
    );
    assert!(
        excess_backpressured.is_err(),
        "excess terminal calls should wait outside the hard resident-work bound"
    );
    assert!(first_completed.is_ok(), "released fallback should complete");
    if excess_recovered.is_err() {
        let snapshot = runtime.metrics_snapshot();
        panic!(
            "backpressured terminal calls should recover after persistence resumes: \
             remaining={} lifecycle_pending={} ordered_pending={} terminal_pending={} \
             terminal_in_flight={} writes_completed={} successful_appends={}",
            excess.len(),
            snapshot.lifecycle_submission_pending,
            snapshot.ordered_lifecycle_pending,
            snapshot.terminal_submission_pending,
            snapshot.terminal_submission_in_flight,
            store.writes_completed.load(Ordering::Acquire),
            flaky_queue.successful_appends.load(Ordering::Acquire),
        );
    }
    assert_eq!(saturated_snapshot.terminal_direct_fallback_limit, 1);
    assert_eq!(saturated_snapshot.terminal_direct_fallback_in_flight, 1);
    assert_eq!(saturated_snapshot.terminal_direct_fallback_max_in_flight, 1);
    assert_eq!(saturated_snapshot.worker_record_concurrency_in_flight, 1);
    assert_eq!(
        saturated_snapshot.worker_record_concurrency_max_in_flight,
        1
    );
    assert_eq!(saturated_snapshot.worker_record_deferred_total, 0);
    assert_eq!(
        saturated_snapshot.terminal_direct_fallback_rejected_total,
        0
    );
    assert_eq!(
        saturated_snapshot.terminal_enqueue_deferred_retry_total
            + saturated_snapshot.terminal_enqueue_deferred_dropped_total,
        0
    );
    assert_eq!(saturated_snapshot.enqueue_retry_pending, 0);
    assert_eq!(store.writes_completed.load(Ordering::Acquire), 1);
    assert_eq!(
        flaky_queue.successful_appends.load(Ordering::Acquire),
        EXCESS_SUBMISSIONS
    );
    assert_eq!(recovered_snapshot.terminal_direct_fallback_in_flight, 0);
    assert_eq!(recovered_snapshot.worker_record_concurrency_in_flight, 0);
    assert_eq!(recovered_snapshot.lifecycle_submission_pending, 0);
    assert_eq!(recovered_snapshot.ordered_lifecycle_pending, 0);
    assert_eq!(
        recovered_snapshot.terminal_direct_fallback_succeeded_total,
        1
    );
}

#[tokio::test]
async fn terminal_seed_preserves_pending_streaming_terminal_order() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        ..UsageRuntimeConfig::default()
    };
    let queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-terminal-seed-ordered";
    let plan = terminal_test_plan(request_id);
    let lifecycle_seed = build_lifecycle_usage_seed(&plan, None);
    let (context_seed, payload_seed) = sync_terminal_test_seeds(request_id);

    runtime.record_pending(&store, lifecycle_seed.clone());
    runtime.record_stream_started(&store, &lifecycle_seed, 200, None);
    runtime
        .record_sync_terminal(&store, context_seed, payload_seed)
        .await;

    timeout(Duration::from_secs(2), async {
        loop {
            let records = store.records.lock().expect("records lock").len();
            if records == 3 && runtime.metrics_snapshot().lifecycle_submission_pending == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered terminal seed should fully persist");

    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 3);
    assert!(records.iter().all(|record| record.request_id == request_id));
    assert_eq!(records[0].status, "pending");
    assert_eq!(records[1].status, "streaming");
    assert_eq!(records[2].status, "completed");
}

#[tokio::test]
async fn awaited_terminal_does_not_hold_permit_behind_queued_terminal_seed() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = BlockingWriteQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue,
        write_started: Arc::new(tokio::sync::Notify::new()),
        release_writes: Arc::new(tokio::sync::Notify::new()),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let blocker_started = Arc::new(tokio::sync::Notify::new());
    let release_blocker = Arc::new(tokio::sync::Notify::new());
    let seen = Arc::new(Mutex::new(Vec::new()));

    runtime
        .lifecycle_submission
        .dispatch(Box::new(TestLifecycleSubmissionItem {
            request_id: "req-terminal-seed-deadlock-blocker".to_string(),
            priority: LifecycleSubmissionPriority::Pending,
            started: Some(Arc::clone(&blocker_started)),
            release: Some(Arc::clone(&release_blocker)),
            seen,
        }));
    timeout(Duration::from_secs(1), blocker_started.notified())
        .await
        .expect("lifecycle blocker should start");

    let (context_seed, payload_seed) = sync_terminal_test_seeds("req-terminal-seed-before-awaited");
    runtime
        .record_sync_terminal(&store, context_seed, payload_seed)
        .await;

    let awaited_runtime = runtime.clone();
    let awaited_store = store.clone();
    let awaited_terminal = tokio::spawn(async move {
        awaited_runtime
            .record_terminal_event(
                &awaited_store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    "req-awaited-terminal-after-seed",
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    });

    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().lifecycle_submission_pending < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal seed and awaited barrier should queue behind the blocker");
    assert_eq!(runtime.metrics_snapshot().terminal_submission_pending, 0);

    let first_write_started = store.write_started.notified();
    release_blocker.notify_one();
    timeout(Duration::from_secs(1), first_write_started)
        .await
        .expect("terminal seed write should start");
    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().terminal_submission_pending != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("awaited terminal should queue behind the terminal seed permit");

    let second_write_started = store.write_started.notified();
    store.release_writes.notify_one();
    timeout(Duration::from_secs(1), second_write_started)
        .await
        .expect("awaited terminal write should start after the seed completes");
    store.release_writes.notify_one();
    timeout(Duration::from_secs(2), awaited_terminal)
        .await
        .expect("awaited terminal must not deadlock behind the queued seed")
        .expect("awaited terminal task should not panic");
    timeout(Duration::from_secs(2), async {
        while store.records.lock().expect("records lock").len() != 2
            || runtime.metrics_snapshot().lifecycle_submission_pending != 0
            || store.writes_completed.load(Ordering::Acquire) != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both terminal records should persist");

    let records = store.records.lock().expect("records lock");
    assert!(records
        .iter()
        .any(|record| record.request_id == "req-terminal-seed-before-awaited"));
    assert!(records
        .iter()
        .any(|record| record.request_id == "req-awaited-terminal-after-seed"));
    assert_eq!(
        runtime.metrics_snapshot().terminal_submission_max_pending,
        2
    );
}

#[tokio::test]
async fn submitted_terminal_does_not_hold_permit_behind_queued_terminal_seed() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        ..UsageRuntimeConfig::default()
    };
    let queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = BlockingWriteQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue,
        write_started: Arc::new(tokio::sync::Notify::new()),
        release_writes: Arc::new(tokio::sync::Notify::new()),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let blocker_started = Arc::new(tokio::sync::Notify::new());
    let release_blocker = Arc::new(tokio::sync::Notify::new());

    runtime
        .lifecycle_submission
        .dispatch(Box::new(TestLifecycleSubmissionItem {
            request_id: "req-submitted-terminal-deadlock-blocker".to_string(),
            priority: LifecycleSubmissionPriority::Pending,
            started: Some(Arc::clone(&blocker_started)),
            release: Some(Arc::clone(&release_blocker)),
            seen: Arc::new(Mutex::new(Vec::new())),
        }));
    timeout(Duration::from_secs(1), blocker_started.notified())
        .await
        .expect("lifecycle blocker should start");

    let (context_seed, payload_seed) =
        sync_terminal_test_seeds("req-terminal-seed-before-submitted");
    runtime
        .record_sync_terminal(&store, context_seed, payload_seed)
        .await;
    runtime
        .submit_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                "req-submitted-terminal-after-seed",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    assert_eq!(runtime.metrics_snapshot().terminal_submission_pending, 0);
    assert!(runtime.metrics_snapshot().lifecycle_submission_pending >= 3);

    let first_write_started = store.write_started.notified();
    release_blocker.notify_one();
    timeout(Duration::from_secs(1), first_write_started)
        .await
        .expect("terminal seed write should start");
    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().terminal_submission_pending != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("submitted terminal should queue behind the terminal seed permit");

    let second_write_started = store.write_started.notified();
    store.release_writes.notify_one();
    timeout(Duration::from_secs(1), second_write_started)
        .await
        .expect("submitted terminal write should start after the seed completes");
    store.release_writes.notify_one();
    timeout(Duration::from_secs(2), async {
        while store.records.lock().expect("records lock").len() != 2
            || runtime.metrics_snapshot().lifecycle_submission_pending != 0
            || store.writes_completed.load(Ordering::Acquire) != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued seed and submitted terminal should both persist");

    let records = store.records.lock().expect("records lock");
    assert!(records
        .iter()
        .any(|record| record.request_id == "req-terminal-seed-before-submitted"));
    assert!(records
        .iter()
        .any(|record| record.request_id == "req-submitted-terminal-after-seed"));
    assert_eq!(
        runtime.metrics_snapshot().terminal_submission_max_pending,
        2
    );
}

#[tokio::test]
async fn terminal_seed_waits_for_its_ordered_turn_before_admission() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        stream_key: "usage:events:test:terminal-seed-turn".to_string(),
        consumer_group: "usage_consumers_test_terminal_seed_turn".to_string(),
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let tracked_queue = Arc::new(FlakyAppendQueueStore::new(inner_queue, 0));
    let queue: Arc<dyn RuntimeQueueStore> = tracked_queue.clone();
    let store = BlockingPolicyQueueConfiguredUsageStore::new(queue);
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let request_id = "req-terminal-seed-waits-for-turn";
    let plan = terminal_test_plan(request_id);
    let policy_started = store.policy_started.notified();

    runtime.record_pending(&store, build_lifecycle_usage_seed(&plan, None));
    timeout(Duration::from_secs(1), policy_started)
        .await
        .expect("pending phase should block in body policy");

    let (context_seed, payload_seed) = sync_terminal_test_seeds(request_id);
    runtime
        .record_sync_terminal(&store, context_seed, payload_seed)
        .await;

    let blocked_snapshot = runtime.metrics_snapshot();
    assert_eq!(blocked_snapshot.terminal_submission_pending, 0);
    assert_eq!(blocked_snapshot.terminal_submission_max_pending, 0);
    assert_eq!(blocked_snapshot.terminal_submission_in_flight, 0);
    assert!(blocked_snapshot.lifecycle_submission_pending >= 2);

    store.release_blocked_policy();
    timeout(Duration::from_secs(2), async {
        loop {
            store.release_blocked_policy();
            let snapshot = runtime.metrics_snapshot();
            if tracked_queue.successful_appends.load(Ordering::Acquire) == 1
                && snapshot.lifecycle_submission_pending == 0
                && snapshot.terminal_submission_pending == 0
            {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("terminal seed should persist after its ordered turn is released");

    let recovered_snapshot = runtime.metrics_snapshot();
    assert_eq!(recovered_snapshot.terminal_submission_max_pending, 1);
    assert_eq!(recovered_snapshot.terminal_submission_max_in_flight, 1);
    assert_eq!(recovered_snapshot.terminal_submission_rejected_total, 0);
    assert_eq!(
        recovered_snapshot.terminal_enqueue_deferred_dropped_total,
        0
    );
}

#[tokio::test]
async fn terminal_seed_backlog_does_not_create_per_request_admission_waiters() {
    const BACKLOG: usize = 16;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        stream_key: "usage:events:test:terminal-seed-backlog".to_string(),
        consumer_group: "usage_consumers_test_terminal_seed_backlog".to_string(),
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let tracked_queue = Arc::new(FlakyAppendQueueStore::new(inner_queue, 0));
    let queue: Arc<dyn RuntimeQueueStore> = tracked_queue.clone();
    let store = BlockingPolicyQueueConfiguredUsageStore::new(queue);
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let policy_started = store.policy_started.notified();

    runtime
        .submit_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                "req-terminal-seed-backlog-blocker",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;
    timeout(Duration::from_secs(1), policy_started)
        .await
        .expect("blocking terminal should hold the only admission permit");

    for index in 0..BACKLOG {
        let request_id = format!("req-terminal-seed-backlog-{index}");
        let (context_seed, payload_seed) = sync_terminal_test_seeds(&request_id);
        runtime
            .record_sync_terminal(&store, context_seed, payload_seed)
            .await;
    }

    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().terminal_submission_pending < BACKLOG + 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal execution queue should receive the full backlog");
    let blocked_snapshot = runtime.metrics_snapshot();
    assert_eq!(blocked_snapshot.terminal_submission_pending, BACKLOG + 1);
    assert_eq!(
        blocked_snapshot.terminal_submission_max_pending,
        BACKLOG + 1
    );
    assert_eq!(blocked_snapshot.terminal_submission_in_flight, 1);
    assert!(blocked_snapshot.lifecycle_submission_pending <= BACKLOG + 1);

    store.release_blocked_policy();
    timeout(Duration::from_secs(5), async {
        loop {
            store.release_blocked_policy();
            let snapshot = runtime.metrics_snapshot();
            if tracked_queue.successful_appends.load(Ordering::Acquire) == BACKLOG + 1
                && snapshot.lifecycle_submission_pending == 0
                && snapshot.terminal_submission_pending == 0
            {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("all terminal seeds should recover from admission backpressure");

    let recovered_snapshot = runtime.metrics_snapshot();
    assert_eq!(
        recovered_snapshot.terminal_submission_max_pending,
        BACKLOG + 1
    );
    assert_eq!(recovered_snapshot.terminal_submission_max_in_flight, 1);
    assert_eq!(recovered_snapshot.terminal_submission_rejected_total, 0);
    assert_eq!(recovered_snapshot.terminal_enqueue_failed_total, 0);
    assert_eq!(
        recovered_snapshot.terminal_enqueue_deferred_dropped_total,
        0
    );
}

#[tokio::test]
async fn terminal_permit_wait_does_not_block_unrelated_lifecycle_on_same_shard() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        ..UsageRuntimeConfig::default()
    };
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    let held_permit = runtime
        .terminal_submission_state
        .acquire()
        .await
        .expect("test should hold the only terminal permit");
    let blocked_request_id = "req-terminal-permit-hol-blocked";
    let (context_seed, payload_seed) = sync_terminal_test_seeds(blocked_request_id);
    runtime
        .record_sync_terminal(&store, context_seed, payload_seed)
        .await;
    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().terminal_submission_pending < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal work should reach the dedicated execution queue");

    let unrelated_request_id = "req-terminal-permit-hol-unrelated";
    runtime.record_pending(
        &store,
        build_lifecycle_usage_seed(&terminal_test_plan(unrelated_request_id), None),
    );
    timeout(Duration::from_secs(1), async {
        loop {
            let unrelated_written = {
                let records = store.records.lock().expect("records lock");
                records.iter().any(|record| {
                    record.request_id == unrelated_request_id && record.status == "pending"
                })
            };
            if unrelated_written {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unrelated lifecycle work must bypass terminal permit wait");
    assert!(!store
        .records
        .lock()
        .expect("records lock")
        .iter()
        .any(|record| record.request_id == blocked_request_id));

    drop(held_permit);
    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            let terminal_written = store
                .records
                .lock()
                .expect("records lock")
                .iter()
                .any(|record| record.request_id == blocked_request_id);
            if terminal_written
                && snapshot.terminal_submission_pending == 0
                && snapshot.lifecycle_submission_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal work should drain after the permit is released");
}

#[tokio::test]
async fn terminal_build_does_not_block_unrelated_lifecycle_on_same_shard() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        ..UsageRuntimeConfig::default()
    };
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let blocked_request_id = "req-terminal-build-hol-blocked";
    let build_started = Arc::new(tokio::sync::Notify::new());
    let release_build = Arc::new(tokio::sync::Notify::new());
    runtime
        .dispatch_terminal_seed(
            &store,
            blocked_request_id.to_string(),
            LifecycleTerminalUsageSeed::BlockedBuild {
                event: UsageEvent::new(
                    UsageEventType::Completed,
                    blocked_request_id,
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
                started: Arc::clone(&build_started),
                release: Arc::clone(&release_build),
            },
        )
        .await;
    timeout(Duration::from_secs(1), build_started.notified())
        .await
        .expect("terminal build should start on the dedicated worker");

    let unrelated_request_id = "req-terminal-build-hol-unrelated";
    runtime.record_pending(
        &store,
        build_lifecycle_usage_seed(&terminal_test_plan(unrelated_request_id), None),
    );
    timeout(Duration::from_secs(1), async {
        loop {
            let unrelated_written = {
                let records = store.records.lock().expect("records lock");
                records.iter().any(|record| {
                    record.request_id == unrelated_request_id && record.status == "pending"
                })
            };
            if unrelated_written {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unrelated lifecycle work must bypass terminal build");
    assert!(!store
        .records
        .lock()
        .expect("records lock")
        .iter()
        .any(|record| record.request_id == blocked_request_id));

    release_build.notify_one();
    timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = runtime.metrics_snapshot();
            let terminal_written = store
                .records
                .lock()
                .expect("records lock")
                .iter()
                .any(|record| record.request_id == blocked_request_id);
            if terminal_written
                && snapshot.terminal_submission_pending == 0
                && snapshot.lifecycle_submission_pending == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal build should drain after release");
}

#[tokio::test]
async fn terminal_submission_pending_decrements_when_waiter_is_cancelled() {
    let state = Arc::new(super::TerminalSubmissionState::new(1));
    let first = state
        .acquire()
        .await
        .expect("first terminal submission should acquire");

    let waiter_state = Arc::clone(&state);
    let waiter = tokio::spawn(async move { waiter_state.acquire().await });
    timeout(Duration::from_secs(1), async {
        while state.pending() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled waiter should be counted while it is queued");

    waiter.abort();
    assert!(
        matches!(waiter.await, Err(err) if err.is_cancelled()),
        "admission waiter task should report cancellation"
    );
    assert_eq!(state.pending(), 1);

    drop(first);
    assert_eq!(state.pending(), 0);
    assert_eq!(state.max_pending(), 2);
}

#[tokio::test]
async fn direct_terminal_write_is_visible_to_submission_metrics_until_completion() {
    let config = UsageRuntimeConfig {
        enabled: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        ..UsageRuntimeConfig::default()
    };
    let store = BlockingWriteQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue: Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default())),
        write_started: Arc::new(tokio::sync::Notify::new()),
        release_writes: Arc::new(tokio::sync::Notify::new()),
        writes_completed: Arc::new(AtomicUsize::new(0)),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let write_started = store.write_started.notified();
    let write_runtime = runtime.clone();
    let write_store = store.clone();
    let write = tokio::spawn(async move {
        write_runtime
            .record_terminal_event_direct(
                &write_store,
                UsageEvent::new(
                    UsageEventType::Completed,
                    "req-direct-terminal-submission-metrics",
                    UsageEventData {
                        provider_name: "openai".to_string(),
                        model: "gpt-5".to_string(),
                        status_code: Some(200),
                        ..UsageEventData::default()
                    },
                ),
            )
            .await;
    });

    timeout(Duration::from_secs(1), write_started)
        .await
        .expect("the direct terminal write should start");
    let in_flight = runtime.metrics_snapshot();
    assert_eq!(in_flight.terminal_submission_pending, 1);
    assert_eq!(in_flight.terminal_submission_in_flight, 1);

    store.release_writes.notify_one();
    timeout(Duration::from_secs(1), write)
        .await
        .expect("the direct terminal write should finish")
        .expect("the direct terminal write task should not panic");
    let completed = runtime.metrics_snapshot();
    assert_eq!(completed.terminal_submission_pending, 0);
    assert_eq!(completed.terminal_submission_in_flight, 0);
    assert_eq!(store.writes_completed.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn terminal_submission_admission_backpressures_and_recovers_all_events() {
    const EXCESS_SUBMISSIONS: usize = 16;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        worker_record_concurrency_limit: Some(1),
        terminal_submission_max_in_flight: 1,
        terminal_enqueue_max_in_flight: 1,
        stream_key: "usage:events:test:terminal-submission-admission".to_string(),
        consumer_group: "usage_consumers_test_terminal_submission_admission".to_string(),
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let tracked_queue = Arc::new(FlakyAppendQueueStore::new(inner_queue, 0));
    let queue: Arc<dyn RuntimeQueueStore> = tracked_queue.clone();
    let store = BlockingPolicyQueueConfiguredUsageStore::new(queue);
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let policy_started = store.policy_started.notified();
    runtime
        .submit_terminal_event(
            &store,
            UsageEvent::new(
                UsageEventType::Completed,
                "req-terminal-submission-admission-first",
                UsageEventData {
                    provider_name: "openai".to_string(),
                    model: "gpt-5".to_string(),
                    total_tokens: Some(12),
                    status_code: Some(200),
                    ..UsageEventData::default()
                },
            ),
        )
        .await;

    let first_policy_started = timeout(Duration::from_secs(1), policy_started).await;
    let mut pending_submissions = Vec::with_capacity(EXCESS_SUBMISSIONS);
    for index in 0..EXCESS_SUBMISSIONS {
        let runtime = runtime.clone();
        let store = store.clone();
        pending_submissions.push(tokio::spawn(async move {
            runtime
                .submit_terminal_event(
                    &store,
                    UsageEvent::new(
                        UsageEventType::Completed,
                        format!("req-terminal-submission-admission-excess-{index}"),
                        UsageEventData {
                            provider_name: "openai".to_string(),
                            model: "gpt-5".to_string(),
                            total_tokens: Some(12),
                            status_code: Some(200),
                            ..UsageEventData::default()
                        },
                    ),
                )
                .await;
        }));
    }
    for submission in pending_submissions {
        timeout(Duration::from_secs(1), submission)
            .await
            .expect("terminal ownership handoff should not wait for admission")
            .expect("terminal submission handoff task should complete");
    }
    timeout(Duration::from_secs(1), async {
        while runtime.metrics_snapshot().terminal_submission_pending < EXCESS_SUBMISSIONS + 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal submissions should reach the execution backlog");
    let saturated_snapshot = runtime.metrics_snapshot();

    store.release_blocked_policy();
    let all_completed = timeout(Duration::from_secs(2), async {
        loop {
            store.release_blocked_policy();
            if tracked_queue.successful_appends.load(Ordering::Acquire) == EXCESS_SUBMISSIONS + 1
                && runtime.metrics_snapshot().terminal_submission_in_flight == 0
                && runtime.metrics_snapshot().lifecycle_submission_pending == 0
            {
                break;
            }
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    let recovered_snapshot = runtime.metrics_snapshot();

    assert!(
        first_policy_started.is_ok(),
        "first terminal submission should reach policy I/O"
    );
    assert!(all_completed.is_ok(), "all submissions should complete");
    assert_eq!(
        tracked_queue.successful_appends.load(Ordering::Acquire),
        EXCESS_SUBMISSIONS + 1,
        "every admitted terminal event should reach the queue"
    );
    assert_eq!(saturated_snapshot.terminal_submission_limit, 1);
    assert_eq!(
        saturated_snapshot.terminal_submission_pending,
        EXCESS_SUBMISSIONS + 1
    );
    assert_eq!(
        saturated_snapshot.terminal_submission_max_pending,
        EXCESS_SUBMISSIONS + 1
    );
    assert_eq!(saturated_snapshot.terminal_submission_in_flight, 1);
    assert_eq!(saturated_snapshot.terminal_submission_max_in_flight, 1);
    assert_eq!(saturated_snapshot.terminal_submission_rejected_total, 0);
    assert_eq!(store.policy_reads.load(Ordering::Acquire), 1);
    assert_eq!(recovered_snapshot.terminal_submission_in_flight, 0);
}

#[tokio::test]
async fn terminal_enqueue_limit_bounds_primary_burst_and_recovers_all_events() {
    const WORKERS: usize = 4;
    const PRIMARY_LIMIT: usize = 4;
    const BURST: usize = 64;
    const FORCED_FAILURES: usize = 8;

    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        stream_key: "usage:events:test:terminal-dispatch-burst".to_string(),
        consumer_group: "usage_consumers_test_terminal_dispatch_burst".to_string(),
        consumer_block_ms: 1,
        terminal_enqueue_max_in_flight: PRIMARY_LIMIT as u64,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: BURST,
        enqueue_retry_workers: WORKERS,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let tracked_queue = Arc::new(
        FlakyAppendQueueStore::new(Arc::clone(&inner_queue), FORCED_FAILURES)
            .with_append_delay_ms(2),
    );
    let queue: Arc<dyn RuntimeQueueStore> = tracked_queue.clone();
    let store = CloneQueueConfiguredUsageStore {
        records: Arc::new(Mutex::new(Vec::new())),
        queue,
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");

    let mut submissions = Vec::with_capacity(BURST);
    for index in 0..BURST {
        let runtime = runtime.clone();
        let store = store.clone();
        submissions.push(tokio::spawn(async move {
            runtime
                .record_terminal_event(
                    &store,
                    UsageEvent::new(
                        UsageEventType::Completed,
                        format!("req-terminal-dispatch-burst-{index}"),
                        UsageEventData {
                            provider_name: "openai".to_string(),
                            model: "gpt-5".to_string(),
                            total_tokens: Some(12),
                            status_code: Some(200),
                            ..UsageEventData::default()
                        },
                    ),
                )
                .await;
        }));
    }
    for submission in submissions {
        submission
            .await
            .expect("terminal submission should not panic");
    }
    let scheduled = runtime.metrics_snapshot().enqueue_retry_scheduled_total;
    wait_for_enqueue_dispatcher_to_drain(&runtime, scheduled).await;

    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.enqueue_retry_scheduled_total, scheduled);
    assert_eq!(snapshot.enqueue_retry_recovered_total, scheduled);
    assert_eq!(snapshot.enqueue_retry_pending, 0);
    assert_eq!(snapshot.enqueue_retry_closed_or_unavailable_total, 0);
    assert_eq!(
        tracked_queue.successful_appends.load(Ordering::Acquire)
            + store.records.lock().expect("records lock").len(),
        BURST,
        "every terminal event should reach Redis or bounded direct fallback"
    );
    assert_eq!(
        tracked_queue.append_attempts.load(Ordering::Acquire),
        tracked_queue.successful_appends.load(Ordering::Acquire)
            + snapshot.terminal_enqueue_failed_total as usize
            + snapshot.enqueue_retry_failed_total as usize
    );
    assert!(
        tracked_queue.max_active_appends.load(Ordering::Acquire) <= PRIMARY_LIMIT + WORKERS,
        "Redis queue enqueue concurrency must be bounded by primary and retry limits"
    );
    assert_eq!(snapshot.terminal_enqueue_in_flight, 0);
    assert!(
        snapshot.terminal_enqueue_deferred_direct_write_total
            + snapshot.terminal_enqueue_deferred_retry_total
            > 0,
        "burst submissions above the primary limit should use a bounded fallback"
    );
    assert_eq!(snapshot.terminal_enqueue_deferred_dropped_total, 0);
    assert!(snapshot.terminal_direct_fallback_max_in_flight <= 32);
}

#[tokio::test]
async fn queue_append_failure_prefers_bounded_direct_write_before_retry() {
    let config = UsageRuntimeConfig {
        enabled: true,
        queue_terminal_events: true,
        stream_key: "usage:events:test:retry".to_string(),
        consumer_group: "usage_consumers_test_retry".to_string(),
        consumer_block_ms: 1,
        enqueue_retry_initial_backoff_ms: 1,
        enqueue_retry_max_backoff_ms: 5,
        enqueue_retry_buffer_capacity: 16,
        enqueue_retry_workers: 1,
        ..UsageRuntimeConfig::default()
    };
    let inner_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let flaky_queue: Arc<dyn RuntimeQueueStore> =
        Arc::new(FlakyAppendQueueStore::new(Arc::clone(&inner_queue), 1));
    let queue = UsageQueue::new(Arc::clone(&inner_queue), config.clone())
        .expect("usage queue should build");
    queue
        .ensure_consumer_group()
        .await
        .expect("consumer group should initialize");
    let store = EnrichmentCountingQueueStore {
        records: Mutex::new(Vec::new()),
        queue: flaky_queue,
        enrich_calls: AtomicUsize::new(0),
    };
    let runtime = UsageRuntime::new(config).expect("usage runtime should build");
    let event = UsageEvent::new(
        UsageEventType::Completed,
        "req-terminal-retry-1",
        UsageEventData {
            user_id: Some("user-terminal-retry-1".to_string()),
            provider_name: "openai".to_string(),
            model: "gpt-5".to_string(),
            total_tokens: Some(12),
            status_code: Some(200),
            ..UsageEventData::default()
        },
    );

    runtime.record_terminal_event(&store, event).await;

    assert_eq!(store.enrich_calls.load(Ordering::Acquire), 1);
    assert_eq!(store.records.lock().expect("records lock").len(), 1);
    assert!(
        queue
            .read_group("usage-test-retry-consumer")
            .await
            .expect("queue read should succeed")
            .is_empty(),
        "successful direct fallback should not also schedule Redis retry"
    );
    let snapshot = runtime.metrics_snapshot();
    assert_eq!(snapshot.terminal_direct_fallback_succeeded_total, 1);
    assert_eq!(snapshot.enqueue_retry_scheduled_total, 0);
}

#[tokio::test]
async fn body_capture_policy_read_failure_is_short_cached_as_default() {
    let runtime = UsageRuntime::new(UsageRuntimeConfig {
        enabled: true,
        ..UsageRuntimeConfig::default()
    })
    .expect("usage runtime should build");
    let store = FailingPolicyUsageStore::default();

    let first = runtime.body_capture_policy_for(&store).await;
    assert!(first.is_err(), "first failed read should be surfaced");

    let second = runtime
        .body_capture_policy_for(&store)
        .await
        .expect("short cached fallback should be returned");
    let third = runtime
        .body_capture_policy_for(&store)
        .await
        .expect("short cached fallback should be reused");

    assert_eq!(second, UsageBodyCapturePolicy::default());
    assert_eq!(third, UsageBodyCapturePolicy::default());
    assert_eq!(
        store.policy_reads.load(Ordering::Acquire),
        1,
        "policy read failures should be short cached to avoid concurrent DB storms"
    );
}

#[test]
fn preserve_request_facts_ignores_post_capture_truncation_placeholders() {
    let truncated = json!({
        "truncated": true,
        "reason": "body_capture_limit_exceeded"
    });
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-truncated-preserve",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            request_body: Some(truncated.clone()),
            request_body_state: Some(UsageBodyCaptureState::Truncated),
            provider_request_body: Some(truncated),
            provider_request_body_state: Some(UsageBodyCaptureState::Truncated),
            request_metadata: Some(json!({
                "requested_reasoning_effort": "xhigh",
                "provider_reasoning_effort": "max",
                "provider_service_tier": "priority"
            })),
            ..UsageEventData::default()
        },
    );

    preserve_request_facts(&mut event);

    let metadata = event
        .data
        .request_metadata
        .as_ref()
        .expect("metadata remains");
    assert_eq!(metadata["requested_reasoning_effort"], "xhigh");
    assert_eq!(metadata["provider_reasoning_effort"], "max");
    assert_eq!(metadata["provider_service_tier"], "priority");
}

#[test]
fn preserve_request_facts_clears_stale_facts_when_final_bodies_are_missing() {
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "req-missing-final-body",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            request_body_state: Some(UsageBodyCaptureState::None),
            provider_request_body_state: Some(UsageBodyCaptureState::None),
            request_metadata: Some(json!({
                "requested_reasoning_effort": "xhigh",
                "provider_reasoning_effort": "max",
                "provider_service_tier": "priority",
                "provider_actual_service_tier": "priority"
            })),
            ..UsageEventData::default()
        },
    );

    preserve_request_facts(&mut event);

    let metadata = event
        .data
        .request_metadata
        .as_ref()
        .expect("response audit fact remains");
    assert!(metadata.get("requested_reasoning_effort").is_none());
    assert!(metadata.get("provider_reasoning_effort").is_none());
    assert!(metadata.get("provider_service_tier").is_none());
    assert_eq!(metadata["provider_actual_service_tier"], "priority");
}

#[test]
fn event_capture_budget_full_policy_preserves_facts_before_zero_budget_and_database_mapping() {
    let mut event = UsageEvent::new(
        UsageEventType::Completed,
        "event-capture-budget-facts",
        UsageEventData {
            provider_name: "openai".to_string(),
            model: "gpt-5.6-sol".to_string(),
            endpoint_api_format: Some("openai:responses".to_string()),
            input_tokens: Some(100),
            output_tokens: Some(500),
            total_tokens: Some(600),
            cache_creation_input_tokens: Some(25),
            cache_read_input_tokens: Some(0),
            request_body: Some(json!({"reasoning": {"effort": "high"}})),
            provider_request_body: Some(json!({
                "model": "gpt-5.6-sol", "reasoning": {"effort": "medium"},
                "service_tier": "priority"
            })),
            response_body: Some(json!({"service_tier": "Default"})),
            request_metadata: Some(json!({
                "plan_usage_reservation_token": "550e8400-e29b-41d4-a716-446655440000"
            })),
            ..UsageEventData::default()
        },
    );
    preserve_request_facts(&mut event);
    preserve_provider_response_facts(&mut event);
    apply_usage_body_capture_policy_to_event(
        UsageBodyCapturePolicy {
            record_level: UsageRequestRecordLevel::Full,
            ..UsageBodyCapturePolicy::default()
        },
        &mut event,
    );
    let budget = Arc::new(crate::event_capture_budget::EventCaptureMemoryBudget::new(
        0,
    ));
    event.data.apply_capture_memory_budget(Arc::clone(&budget));
    let record = crate::build_upsert_usage_record_from_event(&event).expect("database mapping");
    assert_eq!(record.input_tokens, Some(100));
    assert_eq!(record.output_tokens, Some(500));
    assert_eq!(record.cache_creation_input_tokens, Some(25));
    assert_eq!(record.cache_read_input_tokens, Some(0));
    assert!(record.request_body.is_none());
    assert!(record.provider_request_body.is_none());
    assert!(record.response_body.is_none());
    assert_eq!(
        record.provider_request_body_state,
        Some(UsageBodyCaptureState::Truncated)
    );
    let metadata = record.request_metadata.expect("preserved billing facts");
    assert_eq!(metadata["requested_reasoning_effort"], "high");
    assert_eq!(metadata["provider_reasoning_effort"], "medium");
    assert_eq!(metadata["provider_service_tier"], "priority");
    assert_eq!(metadata["provider_actual_service_tier"], "default");
    assert_eq!(metadata["provider_cache_ttl_minutes"], 30);
    assert_eq!(
        metadata["plan_usage_reservation_token"],
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(budget.retained_bytes(), 0);
}

#[test]
fn basic_request_record_level_strips_body_capture_but_preserves_derived_fields() {
    let mut event = UsageEvent::new(
        UsageEventType::Failed,
        "req-basic-1",
        UsageEventData {
            provider_name: "OpenAI".to_string(),
            model: "gpt-5".to_string(),
            total_tokens: Some(42),
            error_message: Some("upstream failed".to_string()),
            request_body: Some(json!({
                "messages":[{"role":"user","content":"hello"}],
                "reasoning": {"effort": "xhigh"}
            })),
            request_body_ref: Some("usage://request/req-basic-1/request_body".to_string()),
            provider_request_body: Some(json!({
                "model":"gpt-5",
                "reasoning": {"effort": "max"},
                "service_tier": "priority"
            })),
            provider_request_body_ref: Some(
                "usage://request/req-basic-1/provider_request_body".to_string(),
            ),
            response_body: Some(json!({
                "error":{"message":"bad gateway"},
                "service_tier": "Default"
            })),
            response_body_ref: Some("usage://request/req-basic-1/response_body".to_string()),
            client_response_body: Some(json!({"detail":"bad gateway"})),
            client_response_body_ref: Some(
                "usage://request/req-basic-1/client_response_body".to_string(),
            ),
            request_metadata: Some(json!({
                "requested_reasoning_effort": "low",
                "provider_reasoning_effort": "medium",
                "provider_service_tier": "standard"
            })),
            ..UsageEventData::default()
        },
    );

    preserve_request_facts(&mut event);
    preserve_provider_response_facts(&mut event);
    apply_usage_body_capture_policy_to_event(
        UsageBodyCapturePolicy {
            record_level: UsageRequestRecordLevel::Basic,
            ..UsageBodyCapturePolicy::default()
        },
        &mut event,
    );

    assert_eq!(event.data.total_tokens, Some(42));
    assert_eq!(event.data.error_message.as_deref(), Some("upstream failed"));
    assert!(event.data.request_body.is_none());
    assert!(event.data.request_body_ref.is_none());
    assert!(event.data.provider_request_body.is_none());
    assert!(event.data.provider_request_body_ref.is_none());
    assert!(event.data.response_body.is_none());
    assert!(event.data.response_body_ref.is_none());
    assert!(event.data.client_response_body.is_none());
    assert!(event.data.client_response_body_ref.is_none());
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("requested_reasoning_effort"))
            .and_then(serde_json::Value::as_str),
        Some("xhigh")
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("provider_reasoning_effort"))
            .and_then(serde_json::Value::as_str),
        Some("max")
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("provider_service_tier"))
            .and_then(serde_json::Value::as_str),
        Some("priority")
    );
    assert_eq!(
        event
            .data
            .request_metadata
            .as_ref()
            .and_then(|metadata| metadata.get("provider_actual_service_tier"))
            .and_then(serde_json::Value::as_str),
        Some("default")
    );
}
