mod dead_letter_transfer {
    include!("../worker_dead_letter_tests.rs");
}
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aether_data_contracts::repository::settlement::{
    ReconcileUsagePolicyCostInput, StoredUsagePolicyCostReservation, StoredUsageSettlement,
    UsageSettlementInput,
};
use aether_data_contracts::repository::usage::{
    StoredRequestUsageAudit, UpsertUsageRecord, UsageBodyCaptureState,
};
use aether_data_contracts::DataLayerError;
use aether_runtime_state::{
    MemoryRuntimeStateConfig, RuntimeQueueEntry, RuntimeQueueReclaimConfig,
    RuntimeQueueReclaimPage, RuntimeQueueStats, RuntimeQueueStore, RuntimeState,
};
use async_trait::async_trait;
use tokio::sync::Notify;

use super::{
    build_usage_queue_worker_with_record_gate, usage_event_record_error_is_permanent,
    write_event_record, ManualProxyNodeCounter, UsageEventRecorder, UsageQueueWorker,
    UsageRecordWriter, UsageWorkerControl,
};
use crate::dead_letter_encoding::DeadLetterEncodingBudget;
use crate::event_capture_budget::EventCaptureMemoryBudget;
use crate::queue_read_budget::QueueReadBudget;
use crate::runtime::UsageWorkerRecordConcurrencyGate;
use crate::UsageBillingEventEnricher;
use crate::{
    UsageEvent, UsageEventData, UsageEventType, UsageQueue, UsageRuntimeConfig,
    UsageSettlementWriter,
};

#[derive(Default)]
struct TestUsageStore {
    records: Mutex<Vec<UpsertUsageRecord>>,
    settlements: Mutex<Vec<UsageSettlementInput>>,
    reconciliations: Mutex<Vec<ReconcileUsagePolicyCostInput>>,
    enrich_calls: Mutex<Vec<String>>,
    enrich_outcomes: Mutex<VecDeque<TestEnrichmentOutcome>>,
    manual_proxy_counter_calls: AtomicUsize,
}

enum TestEnrichmentOutcome {
    TimedOut,
    Unpriced,
    Priced { listed: f64, actual: f64 },
}

#[derive(Default)]
struct ControlledRecorder {
    entered: Notify,
    release: Notify,
    calls: AtomicUsize,
}

#[async_trait]
impl UsageEventRecorder for ControlledRecorder {
    async fn record_usage_event(&self, _event: &UsageEvent) -> Result<(), DataLayerError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

#[derive(Default)]
struct SelectiveFailingRecorder {
    calls: Mutex<Vec<String>>,
}

enum CaptureRecordOutcome {
    RetryOnce,
    PermanentFailure,
    Wait,
}

struct CaptureBudgetRecorder {
    budget: Arc<EventCaptureMemoryBudget>,
    outcome: CaptureRecordOutcome,
    calls: AtomicUsize,
    entered: Notify,
}

#[async_trait]
impl UsageEventRecorder for CaptureBudgetRecorder {
    async fn record_usage_event(&self, event: &UsageEvent) -> Result<(), DataLayerError> {
        let call = self.calls.fetch_add(1, Ordering::AcqRel);
        assert_eq!(event.data.input_tokens, Some(4));
        assert_eq!(event.data.output_tokens, Some(6));
        assert_eq!(event.data.total_tokens, Some(10));
        assert_eq!(event.data.cache_read_input_tokens, Some(0));
        assert_eq!(event.data.actual_total_cost_usd, Some(0.123));
        let metadata = event.data.request_metadata.as_ref().expect("billing facts");
        assert_eq!(metadata["requested_reasoning_effort"], "high");
        assert_eq!(metadata["provider_service_tier"], "priority");
        assert_eq!(metadata["provider_actual_service_tier"], "default");
        if event.data.response_body.is_some() {
            let retained = self.budget.retained_bytes();
            assert!(retained > 0);
            let recorder_copy = event.clone();
            assert!(recorder_copy.data.response_body.is_some());
            assert!(self.budget.retained_bytes() > retained);
            drop(recorder_copy);
            assert_eq!(self.budget.retained_bytes(), retained);
        } else {
            assert_eq!(self.budget.retained_bytes(), 0);
            assert_eq!(
                event.data.response_body_state,
                Some(UsageBodyCaptureState::Truncated)
            );
        }
        self.entered.notify_one();
        match self.outcome {
            CaptureRecordOutcome::RetryOnce if call == 0 => Err(DataLayerError::TimedOut(
                "retry test database write".to_string(),
            )),
            CaptureRecordOutcome::RetryOnce => Ok(()),
            CaptureRecordOutcome::PermanentFailure => Err(DataLayerError::UnexpectedValue(
                "permanent capture test error".to_string(),
            )),
            CaptureRecordOutcome::Wait => std::future::pending().await,
        }
    }
}

async fn capture_budget_worker(
    budget_bytes: usize,
    outcome: CaptureRecordOutcome,
) -> (
    Arc<RuntimeState>,
    UsageQueueWorker,
    Arc<CaptureBudgetRecorder>,
) {
    let runner = Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue_runner: Arc<dyn RuntimeQueueStore> = runner.clone();
    let budget = Arc::new(EventCaptureMemoryBudget::new(budget_bytes));
    let recorder = Arc::new(CaptureBudgetRecorder {
        budget: Arc::clone(&budget),
        outcome,
        calls: AtomicUsize::new(0),
        entered: Notify::new(),
    });
    let config = UsageRuntimeConfig {
        enabled: true,
        stream_key: "usage:test:worker:capture".to_string(),
        consumer_group: "usage:test:worker:capture-group".to_string(),
        dlq_stream_key: "usage:test:worker:capture-dlq".to_string(),
        consumer_batch_size: 1,
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let mut worker = UsageQueueWorker::new(queue_runner, recorder.clone(), config, None)
        .expect("worker should build");
    worker.capture_memory_budget = budget;
    worker.queue =
        worker
            .queue
            .with_dead_letter_encoding_budget(Arc::new(DeadLetterEncodingBudget::new(
                64 * 1024 * 1024,
                4,
            )));
    worker
        .queue
        .ensure_consumer_group()
        .await
        .expect("consumer group");
    (runner, worker, recorder)
}

fn captured_worker_event() -> UsageEvent {
    let mut event = sample_event();
    event.data.model = "gpt-5.6-sol".to_string();
    event.data.endpoint_api_format = Some("openai:responses".to_string());
    event.data.request_body = Some(serde_json::json!({"reasoning": {"effort": "high"}}));
    event.data.provider_request_body = Some(serde_json::json!({
        "model": "gpt-5.6-sol", "service_tier": "priority"
    }));
    event.data.response_body = Some(serde_json::json!({
        "service_tier": "Default", "output": "x".repeat(1024)
    }));
    event.data.cache_read_input_tokens = Some(0);
    event.data.actual_total_cost_usd = Some(0.123);
    event
}

#[derive(Default)]
struct SlowUsageStore {
    active: std::sync::atomic::AtomicUsize,
    max_active: std::sync::atomic::AtomicUsize,
    db_pressure: AtomicBool,
    records: Mutex<Vec<String>>,
}

struct ReadReclaimRaceProbeQueue {
    entry: RuntimeQueueEntry,
    read_calls: AtomicUsize,
    first_read_cancelled: AtomicBool,
    read_completed: AtomicUsize,
    release_read: Notify,
    reclaim_calls: AtomicUsize,
    reclaim_pages: Mutex<Option<VecDeque<Result<RuntimeQueueReclaimPage, DataLayerError>>>>,
    reclaim_cursors: Mutex<Vec<String>>,
    acked: AtomicBool,
    requested_counts: Mutex<Vec<usize>>,
    block_ack: AtomicBool,
    ack_entered: Notify,
    release_ack: Notify,
    block_reclaim: AtomicBool,
    reclaim_cancelled: AtomicBool,
}

impl ReadReclaimRaceProbeQueue {
    fn new(entry: RuntimeQueueEntry) -> Self {
        Self {
            entry,
            read_calls: AtomicUsize::new(0),
            first_read_cancelled: AtomicBool::new(false),
            read_completed: AtomicUsize::new(0),
            release_read: Notify::new(),
            reclaim_calls: AtomicUsize::new(0),
            reclaim_pages: Mutex::new(None),
            reclaim_cursors: Mutex::new(Vec::new()),
            acked: AtomicBool::new(false),
            requested_counts: Mutex::new(Vec::new()),
            block_ack: AtomicBool::new(false),
            ack_entered: Notify::new(),
            release_ack: Notify::new(),
            block_reclaim: AtomicBool::new(false),
            reclaim_cancelled: AtomicBool::new(false),
        }
    }
}

struct FirstReadDropGuard<'a> {
    cancelled: &'a AtomicBool,
    completed: bool,
}

impl Drop for FirstReadDropGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled.store(true, Ordering::Release);
        }
    }
}

#[async_trait]
impl RuntimeQueueStore for ReadReclaimRaceProbeQueue {
    async fn ensure_consumer_group(
        &self,
        _stream: &str,
        _group: &str,
        _start_id: &str,
    ) -> Result<(), DataLayerError> {
        Ok(())
    }

    async fn append_fields_with_maxlen(
        &self,
        _stream: &str,
        _fields: &BTreeMap<String, String>,
        _maxlen: Option<usize>,
    ) -> Result<String, DataLayerError> {
        Ok("0-0".to_string())
    }

    async fn read_group(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        count: usize,
        _block_ms: Option<u64>,
    ) -> Result<Vec<RuntimeQueueEntry>, DataLayerError> {
        let call_index = self.read_calls.fetch_add(1, Ordering::AcqRel);
        self.requested_counts
            .lock()
            .expect("requested counts lock")
            .push(count);
        let mut first_read_guard = (call_index == 0).then(|| FirstReadDropGuard {
            cancelled: &self.first_read_cancelled,
            completed: false,
        });
        self.release_read.notified().await;
        if let Some(guard) = first_read_guard.as_mut() {
            guard.completed = true;
        }
        self.read_completed.fetch_add(1, Ordering::AcqRel);
        Ok((call_index == 0)
            .then(|| self.entry.clone())
            .into_iter()
            .collect())
    }

    async fn claim_stale(
        &self,
        _stream: &str,
        _group: &str,
        _consumer: &str,
        _start_id: &str,
        _config: RuntimeQueueReclaimConfig,
    ) -> Result<Vec<RuntimeQueueEntry>, DataLayerError> {
        self.reclaim_calls.fetch_add(1, Ordering::AcqRel);
        Ok((!self.acked.load(Ordering::Acquire))
            .then(|| self.entry.clone())
            .into_iter()
            .collect())
    }

    async fn claim_stale_page(
        &self,
        stream: &str,
        group: &str,
        consumer: &str,
        start_id: &str,
        config: RuntimeQueueReclaimConfig,
    ) -> Result<RuntimeQueueReclaimPage, DataLayerError> {
        self.reclaim_cursors
            .lock()
            .expect("reclaim cursors lock")
            .push(start_id.to_string());
        if self.block_reclaim.load(Ordering::Acquire) {
            let _guard = FirstReadDropGuard {
                cancelled: &self.reclaim_cancelled,
                completed: false,
            };
            return std::future::pending().await;
        }
        let scripted = self
            .reclaim_pages
            .lock()
            .expect("reclaim pages lock")
            .as_mut()
            .map(|pages| pages.pop_front().expect("scripted reclaim page"));
        if let Some(page) = scripted {
            self.reclaim_calls.fetch_add(1, Ordering::AcqRel);
            return page;
        }
        Ok(RuntimeQueueReclaimPage {
            next_start_id: "0-0".to_string(),
            entries: self
                .claim_stale(stream, group, consumer, start_id, config)
                .await?,
            deleted_ids: Vec::new(),
        })
    }

    async fn ack(
        &self,
        _stream: &str,
        _group: &str,
        ids: &[String],
    ) -> Result<usize, DataLayerError> {
        if self.block_ack.load(Ordering::Acquire) {
            self.ack_entered.notify_one();
            self.release_ack.notified().await;
        }
        if ids.iter().any(|id| id == &self.entry.id) {
            self.acked.store(true, Ordering::Release);
            Ok(1)
        } else {
            Ok(0)
        }
    }

    async fn delete(&self, _stream: &str, ids: &[String]) -> Result<usize, DataLayerError> {
        Ok(ids.len())
    }

    async fn stats(
        &self,
        _stream: &str,
        _group: Option<&str>,
    ) -> Result<RuntimeQueueStats, DataLayerError> {
        Ok(RuntimeQueueStats::default())
    }
}

#[async_trait]
impl UsageRecordWriter for TestUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, aether_data_contracts::DataLayerError> {
        self.records
            .lock()
            .expect("records lock")
            .push(record.clone());
        let mut stored = StoredRequestUsageAudit::new(
            "usage-1".to_string(),
            record.request_id,
            record.user_id,
            record.api_key_id,
            record.username,
            record.api_key_name,
            record.provider_name,
            record.model,
            record.target_model,
            record.provider_id,
            record.provider_endpoint_id,
            record.provider_api_key_id,
            record.request_type,
            record.api_format,
            record.api_family,
            record.endpoint_kind,
            record.endpoint_api_format,
            record.provider_api_family,
            record.provider_endpoint_kind,
            record.has_format_conversion.unwrap_or(false),
            record.is_stream.unwrap_or(false),
            record.input_tokens.unwrap_or_default() as i32,
            record.output_tokens.unwrap_or_default() as i32,
            record.total_tokens.unwrap_or_default() as i32,
            record.total_cost_usd.unwrap_or_default(),
            record.actual_total_cost_usd.unwrap_or_default(),
            record.status_code.map(i32::from),
            record.error_message,
            record.error_category,
            record.response_time_ms.map(|value| value as i32),
            record.first_byte_time_ms.map(|value| value as i32),
            record.status,
            record.billing_status,
            record
                .created_at_unix_ms
                .unwrap_or(record.updated_at_unix_secs) as i64,
            record.updated_at_unix_secs as i64,
            record.finalized_at_unix_secs.map(|value| value as i64),
        )
        .expect("stored usage should build");
        stored.request_metadata = record.request_metadata;
        Ok(Some(stored))
    }
}

#[async_trait]
impl UsageSettlementWriter for TestUsageStore {
    fn has_usage_settlement_writer(&self) -> bool {
        true
    }

    async fn reconcile_usage_policy_cost(
        &self,
        input: ReconcileUsagePolicyCostInput,
    ) -> Result<Option<StoredUsagePolicyCostReservation>, DataLayerError> {
        self.reconciliations
            .lock()
            .expect("reconciliations lock")
            .push(input);
        Ok(None)
    }

    async fn settle_usage(
        &self,
        input: UsageSettlementInput,
    ) -> Result<Option<StoredUsageSettlement>, aether_data_contracts::DataLayerError> {
        self.settlements
            .lock()
            .expect("settlements lock")
            .push(input);
        Ok(None)
    }
}

#[async_trait]
impl ManualProxyNodeCounter for TestUsageStore {
    async fn increment_manual_proxy_node_requests(
        &self,
        _node_id: &str,
        _total_delta: i64,
        _failed_delta: i64,
        _latency_ms: Option<i64>,
    ) -> Result<(), aether_data_contracts::DataLayerError> {
        self.manual_proxy_counter_calls
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[async_trait]
impl UsageBillingEventEnricher for TestUsageStore {
    async fn enrich_usage_event(&self, event: &mut UsageEvent) -> Result<(), DataLayerError> {
        self.enrich_calls
            .lock()
            .expect("enrich calls lock")
            .push(event.request_id.clone());
        match self
            .enrich_outcomes
            .lock()
            .expect("enrich outcomes lock")
            .pop_front()
        {
            Some(TestEnrichmentOutcome::TimedOut) => {
                return Err(DataLayerError::TimedOut("test pricing lookup".to_string()));
            }
            Some(TestEnrichmentOutcome::Unpriced) => return Ok(()),
            Some(TestEnrichmentOutcome::Priced { listed, actual }) => {
                event.data.total_cost_usd = Some(listed);
                event.data.actual_total_cost_usd = Some(actual);
                return Ok(());
            }
            None => {}
        }
        event.data.total_cost_usd = Some(0.456);
        Ok(())
    }
}

impl crate::runtime::UsageRuntimeAccess for TestUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        None
    }
}

#[async_trait]
impl UsageRecordWriter for SlowUsageStore {
    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        let active = self
            .active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        self.max_active
            .fetch_max(active, std::sync::atomic::Ordering::AcqRel);
        tokio::time::sleep(Duration::from_millis(30)).await;
        self.records
            .lock()
            .expect("records lock")
            .push(record.request_id.clone());
        self.active
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        Ok(None)
    }
}

#[async_trait]
impl UsageSettlementWriter for SlowUsageStore {
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
impl ManualProxyNodeCounter for SlowUsageStore {
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
impl UsageBillingEventEnricher for SlowUsageStore {
    async fn enrich_usage_event(&self, _event: &mut UsageEvent) -> Result<(), DataLayerError> {
        Ok(())
    }
}

impl crate::runtime::UsageRuntimeAccess for SlowUsageStore {
    fn has_usage_writer(&self) -> bool {
        true
    }

    fn has_usage_worker_queue(&self) -> bool {
        true
    }

    fn usage_worker_queue(&self) -> Option<Arc<dyn RuntimeQueueStore>> {
        None
    }

    fn usage_worker_should_defer_for_database_pressure(&self) -> bool {
        self.db_pressure.load(Ordering::Acquire)
    }
}

#[async_trait]
impl UsageEventRecorder for SelectiveFailingRecorder {
    async fn record_usage_event(&self, event: &UsageEvent) -> Result<(), DataLayerError> {
        self.calls
            .lock()
            .expect("calls lock")
            .push(event.request_id.clone());
        if event.request_id == "req-worker-poison" {
            return Err(DataLayerError::UnexpectedValue(
                "permanent test error".to_string(),
            ));
        }
        Ok(())
    }
}

fn sample_event() -> UsageEvent {
    UsageEvent::new(
        UsageEventType::Completed,
        "req-worker-123".to_string(),
        UsageEventData {
            user_id: Some("user-worker-123".to_string()),
            api_key_id: Some("api-key-worker-123".to_string()),
            provider_name: "openai".to_string(),
            provider_id: Some("provider-worker-123".to_string()),
            provider_endpoint_id: Some("endpoint-worker-123".to_string()),
            provider_api_key_id: Some("provider-key-worker-123".to_string()),
            model: "gpt-5".to_string(),
            api_format: Some("openai:chat".to_string()),
            endpoint_api_format: Some("openai:chat".to_string()),
            is_stream: Some(false),
            status_code: Some(200),
            input_tokens: Some(4),
            output_tokens: Some(6),
            total_tokens: Some(10),
            response_time_ms: Some(52),
            ..UsageEventData::default()
        },
    )
}

#[tokio::test]
async fn write_event_record_persists_usage_and_triggers_settlement() {
    let store = TestUsageStore::default();
    let event = sample_event();

    write_event_record(&store, &event)
        .await
        .expect("worker should write usage record");

    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].request_id, "req-worker-123");
    assert_eq!(records[0].status, "completed");
    drop(records);

    let settlements = store.settlements.lock().expect("settlements lock");
    assert_eq!(settlements.len(), 1);
    assert_eq!(settlements[0].request_id, "req-worker-123");
}

#[tokio::test]
async fn same_request_id_terminal_events_reconcile_each_persisted_reservation_token() {
    let store = TestUsageStore::default();
    let mut first = sample_event();
    first.request_id = "shared-client-trace".to_string();
    first.data.actual_total_cost_usd = Some(0.25);
    first.data.request_metadata = Some(serde_json::json!({
        "plan_usage_reservation_token": "server-token-a"
    }));
    let mut second = first.clone();
    second.data.actual_total_cost_usd = Some(0.75);
    second.data.request_metadata = Some(serde_json::json!({
        "plan_usage_reservation_token": "server-token-b"
    }));

    write_event_record(&store, &first)
        .await
        .expect("first terminal event");
    write_event_record(&store, &second)
        .await
        .expect("second terminal event");

    let reconciliations = store.reconciliations.lock().expect("reconciliations lock");
    assert_eq!(reconciliations.len(), 2);
    assert_eq!(reconciliations[0].request_id, "shared-client-trace");
    assert_eq!(reconciliations[0].reservation_token, "server-token-a");
    assert_eq!(reconciliations[0].actual_cost_units, 25_000_000);
    assert_eq!(reconciliations[1].request_id, "shared-client-trace");
    assert_eq!(reconciliations[1].reservation_token, "server-token-b");
    assert_eq!(reconciliations[1].actual_cost_units, 75_000_000);
    assert_eq!(store.records.lock().expect("records lock").len(), 2);
}

#[tokio::test]
async fn replayable_usage_write_does_not_duplicate_transport_owned_proxy_counter() {
    let store = TestUsageStore::default();
    let mut event = sample_event();
    event.data.request_metadata = Some(serde_json::json!({
        "proxy": {"mode": "manual", "node_id": "manual-node-1"}
    }));

    write_event_record(&store, &event)
        .await
        .expect("first usage write should succeed");
    write_event_record(&store, &event)
        .await
        .expect("replayed usage write should succeed");

    assert_eq!(
        store.manual_proxy_counter_calls.load(Ordering::Acquire),
        0,
        "proxy traffic belongs to the transport attempt, not the replayable usage worker"
    );
}

#[tokio::test]
async fn data_event_recorder_enriches_terminal_event_before_write() {
    let store = Arc::new(TestUsageStore::default());
    let recorder = super::UsageDataEventRecorder::new(Arc::clone(&store));
    let event = sample_event();

    recorder
        .record_usage_event(&event)
        .await
        .expect("recorder should enrich and write usage");

    assert_eq!(
        store
            .enrich_calls
            .lock()
            .expect("enrich calls lock")
            .as_slice(),
        ["req-worker-123"]
    );
    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].total_cost_usd, Some(0.456));
}

#[tokio::test]
async fn data_event_recorder_pricing_timeout_stays_pending_until_successful_reclaim() {
    let runner = Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = Arc::new(TestUsageStore::default());
    store
        .enrich_outcomes
        .lock()
        .expect("enrich outcomes lock")
        .extend([
            TestEnrichmentOutcome::TimedOut,
            TestEnrichmentOutcome::Priced {
                listed: 0.456,
                actual: 0.123,
            },
        ]);
    let config = UsageRuntimeConfig {
        consumer_batch_size: 1,
        consumer_block_ms: 1,
        reclaim_count: 1,
        reclaim_idle_ms: 1,
        queue_payload_max_bytes: 4096,
        ..UsageRuntimeConfig::default()
    };
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let mut worker =
        build_usage_queue_worker_with_record_gate(runner, store.clone(), config, None, None)
            .expect("worker should build");
    worker.queue = worker.queue.with_read_budget(Arc::clone(&budget));
    worker.queue.ensure_consumer_group().await.expect("group");
    let mut event = sample_event();
    event.data.request_metadata = Some(serde_json::json!({
        "plan_usage_reservation_token": "pricing-retry-reservation",
        "billing_multiplier_snapshot": {
            "version": 1,
            "factors": {"routing_group": 3.0, "promotion": 0.5},
            "multiplier": 1.5
        }
    }));
    worker.queue.enqueue(&event).await.expect("enqueue");
    let batch = worker
        .queue
        .read_group_reserved(&worker.consumer)
        .await
        .expect("read");
    let error = worker
        .process_entries(batch.entries)
        .await
        .expect_err("pricing timeout must reach the worker");
    drop(batch.reservation);
    assert!(matches!(error, DataLayerError::TimedOut(_)));
    assert!(store.records.lock().expect("records lock").is_empty());
    assert!(store
        .reconciliations
        .lock()
        .expect("reconciliations lock")
        .is_empty());
    assert!(store
        .settlements
        .lock()
        .expect("settlements lock")
        .is_empty());
    let stats = worker.queue.stats().await.expect("pending stats");
    assert_eq!((stats.stream_length, stats.group_pending), (1, 1));
    assert_eq!(
        worker
            .queue
            .dlq_stats()
            .await
            .expect("dlq stats")
            .stream_length,
        0
    );
    assert_eq!(budget.snapshot().reserved_bytes, 0);

    let mut cursor = "0-0".to_string();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            tokio::time::sleep(Duration::from_millis(1)).await;
            worker.reclaim_stale_entries(&mut cursor).await;
            if !store.records.lock().expect("records lock").is_empty() {
                break;
            }
        }
    })
    .await
    .expect("pending entry should become reclaimable");

    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].total_cost_usd, Some(0.456));
        assert_eq!(records[0].actual_total_cost_usd, Some(0.123));
        assert_eq!(records[0].total_tokens, Some(10));
        assert_eq!(
            records[0].request_metadata.as_ref().unwrap()["billing_multiplier_snapshot"]
                ["multiplier"],
            1.5
        );
    }
    {
        let reconciliations = store.reconciliations.lock().expect("reconciliations lock");
        assert_eq!(reconciliations.len(), 1);
        assert_eq!(reconciliations[0].actual_cost_units, 68_400_000);
        assert_eq!(
            reconciliations[0].reservation_token,
            "pricing-retry-reservation"
        );
    }
    {
        let settlements = store.settlements.lock().expect("settlements lock");
        assert_eq!(settlements.len(), 1);
        assert_eq!(settlements[0].billing_cost_usd, Some(0.456 * 1.5));
        assert_eq!(settlements[0].actual_total_cost_usd, 0.123);
    }
    assert_eq!(
        store.enrich_calls.lock().expect("enrich calls lock").len(),
        2
    );
    let stats = worker.queue.stats().await.expect("acked stats");
    assert_eq!((stats.stream_length, stats.group_pending), (0, 0));
    assert_eq!(
        worker
            .queue
            .dlq_stats()
            .await
            .expect("dlq stats")
            .stream_length,
        0
    );
    assert_eq!(budget.snapshot().reserved_bytes, 0);
}

#[tokio::test]
async fn data_event_recorder_successful_unpriced_enrichment_keeps_existing_write_behavior() {
    let store = Arc::new(TestUsageStore::default());
    store
        .enrich_outcomes
        .lock()
        .expect("enrich outcomes lock")
        .push_back(TestEnrichmentOutcome::Unpriced);
    let recorder = super::UsageDataEventRecorder::new(Arc::clone(&store));
    recorder
        .record_usage_event(&sample_event())
        .await
        .expect("missing pricing is a successful enrichment outcome");
    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].total_cost_usd, None);
    assert_eq!(records[0].actual_total_cost_usd, None);
}

#[tokio::test]
async fn data_event_recorder_skips_enrichment_for_lifecycle_event() {
    let store = Arc::new(TestUsageStore::default());
    let recorder = super::UsageDataEventRecorder::new(Arc::clone(&store));
    let mut event = sample_event();
    event.event_type = UsageEventType::Pending;

    recorder
        .record_usage_event(&event)
        .await
        .expect("recorder should write lifecycle usage");

    assert!(store
        .enrich_calls
        .lock()
        .expect("enrich calls lock")
        .is_empty());
    let records = store.records.lock().expect("records lock");
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].total_cost_usd, None);
}

#[tokio::test]
async fn data_event_recorder_serializes_same_request_id_writes() {
    let store = Arc::new(SlowUsageStore::default());
    let recorder = Arc::new(super::UsageDataEventRecorder::new(Arc::clone(&store)));
    let mut first = sample_event();
    first.request_id = "req-same".to_string();
    first.event_type = UsageEventType::Pending;
    let mut second = sample_event();
    second.request_id = "req-same".to_string();
    second.event_type = UsageEventType::Completed;

    let first_recorder = Arc::clone(&recorder);
    let second_recorder = Arc::clone(&recorder);
    tokio::try_join!(
        async move { first_recorder.record_usage_event(&first).await },
        async move { second_recorder.record_usage_event(&second).await }
    )
    .expect("same request writes should both succeed");

    assert_eq!(
        store.max_active.load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert_eq!(store.records.lock().expect("records lock").len(), 2);
}

#[tokio::test]
async fn data_event_recorder_defers_when_database_pool_is_under_pressure() {
    let store = Arc::new(SlowUsageStore::default());
    store.db_pressure.store(true, Ordering::Release);
    let gate = Arc::new(UsageWorkerRecordConcurrencyGate::new(1));
    let recorder = super::UsageDataEventRecorder::with_record_gate_and_database_pressure_defer(
        Arc::clone(&store),
        Some(Arc::clone(&gate)),
    );

    recorder
        .record_usage_event(&sample_event())
        .await
        .expect("recorder should write after brief defer");

    assert_eq!(gate.deferred_total(), 1);
    assert_eq!(store.records.lock().expect("records lock").len(), 1);
}

#[tokio::test]
async fn usage_worker_record_gate_limits_concurrent_record_writes() {
    let runner = Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue_runner: Arc<dyn RuntimeQueueStore> = runner.clone();
    let store = Arc::new(SlowUsageStore::default());
    let gate = Arc::new(UsageWorkerRecordConcurrencyGate::new(2));
    let config = UsageRuntimeConfig {
        enabled: true,
        stream_key: "usage:test:worker:record-gate".to_string(),
        consumer_group: "usage:test:worker:record-gate-group".to_string(),
        dlq_stream_key: "usage:test:worker:record-gate-dlq".to_string(),
        consumer_batch_size: 1,
        consumer_block_ms: 1,
        worker_record_concurrency_limit: Some(2),
        ..UsageRuntimeConfig::default()
    };
    let mut handles = Vec::new();
    for worker_index in 0..4 {
        let worker = build_usage_queue_worker_with_record_gate(
            Arc::clone(&queue_runner),
            Arc::clone(&store),
            config.clone(),
            Some(Arc::clone(&gate)),
            Some(worker_index),
        )
        .expect("worker should build");
        worker
            .queue
            .ensure_consumer_group()
            .await
            .expect("group should initialize");
        handles.push(tokio::spawn(async move {
            let entries = worker
                .queue
                .read_group(&worker.consumer)
                .await
                .expect("event should read");
            worker
                .process_entries(entries)
                .await
                .expect("event should process");
        }));
    }

    for index in 0..4 {
        let mut event = sample_event();
        event.request_id = format!("req-record-gate-{index}");
        UsageQueue::new(queue_runner.clone(), config.clone())
            .expect("queue should build")
            .enqueue(&event)
            .await
            .expect("event should enqueue");
    }

    for handle in handles {
        handle.await.expect("worker should complete");
    }

    assert_eq!(
        store.max_active.load(std::sync::atomic::Ordering::Acquire),
        2
    );
    assert_eq!(gate.max_in_flight(), 2);
    assert!(gate.wait_total() > 0);
    assert_eq!(store.records.lock().expect("records lock").len(), 4);
}

#[tokio::test]
async fn usage_worker_reclaim_cursor_advances_on_empty_pages_and_retries_read_errors() {
    let event = sample_event();
    let entry = RuntimeQueueEntry {
        id: "43-0".to_string(),
        fields: event.to_stream_fields().expect("event fields"),
    };
    let runner = Arc::new(ReadReclaimRaceProbeQueue::new(entry.clone()));
    let page = |next_start_id: &str, entries, deleted_ids| RuntimeQueueReclaimPage {
        next_start_id: next_start_id.to_string(),
        entries,
        deleted_ids,
    };
    *runner.reclaim_pages.lock().expect("reclaim pages lock") = Some(VecDeque::from([
        Ok(page("11-0", Vec::new(), vec!["9-0".to_string()])),
        Err(DataLayerError::Redis("temporary reclaim error".to_string())),
        Ok(page("42-0", Vec::new(), Vec::new())),
        Ok(page("0-0", vec![entry], Vec::new())),
        Ok(page("0-0", Vec::new(), Vec::new())),
    ]));
    let recorder = Arc::new(SelectiveFailingRecorder::default());
    let worker = UsageQueueWorker::new(
        runner.clone(),
        recorder.clone(),
        UsageRuntimeConfig::default(),
        None,
    )
    .expect("cursor worker");
    let mut cursor = "0-0".to_string();
    for expected in ["11-0", "11-0", "42-0", "0-0", "0-0"] {
        worker.reclaim_stale_entries(&mut cursor).await;
        assert_eq!(cursor, expected);
    }
    assert_eq!(
        runner
            .reclaim_cursors
            .lock()
            .expect("reclaim cursors lock")
            .as_slice(),
        ["0-0", "11-0", "11-0", "42-0", "0-0"]
    );
    assert_eq!(
        recorder.calls.lock().expect("calls lock").as_slice(),
        [event.request_id]
    );
    assert!(runner.acked.load(Ordering::Acquire));
    assert_eq!(runner.reclaim_calls.load(Ordering::Acquire), 5);
}

#[tokio::test]
async fn usage_worker_reclaim_cursor_advances_after_write_failure_and_revisits_on_wrap() {
    struct FailOnceRecorder(AtomicUsize);

    #[async_trait]
    impl UsageEventRecorder for FailOnceRecorder {
        async fn record_usage_event(&self, _event: &UsageEvent) -> Result<(), DataLayerError> {
            if self.0.fetch_add(1, Ordering::AcqRel) == 0 {
                Err(DataLayerError::TimedOut(
                    "temporary write failure".to_string(),
                ))
            } else {
                Ok(())
            }
        }
    }

    let entry = RuntimeQueueEntry {
        id: "43-0".to_string(),
        fields: sample_event().to_stream_fields().expect("event fields"),
    };
    let runner = Arc::new(ReadReclaimRaceProbeQueue::new(entry.clone()));
    *runner.reclaim_pages.lock().expect("reclaim pages lock") = Some(VecDeque::from([
        Ok(RuntimeQueueReclaimPage {
            next_start_id: "50-0".to_string(),
            entries: vec![entry.clone()],
            deleted_ids: Vec::new(),
        }),
        Ok(RuntimeQueueReclaimPage {
            next_start_id: "0-0".to_string(),
            entries: Vec::new(),
            deleted_ids: Vec::new(),
        }),
        Ok(RuntimeQueueReclaimPage {
            next_start_id: "0-0".to_string(),
            entries: vec![entry],
            deleted_ids: Vec::new(),
        }),
    ]));
    let recorder = Arc::new(FailOnceRecorder(AtomicUsize::new(0)));
    let worker = UsageQueueWorker::new(
        runner.clone(),
        recorder.clone(),
        UsageRuntimeConfig::default(),
        None,
    )
    .expect("cursor worker");
    let mut cursor = "0-0".to_string();
    worker.reclaim_stale_entries(&mut cursor).await;
    assert_eq!(cursor, "50-0");
    assert!(!runner.acked.load(Ordering::Acquire));
    worker.reclaim_stale_entries(&mut cursor).await;
    worker.reclaim_stale_entries(&mut cursor).await;
    assert_eq!(cursor, "0-0");
    assert!(runner.acked.load(Ordering::Acquire));
    assert_eq!(recorder.0.load(Ordering::Acquire), 2);
    assert_eq!(
        runner
            .reclaim_cursors
            .lock()
            .expect("reclaim cursors lock")
            .as_slice(),
        ["0-0", "50-0", "0-0"]
    );
}

#[tokio::test]
async fn usage_worker_defers_reclaim_until_inflight_read_is_processed() {
    let event = sample_event();
    let queue = Arc::new(ReadReclaimRaceProbeQueue::new(RuntimeQueueEntry {
        id: "1-0".to_string(),
        fields: event
            .to_stream_fields()
            .expect("usage event should serialize"),
    }));
    let queue_runner: Arc<dyn RuntimeQueueStore> = queue.clone();
    let config = UsageRuntimeConfig {
        enabled: true,
        stream_key: "usage:test:worker:read-reclaim-race".to_string(),
        consumer_group: "usage:test:worker:read-reclaim-race-group".to_string(),
        dlq_stream_key: "usage:test:worker:read-reclaim-race-dlq".to_string(),
        consumer_batch_size: 1,
        consumer_block_ms: 1_000,
        reclaim_interval_ms: 10,
        reclaim_idle_ms: 1,
        reclaim_count: 1,
        ..UsageRuntimeConfig::default()
    };
    let recorder = Arc::new(SelectiveFailingRecorder::default());
    let worker_recorder: Arc<dyn UsageEventRecorder> = recorder.clone();
    let control = UsageWorkerControl::default();
    let (telemetry_tx, _telemetry_rx) = tokio::sync::mpsc::channel(8);
    let worker = UsageQueueWorker::new(queue_runner, worker_recorder, config, None)
        .expect("worker should build")
        .with_supervisor(control.clone(), telemetry_tx);
    let handle = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(1), async {
        while queue.read_calls.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker should start the blocking read");

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        queue.reclaim_calls.load(Ordering::Acquire),
        0,
        "reclaim must not run while XREADGROUP can still return the same PEL entry"
    );
    assert!(recorder.calls.lock().expect("calls lock").is_empty());

    queue.release_read.notify_one();
    tokio::time::timeout(Duration::from_secs(1), async {
        while !queue.acked.load(Ordering::Acquire)
            || queue.reclaim_calls.load(Ordering::Acquire) == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("read entry should be processed before deferred reclaim runs");

    assert_eq!(
        recorder.calls.lock().expect("calls lock").as_slice(),
        [event.request_id.as_str()],
        "the stream entry must be recorded exactly once"
    );
    assert_eq!(queue.read_completed.load(Ordering::Acquire), 1);
    assert!(!queue.first_read_cancelled.load(Ordering::Acquire));

    control.request_shutdown();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker should stop promptly")
        .expect("worker task should not panic");
}

#[tokio::test]
async fn usage_worker_shutdown_cancels_blocking_read_promptly() {
    let event = sample_event();
    let queue = Arc::new(ReadReclaimRaceProbeQueue::new(RuntimeQueueEntry {
        id: "2-0".to_string(),
        fields: event
            .to_stream_fields()
            .expect("usage event should serialize"),
    }));
    let queue_runner: Arc<dyn RuntimeQueueStore> = queue.clone();
    let config = UsageRuntimeConfig {
        enabled: true,
        stream_key: "usage:test:worker:shutdown-read".to_string(),
        consumer_group: "usage:test:worker:shutdown-read-group".to_string(),
        dlq_stream_key: "usage:test:worker:shutdown-read-dlq".to_string(),
        consumer_batch_size: 1,
        consumer_block_ms: 60_000,
        reclaim_interval_ms: 10,
        reclaim_idle_ms: 1,
        reclaim_count: 1,
        ..UsageRuntimeConfig::default()
    };
    let recorder: Arc<dyn UsageEventRecorder> = Arc::new(SelectiveFailingRecorder::default());
    let control = UsageWorkerControl::default();
    let (telemetry_tx, _telemetry_rx) = tokio::sync::mpsc::channel(8);
    let worker = UsageQueueWorker::new(queue_runner, recorder, config, None)
        .expect("worker should build")
        .with_supervisor(control.clone(), telemetry_tx);
    let handle = tokio::spawn(worker.run());

    tokio::time::timeout(Duration::from_secs(1), async {
        while queue.read_calls.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("worker should start the blocking read");

    control.request_shutdown();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("shutdown should interrupt the blocking read")
        .expect("worker task should not panic");

    assert!(queue.first_read_cancelled.load(Ordering::Acquire));
    assert_eq!(queue.read_completed.load(Ordering::Acquire), 0);
    assert_eq!(queue.reclaim_calls.load(Ordering::Acquire), 0);
}

fn receive_budget_worker_config() -> UsageRuntimeConfig {
    UsageRuntimeConfig {
        consumer_batch_size: 128,
        consumer_block_ms: 60_000,
        reclaim_interval_ms: 60_000,
        queue_payload_max_bytes: 4096,
        ..UsageRuntimeConfig::default()
    }
}

#[tokio::test]
async fn usage_worker_shared_read_budget_waits_for_slow_recorder_and_releases_on_cancel() {
    let runner = Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let config = receive_budget_worker_config();
    let first_recorder = Arc::new(ControlledRecorder::default());
    let second_recorder = Arc::new(ControlledRecorder::default());
    let first_control = UsageWorkerControl::default();
    let (telemetry, _observations) = tokio::sync::mpsc::channel(16);
    let mut first = UsageQueueWorker::new(
        runner.clone(),
        first_recorder.clone(),
        config.clone(),
        Some(0),
    )
    .expect("first worker")
    .with_supervisor(first_control.clone(), telemetry);
    first.queue = first.queue.with_read_budget(Arc::clone(&budget));
    let queue = first.queue.clone();
    let mut second = UsageQueueWorker::new(runner, second_recorder.clone(), config, Some(1))
        .expect("second worker");
    second.queue = second.queue.with_read_budget(Arc::clone(&budget));
    for index in 0..2 {
        let mut event = sample_event();
        event.request_id = format!("receive-budget-{index}");
        queue.enqueue(&event).await.expect("enqueue");
    }

    let first_handle = tokio::spawn(first.run());
    tokio::time::timeout(Duration::from_secs(1), first_recorder.entered.notified())
        .await
        .expect("first recorder should hold a batch");
    assert!(budget.snapshot().reserved_bytes > 0);
    let second_handle = tokio::spawn(second.run());
    tokio::time::timeout(Duration::from_secs(1), async {
        while budget.snapshot().waiters == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("second worker should wait before reading");
    assert_eq!(second_recorder.calls.load(Ordering::Acquire), 0);
    let stats = queue.stats().await.expect("blocked stats");
    assert_eq!((stats.stream_length, stats.group_pending), (2, 1));

    first_control.request_shutdown();
    first_recorder.release.notify_one();
    tokio::time::timeout(Duration::from_secs(1), first_handle)
        .await
        .expect("first worker should finish its acquired batch")
        .expect("first worker task");
    tokio::time::timeout(Duration::from_secs(1), second_recorder.entered.notified())
        .await
        .expect("second worker should acquire released allowance");
    assert!(budget.snapshot().reserved_bytes > 0);
    second_handle.abort();
    assert!(second_handle
        .await
        .expect_err("cancel worker")
        .is_cancelled());
    assert_eq!(budget.snapshot().reserved_bytes, 0);
    assert_eq!(budget.snapshot().waiters, 0);
    let stats = queue.stats().await.expect("cancelled stats");
    assert_eq!((stats.stream_length, stats.group_pending), (1, 1));
}

#[tokio::test]
async fn usage_worker_read_budget_survives_ack_and_reports_actual_requested_count() {
    let runner = Arc::new(ReadReclaimRaceProbeQueue::new(RuntimeQueueEntry {
        id: "7-0".to_string(),
        fields: sample_event().to_stream_fields().expect("event fields"),
    }));
    runner.block_ack.store(true, Ordering::Release);
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let control = UsageWorkerControl::default();
    let (telemetry, mut observations) = tokio::sync::mpsc::channel(16);
    let mut worker = UsageQueueWorker::new(
        runner.clone(),
        Arc::new(SelectiveFailingRecorder::default()),
        receive_budget_worker_config(),
        Some(3),
    )
    .expect("worker")
    .with_supervisor(control.clone(), telemetry);
    worker.queue = worker.queue.with_read_budget(Arc::clone(&budget));
    runner.release_read.notify_one();
    let handle = tokio::spawn(worker.run());
    tokio::time::timeout(Duration::from_secs(1), runner.ack_entered.notified())
        .await
        .expect("worker should reach ACK");
    assert!(budget.snapshot().reserved_bytes > 0);
    assert!(!runner.acked.load(Ordering::Acquire));
    let observation = observations.recv().await.expect("read observation");
    assert_eq!(observation.worker_index, Some(3));
    assert_eq!(observation.entries_read, 1);
    assert_eq!(observation.batch_size, 1);
    assert_eq!(
        runner
            .requested_counts
            .lock()
            .expect("requested counts lock")
            .as_slice(),
        [1]
    );

    control.request_shutdown();
    runner.release_ack.notify_one();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker should finish ACK before shutdown")
        .expect("worker task");
    assert!(runner.acked.load(Ordering::Acquire));
    assert_eq!(budget.snapshot().reserved_bytes, 0);
}

#[tokio::test]
async fn usage_worker_shutdown_cancels_reclaim_budget_wait_without_reading() {
    use std::future::Future;
    use std::task::Poll;

    let runner = Arc::new(ReadReclaimRaceProbeQueue::new(RuntimeQueueEntry {
        id: "8-0".to_string(),
        fields: sample_event().to_stream_fields().expect("event fields"),
    }));
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let (_, occupied) = budget.reserve(1, 4096).await.expect("occupy budget");
    let control = UsageWorkerControl::default();
    let (telemetry, _observations) = tokio::sync::mpsc::channel(16);
    let mut worker = UsageQueueWorker::new(
        runner.clone(),
        Arc::new(SelectiveFailingRecorder::default()),
        receive_budget_worker_config(),
        None,
    )
    .expect("worker")
    .with_supervisor(control.clone(), telemetry);
    worker.queue = worker.queue.with_read_budget(Arc::clone(&budget));
    let mut cursor = "8-0".to_string();
    let mut reclaim = Box::pin(worker.reclaim_stale_entries(&mut cursor));
    std::future::poll_fn(|cx| {
        assert!(reclaim.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(budget.snapshot().waiters, 1);
    assert!(runner
        .reclaim_cursors
        .lock()
        .expect("cursors lock")
        .is_empty());
    control.request_shutdown();
    tokio::time::timeout(Duration::from_secs(1), &mut reclaim)
        .await
        .expect("shutdown should cancel budget wait");
    drop(reclaim);
    assert_eq!(cursor, "8-0");
    assert_eq!(budget.snapshot().waiters, 0);
    assert_eq!(budget.snapshot().reserved_bytes, 4096);
    drop(occupied);
    assert_eq!(budget.snapshot().reserved_bytes, 0);
}

#[tokio::test]
async fn usage_worker_shutdown_cancels_inflight_reclaim_and_releases_budget() {
    use std::future::Future;
    use std::task::Poll;

    let runner = Arc::new(ReadReclaimRaceProbeQueue::new(RuntimeQueueEntry {
        id: "9-0".to_string(),
        fields: sample_event().to_stream_fields().expect("event fields"),
    }));
    runner.block_reclaim.store(true, Ordering::Release);
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let control = UsageWorkerControl::default();
    let (telemetry, _observations) = tokio::sync::mpsc::channel(16);
    let mut worker = UsageQueueWorker::new(
        runner.clone(),
        Arc::new(SelectiveFailingRecorder::default()),
        receive_budget_worker_config(),
        None,
    )
    .expect("worker")
    .with_supervisor(control.clone(), telemetry);
    worker.queue = worker.queue.with_read_budget(Arc::clone(&budget));
    let mut cursor = "9-0".to_string();
    let mut reclaim = Box::pin(worker.reclaim_stale_entries(&mut cursor));
    std::future::poll_fn(|cx| {
        assert!(reclaim.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(budget.snapshot().reserved_bytes, 4096);
    assert_eq!(
        runner
            .reclaim_cursors
            .lock()
            .expect("cursors lock")
            .as_slice(),
        ["9-0"]
    );
    control.request_shutdown();
    tokio::time::timeout(Duration::from_secs(1), &mut reclaim)
        .await
        .expect("shutdown should cancel reclaim I/O");
    drop(reclaim);
    assert_eq!(cursor, "9-0");
    assert!(runner.reclaim_cancelled.load(Ordering::Acquire));
    assert_eq!(budget.snapshot().reserved_bytes, 0);
    assert!(!runner.acked.load(Ordering::Acquire));
}

#[tokio::test]
async fn usage_worker_shutdown_finishes_acquired_reclaim_before_releasing_budget() {
    let runner = Arc::new(ReadReclaimRaceProbeQueue::new(RuntimeQueueEntry {
        id: "10-0".to_string(),
        fields: sample_event().to_stream_fields().expect("event fields"),
    }));
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let recorder = Arc::new(ControlledRecorder::default());
    let control = UsageWorkerControl::default();
    let (telemetry, _observations) = tokio::sync::mpsc::channel(16);
    let mut worker = UsageQueueWorker::new(
        runner.clone(),
        recorder.clone(),
        receive_budget_worker_config(),
        None,
    )
    .expect("worker")
    .with_supervisor(control.clone(), telemetry);
    worker.queue = worker.queue.with_read_budget(Arc::clone(&budget));
    let handle = tokio::spawn(async move {
        let mut cursor = "10-0".to_string();
        worker.reclaim_stale_entries(&mut cursor).await;
        cursor
    });
    tokio::time::timeout(Duration::from_secs(1), recorder.entered.notified())
        .await
        .expect("reclaimed entry should reach recorder");
    control.request_shutdown();
    tokio::task::yield_now().await;
    assert!(!handle.is_finished());
    assert!(budget.snapshot().reserved_bytes > 0);
    assert!(!runner.acked.load(Ordering::Acquire));
    recorder.release.notify_one();
    let cursor = tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("acquired page should finish processing")
        .expect("reclaim task");
    assert_eq!(cursor, "0-0");
    assert!(runner.acked.load(Ordering::Acquire));
    assert_eq!(budget.snapshot().reserved_bytes, 0);
}

#[tokio::test]
async fn usage_worker_read_budget_processes_oversized_historical_payload() {
    let runner = Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let store = Arc::new(TestUsageStore::default());
    let budget = Arc::new(QueueReadBudget::new(4096, 4096));
    let control = UsageWorkerControl::default();
    let (telemetry, mut observations) = tokio::sync::mpsc::channel(16);
    let mut worker = build_usage_queue_worker_with_record_gate(
        runner.clone(),
        store.clone(),
        receive_budget_worker_config(),
        None,
        None,
    )
    .expect("worker")
    .with_supervisor(control.clone(), telemetry);
    worker.queue = worker.queue.with_read_budget(Arc::clone(&budget));
    worker.capture_memory_budget = Arc::new(EventCaptureMemoryBudget::new(128 * 1024));
    let queue = worker.queue.clone();
    let mut event = sample_event();
    event.data.response_body = Some(serde_json::json!({"legacy": "x".repeat(8192)}));
    let fields = event.to_stream_fields().expect("historical envelope");
    assert!(fields.values().map(String::len).sum::<usize>() > 4096);
    runner
        .append_fields_with_maxlen(&worker.config.stream_key, &fields, None)
        .await
        .expect("append pre-limit message");
    let handle = tokio::spawn(worker.run());
    tokio::time::timeout(Duration::from_secs(1), async {
        while observations
            .recv()
            .await
            .expect("worker observation")
            .acked_entries
            == 0
        {}
    })
    .await
    .expect("oversized message should be recorded and ACKed");
    control.request_shutdown();
    tokio::time::timeout(Duration::from_secs(1), handle)
        .await
        .expect("worker should stop")
        .expect("worker task");
    {
        let records = store.records.lock().expect("records lock");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].response_body, event.data.response_body);
        assert_eq!(records[0].total_tokens, Some(10));
    }
    let snapshot = budget.snapshot();
    assert_eq!(snapshot.reserved_bytes, 0);
    assert_eq!(snapshot.oversized_entries_total, 1);
    assert_eq!(snapshot.oversized_batches_total, 1);
    let stats = queue.stats().await.expect("acked stats");
    assert_eq!((stats.stream_length, stats.group_pending), (0, 0));
    assert_eq!(queue.dlq_stats().await.expect("dlq stats").stream_length, 0);
}

#[test]
fn usage_event_record_error_classifies_permanent_failures() {
    assert!(usage_event_record_error_is_permanent(
        &DataLayerError::UnexpectedValue("bad payload".to_string())
    ));
    assert!(usage_event_record_error_is_permanent(
        &DataLayerError::Postgres(
            "error returned from database: violates foreign key constraint (SQLSTATE 23503)"
                .to_string()
        )
    ));
    assert!(!usage_event_record_error_is_permanent(
        &DataLayerError::Redis("connection refused".to_string())
    ));
    assert!(!usage_event_record_error_is_permanent(
        &DataLayerError::TimedOut("postgres acquire".to_string())
    ));
}

#[tokio::test]
async fn capture_budget_retry_releases_decoded_lease_and_preserves_pending_payload() {
    let (_runner, worker, recorder) =
        capture_budget_worker(64 * 1024, CaptureRecordOutcome::RetryOnce).await;
    worker
        .queue
        .enqueue(&captured_worker_event())
        .await
        .expect("enqueue");
    let entries = worker
        .queue
        .read_group(&worker.consumer)
        .await
        .expect("read event");
    assert_eq!(entries.len(), 1);
    let retry_entries = entries.clone();
    assert!(matches!(
        worker.process_entries(entries).await,
        Err(DataLayerError::TimedOut(_))
    ));
    assert_eq!(recorder.budget.retained_bytes(), 0);
    let stats = worker.queue.stats().await.expect("pending stats");
    assert_eq!(stats.stream_length, 1);
    assert_eq!(stats.group_pending, 1);
    assert_eq!(
        worker
            .queue
            .dlq_stats()
            .await
            .expect("dlq stats")
            .stream_length,
        0
    );

    // Replay the same pending entry, as reclamation does, without a timing-dependent idle wait.
    worker
        .process_entries(retry_entries)
        .await
        .expect("retry should succeed");
    assert_eq!(recorder.calls.load(Ordering::Acquire), 2);
    assert_eq!(recorder.budget.retained_bytes(), 0);
    let stats = worker.queue.stats().await.expect("ack stats");
    assert_eq!(stats.stream_length, 0);
    assert_eq!(stats.group_pending, 0);
}

#[tokio::test]
async fn capture_budget_downgrade_dead_letter_keeps_exact_original_fields() {
    let (runner, worker, recorder) =
        capture_budget_worker(0, CaptureRecordOutcome::PermanentFailure).await;
    let mut original = captured_worker_event()
        .to_stream_fields()
        .expect("wire serialization");
    original.insert(
        "legacy_marker".to_string(),
        "preserve this field".to_string(),
    );
    runner
        .append_fields_with_maxlen(&worker.config.stream_key, &original, None)
        .await
        .expect("enqueue raw fields");
    let entries = worker
        .queue
        .read_group(&worker.consumer)
        .await
        .expect("read event");
    worker
        .process_entries(entries)
        .await
        .expect("permanent failure should dead letter");
    assert_eq!(recorder.calls.load(Ordering::Acquire), 1);
    assert_eq!(recorder.budget.retained_bytes(), 0);
    assert_eq!(recorder.budget.downgraded_total(), 1);
    let stats = worker.queue.stats().await.expect("ack stats");
    assert_eq!(stats.stream_length, 0);
    assert_eq!(stats.group_pending, 0);
    runner
        .ensure_consumer_group(
            &worker.config.dlq_stream_key,
            "capture-dlq-inspection",
            "0-0",
        )
        .await
        .expect("dlq group");
    let dlq = runner
        .read_group(
            &worker.config.dlq_stream_key,
            "capture-dlq-inspection",
            "capture-inspector",
            1,
            Some(1),
        )
        .await
        .expect("read dlq");
    assert_eq!(dlq.len(), 1);
    let payload: serde_json::Value =
        serde_json::from_str(&dlq[0].fields["payload"]).expect("dlq json");
    assert_eq!(
        payload["fields"],
        serde_json::to_value(original).expect("original fields json")
    );
    assert_eq!(
        payload["error"].as_str(),
        Some("unexpected database value: permanent capture test error")
    );
}

#[tokio::test]
async fn capture_budget_malformed_entry_dead_letters_without_recording() {
    let (runner, worker, recorder) =
        capture_budget_worker(1024, CaptureRecordOutcome::PermanentFailure).await;
    let original = BTreeMap::from([(
        "payload".to_string(),
        "malformed legacy payload".to_string(),
    )]);
    runner
        .append_fields_with_maxlen(&worker.config.stream_key, &original, None)
        .await
        .expect("enqueue raw fields");
    let entries = worker
        .queue
        .read_group(&worker.consumer)
        .await
        .expect("read event");
    worker
        .process_entries(entries)
        .await
        .expect("malformed event should dead letter");
    assert_eq!(recorder.calls.load(Ordering::Acquire), 0);
    assert_eq!(recorder.budget.retained_bytes(), 0);
    assert_eq!(recorder.budget.downgraded_total(), 0);
    let stats = worker.queue.stats().await.expect("ack stats");
    assert_eq!(stats.stream_length, 0);
    assert_eq!(stats.group_pending, 0);
    runner
        .ensure_consumer_group(
            &worker.config.dlq_stream_key,
            "capture-dlq-inspection",
            "0-0",
        )
        .await
        .expect("dlq group");
    let dlq = runner
        .read_group(
            &worker.config.dlq_stream_key,
            "capture-dlq-inspection",
            "capture-inspector",
            1,
            Some(1),
        )
        .await
        .expect("read dlq");
    assert_eq!(dlq.len(), 1);
    let payload: serde_json::Value =
        serde_json::from_str(&dlq[0].fields["payload"]).expect("dlq json");
    assert_eq!(
        payload["fields"],
        serde_json::to_value(original).expect("original fields json")
    );
}

#[tokio::test]
async fn capture_budget_cancelled_record_releases_lease_without_acknowledging() {
    let (_runner, worker, recorder) =
        capture_budget_worker(64 * 1024, CaptureRecordOutcome::Wait).await;
    worker
        .queue
        .enqueue(&captured_worker_event())
        .await
        .expect("enqueue");
    let entries = worker
        .queue
        .read_group(&worker.consumer)
        .await
        .expect("read event");
    let worker = Arc::new(worker);
    let task_worker = Arc::clone(&worker);
    let task = tokio::spawn(async move { task_worker.process_entries(entries).await });
    tokio::time::timeout(Duration::from_secs(1), recorder.entered.notified())
        .await
        .expect("recorder should start");
    assert!(recorder.budget.retained_bytes() > 0);
    task.abort();
    assert!(task
        .await
        .expect_err("task should be cancelled")
        .is_cancelled());
    assert_eq!(recorder.budget.retained_bytes(), 0);
    let stats = worker.queue.stats().await.expect("pending stats");
    assert_eq!(stats.stream_length, 1);
    assert_eq!(stats.group_pending, 1);
}

#[tokio::test]
async fn process_entries_dead_letters_permanent_record_error_and_continues() {
    let runner = Arc::new(RuntimeState::memory(MemoryRuntimeStateConfig::default()));
    let queue_runner: Arc<dyn RuntimeQueueStore> = runner.clone();
    let recorder = Arc::new(SelectiveFailingRecorder::default());
    let config = UsageRuntimeConfig {
        enabled: true,
        stream_key: "usage:test:worker:events".to_string(),
        consumer_group: "usage:test:worker:group".to_string(),
        dlq_stream_key: "usage:test:worker:dlq".to_string(),
        consumer_batch_size: 10,
        consumer_block_ms: 1,
        ..UsageRuntimeConfig::default()
    };
    let mut worker = UsageQueueWorker::new(queue_runner, recorder.clone(), config, None)
        .expect("worker should build");
    worker.queue =
        worker
            .queue
            .with_dead_letter_encoding_budget(Arc::new(DeadLetterEncodingBudget::new(
                64 * 1024 * 1024,
                4,
            )));
    worker
        .queue
        .ensure_consumer_group()
        .await
        .expect("group should initialize");

    let mut poison = sample_event();
    poison.request_id = "req-worker-poison".to_string();
    let mut ok = sample_event();
    ok.request_id = "req-worker-ok".to_string();
    worker
        .queue
        .enqueue(&poison)
        .await
        .expect("poison event should enqueue");
    worker
        .queue
        .enqueue(&ok)
        .await
        .expect("ok event should enqueue");

    let entries = worker
        .queue
        .read_group(&worker.consumer)
        .await
        .expect("events should read");
    assert_eq!(entries.len(), 2);

    worker
        .process_entries(entries)
        .await
        .expect("permanent failure should not block batch");

    assert_eq!(
        recorder.calls.lock().expect("calls lock").as_slice(),
        ["req-worker-poison", "req-worker-ok"]
    );

    runner
        .ensure_consumer_group(
            "usage:test:worker:dlq",
            "usage:test:worker:dlq-group",
            "0-0",
        )
        .await
        .expect("dlq group should initialize");
    let dlq_entries = runner
        .read_group(
            "usage:test:worker:dlq",
            "usage:test:worker:dlq-group",
            "usage-test-dlq-consumer",
            10,
            Some(1),
        )
        .await
        .expect("dlq should read");
    assert_eq!(dlq_entries.len(), 1);
    let payload = dlq_entries[0]
        .fields
        .get("payload")
        .expect("dlq payload should exist");
    assert!(payload.contains("req-worker-poison"));
    assert!(payload.contains("permanent test error"));
}
