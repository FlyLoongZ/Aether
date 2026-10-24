use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use aether_data_contracts::repository::usage::{StoredRequestUsageAudit, UpsertUsageRecord};
use aether_data_contracts::DataLayerError;
use aether_runtime_state::{RuntimeQueueEntry, RuntimeQueueStore};
use async_trait::async_trait;
use tokio::sync::{mpsc, Notify};
use tracing::warn;

use crate::event_capture_budget::{shared_capture_memory_budget, EventCaptureMemoryBudget};
use crate::executor::spawn_on_usage_background_runtime;
use crate::keyed_lock::KeyedAsyncLockPool;
use crate::queue::UsageDeadLetterOutcome;
use crate::runtime::{
    UsageBillingEventEnricher, UsageRuntimeAccess, UsageWorkerRecordConcurrencyGate,
};
use crate::settlement::settle_usage_after_upsert;
use crate::{
    build_upsert_usage_record_from_event, UsageEvent, UsageEventType, UsageQueue,
    UsageRuntimeConfig, UsageSettlementWriter,
};

const USAGE_WORKER_DB_PRESSURE_DEFER_MS: u64 = 10;
const USAGE_WORKER_ACK_CHUNK_SIZE: usize = 100;

enum EntryDisposition {
    NeedsAck,
    Complete,
    Deferred(DataLayerError),
}

#[async_trait]
pub trait UsageEventRecorder: Send + Sync {
    async fn record_usage_event(&self, event: &UsageEvent) -> Result<(), DataLayerError>;
}

#[async_trait]
pub trait ManualProxyNodeCounter: Send + Sync {
    async fn increment_manual_proxy_node_requests(
        &self,
        node_id: &str,
        total_delta: i64,
        failed_delta: i64,
        latency_ms: Option<i64>,
    ) -> Result<(), DataLayerError>;
}

#[async_trait]
pub trait UsageRecordWriter: Send + Sync {
    /// Native batch support is opt-in; the default preserves one-row writes for other backends.
    fn supports_first_byte_usage_batch(&self) -> bool {
        false
    }

    /// Stable identity for the underlying first-byte writer. Implementations that opt into
    /// batching must return the same value for clones backed by the same repository.
    fn first_byte_usage_writer_identity(&self) -> Option<usize> {
        None
    }

    /// Native pending batching is opt-in because it must retain the complete usage audit write
    /// contract, not just the base lifecycle row.
    fn supports_pending_usage_batch(&self) -> bool {
        false
    }

    /// Stable identity for clones backed by the same pending usage repository.
    fn pending_usage_writer_identity(&self) -> Option<usize> {
        None
    }

    async fn upsert_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError>;

    async fn upsert_first_byte_usage_record(
        &self,
        record: UpsertUsageRecord,
    ) -> Result<(), DataLayerError> {
        self.upsert_usage_record(record).await.map(|_| ())
    }

    async fn upsert_first_byte_usage_records(
        &self,
        records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        for record in records {
            self.upsert_first_byte_usage_record(record).await?;
        }
        Ok(())
    }

    async fn upsert_pending_usage_records(
        &self,
        records: Vec<UpsertUsageRecord>,
    ) -> Result<(), DataLayerError> {
        for record in records {
            self.upsert_usage_record(record).await?;
        }
        Ok(())
    }
}

pub struct UsageDataEventRecorder<T> {
    data: Arc<T>,
    record_gate: Option<Arc<UsageWorkerRecordConcurrencyGate>>,
    defer_for_database_pressure: bool,
}

impl<T> UsageDataEventRecorder<T> {
    pub fn new(data: Arc<T>) -> Self {
        Self::with_record_gate(data, None)
    }

    pub(crate) fn with_record_gate(
        data: Arc<T>,
        record_gate: Option<Arc<UsageWorkerRecordConcurrencyGate>>,
    ) -> Self {
        Self {
            data,
            record_gate,
            defer_for_database_pressure: false,
        }
    }

    pub(crate) fn with_record_gate_and_database_pressure_defer(
        data: Arc<T>,
        record_gate: Option<Arc<UsageWorkerRecordConcurrencyGate>>,
    ) -> Self {
        Self {
            data,
            record_gate,
            defer_for_database_pressure: true,
        }
    }
}

#[async_trait]
impl<T> UsageEventRecorder for UsageDataEventRecorder<T>
where
    T: UsageRuntimeAccess,
{
    async fn record_usage_event(&self, event: &UsageEvent) -> Result<(), DataLayerError> {
        if self.defer_for_database_pressure
            && self.data.usage_worker_should_defer_for_database_pressure()
        {
            if let Some(gate) = self.record_gate.as_ref() {
                gate.record_deferred();
            }
            tokio::time::sleep(Duration::from_millis(USAGE_WORKER_DB_PRESSURE_DEFER_MS)).await;
        }
        let _record_gate_permit = match self.record_gate.as_ref() {
            Some(gate) => Some(gate.acquire().await),
            None => None,
        };
        let request_lock = usage_request_lock(&event.request_id);
        let _guard = request_lock.lock().await;
        let mut event = event.clone();
        enrich_terminal_event(self.data.as_ref(), &mut event).await?;
        write_event_record(self.data.as_ref(), &event).await
    }
}

fn usage_request_lock(request_id: &str) -> Arc<tokio::sync::Mutex<()>> {
    static LOCKS: OnceLock<KeyedAsyncLockPool> = OnceLock::new();
    LOCKS
        .get_or_init(KeyedAsyncLockPool::default)
        .lock_for(request_id)
}

pub struct UsageQueueWorker {
    queue: UsageQueue,
    recorder: Arc<dyn UsageEventRecorder>,
    consumer: String,
    worker_index: Option<usize>,
    control: Option<UsageWorkerControl>,
    telemetry: Option<mpsc::Sender<UsageWorkerObservation>>,
    config: UsageRuntimeConfig,
    capture_memory_budget: Arc<EventCaptureMemoryBudget>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct UsageWorkerControl {
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl UsageWorkerControl {
    pub(crate) fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
        self.shutdown_notify.notify_one();
    }

    fn should_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub(crate) async fn wait_for_shutdown(&self) {
        loop {
            let notified = self.shutdown_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.should_shutdown() {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UsageWorkerObservation {
    pub worker_index: Option<usize>,
    pub entries_read: usize,
    pub batch_size: usize,
    pub reclaimed_entries: usize,
    pub acked_entries: usize,
    pub dead_lettered_entries: usize,
    pub process_failures: usize,
    pub read_failures: usize,
    pub reclaim_failures: usize,
}

impl UsageWorkerObservation {
    fn read(worker_index: Option<usize>, entries_read: usize, batch_size: usize) -> Self {
        Self {
            worker_index,
            entries_read,
            batch_size,
            reclaimed_entries: 0,
            acked_entries: 0,
            dead_lettered_entries: 0,
            process_failures: 0,
            read_failures: 0,
            reclaim_failures: 0,
        }
    }

    fn reclaimed(worker_index: Option<usize>, reclaimed_entries: usize) -> Self {
        Self {
            worker_index,
            entries_read: 0,
            batch_size: 0,
            reclaimed_entries,
            acked_entries: 0,
            dead_lettered_entries: 0,
            process_failures: 0,
            read_failures: 0,
            reclaim_failures: 0,
        }
    }

    fn acked(worker_index: Option<usize>, acked_entries: usize) -> Self {
        Self {
            worker_index,
            entries_read: 0,
            batch_size: 0,
            reclaimed_entries: 0,
            acked_entries,
            dead_lettered_entries: 0,
            process_failures: 0,
            read_failures: 0,
            reclaim_failures: 0,
        }
    }

    fn dead_lettered(worker_index: Option<usize>, dead_lettered_entries: usize) -> Self {
        Self {
            worker_index,
            entries_read: 0,
            batch_size: 0,
            reclaimed_entries: 0,
            acked_entries: 0,
            dead_lettered_entries,
            process_failures: 0,
            read_failures: 0,
            reclaim_failures: 0,
        }
    }

    fn process_failed(worker_index: Option<usize>) -> Self {
        Self {
            worker_index,
            entries_read: 0,
            batch_size: 0,
            reclaimed_entries: 0,
            acked_entries: 0,
            dead_lettered_entries: 0,
            process_failures: 1,
            read_failures: 0,
            reclaim_failures: 0,
        }
    }

    fn read_failed(worker_index: Option<usize>) -> Self {
        Self {
            worker_index,
            entries_read: 0,
            batch_size: 0,
            reclaimed_entries: 0,
            acked_entries: 0,
            dead_lettered_entries: 0,
            process_failures: 0,
            read_failures: 1,
            reclaim_failures: 0,
        }
    }

    fn reclaim_failed(worker_index: Option<usize>) -> Self {
        Self {
            worker_index,
            entries_read: 0,
            batch_size: 0,
            reclaimed_entries: 0,
            acked_entries: 0,
            dead_lettered_entries: 0,
            process_failures: 0,
            read_failures: 0,
            reclaim_failures: 1,
        }
    }
}

impl UsageQueueWorker {
    pub fn new(
        runner: Arc<dyn RuntimeQueueStore>,
        recorder: Arc<dyn UsageEventRecorder>,
        config: UsageRuntimeConfig,
        worker_index: Option<usize>,
    ) -> Result<Self, DataLayerError> {
        let queue = UsageQueue::new(runner, config.clone())?;
        let consumer = consumer_name(worker_index);
        Ok(Self {
            queue,
            recorder,
            consumer,
            worker_index,
            control: None,
            telemetry: None,
            config,
            capture_memory_budget: shared_capture_memory_budget(),
        })
    }

    pub(crate) fn with_supervisor(
        mut self,
        control: UsageWorkerControl,
        telemetry: mpsc::Sender<UsageWorkerObservation>,
    ) -> Self {
        self.control = Some(control);
        self.telemetry = Some(telemetry);
        self
    }

    pub fn spawn(self) -> tokio::task::JoinHandle<()> {
        spawn_on_usage_background_runtime(async move { self.run_forever().await })
    }

    pub(crate) fn with_shutdown(mut self, control: UsageWorkerControl) -> Self {
        self.control = Some(control);
        self
    }

    pub(crate) async fn run(self) {
        self.run_forever().await;
    }

    async fn run_forever(self) {
        if let Err(err) = self.queue.ensure_consumer_group().await {
            warn!(
                event_name = "usage_worker_consumer_group_failed",
                log_type = "ops",
                worker_consumer = %self.consumer,
                worker_group = %self.config.consumer_group,
                error = %err,
                "usage worker failed to ensure consumer group"
            );
            return;
        }

        let mut reclaim_interval =
            tokio::time::interval(Duration::from_millis(self.config.reclaim_interval_ms));
        reclaim_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        reclaim_interval.tick().await;

        let mut reclaim_due = false;
        let mut reclaim_cursor = "0-0".to_string();

        loop {
            if self.should_shutdown() {
                break;
            }

            let result = {
                let mut read_future = Box::pin(self.queue.read_group_reserved(&self.consumer));
                loop {
                    tokio::select! {
                        biased;
                        // A command already delivered by Redis remains in the PEL and is recovered
                        // by a subsequent worker reclaim after the idle period.
                        _ = self.wait_for_shutdown() => return,
                        _ = reclaim_interval.tick(), if !reclaim_due => {
                            // Do not reclaim while XREADGROUP is in flight. Redis can add an entry
                            // to this consumer's PEL before delivering the response; claiming it in
                            // that window would make both paths process the same stream entry.
                            reclaim_due = true;
                        }
                        result = &mut read_future => break result,
                    }
                }
            };

            match result {
                Ok(batch) => {
                    self.report_read(batch.entries.len(), batch.requested_count);
                    let reservation = batch.reservation;
                    let result = self.process_entries(batch.entries).await;
                    // The raw fields and decoded event must be gone, and ACK must finish,
                    // before another worker can reuse this batch's receive allowance.
                    drop(reservation);
                    if let Err(err) = result {
                        self.report_process_failed();
                        warn!(
                            event_name = "usage_worker_process_failed",
                            log_type = "ops",
                            worker_consumer = %self.consumer,
                            worker_group = %self.config.consumer_group,
                            error = %err,
                            "usage worker failed to process queue entries"
                        );
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
                Err(err) => {
                    self.report_read_failed();
                    warn!(
                        event_name = "usage_worker_read_failed",
                        log_type = "ops",
                        worker_consumer = %self.consumer,
                        worker_group = %self.config.consumer_group,
                        error = %err,
                        "usage worker failed to read queue"
                    );
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }

            if self.should_shutdown() {
                break;
            }

            if reclaim_due {
                reclaim_due = false;
                self.reclaim_stale_entries(&mut reclaim_cursor).await;
            }
        }
    }

    async fn wait_for_shutdown(&self) {
        match self.control.as_ref() {
            Some(control) => control.wait_for_shutdown().await,
            None => std::future::pending().await,
        }
    }

    async fn reclaim_stale_entries(&self, cursor: &mut String) {
        let result = tokio::select! {
            biased;
            _ = self.wait_for_shutdown() => return,
            result = self.queue.claim_stale_page_reserved(&self.consumer, cursor) => result,
        };
        match result {
            Ok(batch) => {
                let reservation = batch.reservation;
                let page = batch.page;
                // An empty page can still advance past a large active PEL prefix.
                // Failed writes remain pending and are revisited after the scan wraps.
                *cursor = page.next_start_id;
                self.report_reclaimed(page.entries.len());
                let result = self.process_entries(page.entries).await;
                drop(page.deleted_ids);
                drop(reservation);
                if let Err(err) = result {
                    self.report_process_failed();
                    warn!(
                        event_name = "usage_worker_reclaim_process_failed",
                        log_type = "ops",
                        worker_consumer = %self.consumer,
                        worker_group = %self.config.consumer_group,
                        error = %err,
                        "usage worker failed while reclaiming stale entries"
                    );
                }
            }
            Err(err) => {
                self.report_reclaim_failed();
                warn!(
                    event_name = "usage_worker_reclaim_failed",
                    log_type = "ops",
                    worker_consumer = %self.consumer,
                    worker_group = %self.config.consumer_group,
                    error = %err,
                    "usage worker failed to reclaim stale entries"
                );
            }
        }
    }

    fn should_shutdown(&self) -> bool {
        self.control
            .as_ref()
            .is_some_and(UsageWorkerControl::should_shutdown)
    }

    fn report_read(&self, entries_read: usize, requested_count: usize) {
        self.report(UsageWorkerObservation::read(
            self.worker_index,
            entries_read,
            requested_count,
        ));
    }

    fn report_reclaimed(&self, reclaimed_entries: usize) {
        self.report(UsageWorkerObservation::reclaimed(
            self.worker_index,
            reclaimed_entries,
        ));
    }

    fn report_acked(&self, acked_entries: usize) {
        self.report(UsageWorkerObservation::acked(
            self.worker_index,
            acked_entries,
        ));
    }

    fn report_dead_lettered(&self, dead_lettered_entries: usize) {
        self.report(UsageWorkerObservation::dead_lettered(
            self.worker_index,
            dead_lettered_entries,
        ));
    }

    fn report_process_failed(&self) {
        self.report(UsageWorkerObservation::process_failed(self.worker_index));
    }

    fn report_read_failed(&self) {
        self.report(UsageWorkerObservation::read_failed(self.worker_index));
    }

    fn report_reclaim_failed(&self) {
        self.report(UsageWorkerObservation::reclaim_failed(self.worker_index));
    }

    fn report(&self, observation: UsageWorkerObservation) {
        let Some(telemetry) = &self.telemetry else {
            return;
        };
        let _ = telemetry.try_send(observation);
    }

    async fn process_entries(&self, entries: Vec<RuntimeQueueEntry>) -> Result<(), DataLayerError> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut ack_ids = Vec::new();
        let mut deferred_error = None;
        for entry in entries {
            let id = entry.id.clone();
            let result = self.process_entry(entry).await;
            match result {
                Ok(EntryDisposition::NeedsAck) => {
                    ack_ids.push(id);
                    if ack_ids.len() >= USAGE_WORKER_ACK_CHUNK_SIZE {
                        self.acknowledge_entries(&ack_ids).await?;
                        ack_ids.clear();
                    }
                }
                Ok(EntryDisposition::Complete) => {}
                Ok(EntryDisposition::Deferred(err)) => {
                    // An entry that cannot fit the DLQ encoder must not indefinitely block
                    // the healthy entries returned with it on every reclaim pass.
                    deferred_error.get_or_insert(err);
                }
                Err(err) => {
                    if !ack_ids.is_empty() {
                        let _ = self.acknowledge_entries(&ack_ids).await;
                    }
                    return Err(err);
                }
            }
        }

        if !ack_ids.is_empty() {
            self.acknowledge_entries(&ack_ids).await?;
        }

        match deferred_error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn acknowledge_entries(&self, ids: &[String]) -> Result<(), DataLayerError> {
        let acked = self.queue.ack_and_delete_counted(ids).await?;
        if acked > 0 {
            self.report_acked(acked);
        }
        Ok(())
    }

    async fn dead_letter_entry(
        &self,
        entry: RuntimeQueueEntry,
        error: DataLayerError,
        event_name: &'static str,
    ) -> Result<EntryDisposition, DataLayerError> {
        let id = entry.id.clone();
        let outcome = self
            .queue
            .transfer_dead_letter_owned(entry, error.to_string())
            .await?;
        let (destination_id, disposition) = match outcome {
            UsageDeadLetterOutcome::Transferred {
                destination_id,
                acked,
            } => {
                if acked > 0 {
                    self.report_acked(acked);
                }
                (destination_id, EntryDisposition::Complete)
            }
            UsageDeadLetterOutcome::Appended { destination_id } => {
                (destination_id, EntryDisposition::NeedsAck)
            }
            UsageDeadLetterOutcome::NotPending => {
                warn!(
                    event_name = "usage_worker_dead_letter_source_not_pending",
                    log_type = "ops",
                    worker_consumer = %self.consumer,
                    worker_group = %self.config.consumer_group,
                    entry_id = %id,
                    error = %error,
                    "usage worker skipped dead letter transfer because the source is no longer pending"
                );
                return Ok(EntryDisposition::Complete);
            }
            UsageDeadLetterOutcome::EncodingDeferred { error } => {
                warn!(
                    event_name = "usage_worker_dead_letter_encoding_deferred",
                    log_type = "ops",
                    worker_consumer = %self.consumer,
                    worker_group = %self.config.consumer_group,
                    entry_id = %id,
                    error = %error,
                    "usage worker retained entry pending after dead letter encoding failed"
                );
                return Ok(EntryDisposition::Deferred(error));
            }
        };
        self.report_dead_lettered(1);
        warn!(
            event_name,
            log_type = "ops",
            worker_consumer = %self.consumer,
            worker_group = %self.config.consumer_group,
            entry_id = %id,
            dead_letter_id = %destination_id,
            error = %error,
            "usage worker appended queue entry to dead letter"
        );
        Ok(disposition)
    }

    async fn process_entry(
        &self,
        entry: RuntimeQueueEntry,
    ) -> Result<EntryDisposition, DataLayerError> {
        let event = match UsageEvent::from_stream_fields_with_capture_budget(
            &entry.fields,
            Arc::clone(&self.capture_memory_budget),
        ) {
            Ok(event) => event,
            Err(err) => {
                return self
                    .dead_letter_entry(entry, err, "usage_worker_entry_decode_dead_lettered")
                    .await;
            }
        };

        match self.recorder.record_usage_event(&event).await {
            Ok(()) => Ok(EntryDisposition::NeedsAck),
            Err(err) if usage_event_record_error_is_permanent(&err) => {
                warn!(
                    event_name = "usage_worker_entry_record_permanent_failed",
                    log_type = "ops",
                    worker_consumer = %self.consumer,
                    worker_group = %self.config.consumer_group,
                    entry_id = %entry.id,
                    request_id = %event.request_id,
                    event_type = ?event.event_type,
                    provider_name = %event.data.provider_name,
                    model = %event.data.model,
                    api_format = event.data.api_format.as_deref().unwrap_or(""),
                    provider_id = event.data.provider_id.as_deref().unwrap_or(""),
                    provider_endpoint_id = event.data.provider_endpoint_id.as_deref().unwrap_or(""),
                    provider_api_key_id = event.data.provider_api_key_id.as_deref().unwrap_or(""),
                    error = %err,
                    "usage worker encountered a non-retryable usage record failure"
                );
                drop(event);
                self.dead_letter_entry(entry, err, "usage_worker_entry_record_dead_lettered")
                    .await
            }
            Err(err) => {
                warn!(
                    event_name = "usage_worker_entry_record_retryable_failed",
                    log_type = "ops",
                    worker_consumer = %self.consumer,
                    worker_group = %self.config.consumer_group,
                    entry_id = %entry.id,
                    request_id = %event.request_id,
                    event_type = ?event.event_type,
                    provider_name = %event.data.provider_name,
                    model = %event.data.model,
                    api_format = event.data.api_format.as_deref().unwrap_or(""),
                    provider_id = event.data.provider_id.as_deref().unwrap_or(""),
                    provider_endpoint_id = event.data.provider_endpoint_id.as_deref().unwrap_or(""),
                    provider_api_key_id = event.data.provider_api_key_id.as_deref().unwrap_or(""),
                    error = %err,
                    "usage worker will retry usage event after record failure"
                );
                Err(err)
            }
        }
    }
}

fn usage_event_record_error_is_permanent(err: &DataLayerError) -> bool {
    match err {
        DataLayerError::InvalidConfiguration(_)
        | DataLayerError::InvalidInput(_)
        | DataLayerError::UnexpectedValue(_) => true,
        DataLayerError::Postgres(message) | DataLayerError::Sql(message) => {
            database_error_is_known_permanent(message)
        }
        DataLayerError::Redis(_) | DataLayerError::TimedOut(_) => false,
    }
}

fn database_error_is_known_permanent(message: &str) -> bool {
    message.contains("SQLSTATE 23503") || message.contains("violates foreign key constraint")
}

pub fn build_usage_queue_worker<T>(
    runner: Arc<dyn RuntimeQueueStore>,
    data: Arc<T>,
    config: UsageRuntimeConfig,
    worker_index: Option<usize>,
) -> Result<UsageQueueWorker, DataLayerError>
where
    T: UsageRuntimeAccess + 'static,
{
    build_usage_queue_worker_with_record_gate(runner, data, config, None, worker_index)
}

pub(crate) fn build_usage_queue_worker_with_record_gate<T>(
    runner: Arc<dyn RuntimeQueueStore>,
    data: Arc<T>,
    config: UsageRuntimeConfig,
    record_gate: Option<Arc<UsageWorkerRecordConcurrencyGate>>,
    worker_index: Option<usize>,
) -> Result<UsageQueueWorker, DataLayerError>
where
    T: UsageRuntimeAccess + 'static,
{
    UsageQueueWorker::new(
        runner,
        Arc::new(
            UsageDataEventRecorder::with_record_gate_and_database_pressure_defer(data, record_gate),
        ),
        config,
        worker_index,
    )
}

pub async fn write_event_record<T>(data: &T, event: &UsageEvent) -> Result<(), DataLayerError>
where
    T: UsageRecordWriter + UsageSettlementWriter + Send + Sync,
{
    let record = build_upsert_usage_record_from_event(event)?;
    if let Some(stored) = data.upsert_usage_record(record).await? {
        // Sparse terminal events may omit pricing factors. Only the stored request
        // snapshot is authoritative before finalizing an immutable cost reservation.
        settle_usage_after_upsert(data, &stored, event).await?;
    }
    // Manual proxy traffic is counted at the actual transport-attempt boundary. Usage events are
    // replayable, so emitting that side effect here would count normal requests and reclaims twice.
    Ok(())
}

async fn enrich_terminal_event<T>(data: &T, event: &mut UsageEvent) -> Result<(), DataLayerError>
where
    T: UsageBillingEventEnricher + Send + Sync,
{
    if !matches!(
        event.event_type,
        UsageEventType::Completed | UsageEventType::Failed | UsageEventType::Cancelled
    ) {
        return Ok(());
    }

    if let Err(err) = data.enrich_usage_event(event).await {
        warn!(
            event_name = "usage_worker_billing_enrichment_failed",
            log_type = "event",
            request_id = %event.request_id,
            event_type = ?event.event_type,
            error = %err,
            "usage worker failed to enrich terminal usage event with billing"
        );
        return Err(err);
    }
    Ok(())
}

fn consumer_name(worker_index: Option<usize>) -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "aether-gateway".to_string());
    match worker_index {
        Some(worker_index) => format!("{host}:{}:{worker_index}", std::process::id()),
        None => format!("{host}:{}", std::process::id()),
    }
}

#[cfg(test)]
#[path = "worker/tests.rs"]
mod tests;
