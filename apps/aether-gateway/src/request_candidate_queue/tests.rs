use super::{
    collect_active_micro_batch, compact_records_for_flush, flush_batch,
    parse_request_candidate_background_runtime_threads, request_candidate_retry_delay,
    request_candidate_retry_is_ready, request_candidate_write_error_disposition, run_worker,
    spawn_on_request_candidate_background_runtime, RequestCandidateActiveQueueMessage,
    RequestCandidateQueueConfig, RequestCandidateQueueLane, RequestCandidateQueueMetrics,
    RequestCandidateQueueRuntime, RequestCandidateTerminalBarrier,
    RequestCandidateTerminalQueueRecord, RequestCandidateWriteErrorDisposition,
    MAX_CONSECUTIVE_ACTIVE_FLUSHES, MAX_CONSECUTIVE_PRIORITY_FLUSHES,
};
use aether_data::repository::candidates::InMemoryRequestCandidateRepository;
use aether_data::DataLayerError;
use aether_data_contracts::repository::candidates::{
    RequestCandidateReadRepository, RequestCandidateStatus, RequestCandidateWriteRepository,
    StoredRequestCandidate, UpsertRequestCandidateRecord,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

#[derive(Default)]
struct DelayedPendingRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for DelayedPendingRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        if candidate.status == RequestCandidateStatus::Pending {
            tokio::time::sleep(Duration::from_millis(40)).await;
        }
        self.inner.upsert(candidate).await
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

#[derive(Default)]
struct CountingBatchRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    upsert_calls: AtomicUsize,
    upsert_many_calls: AtomicUsize,
    active_upsert_many: AtomicUsize,
    max_active_upsert_many: AtomicUsize,
}

struct BlockingFirstBatchRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    batches: Mutex<Vec<Vec<UpsertRequestCandidateRecord>>>,
    block_first: AtomicBool,
    first_batch_started: tokio::sync::Notify,
    release_first_batch: tokio::sync::Notify,
}

struct BlockingStreamingBatchRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    block_streaming: AtomicBool,
    streaming_batch_started: tokio::sync::Notify,
    release_streaming_batch: tokio::sync::Notify,
}

struct FailUntilReleasedRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    failing: AtomicBool,
    attempts: AtomicUsize,
    first_attempt: tokio::sync::Notify,
}

#[derive(Default)]
struct PoisonPendingRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    attempts: AtomicUsize,
    poison_attempts: AtomicUsize,
}

#[derive(Default)]
struct GlobalFailureRequestCandidateRepository {
    attempts: AtomicUsize,
}

#[derive(Default)]
struct RetryThenGlobalFailureRequestCandidateRepository {
    attempts: AtomicUsize,
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for PoisonPendingRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        if candidates.iter().any(|candidate| {
            candidate.request_id == "poison" && candidate.status == RequestCandidateStatus::Pending
        }) {
            self.poison_attempts.fetch_add(1, Ordering::AcqRel);
            return Err(DataLayerError::Postgres(
                "error returned from database: unsupported Unicode escape sequence (SQLSTATE 22P05)"
                    .to_string(),
            ));
        }
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for GlobalFailureRequestCandidateRepository {
    async fn upsert(
        &self,
        _candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        unreachable!("queue tests use batched candidate persistence")
    }

    async fn upsert_many(
        &self,
        _candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Err(DataLayerError::Postgres(
            "relation does not exist (SQLSTATE 42P01)".to_string(),
        ))
    }

    async fn delete_created_before(
        &self,
        _created_before_unix_secs: u64,
        _limit: usize,
    ) -> Result<usize, DataLayerError> {
        Ok(0)
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for RetryThenGlobalFailureRequestCandidateRepository {
    async fn upsert(
        &self,
        _candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        unreachable!("queue tests use batched candidate persistence")
    }

    async fn upsert_many(
        &self,
        _candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        match self.attempts.fetch_add(1, Ordering::AcqRel) {
            0 => Err(DataLayerError::TimedOut(
                "injected retryable database failure".to_string(),
            )),
            _ => Err(DataLayerError::Postgres(
                "relation does not exist (SQLSTATE 42P01)".to_string(),
            )),
        }
    }

    async fn delete_created_before(
        &self,
        _created_before_unix_secs: u64,
        _limit: usize,
    ) -> Result<usize, DataLayerError> {
        Ok(0)
    }
}

impl Default for FailUntilReleasedRequestCandidateRepository {
    fn default() -> Self {
        Self {
            inner: InMemoryRequestCandidateRepository::default(),
            failing: AtomicBool::new(true),
            attempts: AtomicUsize::new(0),
            first_attempt: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for FailUntilReleasedRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            self.first_attempt.notify_one();
        }
        if self.failing.load(Ordering::Acquire) {
            return Err(DataLayerError::TimedOut(
                "injected sustained request candidate failure".to_string(),
            ));
        }
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

#[derive(Default)]
struct FailThenBlockPendingRetryRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    batches: Mutex<Vec<Vec<UpsertRequestCandidateRecord>>>,
    pending_attempts: AtomicUsize,
    retry_started: tokio::sync::Notify,
    release_retry: tokio::sync::Notify,
}

impl Default for BlockingStreamingBatchRequestCandidateRepository {
    fn default() -> Self {
        Self {
            inner: InMemoryRequestCandidateRepository::default(),
            block_streaming: AtomicBool::new(true),
            streaming_batch_started: tokio::sync::Notify::new(),
            release_streaming_batch: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for BlockingStreamingBatchRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        if candidates
            .first()
            .is_some_and(|candidate| candidate.status == RequestCandidateStatus::Streaming)
            && self.block_streaming.swap(false, Ordering::AcqRel)
        {
            self.streaming_batch_started.notify_one();
            self.release_streaming_batch.notified().await;
        }
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for FailThenBlockPendingRetryRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        self.batches
            .lock()
            .expect("pending retry batch recorder lock")
            .push(candidates.clone());
        if candidates
            .first()
            .is_some_and(|candidate| candidate.status == RequestCandidateStatus::Pending)
        {
            match self.pending_attempts.fetch_add(1, Ordering::AcqRel) {
                0 => {
                    return Err(DataLayerError::TimedOut(
                        "injected pending batch failure".to_string(),
                    ));
                }
                1 => {
                    self.retry_started.notify_one();
                    self.release_retry.notified().await;
                }
                _ => {}
            }
        }
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

#[derive(Default)]
struct RecordingBatchRequestCandidateRepository {
    inner: InMemoryRequestCandidateRepository,
    batches: Mutex<Vec<Vec<UpsertRequestCandidateRecord>>>,
    fail_next_batch: AtomicBool,
}

impl RecordingBatchRequestCandidateRepository {
    fn fail_next_batch(&self) {
        self.fail_next_batch.store(true, Ordering::Release);
    }

    fn batches(&self) -> Vec<Vec<UpsertRequestCandidateRecord>> {
        self.batches.lock().expect("batch recorder lock").clone()
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for RecordingBatchRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        assert!(super::request_candidate_slots_are_unique(&candidates));
        self.batches
            .lock()
            .expect("batch recorder lock")
            .push(candidates.clone());
        if self.fail_next_batch.swap(false, Ordering::AcqRel) {
            return Err(DataLayerError::TimedOut(
                "injected request candidate batch failure".to_string(),
            ));
        }
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

impl Default for BlockingFirstBatchRequestCandidateRepository {
    fn default() -> Self {
        Self {
            inner: InMemoryRequestCandidateRepository::default(),
            batches: Mutex::new(Vec::new()),
            block_first: AtomicBool::new(true),
            first_batch_started: tokio::sync::Notify::new(),
            release_first_batch: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for BlockingFirstBatchRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        self.batches
            .lock()
            .expect("batch recorder lock")
            .push(candidates.clone());
        if self.block_first.swap(false, Ordering::AcqRel) {
            self.first_batch_started.notify_one();
            self.release_first_batch.notified().await;
        }
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

#[async_trait::async_trait]
impl RequestCandidateWriteRepository for CountingBatchRequestCandidateRepository {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, DataLayerError> {
        self.upsert_calls.fetch_add(1, Ordering::AcqRel);
        self.inner.upsert(candidate).await
    }

    async fn upsert_many(
        &self,
        candidates: Vec<UpsertRequestCandidateRecord>,
    ) -> Result<usize, DataLayerError> {
        self.upsert_many_calls.fetch_add(1, Ordering::AcqRel);
        let active = self.active_upsert_many.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active_upsert_many
            .fetch_max(active, Ordering::AcqRel);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let count = candidates.len();
        for candidate in candidates {
            self.inner.upsert(candidate).await?;
        }
        self.active_upsert_many.fetch_sub(1, Ordering::AcqRel);
        Ok(count)
    }

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, DataLayerError> {
        self.inner
            .delete_created_before(created_before_unix_secs, limit)
            .await
    }
}

fn record(
    request_id: &str,
    candidate_index: u32,
    retry_index: u32,
    status: RequestCandidateStatus,
) -> UpsertRequestCandidateRecord {
    UpsertRequestCandidateRecord {
        id: format!("{request_id}-{candidate_index}-{retry_index}-{status:?}"),
        request_id: request_id.to_string(),
        user_id: None,
        api_key_id: None,
        username: None,
        api_key_name: None,
        candidate_index,
        retry_index,
        provider_id: None,
        endpoint_id: None,
        key_id: None,
        status,
        skip_reason: None,
        is_cached: None,
        status_code: None,
        error_type: None,
        error_message: None,
        latency_ms: None,
        concurrent_requests: None,
        extra_data: None,
        required_capabilities: None,
        created_at_unix_ms: None,
        started_at_unix_ms: None,
        finished_at_unix_ms: None,
    }
}

struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    fn unset(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        if let Some(previous) = self.previous.as_ref() {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

#[test]
fn from_env_defaults_to_async_with_sync_full_fallback() {
    let _mode = EnvGuard::unset(super::MODE_ENV);
    let _full = EnvGuard::unset(super::QUEUE_FULL_ENV);
    let _db_batch = EnvGuard::unset(super::DB_BATCH_SIZE_ENV);

    let config = RequestCandidateQueueConfig::from_env();

    assert_eq!(config.mode, super::RequestCandidateWriteMode::Async);
    assert_eq!(
        config.full_policy,
        super::RequestCandidateQueueFullPolicy::Sync
    );
    assert_eq!(config.db_batch_size, 512);
}

#[test]
fn candidate_background_runtime_threads_are_bounded() {
    assert_eq!(parse_request_candidate_background_runtime_threads(None), 1);
    assert_eq!(
        parse_request_candidate_background_runtime_threads(Some("0")),
        1
    );
    assert_eq!(
        parse_request_candidate_background_runtime_threads(Some("3")),
        3
    );
    assert_eq!(
        parse_request_candidate_background_runtime_threads(Some("999")),
        8
    );
    assert_eq!(
        parse_request_candidate_background_runtime_threads(Some("not-a-number")),
        1
    );
}

#[tokio::test]
async fn candidate_queue_tasks_run_on_dedicated_runtime() {
    let thread_name = spawn_on_request_candidate_background_runtime(async {
        std::thread::current()
            .name()
            .unwrap_or_default()
            .to_string()
    })
    .await
    .expect("candidate background task should complete");

    assert_eq!(thread_name, "aether-candidate-queue");
}

#[test]
fn compact_merges_same_slot_without_losing_terminal_fields() {
    let mut first_success = record("req", 0, 0, RequestCandidateStatus::Success);
    first_success.provider_id = Some("provider-a".to_string());
    first_success.extra_data = Some(serde_json::json!({"first": true}));
    let mut second_success = record("req", 0, 0, RequestCandidateStatus::Success);
    second_success.latency_ms = Some(123);
    second_success.extra_data = Some(serde_json::json!({"second": true}));

    let compacted = compact_records_for_flush(vec![
        record("req", 0, 0, RequestCandidateStatus::Pending),
        first_success,
        record("req", 0, 1, RequestCandidateStatus::Failed),
        second_success,
    ]);

    assert_eq!(compacted.normal.len(), 0);
    assert_eq!(compacted.pending.len(), 1);
    assert_eq!(compacted.streaming.len(), 0);
    assert_eq!(compacted.terminal.len(), 2);
    assert_eq!(
        compacted.pending[0].record.status,
        RequestCandidateStatus::Pending
    );
    assert_eq!(compacted.pending[0].source_count, 1);
    assert_eq!(
        compacted.terminal[0].record.status,
        RequestCandidateStatus::Success
    );
    assert_eq!(compacted.terminal[0].source_count, 2);
    assert_eq!(
        compacted.terminal[0].record.provider_id.as_deref(),
        Some("provider-a")
    );
    assert_eq!(compacted.terminal[0].record.latency_ms, Some(123));
    assert_eq!(
        compacted.terminal[0].record.extra_data,
        Some(serde_json::json!({"first": true, "second": true}))
    );
    assert_eq!(
        compacted.terminal[1].record.status,
        RequestCandidateStatus::Failed
    );
    assert_eq!(compacted.terminal[1].source_count, 1);
}

#[test]
fn compact_splits_lifecycle_stages_in_monotonic_order() {
    let compacted = compact_records_for_flush(vec![
        record("req", 0, 0, RequestCandidateStatus::Success),
        record("req", 0, 0, RequestCandidateStatus::Streaming),
        record("req", 0, 0, RequestCandidateStatus::Unused),
    ]);

    assert_eq!(compacted.normal.len(), 1);
    assert_eq!(compacted.pending.len(), 0);
    assert_eq!(compacted.streaming.len(), 1);
    assert_eq!(compacted.terminal.len(), 1);
    assert_eq!(
        compacted.normal[0].record.status,
        RequestCandidateStatus::Unused
    );
    assert_eq!(compacted.normal[0].source_count, 1);
    assert_eq!(
        compacted.streaming[0].record.status,
        RequestCandidateStatus::Streaming
    );
    assert_eq!(compacted.streaming[0].source_count, 1);
    assert_eq!(
        compacted.terminal[0].record.status,
        RequestCandidateStatus::Success
    );
    assert_eq!(compacted.terminal[0].source_count, 1);
}

#[tokio::test]
async fn active_micro_batch_collects_events_arriving_within_one_ms() {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.queued_current.store(1, Ordering::Release);
    metrics.priority_queued_current.store(1, Ordering::Release);
    metrics.active_queued_current.store(1, Ordering::Release);
    let first = record("micro-batch", 0, 0, RequestCandidateStatus::Streaming);
    let second = record("micro-batch", 1, 0, RequestCandidateStatus::Pending);
    // Seed the channel before starting the bounded receive window. Using
    // a scheduler yield here makes the test occasionally miss a sender
    // that did not run until after the intentionally tiny 1ms deadline.
    sender
        .send(RequestCandidateActiveQueueMessage::Record(Box::new(second)))
        .await
        .expect("micro-batch receiver open");
    let mut batch = vec![first];
    let mut barriers = Vec::new();
    let mut receiver_open = true;
    collect_active_micro_batch(
        &mut receiver,
        &mut batch,
        &mut barriers,
        4,
        &metrics,
        &mut receiver_open,
    )
    .await;

    assert_eq!(batch.len(), 2);
    assert!(barriers.is_empty());
    assert_eq!(metrics.queued_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_queued_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_queued_current.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn sustained_priority_batches_give_ready_normal_records_a_turn() {
    let recorder = Arc::new(RecordingBatchRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        capacity: 16,
        batch_size: 1,
        db_batch_size: 1,
        flush_interval: Duration::from_secs(5),
        workers: 1,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = Arc::new(RequestCandidateQueueMetrics::default());
    let normal_admission = Arc::new(Semaphore::new(0));
    let priority_admission = Arc::new(Semaphore::new(0));
    let (normal_sender, normal_receiver) = tokio::sync::mpsc::channel(1);
    let (active_sender, active_receiver) = tokio::sync::mpsc::channel(16);
    let (terminal_sender, terminal_receiver) = tokio::sync::mpsc::channel(16);

    normal_sender
        .send(record(
            "normal-fairness",
            0,
            0,
            RequestCandidateStatus::Available,
        ))
        .await
        .expect("normal receiver open");
    for index in 0..(MAX_CONSECUTIVE_PRIORITY_FLUSHES * 2) {
        active_sender
            .send(RequestCandidateActiveQueueMessage::Record(Box::new(
                record(
                    &format!("priority-fairness-{index}"),
                    0,
                    0,
                    RequestCandidateStatus::Streaming,
                ),
            )))
            .await
            .expect("priority receiver open");
    }
    drop(normal_sender);
    drop(active_sender);
    drop(terminal_sender);

    let priority_count = MAX_CONSECUTIVE_PRIORITY_FLUSHES * 2;
    metrics
        .queued_current
        .store(priority_count + 1, Ordering::Release);
    metrics
        .pending_current
        .store(priority_count + 1, Ordering::Release);
    metrics
        .priority_queued_current
        .store(priority_count, Ordering::Release);
    metrics
        .priority_pending_current
        .store(priority_count, Ordering::Release);
    metrics
        .active_queued_current
        .store(priority_count, Ordering::Release);
    metrics
        .active_pending_current
        .store(priority_count, Ordering::Release);

    run_worker(
        repository,
        config,
        Arc::clone(&metrics),
        None,
        0,
        normal_receiver,
        active_receiver,
        terminal_receiver,
        normal_admission,
        priority_admission,
    )
    .await;

    let batches = recorder.batches();
    let normal_batch_index = batches
        .iter()
        .position(|batch| {
            batch
                .iter()
                .any(|candidate| candidate.request_id == "normal-fairness")
        })
        .expect("normal record should be flushed");
    assert!(normal_batch_index <= MAX_CONSECUTIVE_PRIORITY_FLUSHES);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn sustained_active_batches_give_ready_terminal_records_a_turn() {
    let recorder = Arc::new(RecordingBatchRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        capacity: 16,
        batch_size: 1,
        db_batch_size: 1,
        flush_interval: Duration::from_secs(5),
        workers: 1,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = Arc::new(RequestCandidateQueueMetrics::default());
    let normal_admission = Arc::new(Semaphore::new(0));
    let priority_admission = Arc::new(Semaphore::new(0));
    let (normal_sender, normal_receiver) = tokio::sync::mpsc::channel(1);
    let (active_sender, active_receiver) = tokio::sync::mpsc::channel(16);
    let (terminal_sender, terminal_receiver) = tokio::sync::mpsc::channel(16);
    let active_count = MAX_CONSECUTIVE_ACTIVE_FLUSHES * 3;
    for index in 0..active_count {
        active_sender
            .send(RequestCandidateActiveQueueMessage::Record(Box::new(
                record(
                    &format!("active-fairness-{index}"),
                    0,
                    0,
                    RequestCandidateStatus::Streaming,
                ),
            )))
            .await
            .expect("active receiver open");
    }
    let barrier = Arc::new(RequestCandidateTerminalBarrier::new());
    barrier.ready.store(true, Ordering::Release);
    terminal_sender
        .send(RequestCandidateTerminalQueueRecord {
            record: record("terminal-fairness", 0, 0, RequestCandidateStatus::Success),
            barrier,
        })
        .await
        .expect("terminal receiver open");
    drop(normal_sender);
    drop(active_sender);
    drop(terminal_sender);

    let priority_count = active_count + 1;
    metrics
        .queued_current
        .store(priority_count, Ordering::Release);
    metrics
        .pending_current
        .store(priority_count, Ordering::Release);
    metrics
        .priority_queued_current
        .store(priority_count, Ordering::Release);
    metrics
        .priority_pending_current
        .store(priority_count, Ordering::Release);
    metrics
        .active_queued_current
        .store(active_count, Ordering::Release);
    metrics
        .active_pending_current
        .store(active_count, Ordering::Release);
    metrics.terminal_queued_current.store(1, Ordering::Release);
    metrics.terminal_pending_current.store(1, Ordering::Release);

    run_worker(
        repository,
        config,
        Arc::clone(&metrics),
        None,
        0,
        normal_receiver,
        active_receiver,
        terminal_receiver,
        normal_admission,
        priority_admission,
    )
    .await;

    let batches = recorder.batches();
    let terminal_batch_index = batches
        .iter()
        .position(|batch| {
            batch
                .iter()
                .any(|candidate| candidate.request_id == "terminal-fairness")
        })
        .expect("terminal record should be flushed");
    assert!(terminal_batch_index <= MAX_CONSECUTIVE_ACTIVE_FLUSHES);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_pending_current.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn sustained_barrier_stream_gives_ready_terminal_records_a_turn() {
    let recorder = Arc::new(RecordingBatchRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        capacity: 32,
        batch_size: 1,
        db_batch_size: 1,
        flush_interval: Duration::from_secs(5),
        workers: 1,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = Arc::new(RequestCandidateQueueMetrics::default());
    let normal_admission = Arc::new(Semaphore::new(0));
    let priority_admission = Arc::new(Semaphore::new(0));
    let (normal_sender, normal_receiver) = tokio::sync::mpsc::channel(1);
    let (active_sender, active_receiver) = tokio::sync::mpsc::channel(32);
    let (terminal_sender, terminal_receiver) = tokio::sync::mpsc::channel(1);
    drop(normal_sender);

    let terminal_barrier = Arc::new(RequestCandidateTerminalBarrier::new());
    terminal_barrier.ready.store(true, Ordering::Release);
    terminal_sender
        .send(RequestCandidateTerminalQueueRecord {
            record: record(
                "terminal-amid-barriers",
                0,
                0,
                RequestCandidateStatus::Success,
            ),
            barrier: terminal_barrier,
        })
        .await
        .expect("terminal receiver open");
    drop(terminal_sender);
    metrics.queued_current.store(1, Ordering::Release);
    metrics.pending_current.store(1, Ordering::Release);
    metrics.priority_queued_current.store(1, Ordering::Release);
    metrics.priority_pending_current.store(1, Ordering::Release);
    metrics.terminal_queued_current.store(1, Ordering::Release);
    metrics.terminal_pending_current.store(1, Ordering::Release);

    for _ in 0..32 {
        let barrier = Arc::new(RequestCandidateTerminalBarrier::new());
        barrier.ready.store(true, Ordering::Release);
        active_sender
            .send(RequestCandidateActiveQueueMessage::Barrier(barrier))
            .await
            .expect("active receiver open");
    }
    let keep_producing = Arc::new(AtomicBool::new(true));
    let keep_producing_for_task = Arc::clone(&keep_producing);
    let producer = tokio::spawn(async move {
        while keep_producing_for_task.load(Ordering::Acquire) {
            let barrier = Arc::new(RequestCandidateTerminalBarrier::new());
            barrier.ready.store(true, Ordering::Release);
            if active_sender
                .send(RequestCandidateActiveQueueMessage::Barrier(barrier))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let worker = tokio::spawn(run_worker(
        repository,
        config,
        Arc::clone(&metrics),
        None,
        0,
        normal_receiver,
        active_receiver,
        terminal_receiver,
        normal_admission,
        priority_admission,
    ));
    let terminal_flushed = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if recorder.batches().iter().any(|batch| {
                batch
                    .iter()
                    .any(|record| record.request_id == "terminal-amid-barriers")
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();

    keep_producing.store(false, Ordering::Release);
    producer.abort();
    let _ = producer.await;
    tokio::time::timeout(Duration::from_secs(1), worker)
        .await
        .expect("worker should stop after the barrier producer closes")
        .expect("barrier fairness worker should complete");
    assert!(
        terminal_flushed,
        "continuous barrier traffic must not starve a ready terminal record"
    );
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_pending_current.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn released_terminal_front_flushes_without_waiting_for_ticker() {
    let recorder = Arc::new(BlockingFirstBatchRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        capacity: 2,
        batch_size: 1,
        db_batch_size: 1,
        flush_interval: Duration::from_secs(5),
        workers: 1,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = Arc::new(RequestCandidateQueueMetrics::default());
    let normal_admission = Arc::new(Semaphore::new(0));
    let priority_admission = Arc::new(Semaphore::new(0));
    let (normal_sender, normal_receiver) = tokio::sync::mpsc::channel(1);
    let (active_sender, active_receiver) = tokio::sync::mpsc::channel(2);
    let (terminal_sender, terminal_receiver) = tokio::sync::mpsc::channel(2);
    let barrier = Arc::new(RequestCandidateTerminalBarrier::new());

    terminal_sender
        .send(RequestCandidateTerminalQueueRecord {
            record: record(
                "terminal-after-barrier",
                0,
                0,
                RequestCandidateStatus::Success,
            ),
            barrier: Arc::clone(&barrier),
        })
        .await
        .expect("terminal receiver open");
    normal_sender
        .send(record(
            "normal-wakeup-sentinel",
            0,
            0,
            RequestCandidateStatus::Available,
        ))
        .await
        .expect("normal receiver open");

    metrics.queued_current.store(2, Ordering::Release);
    metrics.pending_current.store(2, Ordering::Release);
    metrics.priority_queued_current.store(1, Ordering::Release);
    metrics.priority_pending_current.store(1, Ordering::Release);
    metrics.terminal_queued_current.store(1, Ordering::Release);
    metrics.terminal_pending_current.store(1, Ordering::Release);
    metrics.terminal_barrier_pending.store(1, Ordering::Release);

    let worker_metrics = Arc::clone(&metrics);
    let worker = tokio::spawn(async move {
        run_worker(
            repository,
            config,
            worker_metrics,
            None,
            0,
            normal_receiver,
            active_receiver,
            terminal_receiver,
            normal_admission,
            priority_admission,
        )
        .await;
    });

    tokio::time::timeout(
        Duration::from_secs(1),
        recorder.first_batch_started.notified(),
    )
    .await
    .expect("normal sentinel should start after terminal is parked at the front");
    recorder.release_first_batch.notify_one();
    active_sender
        .send(RequestCandidateActiveQueueMessage::Barrier(barrier))
        .await
        .expect("active receiver open");
    drop(normal_sender);
    drop(active_sender);
    drop(terminal_sender);

    tokio::time::timeout(Duration::from_millis(500), worker)
        .await
        .expect("released terminal front must not wait for the five-second ticker")
        .expect("candidate worker should finish");

    let batches = recorder
        .batches
        .lock()
        .expect("batch recorder lock")
        .clone();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0][0].request_id, "normal-wakeup-sentinel");
    assert_eq!(batches[1][0].request_id, "terminal-after-barrier");
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_barrier_pending.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn flush_commits_pending_streaming_and_terminal_in_distinct_batches() {
    let recorder = Arc::new(RecordingBatchRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        db_batch_size: 512,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(6, Ordering::Release);
    metrics.priority_pending_current.store(6, Ordering::Release);
    let mut batch = vec![
        record("ordered", 0, 0, RequestCandidateStatus::Pending),
        record("ordered", 0, 0, RequestCandidateStatus::Streaming),
        record("ordered", 0, 0, RequestCandidateStatus::Success),
        record("ordered", 1, 0, RequestCandidateStatus::Pending),
        record("ordered", 1, 0, RequestCandidateStatus::Streaming),
        record("ordered", 1, 0, RequestCandidateStatus::Failed),
    ];

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        None,
    )
    .await;

    let batches = recorder.batches();
    assert_eq!(batches.len(), 3);
    assert!(batches[0]
        .iter()
        .all(|candidate| candidate.status == RequestCandidateStatus::Pending));
    assert!(batches[1]
        .iter()
        .all(|candidate| candidate.status == RequestCandidateStatus::Streaming));
    assert!(batches[2]
        .iter()
        .all(|candidate| super::request_candidate_status_is_terminal(candidate.status)));
    assert!(batches
        .iter()
        .all(|batch| super::request_candidate_slots_are_unique(batch)));
    assert!(batch.is_empty());
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.flushed_total.load(Ordering::Acquire), 6);
    assert_eq!(metrics.priority_flushed_total.load(Ordering::Acquire), 6);
    assert_eq!(metrics.flush_sql_ops_total.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn pending_is_visible_before_streaming_batch_starts() {
    let recorder = Arc::new(BlockingStreamingBatchRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        db_batch_size: 512,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = Arc::new(RequestCandidateQueueMetrics::default());
    metrics.pending_current.store(3, Ordering::Release);
    metrics.priority_pending_current.store(3, Ordering::Release);
    let worker_metrics = Arc::clone(&metrics);
    let worker_config = config.clone();
    let worker_repository = Arc::clone(&repository);
    let worker = tokio::spawn(async move {
        let mut batch = vec![
            record("visible", 0, 0, RequestCandidateStatus::Pending),
            record("visible", 0, 0, RequestCandidateStatus::Streaming),
            record("visible", 0, 0, RequestCandidateStatus::Success),
        ];
        flush_batch(
            &worker_repository,
            &worker_config,
            &worker_metrics,
            None,
            0,
            RequestCandidateQueueLane::Active,
            &mut batch,
            None,
        )
        .await;
    });

    tokio::time::timeout(
        Duration::from_secs(1),
        recorder.streaming_batch_started.notified(),
    )
    .await
    .expect("streaming stage should start after pending commit");
    let pending = recorder
        .inner
        .list_by_request_id("visible")
        .await
        .expect("pending candidate should be readable");
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].status, RequestCandidateStatus::Pending);

    recorder.release_streaming_batch.notify_one();
    worker.await.expect("lifecycle flush should complete");
    let final_candidate = recorder
        .inner
        .list_by_request_id("visible")
        .await
        .expect("final candidate should be readable");
    assert_eq!(final_candidate[0].status, RequestCandidateStatus::Success);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn retry_backoff_is_lane_scoped_and_does_not_block_active_writes() {
    let recorder = Arc::new(RecordingBatchRequestCandidateRepository::default());
    recorder.fail_next_batch();
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig::default();
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(2, Ordering::Release);
    metrics.priority_pending_current.store(1, Ordering::Release);
    metrics.active_pending_current.store(1, Ordering::Release);
    let mut normal_batch = vec![record(
        "normal-retry",
        0,
        0,
        RequestCandidateStatus::Available,
    )];
    let mut active_batch = vec![record(
        "active-healthy",
        0,
        0,
        RequestCandidateStatus::Pending,
    )];

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Normal,
        &mut normal_batch,
        None,
    )
    .await;

    assert_eq!(normal_batch.len(), 1);
    {
        let retry_states = metrics
            .retry_states
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(
            retry_states
                .get(&(0, RequestCandidateQueueLane::Normal))
                .map(|state| state.attempt),
            Some(1)
        );
        assert!(!retry_states.contains_key(&(0, RequestCandidateQueueLane::Active)));
    }
    assert!(request_candidate_retry_is_ready(
        &metrics,
        0,
        RequestCandidateQueueLane::Active
    ));

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut active_batch,
        None,
    )
    .await;

    assert!(active_batch.is_empty());
    assert_eq!(
        recorder
            .inner
            .list_by_request_id("active-healthy")
            .await
            .expect("active candidate lookup should succeed")
            .len(),
        1
    );
}

#[test]
fn retry_backoff_uses_bounded_equal_jitter() {
    let metrics = RequestCandidateQueueMetrics::default();
    let first = request_candidate_retry_delay(&metrics, 0, RequestCandidateQueueLane::Normal);
    let second = request_candidate_retry_delay(&metrics, 0, RequestCandidateQueueLane::Normal);
    assert!((Duration::from_millis(50)..=Duration::from_millis(100)).contains(&first));
    assert!((Duration::from_millis(100)..=Duration::from_millis(200)).contains(&second));

    let mut capped = second;
    for _ in 0..16 {
        capped = request_candidate_retry_delay(&metrics, 0, RequestCandidateQueueLane::Normal);
    }
    assert!((Duration::from_secs(15)..=Duration::from_secs(30)).contains(&capped));
}

#[tokio::test]
async fn retry_deadline_wakes_worker_before_a_long_flush_interval() {
    let repository = Arc::new(RecordingBatchRequestCandidateRepository::default());
    repository.fail_next_batch();
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 4,
            batch_size: 1,
            db_batch_size: 1,
            flush_interval: Duration::from_secs(5),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    runtime
        .enqueue_or_fallback(record(
            "deadline-wakeup",
            0,
            0,
            RequestCandidateStatus::Available,
        ))
        .await
        .expect("candidate should enter the normal queue");

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if repository
                .inner
                .list_by_request_id("deadline-wakeup")
                .await
                .expect("candidate lookup should succeed")
                .len()
                == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("retry deadline should wake the worker before the five-second ticker");
    assert_eq!(repository.batches().len(), 2);
    assert_eq!(runtime.metrics.pending_current.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn permanent_poison_isolated_without_blocking_healthy_batch_records() {
    let recorder = Arc::new(PoisonPendingRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        db_batch_size: 512,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(12, Ordering::Release);
    metrics
        .priority_pending_current
        .store(12, Ordering::Release);
    metrics.active_pending_current.store(12, Ordering::Release);
    let admission = Semaphore::new(0);
    let mut batch = (0..12)
        .map(|index| {
            let request_id = if index == 5 {
                "poison".to_string()
            } else {
                format!("healthy-{index}")
            };
            record(&request_id, 0, 0, RequestCandidateStatus::Pending)
        })
        .collect::<Vec<_>>();

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        Some(&admission),
    )
    .await;

    assert!(batch.is_empty(), "permanent poison must not be requeued");
    assert_eq!(admission.available_permits(), 12);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.flushed_total.load(Ordering::Acquire), 11);
    assert_eq!(metrics.flush_failed_total.load(Ordering::Acquire), 1);
    assert_eq!(metrics.permanent_dropped_total.load(Ordering::Acquire), 1);
    assert!(recorder.poison_attempts.load(Ordering::Acquire) <= 5);
    assert!(recorder.attempts.load(Ordering::Acquire) <= 9);
    assert!(recorder
        .inner
        .list_by_request_id("poison")
        .await
        .expect("poison lookup should succeed")
        .is_empty());
    for index in 0..12 {
        if index == 5 {
            continue;
        }
        let request_id = format!("healthy-{index}");
        assert_eq!(
            recorder
                .inner
                .list_by_request_id(&request_id)
                .await
                .expect("healthy candidate lookup should succeed")
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn global_database_error_short_circuits_the_entire_flush_batch() {
    let recorder = Arc::new(GlobalFailureRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        db_batch_size: 1,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(12, Ordering::Release);
    metrics
        .priority_pending_current
        .store(12, Ordering::Release);
    metrics.active_pending_current.store(12, Ordering::Release);
    let admission = Semaphore::new(0);
    let mut batch = (0..12)
        .map(|index| {
            record(
                &format!("global-failure-{index}"),
                0,
                0,
                RequestCandidateStatus::Pending,
            )
        })
        .collect::<Vec<_>>();

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        Some(&admission),
    )
    .await;

    assert!(batch.is_empty());
    assert_eq!(recorder.attempts.load(Ordering::Acquire), 1);
    assert_eq!(admission.available_permits(), 12);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.flush_failed_total.load(Ordering::Acquire), 12);
    assert_eq!(metrics.permanent_dropped_total.load(Ordering::Acquire), 12);
}

#[tokio::test]
async fn permanent_pending_poison_does_not_block_same_slot_terminal_record() {
    let recorder = Arc::new(PoisonPendingRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig::default();
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(3, Ordering::Release);
    metrics.priority_pending_current.store(3, Ordering::Release);
    metrics.active_pending_current.store(3, Ordering::Release);
    let mut batch = vec![
        record("poison", 0, 0, RequestCandidateStatus::Pending),
        record("poison", 0, 0, RequestCandidateStatus::Pending),
        record("poison", 0, 0, RequestCandidateStatus::Success),
    ];

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        None,
    )
    .await;

    assert!(batch.is_empty());
    assert_eq!(metrics.permanent_dropped_total.load(Ordering::Acquire), 2);
    assert_eq!(metrics.flushed_total.load(Ordering::Acquire), 1);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
    let stored = recorder
        .inner
        .list_by_request_id("poison")
        .await
        .expect("terminal candidate lookup should succeed");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].status, RequestCandidateStatus::Success);
}

#[test]
fn candidate_write_errors_distinguish_permanent_data_from_transient_failures() {
    assert_eq!(
        request_candidate_write_error_disposition(&DataLayerError::Postgres(
            "error returned from database: unsupported Unicode escape sequence".to_string()
        )),
        RequestCandidateWriteErrorDisposition::IsolateRecord
    );
    assert_eq!(
        request_candidate_write_error_disposition(&DataLayerError::Postgres(
            "invalid input syntax (SQLSTATE 22P05)".to_string()
        )),
        RequestCandidateWriteErrorDisposition::IsolateRecord
    );
    assert_eq!(
        request_candidate_write_error_disposition(&DataLayerError::Postgres(
            "serialization failure (SQLSTATE 40001)".to_string()
        )),
        RequestCandidateWriteErrorDisposition::Retry
    );
    assert_eq!(
        request_candidate_write_error_disposition(&DataLayerError::TimedOut(
            "postgres acquire".to_string()
        )),
        RequestCandidateWriteErrorDisposition::Retry
    );
    assert_eq!(
        request_candidate_write_error_disposition(&DataLayerError::InvalidInput(
            "bad candidate".to_string()
        )),
        RequestCandidateWriteErrorDisposition::IsolateRecord
    );
    assert_eq!(
        request_candidate_write_error_disposition(&DataLayerError::InvalidConfiguration(
            "database unavailable by configuration".to_string()
        )),
        RequestCandidateWriteErrorDisposition::DropBatch
    );
    for sqlstate in ["28P01", "42P01", "0A000"] {
        assert_eq!(
            request_candidate_write_error_disposition(&DataLayerError::Postgres(format!(
                "non-retryable database failure (SQLSTATE {sqlstate})"
            ))),
            RequestCandidateWriteErrorDisposition::DropBatch
        );
    }
    for sqlstate in ["21000", "44000"] {
        assert_eq!(
            request_candidate_write_error_disposition(&DataLayerError::Postgres(format!(
                "record-specific database failure (SQLSTATE {sqlstate})"
            ))),
            RequestCandidateWriteErrorDisposition::IsolateRecord
        );
    }
    for malformed in [
        "invalid input (SQLSTATE 22P05) trailing",
        "invalid input (SQLSTATE 22p05)",
        "invalid input (SQLSTATE 22P0)",
        "invalid input (SQLSTATE 22P050)",
    ] {
        assert_eq!(
            request_candidate_write_error_disposition(&DataLayerError::Postgres(
                malformed.to_string()
            )),
            RequestCandidateWriteErrorDisposition::Retry
        );
    }
}

#[tokio::test]
async fn failed_pending_stage_blocks_and_retries_later_lifecycle_stages() {
    let recorder = Arc::new(RecordingBatchRequestCandidateRepository::default());
    recorder.fail_next_batch();
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig {
        db_batch_size: 512,
        ..RequestCandidateQueueConfig::default()
    };
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(3, Ordering::Release);
    metrics.priority_pending_current.store(3, Ordering::Release);
    let mut batch = vec![
        record("retry-order", 0, 0, RequestCandidateStatus::Pending),
        record("retry-order", 0, 0, RequestCandidateStatus::Streaming),
        record("retry-order", 0, 0, RequestCandidateStatus::Success),
    ];

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        None,
    )
    .await;

    let first_attempts = recorder.batches();
    assert_eq!(first_attempts.len(), 1);
    assert_eq!(first_attempts[0][0].status, RequestCandidateStatus::Pending);
    assert_eq!(batch.len(), 3);
    assert_eq!(batch[0].status, RequestCandidateStatus::Pending);
    assert_eq!(batch[1].status, RequestCandidateStatus::Streaming);
    assert_eq!(batch[2].status, RequestCandidateStatus::Success);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 3);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 3);
    assert_eq!(metrics.flush_failed_total.load(Ordering::Acquire), 1);

    let retry_not_before = metrics
        .retry_states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(0, RequestCandidateQueueLane::Active))
        .expect("active retry state should be scheduled")
        .retry_not_before;
    tokio::time::sleep_until(retry_not_before).await;
    assert!(request_candidate_retry_is_ready(
        &metrics,
        0,
        RequestCandidateQueueLane::Active
    ));

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        None,
    )
    .await;

    let attempts = recorder.batches();
    assert_eq!(attempts.len(), 4);
    assert_eq!(attempts[1][0].status, RequestCandidateStatus::Pending);
    assert_eq!(attempts[2][0].status, RequestCandidateStatus::Streaming);
    assert_eq!(attempts[3][0].status, RequestCandidateStatus::Success);
    assert!(batch.is_empty());
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.flushed_total.load(Ordering::Acquire), 3);
    assert_eq!(metrics.priority_flushed_total.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn failed_active_retry_keeps_terminal_barrier_closed() {
    let repository = Arc::new(FailThenBlockPendingRetryRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 16,
            batch_size: 16,
            db_batch_size: 16,
            flush_interval: Duration::from_millis(10),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Sync,
        },
    );

    for status in [
        RequestCandidateStatus::Pending,
        RequestCandidateStatus::Streaming,
        RequestCandidateStatus::Success,
    ] {
        runtime
            .try_enqueue_priority_status(record("barrier-retry", 0, 0, status))
            .expect("lifecycle status should enter its split lane");
    }

    tokio::time::timeout(Duration::from_secs(1), repository.retry_started.notified())
        .await
        .expect("pending retry should start after the injected failure");
    assert_eq!(
        runtime
            .metrics
            .terminal_barrier_pending
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_pending_current
            .load(Ordering::Acquire),
        1
    );
    assert!(repository
        .batches
        .lock()
        .expect("pending retry batch recorder lock")
        .iter()
        .flatten()
        .all(|candidate| candidate.status == RequestCandidateStatus::Pending));

    repository.release_retry.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics.pending_current.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("active retry and terminal lane should drain");

    let statuses = repository
        .batches
        .lock()
        .expect("pending retry batch recorder lock")
        .iter()
        .flatten()
        .map(|candidate| candidate.status)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            RequestCandidateStatus::Pending,
            RequestCandidateStatus::Pending,
            RequestCandidateStatus::Streaming,
            RequestCandidateStatus::Success,
        ]
    );
    assert_eq!(
        runtime
            .metrics
            .active_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_barrier_pending
            .load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn global_abort_counts_prior_retry_and_blocked_source_records_exactly() {
    let recorder = Arc::new(RetryThenGlobalFailureRequestCandidateRepository::default());
    let repository: Arc<dyn RequestCandidateWriteRepository> = recorder.clone();
    let config = RequestCandidateQueueConfig::default();
    let metrics = RequestCandidateQueueMetrics::default();
    metrics.pending_current.store(4, Ordering::Release);
    metrics.priority_pending_current.store(4, Ordering::Release);
    metrics.active_pending_current.store(4, Ordering::Release);
    let admission = Semaphore::new(0);
    let mut batch = vec![
        record("retry-then-global", 0, 0, RequestCandidateStatus::Pending),
        record("retry-then-global", 0, 0, RequestCandidateStatus::Pending),
        record("retry-then-global", 0, 0, RequestCandidateStatus::Streaming),
        record("global-trigger", 0, 0, RequestCandidateStatus::Streaming),
    ];

    flush_batch(
        &repository,
        &config,
        &metrics,
        None,
        0,
        RequestCandidateQueueLane::Active,
        &mut batch,
        Some(&admission),
    )
    .await;

    assert!(batch.is_empty());
    assert_eq!(recorder.attempts.load(Ordering::Acquire), 2);
    assert_eq!(admission.available_permits(), 4);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.flush_failed_total.load(Ordering::Acquire), 4);
    assert_eq!(metrics.permanent_dropped_total.load(Ordering::Acquire), 4);
}

#[tokio::test]
async fn async_queue_flushes_enqueued_records() {
    let repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 16,
            batch_size: 2,
            db_batch_size: 128,
            flush_interval: Duration::from_millis(10),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    runtime
        .enqueue_or_fallback(record("req", 0, 0, RequestCandidateStatus::Success))
        .await
        .unwrap();

    for _ in 0..50 {
        let rows = repository.list_by_request_id("req").await.unwrap();
        if rows.len() == 1 {
            assert_eq!(rows[0].status, RequestCandidateStatus::Success);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("async request candidate queue did not flush record in time");
}

#[tokio::test]
async fn streaming_status_flushes_ahead_of_normal_backlog() {
    let repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 64,
            batch_size: 64,
            db_batch_size: 64,
            flush_interval: Duration::from_secs(5),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    runtime
        .enqueue_or_fallback(record(
            "priority-target",
            0,
            0,
            RequestCandidateStatus::Pending,
        ))
        .await
        .unwrap();
    for index in 0..32 {
        runtime
            .enqueue_or_fallback(record(
                &format!("normal-backlog-{index}"),
                0,
                0,
                RequestCandidateStatus::Available,
            ))
            .await
            .unwrap();
    }
    runtime
        .enqueue_or_fallback(record(
            "priority-target",
            0,
            0,
            RequestCandidateStatus::Streaming,
        ))
        .await
        .unwrap();

    let candidate = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let candidates = repository
                .list_by_request_id("priority-target")
                .await
                .unwrap();
            if let Some(candidate) = candidates
                .into_iter()
                .find(|candidate| candidate.status == RequestCandidateStatus::Streaming)
                .filter(|_| {
                    runtime
                        .metrics
                        .priority_flushed_total
                        .load(Ordering::Acquire)
                        >= 2
                })
            {
                break candidate;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("streaming status should bypass the normal persistence backlog");

    assert_eq!(candidate.status, RequestCandidateStatus::Streaming);
    assert_eq!(
        runtime
            .metrics
            .priority_flushed_total
            .load(Ordering::Acquire),
        2
    );
    assert!(runtime.metrics.pending_current.load(Ordering::Acquire) > 0);
}

#[tokio::test]
async fn active_lifecycle_bypasses_queued_terminal_backlog_after_current_write() {
    let repository = Arc::new(BlockingFirstBatchRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 64,
            batch_size: 64,
            db_batch_size: 64,
            flush_interval: Duration::from_secs(5),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Sync,
        },
    );

    runtime
        .try_enqueue_priority_status(record(
            "terminal-blocker",
            0,
            0,
            RequestCandidateStatus::Success,
        ))
        .expect("terminal blocker should enter the terminal lane");
    tokio::time::timeout(
        Duration::from_secs(1),
        repository.first_batch_started.notified(),
    )
    .await
    .expect("first terminal DB batch should start");

    for index in 0..8 {
        runtime
            .try_enqueue_priority_status(record(
                &format!("terminal-backlog-{index}"),
                0,
                0,
                RequestCandidateStatus::Success,
            ))
            .expect("terminal backlog should enter the terminal lane");
    }
    for status in [
        RequestCandidateStatus::Pending,
        RequestCandidateStatus::Streaming,
        RequestCandidateStatus::Success,
    ] {
        runtime
            .try_enqueue_priority_status(record("active-target", 0, 0, status))
            .expect("target lifecycle should enter its lane");
    }

    repository.release_first_batch.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics.pending_current.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("split lifecycle lanes should fully drain");

    let batches = repository
        .batches
        .lock()
        .expect("batch recorder lock")
        .clone();
    let batch_index = |request_id: &str, status: RequestCandidateStatus| {
        batches
            .iter()
            .position(|batch| {
                batch.iter().any(|candidate| {
                    candidate.request_id == request_id && candidate.status == status
                })
            })
            .expect("expected lifecycle batch")
    };
    let pending_index = batch_index("active-target", RequestCandidateStatus::Pending);
    let streaming_index = batch_index("active-target", RequestCandidateStatus::Streaming);
    let terminal_index = batch_index("active-target", RequestCandidateStatus::Success);
    let backlog_index = batch_index("terminal-backlog-0", RequestCandidateStatus::Success);
    assert!(pending_index < streaming_index);
    assert!(streaming_index < terminal_index);
    assert!(streaming_index < backlog_index);
    assert_eq!(
        runtime
            .metrics
            .active_queued_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .active_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_queued_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_barrier_pending
            .load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn bounded_priority_backpressure_preserves_fifo_order_while_db_is_blocked() {
    let repository = Arc::new(BlockingFirstBatchRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 2,
            batch_size: 1,
            db_batch_size: 1,
            flush_interval: Duration::from_secs(5),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Sync,
        },
    );

    runtime
        .enqueue_or_fallback(record(
            "priority-blocker",
            0,
            0,
            RequestCandidateStatus::Streaming,
        ))
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        repository.first_batch_started.notified(),
    )
    .await
    .expect("priority worker should start its first DB batch");
    runtime
        .try_enqueue_priority_status(record(
            "priority-order",
            0,
            0,
            RequestCandidateStatus::Pending,
        ))
        .expect("pending status should synchronously enter the priority lane");
    let overflow = runtime
        .try_enqueue_priority_status(record(
            "priority-order",
            0,
            0,
            RequestCandidateStatus::Streaming,
        ))
        .expect_err("full lifecycle admission should apply backpressure");
    let runtime_for_tail = Arc::clone(&runtime);
    let tail = tokio::spawn(async move {
        runtime_for_tail
            .enqueue_or_fallback(*overflow)
            .await
            .expect("streaming status should wait for bounded admission");
        runtime_for_tail
            .enqueue_or_fallback(record(
                "priority-order",
                0,
                0,
                RequestCandidateStatus::Success,
            ))
            .await
            .expect("terminal status should retain FIFO order behind streaming");
    });
    tokio::task::yield_now().await;
    assert!(
        !tail.is_finished(),
        "overflow enqueue should be backpressured"
    );
    assert_eq!(
        runtime
            .metrics
            .active_queued_current
            .load(Ordering::Acquire),
        1
    );
    assert_eq!(
        runtime.metrics.priority_max_queued.load(Ordering::Acquire),
        1
    );
    assert_eq!(runtime.priority_admission.available_permits(), 0);
    assert!(
        runtime
            .metrics
            .priority_async_overflow_total
            .load(Ordering::Acquire)
            > 0
    );

    repository.release_first_batch.notify_one();
    tokio::time::timeout(Duration::from_secs(2), tail)
        .await
        .expect("bounded lifecycle tail should resume after DB progress")
        .expect("bounded lifecycle tail task should complete");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if runtime.metrics.pending_current.load(Ordering::Acquire) == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ordered priority lane should drain after the first DB batch is released");

    let statuses = repository
        .batches
        .lock()
        .expect("batch recorder lock")
        .iter()
        .flatten()
        .filter(|candidate| candidate.request_id == "priority-order")
        .map(|candidate| candidate.status)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            RequestCandidateStatus::Pending,
            RequestCandidateStatus::Streaming,
            RequestCandidateStatus::Success,
        ]
    );
    assert_eq!(
        runtime
            .metrics
            .active_queued_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .active_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_queued_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_barrier_pending
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(runtime.priority_admission.available_permits(), 2);
}

#[tokio::test]
async fn sustained_db_failure_keeps_lifecycle_resident_set_bounded() {
    let repository = Arc::new(FailUntilReleasedRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 2,
            batch_size: 1,
            db_batch_size: 1,
            flush_interval: Duration::from_millis(5),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    runtime
        .try_enqueue_priority_status(record(
            "outage-bounded-1",
            0,
            0,
            RequestCandidateStatus::Pending,
        ))
        .expect("first lifecycle record should enter admission");
    tokio::time::timeout(Duration::from_secs(1), repository.first_attempt.notified())
        .await
        .expect("worker should observe the injected DB failure");
    runtime
        .try_enqueue_priority_status(record(
            "outage-bounded-2",
            0,
            0,
            RequestCandidateStatus::Pending,
        ))
        .expect("second lifecycle record should fill admission");
    let overflow = runtime
        .try_enqueue_priority_status(record(
            "outage-bounded-3",
            0,
            0,
            RequestCandidateStatus::Pending,
        ))
        .expect_err("third lifecycle record must observe bounded admission");
    let runtime_for_overflow = Arc::clone(&runtime);
    let overflow_task = tokio::spawn(async move {
        runtime_for_overflow
            .enqueue_or_fallback(*overflow)
            .await
            .expect("lossless lifecycle overflow should resume after DB recovery");
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while repository.attempts.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("lifecycle retry should run after its backoff");
    assert!(!overflow_task.is_finished());
    assert_eq!(runtime.priority_admission.available_permits(), 0);
    assert_eq!(
        runtime
            .metrics
            .priority_pending_current
            .load(Ordering::Acquire),
        2
    );
    assert_eq!(
        runtime.metrics.enqueued_total.load(Ordering::Acquire),
        2,
        "sustained failures must not admit an unbounded retry resident set"
    );
    let attempts_during_outage = repository.attempts.load(Ordering::Acquire);
    assert!(attempts_during_outage >= 2);

    repository.failing.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), overflow_task)
        .await
        .expect("overflow should resume after repository recovery")
        .expect("overflow task should complete");
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics.pending_current.load(Ordering::Acquire) != 0
            || runtime.priority_admission.available_permits() != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded lifecycle retries should fully drain after recovery");
    assert_eq!(runtime.priority_admission.available_permits(), 2);
    for request_id in ["outage-bounded-1", "outage-bounded-2", "outage-bounded-3"] {
        assert_eq!(
            repository
                .inner
                .list_by_request_id(request_id)
                .await
                .expect("recovered lifecycle record should be readable")
                .len(),
            1
        );
    }
}

#[tokio::test]
async fn sustained_db_failure_keeps_normal_resident_set_bounded() {
    let repository = Arc::new(FailUntilReleasedRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 2,
            batch_size: 1,
            db_batch_size: 1,
            flush_interval: Duration::from_millis(5),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    runtime
        .enqueue_or_fallback(record(
            "normal-outage-bounded-1",
            0,
            0,
            RequestCandidateStatus::Available,
        ))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), repository.first_attempt.notified())
        .await
        .expect("worker should observe the injected normal-lane DB failure");
    runtime
        .enqueue_or_fallback(record(
            "normal-outage-bounded-2",
            0,
            0,
            RequestCandidateStatus::Available,
        ))
        .await
        .unwrap();
    runtime
        .enqueue_or_fallback(record(
            "normal-outage-dropped",
            0,
            0,
            RequestCandidateStatus::Available,
        ))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        while repository.attempts.load(Ordering::Acquire) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("normal retry should run after its backoff");
    assert_eq!(runtime.normal_admission.available_permits(), 0);
    assert_eq!(runtime.metrics.pending_current.load(Ordering::Acquire), 2);
    assert_eq!(runtime.metrics.enqueued_total.load(Ordering::Acquire), 2);
    assert_eq!(runtime.metrics.dropped_total.load(Ordering::Acquire), 1);
    let attempts_during_outage = repository.attempts.load(Ordering::Acquire);
    assert!(attempts_during_outage >= 2);

    repository.failing.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics.pending_current.load(Ordering::Acquire) != 0
            || runtime.normal_admission.available_permits() != 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded normal retries should fully drain after recovery");
    assert_eq!(runtime.normal_admission.available_permits(), 2);
    for request_id in ["normal-outage-bounded-1", "normal-outage-bounded-2"] {
        assert_eq!(
            repository
                .inner
                .list_by_request_id(request_id)
                .await
                .expect("recovered normal record should be readable")
                .len(),
            1
        );
    }
    assert!(repository
        .inner
        .list_by_request_id("normal-outage-dropped")
        .await
        .expect("dropped normal record lookup should succeed")
        .is_empty());
}

#[tokio::test]
async fn terminal_lane_preserves_fifo_last_write_wins_for_same_slot() {
    let repository = Arc::new(RecordingBatchRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 16,
            batch_size: 16,
            db_batch_size: 16,
            flush_interval: Duration::from_millis(10),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Sync,
        },
    );

    for status in [
        RequestCandidateStatus::Pending,
        RequestCandidateStatus::Streaming,
        RequestCandidateStatus::Failed,
        RequestCandidateStatus::Success,
    ] {
        runtime
            .try_enqueue_priority_status(record("terminal-last-write", 0, 0, status))
            .expect("lifecycle status should enter its split lane");
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while runtime.metrics.pending_current.load(Ordering::Acquire) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal last-write lifecycle should drain");

    let stored = repository
        .inner
        .list_by_request_id("terminal-last-write")
        .await
        .expect("terminal last-write candidate should be readable");
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].status, RequestCandidateStatus::Success);
    let terminal_statuses = repository
        .batches()
        .into_iter()
        .flatten()
        .filter(|candidate| super::request_candidate_status_is_terminal(candidate.status))
        .map(|candidate| candidate.status)
        .collect::<Vec<_>>();
    assert_eq!(terminal_statuses, vec![RequestCandidateStatus::Success]);
    assert_eq!(runtime.metrics.compacted_total.load(Ordering::Acquire), 1);
    assert_eq!(
        runtime
            .metrics
            .terminal_pending_current
            .load(Ordering::Acquire),
        0
    );
    assert_eq!(
        runtime
            .metrics
            .terminal_barrier_pending
            .load(Ordering::Acquire),
        0
    );
}

#[tokio::test]
async fn closed_priority_fast_path_falls_back_without_terminal_regression() {
    let repository = Arc::new(InMemoryRequestCandidateRepository::default());
    let (normal_sender, normal_receiver) = tokio::sync::mpsc::channel(1);
    let (active_sender, active_receiver) = tokio::sync::mpsc::channel(1);
    let (terminal_sender, terminal_receiver) = tokio::sync::mpsc::channel(1);
    drop(normal_receiver);
    drop(active_receiver);
    drop(terminal_receiver);
    let metrics = Arc::new(RequestCandidateQueueMetrics::default());
    let runtime = RequestCandidateQueueRuntime {
        senders: vec![normal_sender],
        active_senders: vec![active_sender],
        terminal_senders: vec![terminal_sender],
        normal_admission: Arc::new(Semaphore::new(1)),
        priority_admission: Arc::new(Semaphore::new(1)),
        priority_capacity: 1,
        repository: repository.clone(),
        config: RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 1,
            batch_size: 1,
            db_batch_size: 1,
            flush_interval: Duration::from_secs(1),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Sync,
        },
        metrics: Arc::clone(&metrics),
        db_write_gate: None,
    };

    for status in [
        RequestCandidateStatus::Pending,
        RequestCandidateStatus::Streaming,
        RequestCandidateStatus::Success,
        RequestCandidateStatus::Streaming,
    ] {
        let fallback = runtime
            .try_enqueue_priority_status(record("closed-priority-fallback", 0, 0, status))
            .expect_err("closed priority lane must return the unpersisted record");
        assert_eq!(fallback.status, status);
        runtime.enqueue_or_fallback(*fallback).await.unwrap();
    }

    let rows = repository
        .list_by_request_id("closed-priority-fallback")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].status, RequestCandidateStatus::Success);
    assert_eq!(metrics.queued_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_queued_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.priority_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_queued_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.active_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_queued_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_pending_current.load(Ordering::Acquire), 0);
    assert_eq!(metrics.terminal_barrier_pending.load(Ordering::Acquire), 0);
    assert_eq!(metrics.sync_fallback_total.load(Ordering::Acquire), 4);
}

#[tokio::test]
async fn async_queue_flushes_records_with_batch_repository_call() {
    let repository = Arc::new(CountingBatchRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 16,
            batch_size: 4,
            db_batch_size: 128,
            flush_interval: Duration::from_millis(100),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    for index in 0..4 {
        runtime
            .enqueue_or_fallback(record(
                "req-batch",
                index,
                0,
                RequestCandidateStatus::Success,
            ))
            .await
            .unwrap();
    }

    for _ in 0..50 {
        if runtime.metrics.pending_current.load(Ordering::Acquire) == 0 {
            assert_eq!(repository.upsert_many_calls.load(Ordering::Acquire), 1);
            assert_eq!(repository.upsert_calls.load(Ordering::Acquire), 0);
            assert_eq!(
                runtime.metrics.flush_sql_ops_total.load(Ordering::Acquire),
                1
            );
            assert_eq!(
                runtime
                    .metrics
                    .flush_sql_records_total
                    .load(Ordering::Acquire),
                4
            );
            assert_eq!(
                runtime
                    .metrics
                    .db_write_max_in_flight
                    .load(Ordering::Acquire),
                1
            );
            assert_eq!(
                runtime.metrics.db_write_in_flight.load(Ordering::Acquire),
                0
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("async request candidate queue did not finish batch flush in time");
}

#[tokio::test]
async fn async_queue_splits_compacted_flush_into_db_batches() {
    let repository = Arc::new(CountingBatchRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 16,
            batch_size: 5,
            db_batch_size: 2,
            flush_interval: Duration::from_millis(100),
            workers: 1,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    for index in 0..5 {
        runtime
            .enqueue_or_fallback(record(
                "req-db-batch",
                index,
                0,
                RequestCandidateStatus::Success,
            ))
            .await
            .unwrap();
    }

    for _ in 0..50 {
        if runtime.metrics.pending_current.load(Ordering::Acquire) == 0 {
            assert_eq!(repository.upsert_many_calls.load(Ordering::Acquire), 3);
            assert_eq!(
                runtime.metrics.flush_batches_total.load(Ordering::Acquire),
                1
            );
            assert_eq!(
                runtime.metrics.flush_sql_ops_total.load(Ordering::Acquire),
                3
            );
            assert_eq!(
                runtime
                    .metrics
                    .flush_sql_records_total
                    .load(Ordering::Acquire),
                5
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("async request candidate queue did not split DB batches in time");
}

#[tokio::test]
async fn async_queue_db_write_gate_limits_concurrent_batch_writes() {
    let repository = Arc::new(CountingBatchRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 64,
            batch_size: 1,
            db_batch_size: 128,
            flush_interval: Duration::from_millis(100),
            workers: 4,
            db_write_concurrency_limit: Some(2),
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    for index in 0..8 {
        runtime
            .enqueue_or_fallback(record(
                &format!("req-gate-{index}"),
                0,
                0,
                RequestCandidateStatus::Success,
            ))
            .await
            .unwrap();
    }

    for _ in 0..100 {
        if runtime.metrics.pending_current.load(Ordering::Acquire) == 0 {
            assert_eq!(repository.max_active_upsert_many.load(Ordering::Acquire), 2);
            assert_eq!(
                runtime
                    .metrics
                    .db_write_max_in_flight
                    .load(Ordering::Acquire),
                2
            );
            assert!(runtime.metrics.db_write_wait_total.load(Ordering::Acquire) > 0);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("async request candidate queue did not finish gated writes in time");
}

#[tokio::test]
async fn async_queue_preserves_same_slot_order_with_multiple_workers() {
    let repository = Arc::new(DelayedPendingRequestCandidateRepository::default());
    let runtime = RequestCandidateQueueRuntime::spawn(
        repository.clone(),
        RequestCandidateQueueConfig {
            mode: super::RequestCandidateWriteMode::Async,
            capacity: 16,
            batch_size: 1,
            db_batch_size: 128,
            flush_interval: Duration::from_millis(100),
            workers: 2,
            db_write_concurrency_limit: None,
            full_policy: super::RequestCandidateQueueFullPolicy::Drop,
        },
    );

    runtime
        .enqueue_or_fallback(record("req-order", 0, 0, RequestCandidateStatus::Pending))
        .await
        .unwrap();
    runtime
        .enqueue_or_fallback(record("req-order", 0, 0, RequestCandidateStatus::Success))
        .await
        .unwrap();

    for _ in 0..50 {
        if runtime.metrics.pending_current.load(Ordering::Acquire) == 0 {
            let rows = repository
                .inner
                .list_by_request_id("req-order")
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].status, RequestCandidateStatus::Success);
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    panic!("async request candidate queue did not finish ordered same-slot writes in time");
}
