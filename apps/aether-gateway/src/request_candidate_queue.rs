use std::collections::{hash_map::RandomState, HashMap, HashSet};
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use aether_data_contracts::repository::candidates::{
    RequestCandidateStatus, RequestCandidateWriteRepository, UpsertRequestCandidateRecord,
};
use aether_runtime::{MetricKind, MetricSample};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{interval, Instant, MissedTickBehavior};
use tracing::{debug, warn};

const MODE_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_WRITE_MODE";
const QUEUE_CAPACITY_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_QUEUE_CAPACITY";
const BATCH_SIZE_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_QUEUE_BATCH_SIZE";
const FLUSH_INTERVAL_MS_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_QUEUE_FLUSH_INTERVAL_MS";
const WORKERS_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_QUEUE_WORKERS";
const QUEUE_FULL_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_QUEUE_FULL";
const DB_WRITE_CONCURRENCY_LIMIT_ENV: &str =
    "AETHER_GATEWAY_REQUEST_CANDIDATE_DB_WRITE_CONCURRENCY_LIMIT";
const DB_BATCH_SIZE_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_DB_BATCH_SIZE";
const RUNTIME_THREADS_ENV: &str = "AETHER_GATEWAY_REQUEST_CANDIDATE_QUEUE_RUNTIME_THREADS";

const DEFAULT_QUEUE_CAPACITY: usize = 65_536;
const DEFAULT_BATCH_SIZE: usize = 512;
const DEFAULT_DB_BATCH_SIZE: usize = 512;
const MAX_DB_BATCH_SIZE: usize = 1_000;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 50;
const PRIORITY_MICRO_BATCH_INTERVAL_MS: u64 = 1;
const MAX_CONSECUTIVE_ACTIVE_FLUSHES: usize = 2;
// Keep normal candidate writes from being indefinitely delayed by a continuous
// lifecycle stream. Priority batches still flush immediately.
const MAX_CONSECUTIVE_PRIORITY_FLUSHES: usize = 4;
const DEFAULT_WORKERS: usize = 2;
const DEFAULT_RUNTIME_THREADS: usize = 1;
const MAX_RUNTIME_THREADS: usize = 8;
const RUNTIME_THREAD_STACK_BYTES: usize = 2 * 1024 * 1024;
const RUNTIME_THREAD_NAME: &str = "aether-candidate-queue";
const FAILED_FLUSH_RETRY_BASE_DELAY_MS: u64 = 100;
const FAILED_FLUSH_RETRY_MAX_DELAY_MS: u64 = 30_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestCandidateWriteMode {
    Sync,
    Async,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestCandidateQueueFullPolicy {
    Drop,
    Sync,
}

#[derive(Debug, Clone)]
pub(crate) struct RequestCandidateQueueConfig {
    pub(crate) mode: RequestCandidateWriteMode,
    pub(crate) capacity: usize,
    pub(crate) batch_size: usize,
    pub(crate) db_batch_size: usize,
    pub(crate) flush_interval: Duration,
    pub(crate) workers: usize,
    pub(crate) db_write_concurrency_limit: Option<usize>,
    pub(crate) full_policy: RequestCandidateQueueFullPolicy,
}

impl Default for RequestCandidateQueueConfig {
    fn default() -> Self {
        Self {
            mode: RequestCandidateWriteMode::Sync,
            capacity: DEFAULT_QUEUE_CAPACITY,
            batch_size: DEFAULT_BATCH_SIZE,
            db_batch_size: DEFAULT_DB_BATCH_SIZE,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
            workers: DEFAULT_WORKERS,
            db_write_concurrency_limit: None,
            full_policy: RequestCandidateQueueFullPolicy::Sync,
        }
    }
}

impl RequestCandidateQueueConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            mode: match env_string(MODE_ENV).as_deref() {
                Some("sync") | Some("inline") => RequestCandidateWriteMode::Sync,
                Some("async") | Some("queued") | Some("queue") => RequestCandidateWriteMode::Async,
                _ => RequestCandidateWriteMode::Async,
            },
            capacity: env_usize(QUEUE_CAPACITY_ENV, DEFAULT_QUEUE_CAPACITY).max(1),
            batch_size: env_usize(BATCH_SIZE_ENV, DEFAULT_BATCH_SIZE).max(1),
            db_batch_size: env_usize(DB_BATCH_SIZE_ENV, DEFAULT_DB_BATCH_SIZE)
                .clamp(1, MAX_DB_BATCH_SIZE),
            flush_interval: Duration::from_millis(
                env_u64(FLUSH_INTERVAL_MS_ENV, DEFAULT_FLUSH_INTERVAL_MS).max(1),
            ),
            workers: env_usize(WORKERS_ENV, DEFAULT_WORKERS).clamp(1, 32),
            db_write_concurrency_limit: env_optional_usize(DB_WRITE_CONCURRENCY_LIMIT_ENV)
                .map(|limit| limit.clamp(1, 32)),
            full_policy: match env_string(QUEUE_FULL_ENV).as_deref() {
                Some("drop") | Some("best_effort") | Some("best-effort") => {
                    RequestCandidateQueueFullPolicy::Drop
                }
                Some("sync") | Some("fallback_sync") | Some("fallback-sync") => {
                    RequestCandidateQueueFullPolicy::Sync
                }
                _ => RequestCandidateQueueFullPolicy::Sync,
            },
        }
    }

    pub(crate) fn async_enabled(&self) -> bool {
        matches!(self.mode, RequestCandidateWriteMode::Async)
    }
}

#[derive(Debug, Default)]
struct RequestCandidateQueueMetrics {
    queued_current: AtomicUsize,
    pending_current: AtomicUsize,
    priority_queued_current: AtomicUsize,
    priority_max_queued: AtomicUsize,
    priority_pending_current: AtomicUsize,
    active_queued_current: AtomicUsize,
    active_max_queued: AtomicUsize,
    active_pending_current: AtomicUsize,
    terminal_queued_current: AtomicUsize,
    terminal_max_queued: AtomicUsize,
    terminal_pending_current: AtomicUsize,
    terminal_barrier_pending: AtomicUsize,
    terminal_barrier_max_pending: AtomicUsize,
    enqueued_total: AtomicU64,
    priority_enqueued_total: AtomicU64,
    priority_async_overflow_total: AtomicU64,
    dropped_total: AtomicU64,
    flushed_total: AtomicU64,
    priority_flushed_total: AtomicU64,
    flush_failed_total: AtomicU64,
    permanent_dropped_total: AtomicU64,
    flush_batches_total: AtomicU64,
    flush_sql_ops_total: AtomicU64,
    flush_sql_records_total: AtomicU64,
    db_write_in_flight: AtomicUsize,
    db_write_max_in_flight: AtomicUsize,
    db_write_wait_total: AtomicU64,
    compacted_total: AtomicU64,
    sync_fallback_total: AtomicU64,
    retry_states: Mutex<HashMap<(usize, RequestCandidateQueueLane), RequestCandidateRetryState>>,
}

#[derive(Debug, Clone, Copy)]
struct RequestCandidateRetryState {
    attempt: u32,
    retry_not_before: Instant,
}

#[derive(Debug)]
struct RequestCandidateTerminalBarrier {
    ready: AtomicBool,
}

impl RequestCandidateTerminalBarrier {
    fn new() -> Self {
        Self {
            ready: AtomicBool::new(false),
        }
    }

    fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    fn release(&self, metrics: &RequestCandidateQueueMetrics) {
        if self
            .ready
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            decrement_atomic_usize(&metrics.terminal_barrier_pending);
        }
    }
}

#[derive(Debug)]
enum RequestCandidateActiveQueueMessage {
    Record(UpsertRequestCandidateRecord),
    Barrier(Arc<RequestCandidateTerminalBarrier>),
}

#[derive(Debug)]
struct RequestCandidateTerminalQueueRecord {
    record: UpsertRequestCandidateRecord,
    barrier: Arc<RequestCandidateTerminalBarrier>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestCandidateLifecycleLane {
    Active,
    Terminal,
}

#[derive(Debug)]
enum RequestCandidatePriorityEnqueueError {
    Full(UpsertRequestCandidateRecord),
    Closed(UpsertRequestCandidateRecord),
}

#[derive(Clone)]
pub(crate) struct RequestCandidateQueueRuntime {
    senders: Vec<mpsc::Sender<UpsertRequestCandidateRecord>>,
    active_senders: Vec<mpsc::Sender<RequestCandidateActiveQueueMessage>>,
    terminal_senders: Vec<mpsc::Sender<RequestCandidateTerminalQueueRecord>>,
    normal_admission: Arc<Semaphore>,
    priority_admission: Arc<Semaphore>,
    priority_capacity: usize,
    repository: Arc<dyn RequestCandidateWriteRepository>,
    config: RequestCandidateQueueConfig,
    metrics: Arc<RequestCandidateQueueMetrics>,
    db_write_gate: Option<Arc<RequestCandidateDbWriteGate>>,
}

impl std::fmt::Debug for RequestCandidateQueueRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RequestCandidateQueueRuntime")
            .field("config", &self.config)
            .field(
                "queued_current",
                &self.metrics.queued_current.load(Ordering::Acquire),
            )
            .field(
                "priority_queued_current",
                &self.metrics.priority_queued_current.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

impl RequestCandidateQueueRuntime {
    pub(crate) fn spawn(
        repository: Arc<dyn RequestCandidateWriteRepository>,
        mut config: RequestCandidateQueueConfig,
    ) -> Arc<Self> {
        config.workers = config.workers.min(config.capacity).max(1);
        config.batch_size = config.batch_size.max(1);
        config.db_batch_size = config.db_batch_size.clamp(1, MAX_DB_BATCH_SIZE);
        let mut senders = Vec::with_capacity(config.workers);
        let mut receivers = Vec::with_capacity(config.workers);
        let priority_capacity = priority_queue_capacity(config.capacity, config.workers);
        let mut active_senders = Vec::with_capacity(config.workers);
        let mut active_receivers = Vec::with_capacity(config.workers);
        let mut terminal_senders = Vec::with_capacity(config.workers);
        let mut terminal_receivers = Vec::with_capacity(config.workers);
        for worker_index in 0..config.workers {
            let capacity = worker_queue_capacity(config.capacity, config.workers, worker_index);
            let (sender, receiver) = mpsc::channel(capacity);
            senders.push(sender);
            receivers.push(receiver);
            let (active_sender, active_receiver) = mpsc::channel(capacity);
            active_senders.push(active_sender);
            active_receivers.push(active_receiver);
            let (terminal_sender, terminal_receiver) = mpsc::channel(capacity);
            terminal_senders.push(terminal_sender);
            terminal_receivers.push(terminal_receiver);
        }
        let db_write_gate = config
            .db_write_concurrency_limit
            .map(RequestCandidateDbWriteGate::new)
            .map(Arc::new);
        let runtime = Arc::new(Self {
            senders,
            active_senders,
            terminal_senders,
            normal_admission: Arc::new(Semaphore::new(config.capacity)),
            priority_admission: Arc::new(Semaphore::new(priority_capacity)),
            priority_capacity,
            repository,
            config,
            metrics: Arc::new(RequestCandidateQueueMetrics::default()),
            db_write_gate,
        });
        runtime.spawn_workers(receivers, active_receivers, terminal_receivers);
        runtime
    }

    pub(crate) async fn enqueue_or_fallback(
        &self,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), aether_data::DataLayerError> {
        let worker_index = self.worker_index_for(&record);
        if request_candidate_status_is_priority(record.status) {
            return self
                .enqueue_priority_or_fallback(worker_index, record)
                .await;
        }

        let Some(sender) = self.senders.get(worker_index) else {
            self.metrics
                .sync_fallback_total
                .fetch_add(1, Ordering::AcqRel);
            return self.repository.upsert(record).await.map(|_| ());
        };
        let admission = match self.normal_admission.try_acquire() {
            Ok(admission) => admission,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return self.handle_normal_queue_full(worker_index, record).await;
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                self.metrics
                    .sync_fallback_total
                    .fetch_add(1, Ordering::AcqRel);
                return self.repository.upsert(record).await.map(|_| ());
            }
        };
        self.metrics.queued_current.fetch_add(1, Ordering::AcqRel);
        self.metrics.pending_current.fetch_add(1, Ordering::AcqRel);
        match sender.try_send(record) {
            Ok(()) => {
                admission.forget();
                self.metrics.enqueued_total.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Err(mpsc::error::TrySendError::Full(record)) => {
                drop(admission);
                decrement_atomic_usize(&self.metrics.queued_current);
                decrement_atomic_usize(&self.metrics.pending_current);
                self.handle_normal_queue_full(worker_index, record).await
            }
            Err(mpsc::error::TrySendError::Closed(record)) => {
                drop(admission);
                decrement_atomic_usize(&self.metrics.queued_current);
                decrement_atomic_usize(&self.metrics.pending_current);
                self.metrics
                    .sync_fallback_total
                    .fetch_add(1, Ordering::AcqRel);
                self.repository.upsert(record).await.map(|_| ())
            }
        }
    }

    async fn handle_normal_queue_full(
        &self,
        worker_index: usize,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), aether_data::DataLayerError> {
        warn!(
            event_name = "request_candidate_queue_full",
            log_type = "event",
            full_policy = ?self.config.full_policy,
            priority = false,
            worker_index,
            queued = self.metrics.queued_current.load(Ordering::Acquire),
            capacity = self.config.capacity,
            "gateway request candidate async queue is full"
        );
        match self.config.full_policy {
            RequestCandidateQueueFullPolicy::Sync => {
                self.metrics
                    .sync_fallback_total
                    .fetch_add(1, Ordering::AcqRel);
                self.repository.upsert(record).await.map(|_| ())
            }
            RequestCandidateQueueFullPolicy::Drop => {
                self.metrics.dropped_total.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }
    }

    /// Enqueue a lifecycle status without yielding to the Tokio scheduler.
    ///
    /// The bounded priority lane keeps a synchronous fast path. A returned
    /// record must be handed to the async enqueue path, which waits for
    /// admission without dropping lifecycle state.
    pub(crate) fn try_enqueue_priority_status(
        &self,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), UpsertRequestCandidateRecord> {
        if !request_candidate_status_is_priority(record.status) {
            return Err(record);
        }
        let worker_index = self.worker_index_for(&record);
        match self.try_enqueue_priority(worker_index, record) {
            Ok(()) => Ok(()),
            Err(RequestCandidatePriorityEnqueueError::Full(record)) => {
                self.observe_priority_backpressure(worker_index);
                Err(record)
            }
            Err(RequestCandidatePriorityEnqueueError::Closed(record)) => Err(record),
        }
    }

    async fn enqueue_priority_or_fallback(
        &self,
        worker_index: usize,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), aether_data::DataLayerError> {
        let record = match self
            .enqueue_priority_with_backpressure(worker_index, record)
            .await
        {
            Ok(()) => return Ok(()),
            Err(record) => record,
        };
        self.metrics
            .sync_fallback_total
            .fetch_add(1, Ordering::AcqRel);
        self.repository.upsert(record).await.map(|_| ())
    }

    fn try_enqueue_priority(
        &self,
        worker_index: usize,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), RequestCandidatePriorityEnqueueError> {
        if request_candidate_status_is_active(record.status) {
            return self.try_enqueue_active(worker_index, record);
        }
        if request_candidate_status_is_terminal(record.status) {
            return self.try_enqueue_terminal(worker_index, record);
        }
        Err(RequestCandidatePriorityEnqueueError::Closed(record))
    }

    fn try_enqueue_active(
        &self,
        worker_index: usize,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), RequestCandidatePriorityEnqueueError> {
        let Some(sender) = self.active_senders.get(worker_index) else {
            return Err(RequestCandidatePriorityEnqueueError::Closed(record));
        };
        let admission = match self.priority_admission.try_acquire() {
            Ok(admission) => admission,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Err(RequestCandidatePriorityEnqueueError::Full(record));
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(RequestCandidatePriorityEnqueueError::Closed(record));
            }
        };
        let sender_permit = match sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(RequestCandidatePriorityEnqueueError::Full(record));
            }
            Err(_) => return Err(RequestCandidatePriorityEnqueueError::Closed(record)),
        };
        self.begin_lifecycle_enqueue(RequestCandidateLifecycleLane::Active);
        sender_permit.send(RequestCandidateActiveQueueMessage::Record(record));
        admission.forget();
        self.finish_lifecycle_enqueue();
        Ok(())
    }

    fn try_enqueue_terminal(
        &self,
        worker_index: usize,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), RequestCandidatePriorityEnqueueError> {
        let (Some(active_sender), Some(terminal_sender)) = (
            self.active_senders.get(worker_index),
            self.terminal_senders.get(worker_index),
        ) else {
            return Err(RequestCandidatePriorityEnqueueError::Closed(record));
        };
        let admission = match self.priority_admission.try_acquire() {
            Ok(admission) => admission,
            Err(tokio::sync::TryAcquireError::NoPermits) => {
                return Err(RequestCandidatePriorityEnqueueError::Full(record));
            }
            Err(tokio::sync::TryAcquireError::Closed) => {
                return Err(RequestCandidatePriorityEnqueueError::Closed(record));
            }
        };
        let active_permit = match active_sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(RequestCandidatePriorityEnqueueError::Full(record));
            }
            Err(_) => return Err(RequestCandidatePriorityEnqueueError::Closed(record)),
        };
        let terminal_permit = match terminal_sender.try_reserve() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(RequestCandidatePriorityEnqueueError::Full(record));
            }
            Err(_) => return Err(RequestCandidatePriorityEnqueueError::Closed(record)),
        };
        self.begin_lifecycle_enqueue(RequestCandidateLifecycleLane::Terminal);
        let barrier_pending = self
            .metrics
            .terminal_barrier_pending
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        self.metrics
            .terminal_barrier_max_pending
            .fetch_max(barrier_pending, Ordering::AcqRel);
        let barrier = Arc::new(RequestCandidateTerminalBarrier::new());
        active_permit.send(RequestCandidateActiveQueueMessage::Barrier(Arc::clone(
            &barrier,
        )));
        terminal_permit.send(RequestCandidateTerminalQueueRecord { record, barrier });
        admission.forget();
        self.finish_lifecycle_enqueue();
        Ok(())
    }

    async fn enqueue_priority_with_backpressure(
        &self,
        worker_index: usize,
        record: UpsertRequestCandidateRecord,
    ) -> Result<(), UpsertRequestCandidateRecord> {
        let (Some(active_sender), Some(terminal_sender)) = (
            self.active_senders.get(worker_index),
            self.terminal_senders.get(worker_index),
        ) else {
            return Err(record);
        };
        let lane = if request_candidate_status_is_active(record.status) {
            RequestCandidateLifecycleLane::Active
        } else if request_candidate_status_is_terminal(record.status) {
            RequestCandidateLifecycleLane::Terminal
        } else {
            return Err(record);
        };
        if self.priority_admission.available_permits() == 0
            || active_sender.capacity() == 0
            || (lane == RequestCandidateLifecycleLane::Terminal && terminal_sender.capacity() == 0)
        {
            self.observe_priority_backpressure(worker_index);
        }
        let active_permit = match active_sender.reserve().await {
            Ok(permit) => permit,
            Err(_) => return Err(record),
        };
        if lane == RequestCandidateLifecycleLane::Active {
            let admission = match Arc::clone(&self.priority_admission).acquire_owned().await {
                Ok(admission) => admission,
                Err(_) => return Err(record),
            };
            self.begin_lifecycle_enqueue(lane);
            active_permit.send(RequestCandidateActiveQueueMessage::Record(record));
            admission.forget();
            self.finish_lifecycle_enqueue();
            return Ok(());
        }

        let terminal_permit = match terminal_sender.reserve().await {
            Ok(permit) => permit,
            Err(_) => return Err(record),
        };
        let admission = match Arc::clone(&self.priority_admission).acquire_owned().await {
            Ok(admission) => admission,
            Err(_) => return Err(record),
        };
        self.begin_lifecycle_enqueue(lane);
        let barrier_pending = self
            .metrics
            .terminal_barrier_pending
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        self.metrics
            .terminal_barrier_max_pending
            .fetch_max(barrier_pending, Ordering::AcqRel);
        let barrier = Arc::new(RequestCandidateTerminalBarrier::new());
        active_permit.send(RequestCandidateActiveQueueMessage::Barrier(Arc::clone(
            &barrier,
        )));
        terminal_permit.send(RequestCandidateTerminalQueueRecord { record, barrier });
        admission.forget();
        self.finish_lifecycle_enqueue();
        Ok(())
    }

    fn begin_lifecycle_enqueue(&self, lane: RequestCandidateLifecycleLane) {
        self.metrics.queued_current.fetch_add(1, Ordering::AcqRel);
        self.metrics.pending_current.fetch_add(1, Ordering::AcqRel);
        let priority_queued = self
            .metrics
            .priority_queued_current
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        self.metrics
            .priority_max_queued
            .fetch_max(priority_queued, Ordering::AcqRel);
        self.metrics
            .priority_pending_current
            .fetch_add(1, Ordering::AcqRel);
        match lane {
            RequestCandidateLifecycleLane::Active => {
                let queued = self
                    .metrics
                    .active_queued_current
                    .fetch_add(1, Ordering::AcqRel)
                    + 1;
                self.metrics
                    .active_max_queued
                    .fetch_max(queued, Ordering::AcqRel);
                self.metrics
                    .active_pending_current
                    .fetch_add(1, Ordering::AcqRel);
            }
            RequestCandidateLifecycleLane::Terminal => {
                let queued = self
                    .metrics
                    .terminal_queued_current
                    .fetch_add(1, Ordering::AcqRel)
                    + 1;
                self.metrics
                    .terminal_max_queued
                    .fetch_max(queued, Ordering::AcqRel);
                self.metrics
                    .terminal_pending_current
                    .fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn finish_lifecycle_enqueue(&self) {
        self.metrics.enqueued_total.fetch_add(1, Ordering::AcqRel);
        self.metrics
            .priority_enqueued_total
            .fetch_add(1, Ordering::AcqRel);
    }

    fn observe_priority_backpressure(&self, worker_index: usize) {
        let backpressure_total = self
            .metrics
            .priority_async_overflow_total
            .fetch_add(1, Ordering::AcqRel)
            + 1;
        if should_log_queue_counter(backpressure_total) {
            warn!(
                event_name = "request_candidate_priority_queue_backpressure",
                log_type = "event",
                worker_index,
                queued = self.metrics.priority_queued_current.load(Ordering::Acquire),
                capacity = self.priority_capacity,
                backpressure_total,
                "gateway applied bounded lifecycle candidate queue backpressure"
            );
        }
    }

    pub(crate) fn pending_writes(&self) -> usize {
        [
            self.metrics.pending_current.load(Ordering::Acquire),
            self.metrics
                .priority_pending_current
                .load(Ordering::Acquire),
            self.metrics.active_pending_current.load(Ordering::Acquire),
            self.metrics
                .terminal_pending_current
                .load(Ordering::Acquire),
            self.metrics
                .terminal_barrier_pending
                .load(Ordering::Acquire),
        ]
        .into_iter()
        .fold(0_usize, usize::saturating_add)
    }

    pub(crate) fn metric_samples(&self) -> Vec<MetricSample> {
        vec![
            MetricSample::new(
                "request_candidate_queue_depth",
                "Current number of request candidate records waiting in the async persistence queue.",
                MetricKind::Gauge,
                self.metrics.queued_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_pending_depth",
                "Current number of request candidate records accepted into the async persistence queue but not yet flushed.",
                MetricKind::Gauge,
                self.metrics.pending_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_priority_queue_depth",
                "Current number of lifecycle request candidate records waiting in the priority persistence queue.",
                MetricKind::Gauge,
                self.metrics.priority_queued_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_priority_queue_pending_depth",
                "Current number of lifecycle request candidate records accepted into the priority persistence queue but not yet flushed.",
                MetricKind::Gauge,
                self.metrics.priority_pending_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_active_queue_depth",
                "Current number of pending or streaming request candidate records waiting in the active lifecycle lane.",
                MetricKind::Gauge,
                self.metrics.active_queued_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_active_queue_pending_depth",
                "Current number of active lifecycle records accepted but not yet flushed.",
                MetricKind::Gauge,
                self.metrics.active_pending_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_active_queue_max_depth",
                "Maximum observed active lifecycle backlog depth since process start.",
                MetricKind::Gauge,
                self.metrics.active_max_queued.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_terminal_queue_depth",
                "Current number of terminal request candidate records waiting in the terminal lifecycle lane.",
                MetricKind::Gauge,
                self.metrics.terminal_queued_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_terminal_queue_pending_depth",
                "Current number of terminal lifecycle records accepted but not yet flushed.",
                MetricKind::Gauge,
                self.metrics.terminal_pending_current.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_terminal_queue_max_depth",
                "Maximum observed terminal lifecycle backlog depth since process start.",
                MetricKind::Gauge,
                self.metrics.terminal_max_queued.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_terminal_barrier_pending",
                "Current number of terminal records waiting for earlier active lifecycle writes to commit.",
                MetricKind::Gauge,
                self.metrics
                    .terminal_barrier_pending
                    .load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_terminal_barrier_max_pending",
                "Maximum observed number of unresolved terminal ordering barriers since process start.",
                MetricKind::Gauge,
                self.metrics
                    .terminal_barrier_max_pending
                    .load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_capacity",
                "Configured request candidate async persistence queue capacity.",
                MetricKind::Gauge,
                self.config.capacity as u64,
            ),
            MetricSample::new(
                "request_candidate_priority_queue_capacity",
                "Configured hard admission limit shared by the active and terminal lifecycle queues.",
                MetricKind::Gauge,
                self.priority_capacity as u64,
            ),
            MetricSample::new(
                "request_candidate_priority_queue_soft_limit",
                "Compatibility alias for the bounded priority lifecycle admission limit.",
                MetricKind::Gauge,
                self.priority_capacity as u64,
            ),
            MetricSample::new(
                "request_candidate_priority_queue_max_depth",
                "Maximum observed priority lifecycle backlog depth since process start.",
                MetricKind::Gauge,
                self.metrics.priority_max_queued.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_priority_queue_over_limit_depth",
                "Current priority lifecycle backlog above its configured admission limit; expected to remain zero.",
                MetricKind::Gauge,
                self.metrics
                    .priority_queued_current
                    .load(Ordering::Acquire)
                    .saturating_sub(self.priority_capacity) as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_enqueued_total",
                "Total request candidate records accepted into the async persistence queue.",
                MetricKind::Counter,
                self.metrics.enqueued_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_priority_queue_enqueued_total",
                "Total lifecycle request candidate records accepted into the priority persistence queue.",
                MetricKind::Counter,
                self.metrics.priority_enqueued_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_priority_queue_async_overflow_total",
                "Total lifecycle enqueue attempts that encountered bounded queue backpressure.",
                MetricKind::Counter,
                self.metrics
                    .priority_async_overflow_total
                    .load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_dropped_total",
                "Total request candidate records dropped because the async persistence queue was full.",
                MetricKind::Counter,
                self.metrics.dropped_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_flushed_total",
                "Total request candidate records flushed by async persistence workers.",
                MetricKind::Counter,
                self.metrics.flushed_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_priority_queue_flushed_total",
                "Total lifecycle request candidate records flushed by priority persistence workers.",
                MetricKind::Counter,
                self.metrics.priority_flushed_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_flush_failed_total",
                "Total request candidate records that failed during async persistence flush.",
                MetricKind::Counter,
                self.metrics.flush_failed_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_permanent_dropped_total",
                "Total request candidate records dropped after isolation identified a non-retryable persistence error.",
                MetricKind::Counter,
                self.metrics.permanent_dropped_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_flush_batches_total",
                "Total async request candidate persistence flush batches.",
                MetricKind::Counter,
                self.metrics.flush_batches_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_flush_sql_ops_total",
                "Total repository batch upsert operations issued by async request candidate persistence workers after compaction.",
                MetricKind::Counter,
                self.metrics.flush_sql_ops_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_flush_sql_records_total",
                "Total request candidate records submitted to repository batch upsert operations after compaction.",
                MetricKind::Counter,
                self.metrics.flush_sql_records_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_db_batch_size",
                "Maximum request candidate records submitted in one async DB batch upsert after compaction.",
                MetricKind::Gauge,
                self.config.db_batch_size as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_db_write_concurrency_limit",
                "Maximum concurrent request candidate async DB write batches; zero means unlimited.",
                MetricKind::Gauge,
                self.config.db_write_concurrency_limit.unwrap_or_default() as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_db_write_in_flight",
                "Current request candidate async DB write batches in flight.",
                MetricKind::Gauge,
                self.metrics.db_write_in_flight.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_db_write_max_in_flight",
                "Maximum observed request candidate async DB write batches in flight.",
                MetricKind::Gauge,
                self.metrics.db_write_max_in_flight.load(Ordering::Acquire) as u64,
            ),
            MetricSample::new(
                "request_candidate_queue_db_write_wait_total",
                "Total request candidate async DB write batches that had to wait for the DB write gate.",
                MetricKind::Counter,
                self.metrics.db_write_wait_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_compacted_total",
                "Total request candidate records compacted before async persistence because a later queued record covered the same request candidate slot.",
                MetricKind::Counter,
                self.metrics.compacted_total.load(Ordering::Acquire),
            ),
            MetricSample::new(
                "request_candidate_queue_sync_fallback_total",
                "Total request candidate records synchronously persisted after async queue fallback.",
                MetricKind::Counter,
                self.metrics.sync_fallback_total.load(Ordering::Acquire),
            ),
        ]
    }

    fn spawn_workers(
        self: &Arc<Self>,
        receivers: Vec<mpsc::Receiver<UpsertRequestCandidateRecord>>,
        active_receivers: Vec<mpsc::Receiver<RequestCandidateActiveQueueMessage>>,
        terminal_receivers: Vec<mpsc::Receiver<RequestCandidateTerminalQueueRecord>>,
    ) {
        for (worker_index, ((receiver, active_receiver), terminal_receiver)) in receivers
            .into_iter()
            .zip(active_receivers)
            .zip(terminal_receivers)
            .enumerate()
        {
            let repository = Arc::clone(&self.repository);
            let config = self.config.clone();
            let metrics = Arc::clone(&self.metrics);
            let db_write_gate = self.db_write_gate.clone();
            let normal_admission = Arc::clone(&self.normal_admission);
            let priority_admission = Arc::clone(&self.priority_admission);
            spawn_on_request_candidate_background_runtime(async move {
                run_worker(
                    repository,
                    config,
                    metrics,
                    db_write_gate,
                    worker_index,
                    receiver,
                    active_receiver,
                    terminal_receiver,
                    normal_admission,
                    priority_admission,
                )
                .await;
            });
        }
    }

    fn worker_index_for(&self, record: &UpsertRequestCandidateRecord) -> usize {
        let worker_count = self.senders.len();
        if worker_count <= 1 {
            return 0;
        }
        (request_candidate_slot_hash(record) % worker_count as u64) as usize
    }
}

fn spawn_on_request_candidate_background_runtime<F>(task: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    request_candidate_background_runtime().handle().spawn(task)
}

fn request_candidate_background_runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<&'static tokio::runtime::Runtime> = OnceLock::new();

    RUNTIME.get_or_init(|| {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(request_candidate_background_runtime_threads())
            .thread_name(RUNTIME_THREAD_NAME)
            .thread_stack_size(RUNTIME_THREAD_STACK_BYTES)
            .build()
            .expect("request candidate background runtime should build");
        Box::leak(Box::new(runtime))
    })
}

fn request_candidate_background_runtime_threads() -> usize {
    parse_request_candidate_background_runtime_threads(
        std::env::var(RUNTIME_THREADS_ENV).ok().as_deref(),
    )
}

fn parse_request_candidate_background_runtime_threads(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or(DEFAULT_RUNTIME_THREADS)
        .clamp(1, MAX_RUNTIME_THREADS)
}

fn push_active_message(
    message: RequestCandidateActiveQueueMessage,
    batch: &mut Vec<UpsertRequestCandidateRecord>,
    barriers: &mut Vec<Arc<RequestCandidateTerminalBarrier>>,
    metrics: &RequestCandidateQueueMetrics,
) {
    match message {
        RequestCandidateActiveQueueMessage::Record(record) => {
            decrement_atomic_usize(&metrics.queued_current);
            decrement_atomic_usize(&metrics.priority_queued_current);
            decrement_atomic_usize(&metrics.active_queued_current);
            batch.push(record);
        }
        RequestCandidateActiveQueueMessage::Barrier(barrier) => barriers.push(barrier),
    }
}

/// Give active lifecycle events a very short coalescing window so a burst does
/// not turn into one database transaction per event. Terminal barriers share
/// this FIFO and are only released after the collected active records commit.
async fn collect_active_micro_batch(
    receiver: &mut mpsc::Receiver<RequestCandidateActiveQueueMessage>,
    batch: &mut Vec<UpsertRequestCandidateRecord>,
    barriers: &mut Vec<Arc<RequestCandidateTerminalBarrier>>,
    batch_size: usize,
    metrics: &RequestCandidateQueueMetrics,
    receiver_open: &mut bool,
) {
    if batch.len() >= batch_size.max(1) || !*receiver_open {
        return;
    }

    // Wait on the receiver for the bounded window instead of sleeping and
    // polling once. Polling makes the batch size depend on scheduler timing:
    // a sender that is ready during the window can still be missed by a
    // `try_recv` immediately after the sleep.
    let deadline = tokio::time::sleep(Duration::from_millis(PRIORITY_MICRO_BATCH_INTERVAL_MS));
    tokio::pin!(deadline);
    while batch.len() < batch_size.max(1) {
        tokio::select! {
            received = receiver.recv(), if *receiver_open => match received {
                Some(message) => push_active_message(message, batch, barriers, metrics),
                None => {
                    *receiver_open = false;
                    break;
                }
            },
            _ = &mut deadline => break,
        }
    }
}

fn collect_ready_terminal_batch(
    receiver: &mut mpsc::Receiver<RequestCandidateTerminalQueueRecord>,
    front: &mut Option<RequestCandidateTerminalQueueRecord>,
    batch: &mut Vec<UpsertRequestCandidateRecord>,
    batch_size: usize,
    metrics: &RequestCandidateQueueMetrics,
    receiver_open: &mut bool,
) {
    if !batch.is_empty() {
        return;
    }
    while batch.len() < batch_size.max(1) {
        if front.is_none() && *receiver_open {
            match receiver.try_recv() {
                Ok(record) => *front = Some(record),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    *receiver_open = false;
                    break;
                }
            }
        }
        let Some(record) = front.as_ref() else {
            break;
        };
        if !record.barrier.is_ready() {
            break;
        }
        let record = front.take().expect("ready terminal front should exist");
        decrement_atomic_usize(&metrics.queued_current);
        decrement_atomic_usize(&metrics.priority_queued_current);
        decrement_atomic_usize(&metrics.terminal_queued_current);
        batch.push(record.record);
    }
}

fn release_terminal_barriers(
    barriers: &mut Vec<Arc<RequestCandidateTerminalBarrier>>,
    metrics: &RequestCandidateQueueMetrics,
) {
    for barrier in barriers.drain(..) {
        barrier.release(metrics);
    }
}

fn collect_ready_normal_batch(
    receiver: &mut mpsc::Receiver<UpsertRequestCandidateRecord>,
    batch: &mut Vec<UpsertRequestCandidateRecord>,
    batch_size: usize,
    metrics: &RequestCandidateQueueMetrics,
    receiver_open: &mut bool,
) {
    while *receiver_open && batch.len() < batch_size.max(1) {
        match receiver.try_recv() {
            Ok(record) => {
                decrement_atomic_usize(&metrics.queued_current);
                batch.push(record);
            }
            Err(mpsc::error::TryRecvError::Empty) => break,
            Err(mpsc::error::TryRecvError::Disconnected) => {
                *receiver_open = false;
            }
        }
    }
}

async fn run_worker(
    repository: Arc<dyn RequestCandidateWriteRepository>,
    config: RequestCandidateQueueConfig,
    metrics: Arc<RequestCandidateQueueMetrics>,
    db_write_gate: Option<Arc<RequestCandidateDbWriteGate>>,
    worker_index: usize,
    mut receiver: mpsc::Receiver<UpsertRequestCandidateRecord>,
    mut active_receiver: mpsc::Receiver<RequestCandidateActiveQueueMessage>,
    mut terminal_receiver: mpsc::Receiver<RequestCandidateTerminalQueueRecord>,
    normal_admission: Arc<Semaphore>,
    priority_admission: Arc<Semaphore>,
) {
    let mut ticker = interval(config.flush_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut batch = Vec::with_capacity(config.batch_size);
    let mut active_batch = Vec::with_capacity(config.batch_size);
    let mut active_barriers = Vec::new();
    let mut terminal_batch = Vec::with_capacity(config.batch_size);
    let mut terminal_front: Option<RequestCandidateTerminalQueueRecord> = None;
    let mut receiver_open = true;
    let mut active_receiver_open = true;
    let mut terminal_receiver_open = true;
    let mut consecutive_active_flushes = 0_usize;
    let mut consecutive_priority_flushes = 0_usize;

    while receiver_open
        || active_receiver_open
        || terminal_receiver_open
        || !batch.is_empty()
        || !active_batch.is_empty()
        || !active_barriers.is_empty()
        || !terminal_batch.is_empty()
        || terminal_front.is_some()
    {
        if consecutive_priority_flushes >= MAX_CONSECUTIVE_PRIORITY_FLUSHES {
            collect_ready_normal_batch(
                &mut receiver,
                &mut batch,
                config.batch_size,
                &metrics,
                &mut receiver_open,
            );
            if !batch.is_empty() {
                flush_batch(
                    &repository,
                    &config,
                    &metrics,
                    db_write_gate.as_ref(),
                    worker_index,
                    RequestCandidateQueueLane::Normal,
                    &mut batch,
                    Some(&normal_admission),
                )
                .await;
                consecutive_priority_flushes = 0;
                continue;
            }
        }

        // A terminal record can already be parked at the FIFO front while its
        // active barrier is unresolved. Once that barrier is released, the
        // terminal receiver cannot wake this worker because the record has
        // already been received. Flush it here when no active work is waiting;
        // sustained active traffic still uses the 2:1 fairness path below.
        if terminal_batch.is_empty()
            && active_batch.is_empty()
            && active_receiver.is_empty()
            && terminal_front
                .as_ref()
                .is_some_and(|record| record.barrier.is_ready())
        {
            collect_ready_terminal_batch(
                &mut terminal_receiver,
                &mut terminal_front,
                &mut terminal_batch,
                config.batch_size,
                &metrics,
                &mut terminal_receiver_open,
            );
            if !terminal_batch.is_empty() {
                flush_batch(
                    &repository,
                    &config,
                    &metrics,
                    db_write_gate.as_ref(),
                    worker_index,
                    RequestCandidateQueueLane::Terminal,
                    &mut terminal_batch,
                    Some(&priority_admission),
                )
                .await;
                consecutive_active_flushes = 0;
                consecutive_priority_flushes = consecutive_priority_flushes.saturating_add(1);
                continue;
            }
        }

        if consecutive_active_flushes >= MAX_CONSECUTIVE_ACTIVE_FLUSHES {
            collect_ready_terminal_batch(
                &mut terminal_receiver,
                &mut terminal_front,
                &mut terminal_batch,
                config.batch_size,
                &metrics,
                &mut terminal_receiver_open,
            );
            if !terminal_batch.is_empty() {
                flush_batch(
                    &repository,
                    &config,
                    &metrics,
                    db_write_gate.as_ref(),
                    worker_index,
                    RequestCandidateQueueLane::Terminal,
                    &mut terminal_batch,
                    Some(&priority_admission),
                )
                .await;
                consecutive_active_flushes = 0;
                consecutive_priority_flushes = consecutive_priority_flushes.saturating_add(1);
                continue;
            }
        }

        let retry_deadline = request_candidate_next_retry_deadline(&metrics, worker_index);
        let scheduled_flush = async {
            if let Some(deadline) = retry_deadline {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = tokio::time::sleep_until(deadline) => {}
                }
            } else {
                ticker.tick().await;
            }
        };
        tokio::pin!(scheduled_flush);

        tokio::select! {
            biased;
            received = active_receiver.recv(), if active_receiver_open => {
                match received {
                    Some(message) => {
                        push_active_message(
                            message,
                            &mut active_batch,
                            &mut active_barriers,
                            &metrics,
                        );
                        collect_active_micro_batch(
                            &mut active_receiver,
                            &mut active_batch,
                            &mut active_barriers,
                            config.batch_size,
                            &metrics,
                            &mut active_receiver_open,
                        )
                        .await;
                        let had_active_records = !active_batch.is_empty();
                        if had_active_records {
                            flush_batch(
                                &repository,
                                &config,
                                &metrics,
                                db_write_gate.as_ref(),
                                worker_index,
                                RequestCandidateQueueLane::Active,
                                &mut active_batch,
                                Some(&priority_admission),
                            ).await;
                        }
                        if active_batch.is_empty() {
                            release_terminal_barriers(&mut active_barriers, &metrics);
                        }
                        // Barriers are active-lane work too. Counting only DB
                        // flushes lets a continuous barrier stream win the
                        // biased select forever and starve its own terminal
                        // records.
                        consecutive_active_flushes =
                            consecutive_active_flushes.saturating_add(1);
                        consecutive_priority_flushes =
                            consecutive_priority_flushes.saturating_add(1);
                    }
                    None => {
                        active_receiver_open = false;
                        if !active_batch.is_empty() {
                            flush_batch(
                                &repository,
                                &config,
                                &metrics,
                                db_write_gate.as_ref(),
                                worker_index,
                                RequestCandidateQueueLane::Active,
                                &mut active_batch,
                                Some(&priority_admission),
                            ).await;
                        }
                        if active_batch.is_empty() {
                            release_terminal_barriers(&mut active_barriers, &metrics);
                        }
                    }
                }
            }
            _ = &mut scheduled_flush => {
                if !active_batch.is_empty() {
                    flush_batch(
                        &repository,
                        &config,
                        &metrics,
                        db_write_gate.as_ref(),
                        worker_index,
                        RequestCandidateQueueLane::Active,
                        &mut active_batch,
                        Some(&priority_admission),
                    ).await;
                }
                if active_batch.is_empty() {
                    release_terminal_barriers(&mut active_barriers, &metrics);
                }
                collect_ready_terminal_batch(
                    &mut terminal_receiver,
                    &mut terminal_front,
                    &mut terminal_batch,
                    config.batch_size,
                    &metrics,
                    &mut terminal_receiver_open,
                );
                if !terminal_batch.is_empty() {
                    flush_batch(
                        &repository,
                        &config,
                        &metrics,
                        db_write_gate.as_ref(),
                        worker_index,
                        RequestCandidateQueueLane::Terminal,
                        &mut terminal_batch,
                        Some(&priority_admission),
                    ).await;
                }
                consecutive_priority_flushes = 0;
                consecutive_active_flushes = 0;
                if !batch.is_empty() {
                    flush_batch(
                        &repository,
                        &config,
                        &metrics,
                        db_write_gate.as_ref(),
                        worker_index,
                        RequestCandidateQueueLane::Normal,
                        &mut batch,
                        Some(&normal_admission),
                    ).await;
                }
            }
            received = terminal_receiver.recv(), if terminal_receiver_open && terminal_front.is_none() => {
                match received {
                    Some(record) => {
                        terminal_front = Some(record);
                        collect_ready_terminal_batch(
                            &mut terminal_receiver,
                            &mut terminal_front,
                            &mut terminal_batch,
                            config.batch_size,
                            &metrics,
                            &mut terminal_receiver_open,
                        );
                        if !terminal_batch.is_empty() {
                            flush_batch(
                                &repository,
                                &config,
                                &metrics,
                                db_write_gate.as_ref(),
                                worker_index,
                                RequestCandidateQueueLane::Terminal,
                                &mut terminal_batch,
                                Some(&priority_admission),
                            ).await;
                            consecutive_active_flushes = 0;
                            consecutive_priority_flushes =
                                consecutive_priority_flushes.saturating_add(1);
                        }
                    }
                    None => terminal_receiver_open = false,
                }
            }
            received = receiver.recv(), if receiver_open => {
                match received {
                    Some(record) => {
                        consecutive_priority_flushes = 0;
                        decrement_atomic_usize(&metrics.queued_current);
                        batch.push(record);
                        if batch.len() >= config.batch_size {
                            flush_batch(
                                &repository,
                                &config,
                                &metrics,
                                db_write_gate.as_ref(),
                                worker_index,
                                RequestCandidateQueueLane::Normal,
                                &mut batch,
                                Some(&normal_admission),
                            ).await;
                        }
                    }
                    None => {
                        receiver_open = false;
                        if !batch.is_empty() {
                            flush_batch(
                                &repository,
                                &config,
                                &metrics,
                                db_write_gate.as_ref(),
                                worker_index,
                                RequestCandidateQueueLane::Normal,
                                &mut batch,
                                Some(&normal_admission),
                            ).await;
                        }
                    }
                }
            }
        }
    }

    if !active_batch.is_empty() {
        flush_batch(
            &repository,
            &config,
            &metrics,
            db_write_gate.as_ref(),
            worker_index,
            RequestCandidateQueueLane::Active,
            &mut active_batch,
            Some(&priority_admission),
        )
        .await;
    }
    if active_batch.is_empty() {
        release_terminal_barriers(&mut active_barriers, &metrics);
    }
    collect_ready_terminal_batch(
        &mut terminal_receiver,
        &mut terminal_front,
        &mut terminal_batch,
        config.batch_size,
        &metrics,
        &mut terminal_receiver_open,
    );
    if !terminal_batch.is_empty() {
        flush_batch(
            &repository,
            &config,
            &metrics,
            db_write_gate.as_ref(),
            worker_index,
            RequestCandidateQueueLane::Terminal,
            &mut terminal_batch,
            Some(&priority_admission),
        )
        .await;
    }
    if !batch.is_empty() {
        flush_batch(
            &repository,
            &config,
            &metrics,
            db_write_gate.as_ref(),
            worker_index,
            RequestCandidateQueueLane::Normal,
            &mut batch,
            Some(&normal_admission),
        )
        .await;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RequestCandidateQueueLane {
    Normal,
    Active,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestCandidateWriteErrorDisposition {
    Retry,
    IsolateRecord,
    DropBatch,
}

fn request_candidate_write_error_disposition(
    error: &aether_data_contracts::DataLayerError,
) -> RequestCandidateWriteErrorDisposition {
    use aether_data_contracts::DataLayerError;

    match error {
        DataLayerError::InvalidConfiguration(_) => RequestCandidateWriteErrorDisposition::DropBatch,
        DataLayerError::InvalidInput(_) | DataLayerError::UnexpectedValue(_) => {
            RequestCandidateWriteErrorDisposition::IsolateRecord
        }
        DataLayerError::Postgres(message) | DataLayerError::Sql(message) => {
            database_error_disposition(message)
                .unwrap_or(RequestCandidateWriteErrorDisposition::Retry)
        }
        DataLayerError::Redis(_) | DataLayerError::TimedOut(_) => {
            RequestCandidateWriteErrorDisposition::Retry
        }
    }
}

fn database_error_disposition(message: &str) -> Option<RequestCandidateWriteErrorDisposition> {
    if let Some(code) = database_error_sqlstate(message) {
        if code.starts_with("21")
            || code.starts_with("22")
            || code.starts_with("23")
            || code.starts_with("44")
        {
            return Some(RequestCandidateWriteErrorDisposition::IsolateRecord);
        }
        if code.starts_with("0A")
            || code.starts_with("28")
            || code.starts_with("3D")
            || code.starts_with("3F")
            || code.starts_with("42")
        {
            return Some(RequestCandidateWriteErrorDisposition::DropBatch);
        }
    }
    (message.contains("unsupported Unicode escape sequence")
        || message.contains("\\u0000 cannot be converted to text")
        || message.contains("invalid byte sequence for encoding \"UTF8\": 0x00")
        || message.contains("violates foreign key constraint"))
    .then_some(RequestCandidateWriteErrorDisposition::IsolateRecord)
}

fn database_error_sqlstate(message: &str) -> Option<&str> {
    let suffix = message.strip_suffix(')')?;
    let (_, code) = suffix.rsplit_once("(SQLSTATE ")?;
    (code.len() == 5
        && code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit()))
    .then_some(code)
}

fn request_candidate_write_error_kind(
    error: &aether_data_contracts::DataLayerError,
) -> &'static str {
    use aether_data_contracts::DataLayerError;

    match error {
        DataLayerError::InvalidConfiguration(_) => "invalid_configuration",
        DataLayerError::InvalidInput(_) => "invalid_input",
        DataLayerError::UnexpectedValue(_) => "unexpected_value",
        DataLayerError::Postgres(_) => "postgres",
        DataLayerError::Sql(_) => "sql",
        DataLayerError::Redis(_) => "redis",
        DataLayerError::TimedOut(_) => "timed_out",
    }
}

fn request_candidate_write_error_sqlstate(error: &aether_data_contracts::DataLayerError) -> &str {
    use aether_data_contracts::DataLayerError;

    match error {
        DataLayerError::Postgres(message) | DataLayerError::Sql(message) => {
            database_error_sqlstate(message).unwrap_or("")
        }
        _ => "",
    }
}

fn request_candidate_retry_delay(
    metrics: &RequestCandidateQueueMetrics,
    worker_index: usize,
    lane: RequestCandidateQueueLane,
) -> Duration {
    let mut states = metrics
        .retry_states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let attempt = states
        .get(&(worker_index, lane))
        .map(|state| state.attempt)
        .unwrap_or_default()
        .saturating_add(1);
    let exponent = attempt.saturating_sub(1).min(16);
    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    let delay_ceiling = FAILED_FLUSH_RETRY_BASE_DELAY_MS
        .saturating_mul(multiplier)
        .min(FAILED_FLUSH_RETRY_MAX_DELAY_MS);
    let minimum_delay = delay_ceiling / 2;
    let jitter_window = delay_ceiling.saturating_sub(minimum_delay);
    let jitter_seed = request_candidate_retry_jitter_salt()
        ^ (worker_index as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ u64::from(attempt)
        ^ (lane as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    let delay = Duration::from_millis(
        minimum_delay.saturating_add(jitter_seed % jitter_window.saturating_add(1)),
    );
    states.insert(
        (worker_index, lane),
        RequestCandidateRetryState {
            attempt,
            retry_not_before: Instant::now() + delay,
        },
    );
    delay
}

fn request_candidate_retry_is_ready(
    metrics: &RequestCandidateQueueMetrics,
    worker_index: usize,
    lane: RequestCandidateQueueLane,
) -> bool {
    metrics
        .retry_states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(&(worker_index, lane))
        .is_none_or(|state| Instant::now() >= state.retry_not_before)
}

fn request_candidate_next_retry_deadline(
    metrics: &RequestCandidateQueueMetrics,
    worker_index: usize,
) -> Option<Instant> {
    metrics
        .retry_states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .iter()
        .filter_map(|((state_worker_index, _), state)| {
            (*state_worker_index == worker_index).then_some(state.retry_not_before)
        })
        .min()
}

fn request_candidate_retry_jitter_salt() -> u64 {
    static SALT: OnceLock<u64> = OnceLock::new();
    *SALT.get_or_init(|| {
        let random_state = RandomState::new();
        let mut hasher = random_state.build_hasher();
        hasher.write_u32(std::process::id());
        hasher.finish()
    })
}

fn reset_request_candidate_retry_delay(
    metrics: &RequestCandidateQueueMetrics,
    worker_index: usize,
    lane: RequestCandidateQueueLane,
) {
    metrics
        .retry_states
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .remove(&(worker_index, lane));
}

async fn flush_batch(
    repository: &Arc<dyn RequestCandidateWriteRepository>,
    config: &RequestCandidateQueueConfig,
    metrics: &RequestCandidateQueueMetrics,
    db_write_gate: Option<&Arc<RequestCandidateDbWriteGate>>,
    worker_index: usize,
    lane: RequestCandidateQueueLane,
    batch: &mut Vec<UpsertRequestCandidateRecord>,
    admission: Option<&Semaphore>,
) {
    if batch.is_empty() || !request_candidate_retry_is_ready(metrics, worker_index, lane) {
        return;
    }
    let records = std::mem::take(batch);
    let source_count = records.len();
    let flush_plan = compact_records_for_flush(records);
    let compacted = source_count.saturating_sub(flush_plan.record_count());
    if compacted > 0 {
        metrics
            .compacted_total
            .fetch_add(compacted as u64, Ordering::AcqRel);
    }
    metrics.flush_batches_total.fetch_add(1, Ordering::AcqRel);
    let mut retry_normal = Vec::new();
    let mut retry_pending = Vec::new();
    let mut retry_streaming = Vec::new();
    let mut retry_terminal = Vec::new();
    let mut blocked_slots = HashSet::new();
    let mut retry_failed = 0_u64;
    let mut deferred_source_count = 0_u64;
    let mut permanent_dropped = 0_u64;

    let normal_result = flush_ordered_stage(
        repository,
        config,
        metrics,
        db_write_gate,
        worker_index,
        lane,
        false,
        flush_plan.normal,
        &blocked_slots,
        &mut retry_normal,
    )
    .await;
    retry_failed = retry_failed.saturating_add(normal_result.retry_source_count);
    deferred_source_count =
        deferred_source_count.saturating_add(normal_result.deferred_source_count);
    permanent_dropped =
        permanent_dropped.saturating_add(normal_result.permanent_dropped_source_count);
    blocked_slots.extend(normal_result.failed_slots);
    let mut abort_batch = normal_result.abort_batch;

    let pending_result = flush_ordered_stage(
        repository,
        config,
        metrics,
        db_write_gate,
        worker_index,
        lane,
        abort_batch,
        flush_plan.pending,
        &blocked_slots,
        &mut retry_pending,
    )
    .await;
    retry_failed = retry_failed.saturating_add(pending_result.retry_source_count);
    deferred_source_count =
        deferred_source_count.saturating_add(pending_result.deferred_source_count);
    permanent_dropped =
        permanent_dropped.saturating_add(pending_result.permanent_dropped_source_count);
    blocked_slots.extend(pending_result.failed_slots);
    abort_batch |= pending_result.abort_batch;

    let streaming_result = flush_ordered_stage(
        repository,
        config,
        metrics,
        db_write_gate,
        worker_index,
        lane,
        abort_batch,
        flush_plan.streaming,
        &blocked_slots,
        &mut retry_streaming,
    )
    .await;
    retry_failed = retry_failed.saturating_add(streaming_result.retry_source_count);
    deferred_source_count =
        deferred_source_count.saturating_add(streaming_result.deferred_source_count);
    permanent_dropped =
        permanent_dropped.saturating_add(streaming_result.permanent_dropped_source_count);
    blocked_slots.extend(streaming_result.failed_slots);
    abort_batch |= streaming_result.abort_batch;

    let terminal_result = flush_ordered_stage(
        repository,
        config,
        metrics,
        db_write_gate,
        worker_index,
        lane,
        abort_batch,
        flush_plan.terminal,
        &blocked_slots,
        &mut retry_terminal,
    )
    .await;
    retry_failed = retry_failed.saturating_add(terminal_result.retry_source_count);
    deferred_source_count =
        deferred_source_count.saturating_add(terminal_result.deferred_source_count);
    permanent_dropped =
        permanent_dropped.saturating_add(terminal_result.permanent_dropped_source_count);
    abort_batch |= terminal_result.abort_batch;

    if abort_batch {
        let retry_record_count = retry_normal
            .len()
            .saturating_add(retry_pending.len())
            .saturating_add(retry_streaming.len())
            .saturating_add(retry_terminal.len());
        if retry_record_count > 0 {
            decrement_atomic_usize_by(&metrics.pending_current, retry_record_count);
            decrement_lifecycle_pending_by(metrics, lane, retry_record_count);
            metrics
                .permanent_dropped_total
                .fetch_add(deferred_source_count, Ordering::AcqRel);
            permanent_dropped = permanent_dropped.saturating_add(deferred_source_count);
            retry_failed = 0;
            retry_normal.clear();
            retry_pending.clear();
            retry_streaming.clear();
            retry_terminal.clear();
        }
    }

    let failed = retry_failed.saturating_add(permanent_dropped);
    if failed > 0 {
        metrics
            .flush_failed_total
            .fetch_add(failed, Ordering::AcqRel);
    }
    if retry_failed > 0 {
        batch.extend(retry_normal);
        batch.extend(retry_pending);
        batch.extend(retry_streaming);
        batch.extend(retry_terminal);
        let delay = request_candidate_retry_delay(metrics, worker_index, lane);
        warn!(
            event_name = "request_candidate_async_flush_retry_scheduled",
            log_type = "event",
            worker_index,
            lane = ?lane,
            retry_failed,
            retry_delay_ms = delay.as_millis() as u64,
            "gateway scheduled request candidate persistence retry with backoff"
        );
    } else {
        reset_request_candidate_retry_delay(metrics, worker_index, lane);
    }
    let released = source_count.saturating_sub(batch.len());
    if released > 0 {
        if let Some(admission) = admission {
            admission.add_permits(released);
        }
    }
    debug!(
        event_name = "request_candidate_async_flush_completed",
        log_type = "event",
        worker_index,
        lane = ?lane,
        failed,
        retry_failed,
        permanent_dropped,
        "gateway completed request candidate async flush batch"
    );
}

#[allow(clippy::too_many_arguments)]
async fn flush_ordered_stage(
    repository: &Arc<dyn RequestCandidateWriteRepository>,
    config: &RequestCandidateQueueConfig,
    metrics: &RequestCandidateQueueMetrics,
    db_write_gate: Option<&Arc<RequestCandidateDbWriteGate>>,
    worker_index: usize,
    lane: RequestCandidateQueueLane,
    abort_batch: bool,
    records: Vec<CompactedRequestCandidateRecord>,
    blocked_slots: &HashSet<RequestCandidateSlot>,
    retry_records: &mut Vec<UpsertRequestCandidateRecord>,
) -> FlushCompactedRecordsResult {
    if abort_batch {
        return drop_compacted_records_without_write(metrics, lane, records);
    }
    let retry_records_start = retry_records.len();
    let mut blocked_source_count = 0_u64;
    let mut ready_records = Vec::with_capacity(records.len());
    for record in records {
        if blocked_slots.contains(&request_candidate_slot(&record.record)) {
            blocked_source_count = blocked_source_count.saturating_add(record.source_count as u64);
            compact_retry_record(metrics, lane, &record);
            retry_records.push(record.record);
        } else {
            ready_records.push(record);
        }
    }
    let mut result = flush_compacted_records(
        repository,
        config,
        metrics,
        db_write_gate,
        worker_index,
        lane,
        ready_records,
        retry_records,
    )
    .await;
    if result.abort_batch {
        let blocked_record_count = retry_records.len().saturating_sub(retry_records_start);
        retry_records.truncate(retry_records_start);
        decrement_atomic_usize_by(&metrics.pending_current, blocked_record_count);
        decrement_lifecycle_pending_by(metrics, lane, blocked_record_count);
        metrics
            .permanent_dropped_total
            .fetch_add(blocked_source_count, Ordering::AcqRel);
        result.permanent_dropped_source_count = result
            .permanent_dropped_source_count
            .saturating_add(blocked_source_count);
    } else {
        result.deferred_source_count = result
            .deferred_source_count
            .saturating_add(blocked_source_count);
    }
    result
}

#[derive(Debug, Default)]
struct FlushCompactedRecordsResult {
    retry_source_count: u64,
    deferred_source_count: u64,
    permanent_dropped_source_count: u64,
    failed_slots: HashSet<RequestCandidateSlot>,
    abort_batch: bool,
}

fn drop_compacted_records_without_write(
    metrics: &RequestCandidateQueueMetrics,
    lane: RequestCandidateQueueLane,
    records: Vec<CompactedRequestCandidateRecord>,
) -> FlushCompactedRecordsResult {
    let source_count = records
        .iter()
        .map(|record| record.source_count)
        .sum::<usize>();
    if source_count > 0 {
        metrics
            .permanent_dropped_total
            .fetch_add(source_count as u64, Ordering::AcqRel);
        decrement_atomic_usize_by(&metrics.pending_current, source_count);
        decrement_lifecycle_pending_by(metrics, lane, source_count);
    }
    FlushCompactedRecordsResult {
        permanent_dropped_source_count: source_count as u64,
        abort_batch: true,
        ..FlushCompactedRecordsResult::default()
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_compacted_records(
    repository: &Arc<dyn RequestCandidateWriteRepository>,
    config: &RequestCandidateQueueConfig,
    metrics: &RequestCandidateQueueMetrics,
    db_write_gate: Option<&Arc<RequestCandidateDbWriteGate>>,
    worker_index: usize,
    lane: RequestCandidateQueueLane,
    records: Vec<CompactedRequestCandidateRecord>,
    retry_records: &mut Vec<UpsertRequestCandidateRecord>,
) -> FlushCompactedRecordsResult {
    let mut result = FlushCompactedRecordsResult::default();
    let total_source_count = records
        .iter()
        .map(|record| record.source_count)
        .sum::<usize>();
    let retry_records_start = retry_records.len();
    let mut resolved_source_count = 0usize;
    for chunk in records.chunks(config.db_batch_size.max(1)) {
        let mut attempts = vec![chunk.to_vec()];
        while let Some(mut attempt) = attempts.pop() {
            let source_count = attempt
                .iter()
                .map(|record| record.source_count)
                .sum::<usize>();
            let record_count = attempt.len();
            let upsert_records = attempt
                .iter()
                .map(|record| record.record.clone())
                .collect::<Vec<_>>();
            debug_assert!(request_candidate_slots_are_unique(&upsert_records));
            metrics.flush_sql_ops_total.fetch_add(1, Ordering::AcqRel);
            metrics
                .flush_sql_records_total
                .fetch_add(record_count as u64, Ordering::AcqRel);
            let _db_write_permit = match db_write_gate {
                Some(gate) => Some(gate.acquire(metrics).await),
                None => None,
            };
            let _db_write_in_flight = RequestCandidateDbWriteInFlightGuard::new(metrics);
            match repository.upsert_many(upsert_records).await {
                Ok(_) => {
                    resolved_source_count = resolved_source_count.saturating_add(source_count);
                    metrics
                        .flushed_total
                        .fetch_add(source_count as u64, Ordering::AcqRel);
                    decrement_atomic_usize_by(&metrics.pending_current, source_count);
                    if lane != RequestCandidateQueueLane::Normal {
                        metrics
                            .priority_flushed_total
                            .fetch_add(source_count as u64, Ordering::AcqRel);
                    }
                    decrement_lifecycle_pending_by(metrics, lane, source_count);
                }
                Err(error) => match request_candidate_write_error_disposition(&error) {
                    RequestCandidateWriteErrorDisposition::Retry => {
                        resolved_source_count = resolved_source_count.saturating_add(source_count);
                        result.retry_source_count = result
                            .retry_source_count
                            .saturating_add(source_count as u64);
                        result.deferred_source_count = result
                            .deferred_source_count
                            .saturating_add(source_count as u64);
                        result.failed_slots.extend(
                            attempt
                                .iter()
                                .map(|record| request_candidate_slot(&record.record)),
                        );
                        let compacted = source_count.saturating_sub(record_count);
                        decrement_atomic_usize_by(&metrics.pending_current, compacted);
                        decrement_lifecycle_pending_by(metrics, lane, compacted);
                        warn!(
                            event_name = "request_candidate_async_flush_failed",
                            log_type = "event",
                            worker_index,
                            lane = ?lane,
                            record_count,
                            source_count,
                            error_kind = request_candidate_write_error_kind(&error),
                            sqlstate = request_candidate_write_error_sqlstate(&error),
                            "gateway failed to asynchronously persist request candidate DB batch"
                        );
                        retry_records.extend(attempt.into_iter().map(|record| record.record));
                    }
                    RequestCandidateWriteErrorDisposition::IsolateRecord if attempt.len() > 1 => {
                        let right = attempt.split_off(attempt.len() / 2);
                        attempts.push(right);
                        attempts.push(attempt);
                    }
                    RequestCandidateWriteErrorDisposition::IsolateRecord => {
                        let record = attempt
                            .pop()
                            .expect("non-empty isolated candidate persistence attempt");
                        resolved_source_count =
                            resolved_source_count.saturating_add(record.source_count);
                        result.permanent_dropped_source_count = result
                            .permanent_dropped_source_count
                            .saturating_add(record.source_count as u64);
                        metrics
                            .permanent_dropped_total
                            .fetch_add(record.source_count as u64, Ordering::AcqRel);
                        decrement_atomic_usize_by(&metrics.pending_current, record.source_count);
                        decrement_lifecycle_pending_by(metrics, lane, record.source_count);
                        warn!(
                            event_name = "request_candidate_async_permanent_write_dropped",
                            log_type = "event",
                            worker_index,
                            lane = ?lane,
                            candidate_index = record.record.candidate_index,
                            retry_index = record.record.retry_index,
                            status = ?record.record.status,
                            source_count = record.source_count,
                            error_kind = request_candidate_write_error_kind(&error),
                            sqlstate = request_candidate_write_error_sqlstate(&error),
                            "gateway dropped one non-retryable request candidate persistence record"
                        );
                    }
                    RequestCandidateWriteErrorDisposition::DropBatch => {
                        let unresolved_source_count =
                            total_source_count.saturating_sub(resolved_source_count);
                        let prior_retry_record_count =
                            retry_records.len().saturating_sub(retry_records_start);
                        let prior_deferred_source_count = result.deferred_source_count;
                        retry_records.truncate(retry_records_start);
                        decrement_atomic_usize_by(
                            &metrics.pending_current,
                            unresolved_source_count.saturating_add(prior_retry_record_count),
                        );
                        decrement_lifecycle_pending_by(
                            metrics,
                            lane,
                            unresolved_source_count.saturating_add(prior_retry_record_count),
                        );
                        let dropped_source_count = (unresolved_source_count as u64)
                            .saturating_add(prior_deferred_source_count);
                        result.permanent_dropped_source_count = result
                            .permanent_dropped_source_count
                            .saturating_add(dropped_source_count);
                        result.retry_source_count = 0;
                        result.deferred_source_count = 0;
                        result.failed_slots.clear();
                        result.abort_batch = true;
                        metrics
                            .permanent_dropped_total
                            .fetch_add(dropped_source_count, Ordering::AcqRel);
                        warn!(
                            event_name = "request_candidate_async_permanent_batch_dropped",
                            log_type = "event",
                            worker_index,
                            lane = ?lane,
                            record_count = records.len(),
                            source_count = dropped_source_count,
                            error_kind = request_candidate_write_error_kind(&error),
                            sqlstate = request_candidate_write_error_sqlstate(&error),
                            "gateway dropped a request candidate batch after a non-retryable global persistence error"
                        );
                        return result;
                    }
                },
            }
        }
    }
    result
}

fn compact_retry_record(
    metrics: &RequestCandidateQueueMetrics,
    lane: RequestCandidateQueueLane,
    record: &CompactedRequestCandidateRecord,
) {
    let compacted = record.source_count.saturating_sub(1);
    decrement_atomic_usize_by(&metrics.pending_current, compacted);
    decrement_lifecycle_pending_by(metrics, lane, compacted);
}

fn decrement_lifecycle_pending_by(
    metrics: &RequestCandidateQueueMetrics,
    lane: RequestCandidateQueueLane,
    count: usize,
) {
    if lane == RequestCandidateQueueLane::Normal {
        return;
    }
    decrement_atomic_usize_by(&metrics.priority_pending_current, count);
    match lane {
        RequestCandidateQueueLane::Active => {
            decrement_atomic_usize_by(&metrics.active_pending_current, count);
        }
        RequestCandidateQueueLane::Terminal => {
            decrement_atomic_usize_by(&metrics.terminal_pending_current, count);
        }
        RequestCandidateQueueLane::Normal => {}
    }
}

#[derive(Debug, Clone)]
struct CompactedRequestCandidateRecord {
    record: UpsertRequestCandidateRecord,
    source_count: usize,
}

type RequestCandidateSlot = (String, u32, u32);

#[derive(Debug, Default)]
struct CompactedRequestCandidateFlushPlan {
    normal: Vec<CompactedRequestCandidateRecord>,
    pending: Vec<CompactedRequestCandidateRecord>,
    streaming: Vec<CompactedRequestCandidateRecord>,
    terminal: Vec<CompactedRequestCandidateRecord>,
}

impl CompactedRequestCandidateFlushPlan {
    fn record_count(&self) -> usize {
        self.normal
            .len()
            .saturating_add(self.pending.len())
            .saturating_add(self.streaming.len())
            .saturating_add(self.terminal.len())
    }
}

#[derive(Debug, Default)]
struct RequestCandidateSlotCompaction {
    normal: Option<CompactedRequestCandidateRecord>,
    pending: Option<CompactedRequestCandidateRecord>,
    streaming: Option<CompactedRequestCandidateRecord>,
    terminal: Option<CompactedRequestCandidateRecord>,
}

impl RequestCandidateSlotCompaction {
    fn merge(&mut self, record: UpsertRequestCandidateRecord) {
        let stage = match record.status {
            RequestCandidateStatus::Available | RequestCandidateStatus::Unused => &mut self.normal,
            RequestCandidateStatus::Pending => &mut self.pending,
            RequestCandidateStatus::Streaming => &mut self.streaming,
            RequestCandidateStatus::Success
            | RequestCandidateStatus::Failed
            | RequestCandidateStatus::Cancelled
            | RequestCandidateStatus::Skipped => &mut self.terminal,
        };
        match stage.as_mut() {
            Some(compacted) => {
                merge_request_candidate_record_for_flush(&mut compacted.record, record);
                compacted.source_count = compacted.source_count.saturating_add(1);
            }
            None => {
                *stage = Some(CompactedRequestCandidateRecord {
                    record,
                    source_count: 1,
                });
            }
        }
    }
}

fn append_compacted_stage(
    target: &mut Vec<CompactedRequestCandidateRecord>,
    stage: Option<CompactedRequestCandidateRecord>,
    inherited: &mut Option<UpsertRequestCandidateRecord>,
    status: Option<RequestCandidateStatus>,
    has_later_stage: bool,
) {
    let Some(mut stage) = stage else {
        return;
    };
    if let Some(mut inherited_record) = inherited.take() {
        merge_request_candidate_record_for_flush(&mut inherited_record, stage.record);
        stage.record = inherited_record;
    }
    if let Some(status) = status {
        stage.record.status = status;
    }
    if has_later_stage {
        *inherited = Some(stage.record.clone());
    }
    target.push(stage);
}

fn compact_records_for_flush(
    records: Vec<UpsertRequestCandidateRecord>,
) -> CompactedRequestCandidateFlushPlan {
    let mut latest_slot = HashMap::<RequestCandidateSlot, usize>::new();
    let mut compacted = Vec::<RequestCandidateSlotCompaction>::with_capacity(records.len());
    for record in records {
        let slot = request_candidate_slot(&record);
        match latest_slot.get(&slot).copied() {
            Some(index) => compacted[index].merge(record),
            _ => {
                latest_slot.insert(slot, compacted.len());
                let mut slot = RequestCandidateSlotCompaction::default();
                slot.merge(record);
                compacted.push(slot);
            }
        }
    }

    let mut plan = CompactedRequestCandidateFlushPlan {
        normal: Vec::with_capacity(compacted.len()),
        pending: Vec::with_capacity(compacted.len()),
        streaming: Vec::with_capacity(compacted.len()),
        terminal: Vec::with_capacity(compacted.len()),
    };
    for slot in compacted {
        let has_pending = slot.pending.is_some();
        let has_streaming = slot.streaming.is_some();
        let has_terminal = slot.terminal.is_some();
        let mut inherited = None;
        append_compacted_stage(
            &mut plan.normal,
            slot.normal,
            &mut inherited,
            None,
            has_pending || has_streaming || has_terminal,
        );
        append_compacted_stage(
            &mut plan.pending,
            slot.pending,
            &mut inherited,
            Some(RequestCandidateStatus::Pending),
            has_streaming || has_terminal,
        );
        append_compacted_stage(
            &mut plan.streaming,
            slot.streaming,
            &mut inherited,
            Some(RequestCandidateStatus::Streaming),
            has_terminal,
        );
        append_compacted_stage(
            &mut plan.terminal,
            slot.terminal,
            &mut inherited,
            None,
            false,
        );
    }
    plan
}

fn request_candidate_slot(record: &UpsertRequestCandidateRecord) -> RequestCandidateSlot {
    (
        record.request_id.clone(),
        record.candidate_index,
        record.retry_index,
    )
}

fn request_candidate_slots_are_unique(records: &[UpsertRequestCandidateRecord]) -> bool {
    let mut slots = HashSet::with_capacity(records.len());
    records
        .iter()
        .all(|record| slots.insert(request_candidate_slot(record)))
}

fn request_candidate_slot_hash(record: &UpsertRequestCandidateRecord) -> u64 {
    const FNV_OFFSET: u64 = 14_695_981_039_346_656_037;
    const FNV_PRIME: u64 = 1_099_511_628_211;

    let mut hash = FNV_OFFSET;
    for byte in record.request_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    for byte in record.candidate_index.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    for byte in record.retry_index.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn worker_queue_capacity(total_capacity: usize, workers: usize, worker_index: usize) -> usize {
    let workers = workers.max(1);
    let base = total_capacity / workers;
    let remainder = total_capacity % workers;
    (base + usize::from(worker_index < remainder)).max(1)
}

fn priority_queue_capacity(total_capacity: usize, workers: usize) -> usize {
    total_capacity.max(workers.max(1))
}

#[derive(Debug)]
struct RequestCandidateDbWriteGate {
    semaphore: tokio::sync::Semaphore,
}

impl RequestCandidateDbWriteGate {
    fn new(limit: usize) -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(limit.max(1)),
        }
    }

    async fn acquire<'a>(
        &'a self,
        metrics: &'a RequestCandidateQueueMetrics,
    ) -> tokio::sync::SemaphorePermit<'a> {
        if self.semaphore.available_permits() == 0 {
            metrics.db_write_wait_total.fetch_add(1, Ordering::AcqRel);
        }
        self.semaphore
            .acquire()
            .await
            .expect("request candidate DB write gate semaphore should not be closed")
    }
}

struct RequestCandidateDbWriteInFlightGuard<'a> {
    metrics: &'a RequestCandidateQueueMetrics,
}

impl<'a> RequestCandidateDbWriteInFlightGuard<'a> {
    fn new(metrics: &'a RequestCandidateQueueMetrics) -> Self {
        let in_flight = metrics.db_write_in_flight.fetch_add(1, Ordering::AcqRel) + 1;
        metrics
            .db_write_max_in_flight
            .fetch_max(in_flight, Ordering::AcqRel);
        Self { metrics }
    }
}

impl Drop for RequestCandidateDbWriteInFlightGuard<'_> {
    fn drop(&mut self) {
        self.metrics
            .db_write_in_flight
            .fetch_sub(1, Ordering::AcqRel);
    }
}

fn merge_request_candidate_record(
    target: &mut UpsertRequestCandidateRecord,
    incoming: UpsertRequestCandidateRecord,
) {
    if !incoming.id.trim().is_empty() {
        target.id = incoming.id;
    }
    target.request_id = incoming.request_id;
    target.candidate_index = incoming.candidate_index;
    target.retry_index = incoming.retry_index;
    target.status = incoming.status;

    take_if_some(&mut target.user_id, incoming.user_id);
    take_if_some(&mut target.api_key_id, incoming.api_key_id);
    take_if_some(&mut target.username, incoming.username);
    take_if_some(&mut target.api_key_name, incoming.api_key_name);
    take_if_some(&mut target.provider_id, incoming.provider_id);
    take_if_some(&mut target.endpoint_id, incoming.endpoint_id);
    take_if_some(&mut target.key_id, incoming.key_id);
    take_if_some(&mut target.skip_reason, incoming.skip_reason);
    take_if_some(&mut target.is_cached, incoming.is_cached);
    take_if_some(&mut target.status_code, incoming.status_code);
    take_if_some(&mut target.error_type, incoming.error_type);
    take_if_some(&mut target.error_message, incoming.error_message);
    take_if_some(&mut target.latency_ms, incoming.latency_ms);
    take_if_some(
        &mut target.concurrent_requests,
        incoming.concurrent_requests,
    );
    merge_json_value(&mut target.extra_data, incoming.extra_data);
    merge_json_value(
        &mut target.required_capabilities,
        incoming.required_capabilities,
    );

    if target.created_at_unix_ms.is_none() {
        target.created_at_unix_ms = incoming.created_at_unix_ms;
    }
    take_if_some(&mut target.started_at_unix_ms, incoming.started_at_unix_ms);
    take_if_some(
        &mut target.finished_at_unix_ms,
        incoming.finished_at_unix_ms,
    );
}

fn merge_request_candidate_record_for_flush(
    target: &mut UpsertRequestCandidateRecord,
    incoming: UpsertRequestCandidateRecord,
) {
    let target_status = target.status;
    let incoming_status = incoming.status;
    let next_status = merged_request_candidate_status(target_status, incoming_status);
    merge_request_candidate_record(target, incoming);
    target.status = next_status;
}

fn merged_request_candidate_status(
    current: RequestCandidateStatus,
    incoming: RequestCandidateStatus,
) -> RequestCandidateStatus {
    match (
        request_candidate_status_is_terminal(current),
        request_candidate_status_is_terminal(incoming),
    ) {
        (_, true) => incoming,
        (true, false) => current,
        (false, false) => incoming,
    }
}

fn request_candidate_status_is_terminal(status: RequestCandidateStatus) -> bool {
    matches!(
        status,
        RequestCandidateStatus::Success
            | RequestCandidateStatus::Failed
            | RequestCandidateStatus::Cancelled
            | RequestCandidateStatus::Skipped
    )
}

fn request_candidate_status_is_active(status: RequestCandidateStatus) -> bool {
    matches!(
        status,
        RequestCandidateStatus::Pending | RequestCandidateStatus::Streaming
    )
}

fn request_candidate_status_is_priority(status: RequestCandidateStatus) -> bool {
    request_candidate_status_is_active(status) || request_candidate_status_is_terminal(status)
}

fn take_if_some<T>(target: &mut Option<T>, incoming: Option<T>) {
    if incoming.is_some() {
        *target = incoming;
    }
}

fn merge_json_value(target: &mut Option<serde_json::Value>, incoming: Option<serde_json::Value>) {
    match (target.as_mut(), incoming) {
        (Some(serde_json::Value::Object(target)), Some(serde_json::Value::Object(incoming))) => {
            target.extend(incoming);
        }
        (_, Some(incoming)) => {
            *target = Some(incoming);
        }
        (_, None) => {}
    }
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_optional_usize(key: &str) -> Option<usize> {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn decrement_atomic_usize(value: &AtomicUsize) {
    decrement_atomic_usize_by(value, 1);
}

fn decrement_atomic_usize_by(value: &AtomicUsize, amount: usize) {
    let _ = value.try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
        Some(current.saturating_sub(amount))
    });
}

fn should_log_queue_counter(value: u64) -> bool {
    value <= 8 || value.is_power_of_two() || value.is_multiple_of(1_000)
}

#[cfg(test)]
#[path = "request_candidate_queue/tests.rs"]
mod tests;
