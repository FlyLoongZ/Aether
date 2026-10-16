use std::sync::{Arc, Mutex as StdMutex};

use aether_contracts::{
    ExecutionPlan, ExecutionResult, ExecutionTimeouts, RequestBody, ResponseBody,
};
use aether_data::repository::settlement::{
    InMemorySettlementRepository, ReserveUsagePolicyCostInput, ReserveUsagePolicyCostOutcome,
    SettlementWriteRepository, UsagePolicyCostReservationState, UsagePolicyCostWindow,
};
use aether_data_contracts::repository::billing::{
    BillingReadRepository, StoredBillingModelContext, UserPlanEntitlementRecord,
};
use aether_data_contracts::repository::candidates::{
    RequestCandidateStatus, UpsertRequestCandidateRecord,
};
use aether_data_contracts::DataLayerError;
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::Mutex;

use super::*;

struct TestRequestCandidateWriter {
    records: Mutex<Vec<UpsertRequestCandidateRecord>>,
    upstream_gate: Option<aether_runtime::ConcurrencyGate>,
    upstream_queue_budget: Duration,
}

impl Default for TestRequestCandidateWriter {
    fn default() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            upstream_gate: None,
            upstream_queue_budget: Duration::from_millis(250),
        }
    }
}

impl TestRequestCandidateWriter {
    fn with_upstream_gate(limit: usize, queue_budget: Duration) -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            upstream_gate: Some(aether_runtime::ConcurrencyGate::new(
                UPSTREAM_EXECUTION_GATE_NAME,
                limit,
            )),
            upstream_queue_budget: queue_budget,
        }
    }
}

#[async_trait]
impl RequestCandidateRuntimeWriter for TestRequestCandidateWriter {
    fn has_request_candidate_data_writer(&self) -> bool {
        true
    }

    async fn upsert_request_candidate(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<
        Option<aether_data_contracts::repository::candidates::StoredRequestCandidate>,
        GatewayError,
    > {
        self.records.lock().await.push(candidate);
        Ok(None)
    }
}

impl UpstreamExecutionGateProvider for TestRequestCandidateWriter {
    fn upstream_execution_gate(&self) -> Option<&aether_runtime::ConcurrencyGate> {
        self.upstream_gate.as_ref()
    }

    fn upstream_execution_gate_queue_budget(&self) -> Duration {
        self.upstream_queue_budget
    }
}

struct PendingAttemptSource;

#[async_trait]
impl LocalExecutionAttemptSource<()> for PendingAttemptSource {
    async fn next_execution_attempt(&mut self) -> Result<Option<()>, GatewayError> {
        std::future::pending::<()>().await;
        Ok(None)
    }

    async fn drain_execution_attempts(&mut self) -> Result<Vec<()>, GatewayError> {
        Ok(Vec::new())
    }

    async fn skip_credential(&mut self, _key_id: &str) -> Result<(), GatewayError> {
        Ok(())
    }

    async fn skip_endpoint(&mut self, _endpoint_id: &str) -> Result<(), GatewayError> {
        Ok(())
    }

    async fn skip_provider(&mut self, _provider_id: &str) -> Result<(), GatewayError> {
        Ok(())
    }
}

#[derive(Debug)]
struct CostReservationBillingRepository {
    model_context: StoredBillingModelContext,
    entitlement: UserPlanEntitlementRecord,
}

#[async_trait]
impl BillingReadRepository for CostReservationBillingRepository {
    async fn find_model_context(
        &self,
        _provider_id: &str,
        _provider_api_key_id: Option<&str>,
        _global_model_name: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        Ok(Some(self.model_context.clone()))
    }

    async fn find_model_context_by_model_id(
        &self,
        _provider_id: &str,
        _provider_api_key_id: Option<&str>,
        _model_id: &str,
    ) -> Result<Option<StoredBillingModelContext>, DataLayerError> {
        Ok(Some(self.model_context.clone()))
    }

    async fn list_user_plan_entitlements(
        &self,
        user_id: &str,
    ) -> Result<Option<Vec<UserPlanEntitlementRecord>>, DataLayerError> {
        Ok(Some(
            (self.entitlement.user_id == user_id)
                .then(|| self.entitlement.clone())
                .into_iter()
                .collect(),
        ))
    }
}

struct PlanningErrorAfterFirstSyncAttemptSource {
    first: Option<crate::ai_serving::AiSyncAttempt>,
}

#[async_trait]
impl LocalExecutionAttemptSource<crate::ai_serving::AiSyncAttempt>
    for PlanningErrorAfterFirstSyncAttemptSource
{
    async fn next_execution_attempt(
        &mut self,
    ) -> Result<Option<crate::ai_serving::AiSyncAttempt>, GatewayError> {
        match self.first.take() {
            Some(first) => Ok(Some(first)),
            None => Err(GatewayError::Internal(
                "synthetic next-candidate planning failure".to_string(),
            )),
        }
    }

    async fn drain_execution_attempts(
        &mut self,
    ) -> Result<Vec<crate::ai_serving::AiSyncAttempt>, GatewayError> {
        Ok(self.first.take().into_iter().collect())
    }

    async fn skip_credential(&mut self, _key_id: &str) -> Result<(), GatewayError> {
        Ok(())
    }

    async fn skip_endpoint(&mut self, _endpoint_id: &str) -> Result<(), GatewayError> {
        Ok(())
    }

    async fn skip_provider(&mut self, _provider_id: &str) -> Result<(), GatewayError> {
        Ok(())
    }
}

#[derive(Clone)]
struct TransferTestAttempt {
    label: &'static str,
    plan: ExecutionPlan,
    report_context: serde_json::Value,
}

impl AiExecutionAttempt for TransferTestAttempt {
    fn execution_plan(&self) -> &ExecutionPlan {
        &self.plan
    }

    fn report_kind(&self) -> Option<String> {
        None
    }

    fn report_context(&self) -> Option<serde_json::Value> {
        Some(self.report_context.clone())
    }

    fn report_context_ref(&self) -> Option<&serde_json::Value> {
        Some(&self.report_context)
    }
}

struct TransferTestPort<'a> {
    state: &'a AppState,
    tracker: ProviderTransferTracker,
    retry_scope: AiAttemptRetryScope,
    executed: StdMutex<Vec<&'static str>>,
    unused: StdMutex<Vec<&'static str>>,
    /// Status codes handed back as preserved fallbacks, consumed per attempt.
    fallback_statuses: StdMutex<std::collections::VecDeque<u16>>,
}

impl<'a> TransferTestPort<'a> {
    fn new(state: &'a AppState) -> Self {
        Self::with_tracker(state, ProviderTransferTracker::default())
    }

    fn with_tracker(state: &'a AppState, tracker: ProviderTransferTracker) -> Self {
        Self {
            state,
            tracker,
            retry_scope: AiAttemptRetryScope::Candidate,
            executed: StdMutex::new(Vec::new()),
            unused: StdMutex::new(Vec::new()),
            fallback_statuses: StdMutex::new(std::collections::VecDeque::new()),
        }
    }

    fn with_retry_scope(state: &'a AppState, retry_scope: AiAttemptRetryScope) -> Self {
        Self {
            state,
            tracker: ProviderTransferTracker::default(),
            retry_scope,
            executed: StdMutex::new(Vec::new()),
            unused: StdMutex::new(Vec::new()),
            fallback_statuses: StdMutex::new(std::collections::VecDeque::new()),
        }
    }

    /// Every attempt fails and preserves the next status code, so the loop
    /// has to decide which one the client finally receives.
    fn with_retry_fallbacks(
        state: &'a AppState,
        statuses: impl IntoIterator<Item = u16>,
    ) -> Self {
        let mut port = Self::new(state);
        port.fallback_statuses = StdMutex::new(statuses.into_iter().collect());
        port
    }
}

#[async_trait]
impl AiAttemptLoopPort<TransferTestAttempt> for TransferTestPort<'_> {
    type Response = Response<Body>;
    type Exhaustion = crate::executor::LocalExecutionExhaustion;
    type Error = GatewayError;

    async fn should_skip_attempt(
        &self,
        attempt: &TransferTestAttempt,
    ) -> Result<bool, Self::Error> {
        Ok(should_skip_provider_transfer_attempt(
            &self.tracker,
            "trace-transfer-test",
            "transfer_test",
            attempt,
        )
        .await)
    }

    async fn record_attempt_started(
        &self,
        attempt: &TransferTestAttempt,
    ) -> Result<(), Self::Error> {
        record_provider_transfer_attempt_started(&self.tracker, attempt).await;
        Ok(())
    }

    async fn record_attempt_failed(
        &self,
        attempt: &TransferTestAttempt,
    ) -> Result<(), Self::Error> {
        record_provider_transfer_attempt_failed(
            self.state,
            &self.tracker,
            "trace-transfer-test",
            "transfer_test",
            attempt,
        )
        .await;
        Ok(())
    }

    async fn execute_attempt(
        &self,
        attempt: &TransferTestAttempt,
    ) -> Result<AiAttemptExecutionOutcome<Self::Response>, Self::Error> {
        self.executed.lock().unwrap().push(attempt.label);
        if let Some(status) = self.fallback_statuses.lock().unwrap().pop_front() {
            let response = Response::builder()
                .status(status)
                .body(Body::from(format!("upstream error {status}")))
                .expect("fallback response should build");
            return Ok(AiAttemptExecutionOutcome::Retry {
                scope: self.retry_scope,
                fallback_response: Some(response),
            });
        }
        Ok(if attempt.plan.provider_id == "provider-b" {
            AiAttemptExecutionOutcome::Responded(Response::new(Body::from("ok")))
        } else {
            AiAttemptExecutionOutcome::retry(self.retry_scope)
        })
    }

    async fn mark_unused_attempts(
        &self,
        attempts: Vec<TransferTestAttempt>,
    ) -> Result<(), Self::Error> {
        self.unused
            .lock()
            .unwrap()
            .extend(attempts.into_iter().map(|attempt| attempt.label));
        Ok(())
    }

    async fn build_exhaustion(
        &self,
        last_plan: ExecutionPlan,
        last_report_context: Option<serde_json::Value>,
    ) -> Result<Self::Exhaustion, Self::Error> {
        Ok(
            build_local_execution_exhaustion(self.state, &last_plan, last_report_context.as_ref())
                .await,
        )
    }
}

struct TransferTestAttemptSource {
    attempts: std::collections::VecDeque<TransferTestAttempt>,
    skipped_providers: Vec<String>,
}

#[async_trait]
impl LocalExecutionAttemptSource<TransferTestAttempt> for TransferTestAttemptSource {
    async fn next_execution_attempt(
        &mut self,
    ) -> Result<Option<TransferTestAttempt>, GatewayError> {
        Ok(self.attempts.pop_front())
    }

    async fn drain_execution_attempts(&mut self) -> Result<Vec<TransferTestAttempt>, GatewayError> {
        Ok(self.attempts.drain(..).collect())
    }

    async fn skip_credential(&mut self, key_id: &str) -> Result<(), GatewayError> {
        self.attempts
            .retain(|attempt| attempt.plan.key_id != key_id);
        Ok(())
    }

    async fn skip_endpoint(&mut self, endpoint_id: &str) -> Result<(), GatewayError> {
        self.attempts
            .retain(|attempt| attempt.plan.endpoint_id != endpoint_id);
        Ok(())
    }

    async fn skip_provider(&mut self, provider_id: &str) -> Result<(), GatewayError> {
        self.skipped_providers.push(provider_id.to_string());
        self.attempts
            .retain(|attempt| attempt.plan.provider_id != provider_id);
        Ok(())
    }
}

fn transfer_test_attempts() -> Vec<TransferTestAttempt> {
    fn attempt(label: &'static str, provider_id: &str, key_id: &str) -> TransferTestAttempt {
        let mut plan = test_plan(None);
        plan.provider_id = provider_id.to_string();
        plan.key_id = key_id.to_string();
        TransferTestAttempt {
            label,
            plan,
            report_context: json!({
                "local_failover_policy": {
                    "max_transfer_count": 1,
                    "max_transfer_timeout_seconds": 0
                }
            }),
        }
    }

    vec![
        attempt("a-key1-retry0", "provider-a", "key-1"),
        attempt("a-key2-retry0", "provider-a", "key-2"),
        attempt("a-key2-retry1", "provider-a", "key-2"),
        attempt("a-key3-retry0", "provider-a", "key-3"),
        attempt("b-key1-retry0", "provider-b", "key-b"),
    ]
}

#[tokio::test]
async fn static_loop_allows_same_key_retries_then_skips_next_transfer() {
    let state = AppState::new().expect("state should build");
    let port = TransferTestPort::new(&state);

    let outcome = run_ai_attempt_loop(&port, transfer_test_attempts())
        .await
        .expect("attempt loop should succeed");

    assert!(matches!(outcome, AiAttemptLoopOutcome::Responded(_)));
    assert_eq!(
        port.executed.lock().unwrap().as_slice(),
        [
            "a-key1-retry0",
            "a-key2-retry0",
            "a-key2-retry1",
            "b-key1-retry0"
        ]
    );
    assert_eq!(port.unused.lock().unwrap().as_slice(), ["a-key3-retry0"]);
}

#[tokio::test]
async fn routing_transfer_budget_counts_switches_across_providers_not_same_key_retries() {
    for (limit, succeeds) in [(1, false), (2, true)] {
        let state = AppState::new().unwrap();
        let port = TransferTestPort::new(&state);
        let mut attempts = transfer_test_attempts();
        for attempt in &mut attempts {
            attempt.report_context["routing_execution_policy"] =
                json!({ "max_transfer_count": limit });
        }
        let outcome = run_ai_attempt_loop(&port, attempts).await.unwrap();
        assert_eq!(
            matches!(outcome, AiAttemptLoopOutcome::Responded(_)),
            succeeds
        );
        {
            let executed = port.executed.lock().unwrap();
            assert_eq!(
                &executed[..3],
                ["a-key1-retry0", "a-key2-retry0", "a-key2-retry1"]
            );
            assert_eq!(executed.len(), if succeeds { 4 } else { 3 });
        }
        assert_eq!(port.tracker.state.lock().await.global.transfer_count, limit);
    }
}

#[tokio::test]
async fn dynamic_loop_honors_global_transfer_budget_across_providers() {
    let state = AppState::new().unwrap();
    let port = TransferTestPort::new(&state);
    let mut attempts = transfer_test_attempts();
    for attempt in &mut attempts {
        attempt.report_context["routing_execution_policy"] = json!({ "max_transfer_count": 1 });
    }
    let mut source = TransferTestAttemptSource {
        attempts: attempts.into(),
        skipped_providers: Vec::new(),
    };
    let outcome = run_dynamic_attempt_loop(
        &port,
        &mut source,
        "global-budget",
        "test",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        LocalExecutionRequestOutcome::Exhausted(_)
    ));
    assert_eq!(
        port.executed.lock().unwrap().as_slice(),
        ["a-key1-retry0", "a-key2-retry0", "a-key2-retry1"]
    );
    assert_eq!(source.skipped_providers, ["provider-a", "provider-b"]);
}

#[test]
fn routing_time_budget_is_cumulative_and_zero_is_unlimited() {
    let mut global = super::GlobalTransferState::default();
    global.load_policy(Some(
        &json!({ "routing_execution_policy": { "max_transfer_timeout_seconds": 60 } }),
    ));
    let now = tokio::time::Instant::now();
    let plan = test_plan(None);
    global.record_attempt_started(&plan, now);
    global.record_attempt_started(&plan, now + Duration::from_secs(40));
    assert_eq!(global.transfer_count, 0);
    assert_eq!(
        global.check_before_attempt(&plan, now + Duration::from_secs(59)),
        None
    );
    assert_eq!(
        global.check_before_attempt(&plan, now + Duration::from_secs(60)),
        Some((false, true))
    );
    let mut unlimited = super::GlobalTransferState::default();
    unlimited.load_policy(Some(&json!({ "routing_execution_policy": {} })));
    unlimited.record_attempt_started(&plan, now);
    assert_eq!(
        unlimited.check_before_attempt(&plan, now + Duration::from_secs(86_400)),
        None
    );
}

#[tokio::test]
async fn cloned_tracker_preserves_global_budget_across_candidate_loops() {
    let state = AppState::new().unwrap();
    let tracker = ProviderTransferTracker::default();
    let mut attempts = transfer_test_attempts();
    for attempt in &mut attempts {
        attempt.report_context["routing_execution_policy"] = json!({ "max_transfer_count": 1 });
    }
    let remaining = attempts.split_off(3);
    let first_port = TransferTestPort::with_tracker(&state, tracker.clone());
    let first_outcome = run_ai_attempt_loop(&first_port, attempts).await.unwrap();
    assert!(matches!(first_outcome, AiAttemptLoopOutcome::Exhausted(_)));
    assert_eq!(tracker.state.lock().await.global.transfer_count, 1);

    let second_port = TransferTestPort::with_tracker(&state, tracker.clone());
    let mut source = TransferTestAttemptSource {
        attempts: remaining.into(),
        skipped_providers: Vec::new(),
    };
    let second_outcome = run_dynamic_attempt_loop(
        &second_port,
        &mut source,
        "global-budget-across-loops",
        "test",
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert!(matches!(
        second_outcome,
        LocalExecutionRequestOutcome::NoPath
    ));
    assert!(second_port.executed.lock().unwrap().is_empty());
    assert_eq!(source.skipped_providers, ["provider-a", "provider-b"]);
    assert!(tracker.state.lock().await.global.exhausted);
}

#[tokio::test]
async fn cloned_tracker_preserves_transfer_budget_across_candidate_loops() {
    let state = AppState::new().expect("state should build");
    let tracker = ProviderTransferTracker::default();
    let mut attempts = transfer_test_attempts();
    let provider_b = attempts.pop().expect("provider-b attempt should exist");
    let key_3 = attempts.pop().expect("third provider-a key should exist");
    let first_port = TransferTestPort::with_tracker(&state, tracker.clone());

    let first_outcome = run_ai_attempt_loop(&first_port, attempts)
        .await
        .expect("first candidate loop should exhaust");
    assert!(matches!(first_outcome, AiAttemptLoopOutcome::Exhausted(_)));

    let second_port = TransferTestPort::with_tracker(&state, tracker);
    let second_outcome = run_ai_attempt_loop(&second_port, vec![key_3, provider_b])
        .await
        .expect("second candidate loop should succeed");

    assert!(matches!(second_outcome, AiAttemptLoopOutcome::Responded(_)));
    assert_eq!(
        second_port.executed.lock().unwrap().as_slice(),
        ["b-key1-retry0"]
    );
    assert_eq!(
        second_port.unused.lock().unwrap().as_slice(),
        ["a-key3-retry0"]
    );
}

#[tokio::test]
async fn dynamic_loop_skips_exhausted_provider_at_candidate_source() {
    let state = AppState::new().expect("state should build");
    let port = TransferTestPort::new(&state);
    let mut source = TransferTestAttemptSource {
        attempts: transfer_test_attempts().into(),
        skipped_providers: Vec::new(),
    };

    let outcome = run_dynamic_attempt_loop(
        &port,
        &mut source,
        "trace-transfer-test",
        "transfer_test",
        Duration::from_secs(1),
    )
    .await
    .expect("dynamic attempt loop should succeed");

    assert!(matches!(
        outcome,
        LocalExecutionRequestOutcome::Responded(_)
    ));
    assert_eq!(
        port.executed.lock().unwrap().as_slice(),
        [
            "a-key1-retry0",
            "a-key2-retry0",
            "a-key2-retry1",
            "b-key1-retry0"
        ]
    );
    assert_eq!(source.skipped_providers, ["provider-a"]);
}

#[tokio::test]
async fn dynamic_loop_applies_provider_scoped_retry_to_candidate_source() {
    let state = AppState::new().expect("state should build");
    let port = TransferTestPort::with_retry_scope(&state, AiAttemptRetryScope::Provider);
    let mut source = TransferTestAttemptSource {
        attempts: transfer_test_attempts().into(),
        skipped_providers: Vec::new(),
    };

    let outcome = run_dynamic_attempt_loop(
        &port,
        &mut source,
        "trace-provider-scope-test",
        "provider_scope_test",
        Duration::from_secs(1),
    )
    .await
    .expect("dynamic attempt loop should succeed");

    assert!(matches!(
        outcome,
        LocalExecutionRequestOutcome::Responded(_)
    ));
    assert_eq!(
        port.executed.lock().unwrap().as_slice(),
        ["a-key1-retry0", "b-key1-retry0"]
    );
    assert_eq!(source.skipped_providers, ["provider-a"]);
}

#[test]
fn transfer_timeout_is_checked_at_candidate_boundary_and_zero_disables_limits() {
    let started_at = Instant::now();
    let mut first = test_plan(None);
    first.provider_id = "provider-a".to_string();
    first.key_id = "key-1".to_string();

    let mut timeout_tracker = ProviderTransferStateTracker::default();
    timeout_tracker.record_attempt_started(&first, started_at);
    timeout_tracker.set_limits(
        "provider-a",
        ProviderTransferLimits {
            max_transfer_count: 0,
            max_transfer_timeout_seconds: 60,
        },
    );
    assert!(timeout_tracker
        .check_before_attempt(&first, started_at + Duration::from_secs(59))
        .is_none());
    let reached = timeout_tracker
        .check_before_attempt(&first, started_at + Duration::from_secs(60))
        .expect("timeout should stop the provider at the next candidate boundary");
    assert!(reached.timeout_reached);
    assert!(!reached.count_reached);

    let mut count_tracker = ProviderTransferStateTracker::default();
    count_tracker.record_attempt_started(&first, started_at);
    count_tracker.set_limits(
        "provider-a",
        ProviderTransferLimits {
            max_transfer_count: 1,
            max_transfer_timeout_seconds: 60,
        },
    );
    let mut second_key = first.clone();
    second_key.key_id = "key-2".to_string();
    assert!(count_tracker
        .check_before_attempt(&second_key, started_at + Duration::from_secs(1))
        .is_none());
    count_tracker.record_attempt_started(&second_key, started_at + Duration::from_secs(1));
    let mut third_key = first.clone();
    third_key.key_id = "key-3".to_string();
    let reached = count_tracker
        .check_before_attempt(&third_key, started_at + Duration::from_secs(2))
        .expect("count should stop the provider before another key transfer");
    assert!(reached.count_reached);
    assert!(!reached.timeout_reached);

    let mut unlimited_tracker = ProviderTransferStateTracker::default();
    unlimited_tracker.record_attempt_started(&first, started_at);
    unlimited_tracker.set_limits("provider-a", ProviderTransferLimits::default());
    let mut another_key = first.clone();
    another_key.key_id = "key-2".to_string();
    assert!(unlimited_tracker
        .check_before_attempt(&another_key, started_at + Duration::from_secs(3_600))
        .is_none());
}

#[test]
fn transfer_count_and_timeout_limits_use_or_semantics() {
    let started_at = Instant::now();
    let mut first = test_plan(None);
    first.provider_id = "provider-a".to_string();
    first.key_id = "key-1".to_string();
    let limits = ProviderTransferLimits {
        max_transfer_count: 1,
        max_transfer_timeout_seconds: 60,
    };

    let mut count_first = ProviderTransferStateTracker::default();
    count_first.record_attempt_started(&first, started_at);
    count_first.set_limits("provider-a", limits);
    let mut second = first.clone();
    second.key_id = "key-2".to_string();
    count_first.record_attempt_started(&second, started_at + Duration::from_secs(1));
    let mut third = first.clone();
    third.key_id = "key-3".to_string();
    let count_reached = count_first
        .check_before_attempt(&third, started_at + Duration::from_secs(2))
        .expect("count should independently exhaust a provider before timeout");
    assert!(count_reached.count_reached);
    assert!(!count_reached.timeout_reached);

    let mut timeout_first = ProviderTransferStateTracker::default();
    timeout_first.record_attempt_started(&first, started_at);
    timeout_first.set_limits("provider-a", limits);
    let timeout_reached = timeout_first
        .check_before_attempt(&first, started_at + Duration::from_secs(60))
        .expect("timeout should independently exhaust a provider before count");
    assert!(!timeout_reached.count_reached);
    assert!(timeout_reached.timeout_reached);
}

fn test_plan(timeouts: Option<ExecutionTimeouts>) -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req_watchdog".to_string(),
        candidate_id: Some("cand_watchdog".to_string()),
        provider_name: Some("provider".to_string()),
        provider_id: "provider_id".to_string(),
        endpoint_id: "endpoint_id".to_string(),
        key_id: "key_id".to_string(),
        method: "POST".to_string(),
        url: "https://example.com/v1/messages".to_string(),
        headers: Default::default(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-test"})),
        stream: true,
        client_api_format: "claude:messages".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-test".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts,
    }
}

#[tokio::test]
async fn next_execution_attempt_times_out_instead_of_waiting_forever() {
    let mut source = PendingAttemptSource;

    let err = next_execution_attempt_with_timeout(
        &mut source,
        "trace-planning-timeout",
        "openai_responses_sync",
        Duration::from_millis(5),
    )
    .await
    .expect_err("pending candidate planning should time out");

    match err {
        GatewayError::LocalExecutionPlanningTimeout {
            trace_id,
            phase,
            timeout_ms,
        } => {
            assert_eq!(trace_id, "trace-planning-timeout");
            assert_eq!(phase, "next_execution_attempt");
            assert_eq!(timeout_ms, 5);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
async fn dynamic_sync_planning_error_releases_reserved_http_plan_cost() {
    let now_unix_secs = current_unix_ms() / 1_000;
    let request_id = "req-http-reservation-planning-error";
    let subject_id = "user-http-reservation-planning-error";
    let settlement = Arc::new(InMemorySettlementRepository::default());
    let billing = Arc::new(CostReservationBillingRepository {
        model_context: StoredBillingModelContext::new(
            "provider-1".to_string(),
            None,
            Some("key-1".to_string()),
            None,
            None,
            "global-model-1".to_string(),
            "gpt-5".to_string(),
            None,
            Some(0.25),
            None,
            Some("model-1".to_string()),
            Some("gpt-5".to_string()),
            None,
            None,
            None,
        )
        .expect("billing context should build"),
        entitlement: UserPlanEntitlementRecord {
            id: "ent-http-reservation-planning-error".to_string(),
            user_id: subject_id.to_string(),
            plan_id: "plan-http-reservation-planning-error".to_string(),
            payment_order_id: "order-http-reservation-planning-error".to_string(),
            status: "active".to_string(),
            starts_at_unix_secs: now_unix_secs.saturating_sub(60),
            expires_at_unix_secs: now_unix_secs.saturating_add(3_600),
            entitlements_snapshot: json!([{
                "type": "usage_policy",
                "rules": [{
                    "metric": "actual_cost_usd",
                    "window": {"kind": "rolling", "seconds": 3_600},
                    "limit": 10.0
                }]
            }]),
            created_at_unix_secs: now_unix_secs.saturating_sub(60),
            updated_at_unix_secs: now_unix_secs.saturating_sub(60),
        },
    });
    let data = crate::data::GatewayDataState::with_billing_reader_for_tests(billing)
        .with_settlement_writer_for_tests(settlement.clone());
    let state = AppState::new()
        .expect("state should build")
        .with_data_state_for_tests(data)
        .with_usage_runtime_for_tests(crate::usage::UsageRuntimeConfig {
            enabled: true,
            ..crate::usage::UsageRuntimeConfig::default()
        })
        .with_execution_runtime_sync_override_for_tests(|plan| {
            Ok(ExecutionResult {
                request_id: plan.request_id.clone(),
                candidate_id: plan.candidate_id.clone(),
                status_code: http::StatusCode::TOO_MANY_REQUESTS.as_u16(),
                headers: Default::default(),
                response_observation: None,
                body: Some(ResponseBody {
                    json_body: Some(json!({"error": {"message": "retry elsewhere"}})),
                    body_bytes_b64: None,
                }),
                telemetry: None,
                error: None,
            })
        });

    let mut decision = GatewayControlDecision::synthetic(
        "/v1/chat/completions",
        Some("ai_public".to_string()),
        Some("openai".to_string()),
        Some("chat".to_string()),
        Some("openai:chat".to_string()),
    )
    .with_execution_runtime_candidate(true);
    decision.auth_context = Some(crate::control::GatewayControlAuthContext {
        user_id: subject_id.to_string(),
        api_key_id: "api-key-http-reservation-planning-error".to_string(),
        username: None,
        api_key_name: None,
        balance_remaining: None,
        access_allowed: true,
        user_rate_limit: None,
        api_key_rate_limit: None,
        api_key_is_standalone: false,
        admin_bypass_limits: false,
        local_rejection: None,
        allowed_models: None,
        ip_rules: None,
        verified_api_key_hash: None,
    });
    let admission = crate::plan_usage_policy::check_and_acquire_http_plan_usage_policy(
        &state,
        Some(&decision),
        request_id,
        now_unix_secs.saturating_mul(1_000),
    )
    .await
    .expect("HTTP plan admission should succeed");
    let reservation = admission
        .reservation_context
        .expect("cost policy should freeze a reservation context");
    let reservation_token = reservation.token().to_string();
    let admitted_at_unix_secs = reservation.admitted_at_unix_secs();

    let mut request = http::Request::builder()
        .uri("/v1/chat/completions")
        .body(())
        .expect("request should build");
    request.extensions_mut().insert(reservation);
    let (parts, _) = request.into_parts();
    let mut plan = test_plan(None);
    plan.request_id = request_id.to_string();
    plan.candidate_id = Some("cand-http-reservation-planning-error".to_string());
    plan.provider_id = "provider-1".to_string();
    plan.endpoint_id = "endpoint-1".to_string();
    plan.key_id = "key-1".to_string();
    plan.client_api_format = "openai:chat".to_string();
    plan.provider_api_format = "openai:chat".to_string();
    plan.model_name = Some("gpt-5".to_string());
    plan.stream = false;
    plan.body = RequestBody::from_json(json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 16
    }));
    let source = PlanningErrorAfterFirstSyncAttemptSource {
        first: Some(crate::ai_serving::AiSyncAttempt {
            plan,
            report_kind: None,
            report_context: Some(json!({
                "candidate_index": 0,
                "retry_index": 0,
                "model_id": "model-1",
                "global_model_name": "gpt-5"
            })),
        }),
    };

    let error = execute_sync_attempt_source::<crate::ai_serving::AiSyncAttempt, _>(
        &state,
        &parts,
        "trace-http-reservation-planning-error",
        &decision,
        "openai_chat_sync",
        source,
    )
    .await
    .expect_err("second candidate planning should fail");
    assert!(matches!(
        error,
        GatewayError::Internal(message)
            if message == "synthetic next-candidate planning failure"
    ));

    let probe = settlement
        .reserve_usage_policy_cost(ReserveUsagePolicyCostInput {
            request_id: request_id.to_string(),
            subject_id: subject_id.to_string(),
            reservation_token,
            admitted_at_unix_secs,
            reserved_cost_units: 1,
            reservation_expires_at_unix_secs: admitted_at_unix_secs.saturating_add(86_400),
            retain_until_unix_secs: admitted_at_unix_secs.saturating_add(32 * 86_400),
            windows: vec![UsagePolicyCostWindow {
                window_id: "release-state-probe".to_string(),
                starts_at_unix_secs: admitted_at_unix_secs.saturating_sub(1),
                ends_at_unix_secs: admitted_at_unix_secs.saturating_add(1),
                limit_cost_units: 1_000_000_000,
            }],
        })
        .await
        .expect("reservation state probe should succeed");
    assert_eq!(
        probe,
        ReserveUsagePolicyCostOutcome::AlreadyTerminal {
            state: UsagePolicyCostReservationState::Released,
        }
    );
}

fn test_report_context() -> serde_json::Value {
    json!({
        "request_id": "req_watchdog",
        "candidate_id": "cand_watchdog",
        "candidate_index": 2,
        "retry_index": 0,
        "user_id": "user_1",
        "api_key_id": "api_key_1",
    })
}

#[test]
fn request_tracker_preserves_server_reservation_identity_across_clones() {
    let mut request = http::Request::new(());
    let gate = aether_runtime::ConcurrencyGate::new("image_heartbeat_request", 1);
    let admission = AdmissionPermit::from(gate.try_acquire().expect("request admission"));
    request
        .extensions_mut()
        .insert(BackgroundAdmissionPermit::new(admission.clone()));
    request.extensions_mut().insert(
        crate::plan_usage_policy::PlanUsageReservationContext::for_test(
            "user-1",
            "server-token",
            12_345,
            crate::plan_usage_policy::EffectivePlanUsagePolicy::default(),
        ),
    );
    let (parts, _) = request.into_parts();

    let tracker = ProviderTransferTracker::for_request(&parts);
    let cloned = tracker.clone();

    let reservation = tracker
        .usage_policy_reservation
        .as_ref()
        .expect("request reservation snapshot");
    let cloned_reservation = cloned
        .usage_policy_reservation
        .as_ref()
        .expect("cloned reservation snapshot");
    assert_eq!(reservation.admitted_at_unix_secs(), 12_345);
    assert_eq!(reservation.subject_id(), "user-1");
    assert_eq!(reservation.token(), "server-token");
    assert_eq!(cloned_reservation.token(), "server-token");
    assert!(std::ptr::eq(
        reservation.policy(),
        cloned_reservation.policy()
    ));

    drop((parts, admission, tracker));
    assert_eq!(gate.snapshot().in_flight, 1);
    assert!(
        gate.try_acquire().is_err(),
        "image heartbeat tracker clone should retain request admission"
    );
    drop(cloned);
    assert_eq!(gate.snapshot().in_flight, 0);
}

#[test]
fn server_reservation_token_overrides_report_context_value() {
    let context = attach_plan_usage_reservation_token(
        Some(json!({
            "candidate_index": 2,
            "plan_usage_reservation_token": "client-controlled-token"
        })),
        Some("server-token"),
    )
    .expect("reservation context");

    assert_eq!(context["candidate_index"], 2);
    assert_eq!(context["plan_usage_reservation_token"], "server-token");
    assert_eq!(context["plan_usage_reservation_deferred"], false);
}

#[test]
fn missing_reservation_does_not_create_or_propagate_a_token() {
    let request = http::Request::new(());
    let (parts, _) = request.into_parts();
    let tracker = ProviderTransferTracker::for_request(&parts);
    assert!(tracker.usage_policy_reservation.is_none());
    assert!(tracker.usage_policy_reservation_token().is_none());

    let original = json!({
        "candidate_index": 2,
        "plan_usage_reservation_token": "untrusted-seed-token",
        "plan_usage_reservation_deferred": true
    });
    let expected = json!({"candidate_index": 2});
    assert_eq!(
        attach_plan_usage_reservation_token(Some(original), None),
        Some(expected)
    );
    assert_eq!(attach_plan_usage_reservation_token(None, None), None);
}

#[test]
fn stream_candidate_watchdog_prefers_first_byte_timeout() {
    let report_context = json!({"upstream_is_stream": true});
    let timeout = resolve_stream_candidate_watchdog_timeout(
        &test_plan(Some(ExecutionTimeouts {
            first_byte_ms: Some(12_345),
            total_ms: Some(90_000),
            ..ExecutionTimeouts::default()
        })),
        Some(&report_context),
    );

    assert_eq!(timeout, Duration::from_millis(12_345));
}

#[test]
fn stream_candidate_watchdog_uses_default_when_timeouts_missing() {
    let timeout = resolve_stream_candidate_watchdog_timeout(&test_plan(None), None);

    assert_eq!(
        timeout,
        Duration::from_millis(DEFAULT_STREAM_FIRST_BYTE_WATCHDOG_TIMEOUT_MS)
    );
}

#[test]
fn stream_candidate_watchdog_ignores_total_timeout_for_stream_upstream() {
    let report_context = json!({"upstream_is_stream": true});
    let timeout = resolve_stream_candidate_watchdog_timeout(
        &test_plan(Some(ExecutionTimeouts {
            total_ms: Some(90_000),
            ..ExecutionTimeouts::default()
        })),
        Some(&report_context),
    );

    assert_eq!(
        timeout,
        Duration::from_millis(DEFAULT_STREAM_FIRST_BYTE_WATCHDOG_TIMEOUT_MS)
    );
}

#[test]
fn stream_candidate_watchdog_prefers_first_byte_timeout_when_upstream_non_stream() {
    let report_context = json!({"upstream_is_stream": false});
    let timeout = resolve_stream_candidate_watchdog_timeout(
        &test_plan(Some(ExecutionTimeouts {
            first_byte_ms: Some(12_345),
            total_ms: Some(599_000),
            ..ExecutionTimeouts::default()
        })),
        Some(&report_context),
    );

    assert_eq!(timeout, Duration::from_millis(12_345));
}

#[test]
fn stream_candidate_watchdog_ignores_total_timeout_when_upstream_non_stream() {
    let report_context = json!({"upstream_is_stream": false});
    let timeout = resolve_stream_candidate_watchdog_timeout(
        &test_plan(Some(ExecutionTimeouts {
            total_ms: Some(599_000),
            ..ExecutionTimeouts::default()
        })),
        Some(&report_context),
    );

    assert_eq!(
        timeout,
        Duration::from_millis(DEFAULT_STREAM_FIRST_BYTE_WATCHDOG_TIMEOUT_MS)
    );
}

#[test]
fn stream_candidate_watchdog_defaults_to_streaming_when_flag_missing() {
    let report_context = json!({});
    let timeout = resolve_stream_candidate_watchdog_timeout(
        &test_plan(Some(ExecutionTimeouts {
            first_byte_ms: Some(12_345),
            total_ms: Some(90_000),
            ..ExecutionTimeouts::default()
        })),
        Some(&report_context),
    );

    assert_eq!(timeout, Duration::from_millis(12_345));
}

#[test]
fn upstream_execution_stream_hold_mode_defaults_to_first_body() {
    assert_eq!(
        parse_upstream_execution_stream_hold_mode(""),
        UpstreamExecutionStreamHoldMode::FirstBody
    );
    assert_eq!(
        parse_upstream_execution_stream_hold_mode("first_body"),
        UpstreamExecutionStreamHoldMode::FirstBody
    );
    assert_eq!(
        parse_upstream_execution_stream_hold_mode("off"),
        UpstreamExecutionStreamHoldMode::Headers
    );
    assert_eq!(
        parse_upstream_execution_stream_hold_mode("response"),
        UpstreamExecutionStreamHoldMode::Response
    );
}

#[test]
fn unused_persistence_skips_pool_internal_candidates() {
    assert!(should_skip_unused_persistence(Some(&json!({
        "candidate_group_id": "pool-group",
        "pool_key_index": 0,
    }))));
    assert!(should_skip_unused_persistence(Some(&json!({
        "candidate_group_id": "pool-group",
        "pool_key_index": 1,
    }))));
    assert!(!should_skip_unused_persistence(Some(&json!({
        "candidate_group_id": "pool-group",
    }))));
    assert!(!should_skip_unused_persistence(Some(&json!({
        "candidate_index": 1,
    }))));
}

#[tokio::test]
async fn stream_candidate_watchdog_marks_failed_candidate_and_continues() {
    let writer = Arc::new(TestRequestCandidateWriter::default());
    let plan = test_plan(Some(ExecutionTimeouts {
        first_byte_ms: Some(25),
        ..ExecutionTimeouts::default()
    }));
    let report_context = test_report_context();
    let writer_for_task = writer.clone();

    let task = tokio::spawn(async move {
        execute_stream_candidate_with_watchdog(
            writer_for_task.as_ref(),
            "trace_watchdog",
            "claude_cli_stream",
            &plan,
            Some(&report_context),
            false,
            || {
                std::future::pending::<
                    Result<AiAttemptExecutionOutcome<Response<Body>>, GatewayError>,
                >()
            },
        )
        .await
    });

    tokio::time::sleep(Duration::from_millis(40)).await;
    let result = task.await.expect("watchdog task should join");
    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::Executed(
            AiAttemptExecutionOutcome::Retry {
                scope: AiAttemptRetryScope::Candidate,
                fallback_response: None,
            }
        ))
    ));

    let records = writer.records.lock().await;
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.status, RequestCandidateStatus::Failed);
    assert_eq!(record.status_code, None);
    assert_eq!(
        record.error_type.as_deref(),
        Some("local_stream_candidate_watchdog_timeout")
    );
    assert!(record
        .error_message
        .as_deref()
        .is_some_and(|message| message == "Stream first byte timeout"));
    assert_eq!(record.candidate_index, 2);
}

async fn assert_stream_candidate_retry_gets_fresh_first_byte_budget(
    provider_id: &str,
    key_id: &str,
    first_byte_ms: u64,
) {
    let writer = TestRequestCandidateWriter::default();
    let plan = test_plan(Some(ExecutionTimeouts {
        first_byte_ms: Some(100),
        ..ExecutionTimeouts::default()
    }));
    let report_context = test_report_context();

    let result = execute_stream_candidate_with_watchdog(
        &writer,
        "trace_watchdog_retry_budget",
        "claude_cli_stream",
        &plan,
        Some(&report_context),
        false,
        || {
            std::future::pending::<Result<AiAttemptExecutionOutcome<Response<Body>>, GatewayError>>(
            )
        },
    )
    .await;
    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::Executed(
            AiAttemptExecutionOutcome::Retry {
                scope: AiAttemptRetryScope::Candidate,
                fallback_response: None,
            }
        ))
    ));

    let mut next_plan = plan.clone();
    next_plan.candidate_id = Some("cand_watchdog_retry".to_string());
    next_plan.provider_id = provider_id.to_string();
    next_plan.key_id = key_id.to_string();
    next_plan.timeouts = Some(ExecutionTimeouts {
        first_byte_ms: Some(first_byte_ms),
        ..ExecutionTimeouts::default()
    });
    let mut next_report_context = report_context.clone();
    next_report_context["candidate_id"] = json!("cand_watchdog_retry");
    next_report_context["candidate_index"] = json!(3);

    let result = execute_stream_candidate_with_watchdog(
        &writer,
        "trace_watchdog_retry_budget",
        "claude_cli_stream",
        &next_plan,
        Some(&next_report_context),
        false,
        || async {
            tokio::time::sleep(Duration::from_millis(60)).await;
            Ok(AiAttemptExecutionOutcome::Responded(Response::new(
                Body::from("retry succeeded"),
            )))
        },
    )
    .await;
    assert!(
        matches!(
            result,
            Ok(StreamCandidateWatchdogOutcome::Executed(
                AiAttemptExecutionOutcome::Responded(_)
            ))
        ),
        "candidate {provider_id}/{key_id} must receive its own {first_byte_ms} ms budget"
    );

    let records = writer.records.lock().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].id, plan.candidate_id.as_deref().unwrap());
    assert_eq!(records[0].status, RequestCandidateStatus::Failed);
    assert_eq!(
        records[0].error_type.as_deref(),
        Some("local_stream_candidate_watchdog_timeout")
    );
}

#[tokio::test]
async fn stream_candidate_watchdog_failover_gets_fresh_first_byte_budget() {
    for first_byte_ms in [100, 75, 150] {
        assert_stream_candidate_retry_gets_fresh_first_byte_budget(
            "provider_next",
            "key_next",
            first_byte_ms,
        )
        .await;
    }
}

#[tokio::test]
async fn stream_candidate_watchdog_same_provider_retries_get_fresh_first_byte_budget() {
    for key_id in ["key_next", "key_id"] {
        assert_stream_candidate_retry_gets_fresh_first_byte_budget("provider_id", key_id, 100)
            .await;
    }
}

#[tokio::test]
async fn stream_candidate_watchdog_starts_first_byte_budget_after_admission() {
    let writer = TestRequestCandidateWriter::with_upstream_gate(1, Duration::from_secs(1));
    let held_permit = writer
        .upstream_gate
        .as_ref()
        .expect("test gate should exist")
        .try_acquire()
        .expect("test gate permit should acquire");
    let plan = test_plan(Some(ExecutionTimeouts {
        first_byte_ms: Some(50),
        ..ExecutionTimeouts::default()
    }));
    let report_context = test_report_context();

    let (result, ()) = tokio::join!(
        execute_stream_candidate_with_watchdog(
            &writer,
            "trace_watchdog_admission_budget",
            "claude_cli_stream",
            &plan,
            Some(&report_context),
            false,
            || async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                Ok(AiAttemptExecutionOutcome::Responded(Response::new(
                    Body::from("admitted candidate succeeded"),
                )))
            },
        ),
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(held_permit);
        },
    );

    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::Executed(
            AiAttemptExecutionOutcome::Responded(_)
        ))
    ));
    assert!(writer.records.lock().await.is_empty());
}

#[tokio::test]
async fn stream_candidate_watchdog_can_stop_on_transport_error() {
    let writer = Arc::new(TestRequestCandidateWriter::default());
    let plan = test_plan(Some(ExecutionTimeouts {
        first_byte_ms: Some(5),
        ..ExecutionTimeouts::default()
    }));
    let report_context = test_report_context();

    let result = execute_stream_candidate_with_watchdog(
        writer.as_ref(),
        "trace_watchdog_stop",
        "claude_cli_stream",
        &plan,
        Some(&report_context),
        true,
        || {
            std::future::pending::<Result<AiAttemptExecutionOutcome<Response<Body>>, GatewayError>>(
            )
        },
    )
    .await;

    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::TransportTimeout)
    ));
    let records = writer.records.lock().await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status_code, None);
    assert_eq!(
        records[0].error_type.as_deref(),
        Some("local_stream_candidate_watchdog_timeout")
    );
}

#[tokio::test]
async fn stream_candidate_watchdog_does_not_cancel_started_terminalization() {
    let writer = Arc::new(TestRequestCandidateWriter::default());
    let plan = test_plan(Some(ExecutionTimeouts {
        first_byte_ms: Some(5),
        ..ExecutionTimeouts::default()
    }));
    let report_context = test_report_context();

    let result = execute_stream_candidate_with_watchdog(
        writer.as_ref(),
        "trace_terminalization",
        "claude_cli_stream",
        &plan,
        Some(&report_context),
        true,
        || async {
            mark_stream_candidate_watchdog_terminal_started();
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(AiAttemptExecutionOutcome::Responded(Response::new(
                Body::from("terminal response"),
            )))
        },
    )
    .await;

    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::Executed(
            AiAttemptExecutionOutcome::Responded(_)
        ))
    ));
    assert!(writer.records.lock().await.is_empty());
}

#[tokio::test]
async fn stream_candidate_watchdog_does_not_relabel_execution_error_as_timeout() {
    let writer = Arc::new(TestRequestCandidateWriter::default());
    let plan = test_plan(None);
    let report_context = test_report_context();

    let result = execute_stream_candidate_with_watchdog(
        writer.as_ref(),
        "trace_execution_error",
        "claude_cli_stream",
        &plan,
        Some(&report_context),
        true,
        || async {
            Err(GatewayError::UpstreamUnavailable {
                trace_id: "trace_execution_error".to_string(),
                message: "upstream connect failed".to_string(),
            })
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(GatewayError::UpstreamUnavailable { message, .. })
            if message == "upstream connect failed"
    ));
    assert!(writer.records.lock().await.is_empty());
}

#[tokio::test]
async fn stream_candidate_upstream_execution_admission_timeout_marks_failed_and_continues() {
    let writer = Arc::new(TestRequestCandidateWriter::with_upstream_gate(
        1,
        Duration::from_millis(1),
    ));
    let _held_permit = writer
        .upstream_gate
        .as_ref()
        .expect("test gate should exist")
        .try_acquire()
        .expect("test gate permit should acquire");
    let plan = test_plan(None);
    let report_context = test_report_context();

    let result = execute_stream_candidate_with_watchdog(
        writer.as_ref(),
        "trace_admission",
        "claude_cli_stream",
        &plan,
        Some(&report_context),
        false,
        || async {
            panic!("execute future should not run while upstream execution gate is saturated")
        },
    )
    .await;

    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::Executed(
            AiAttemptExecutionOutcome::Retry {
                scope: AiAttemptRetryScope::Candidate,
                fallback_response: None,
            }
        ))
    ));
    let records = writer.records.lock().await;
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.status, RequestCandidateStatus::Failed);
    assert_eq!(
        record.status_code,
        Some(http::StatusCode::TOO_MANY_REQUESTS.as_u16())
    );
    assert_eq!(
        record.error_type.as_deref(),
        Some("gateway_admission_timeout")
    );
    assert!(record
        .error_message
        .as_deref()
        .is_some_and(|message| message.contains(UPSTREAM_EXECUTION_GATE_NAME)));
    assert_eq!(record.candidate_index, 2);
}

#[tokio::test]
async fn stream_candidate_target_admission_timeout_continues_without_duplicate_record() {
    let writer = Arc::new(TestRequestCandidateWriter::default());
    let plan = test_plan(None);
    let report_context = test_report_context();

    let result = execute_stream_candidate_with_watchdog(
        writer.as_ref(),
        "trace_target_admission",
        "claude_cli_stream",
        &plan,
        Some(&report_context),
        false,
        || async {
            Err(GatewayError::AdmissionTimeout {
                trace_id: "trace_target_admission".to_string(),
                gate: UPSTREAM_TARGET_GATE_NAME,
                queue_budget_ms: 5,
            })
        },
    )
    .await;

    assert!(matches!(
        result,
        Ok(StreamCandidateWatchdogOutcome::Executed(
            AiAttemptExecutionOutcome::Retry {
                scope: AiAttemptRetryScope::Candidate,
                fallback_response: None,
            }
        ))
    ));
    assert!(writer.records.lock().await.is_empty());
}
    #[tokio::test]
    async fn exhaustion_returns_the_last_preserved_upstream_error() {
        let state = AppState::new().unwrap();
        let statuses = vec![500u16, 503, 429];
        let attempts = statuses
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let mut plan = test_plan(None);
                plan.provider_id = format!("provider-{index}");
                plan.key_id = format!("key-{index}");
                TransferTestAttempt {
                    label: "candidate",
                    plan,
                    report_context: json!({
                        "local_failover_policy": {
                            "max_transfer_count": 0,
                            "max_transfer_timeout_seconds": 0
                        }
                    }),
                }
            })
            .collect::<Vec<_>>();
        let port = TransferTestPort::with_retry_fallbacks(&state, statuses);
        let mut source = TransferTestAttemptSource {
            attempts: attempts.into(),
            skipped_providers: Vec::new(),
        };

        let outcome = run_dynamic_attempt_loop(
            &port,
            &mut source,
            "exhaustion-passthrough",
            "test",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        let LocalExecutionRequestOutcome::Responded(response) = outcome else {
            panic!("the last preserved upstream error must reach the client");
        };
        assert_eq!(response.status().as_u16(), 429);
        assert!(crate::executor::is_deferred_upstream_response(&response));
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body should read");
        assert_eq!(body.as_ref(), b"upstream error 429");
    }

    #[tokio::test]
    async fn exhaustion_without_preserved_errors_reports_exhausted() {
        let state = AppState::new().unwrap();
        let mut plan = test_plan(None);
        plan.provider_id = "provider-a".to_string();
        plan.key_id = "key-a".to_string();
        let mut source = TransferTestAttemptSource {
            attempts: std::iter::once(TransferTestAttempt {
                label: "candidate",
                plan,
                report_context: json!({
                    "local_failover_policy": {
                        "max_transfer_count": 0,
                        "max_transfer_timeout_seconds": 0
                    }
                }),
            })
            .collect(),
            skipped_providers: Vec::new(),
        };
        let port = TransferTestPort::new(&state);

        let outcome = run_dynamic_attempt_loop(
            &port,
            &mut source,
            "exhaustion-without-fallback",
            "test",
            Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert!(
            matches!(outcome, LocalExecutionRequestOutcome::Exhausted(_)),
            "without a preserved response the loop must report exhaustion"
        );
    }
}
