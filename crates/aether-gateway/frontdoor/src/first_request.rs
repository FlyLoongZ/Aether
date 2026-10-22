//! Connection-level deadline for the first request header block.
//!
//! Hyper's HTTP/1 timer starts only after the auto protocol detector has
//! finished, and HTTP/2 has no equivalent header timer. Racing the whole
//! connection until the first request reaches the service closes that gap
//! without imposing a deadline on request or response bodies.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;

#[derive(Clone)]
pub struct FirstRequestGate {
    seen: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl FirstRequestGate {
    pub fn new() -> Self {
        Self {
            seen: Arc::new(AtomicBool::new(false)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn mark_seen(&self) {
        if !self.seen.swap(true, Ordering::Release) {
            self.notify.notify_one();
        }
    }

    pub fn is_seen(&self) -> bool {
        self.seen.load(Ordering::Acquire)
    }
}

impl Default for FirstRequestGate {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct FirstRequestService<S> {
    inner: S,
    gate: FirstRequestGate,
}

impl<S> FirstRequestService<S> {
    /// Build a service and the gate that records the first request.
    pub fn new(inner: S) -> (Self, FirstRequestGate) {
        let gate = FirstRequestGate::new();
        (
            Self {
                inner,
                gate: gate.clone(),
            },
            gate,
        )
    }
}

impl<S, Req> tower::Service<Req> for FirstRequestService<S>
where
    S: tower::Service<Req>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Req) -> Self::Future {
        self.gate.mark_seen();
        self.inner.call(request)
    }
}

/// Drive one Hyper connection while enforcing the deadline for its first
/// request. The timeout is deliberately limited to protocol detection and the
/// initial headers; after the first request reaches the service, body and
/// response streaming are not interrupted by this helper.
pub async fn drive_first_request_connection<F, E>(
    connection: F,
    gate: FirstRequestGate,
    timeout: Duration,
    timeout_message: Option<&'static str>,
) -> Result<(), E>
where
    F: std::future::Future<Output = Result<(), E>>,
{
    if gate.is_seen() {
        return connection.await;
    }

    let mut connection = Box::pin(connection);
    let timeout = tokio::time::sleep(timeout);
    tokio::pin!(timeout);
    let notified = gate.notify.notified();
    tokio::pin!(notified);

    tokio::select! {
        result = &mut connection => result,
        _ = &mut timeout => {
            if gate.is_seen() {
                (&mut connection).await
            } else {
                if let Some(message) = timeout_message {
                    tracing::debug!("{message}");
                }
                Ok(())
            }
        }
        _ = &mut notified => (&mut connection).await,
    }
}
