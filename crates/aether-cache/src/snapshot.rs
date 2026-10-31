//! Whole-table snapshots with coalesced reloads.

use crate::once_map::{OnceMap, OnceMapError};
use crate::weight::CacheWeight;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

/// One version of a whole-table read.
///
/// Callers get an `Arc` to the current version and a version is replaced as a
/// unit, so a read can never see half of one configuration and half of another.
///
/// The implementation is a [`OnceMap`] with a single key, on purpose: it already
/// provides everything a snapshot needs and that a hand-written version kept
/// getting wrong.
///
/// - **An invalidation outlives an older load.** A load that started before
///   `invalidate` (or `store`) carries the generation it started under, so its
///   result is handed to its own caller but never installed. A write can no
///   longer be followed by the pre-write table appearing with a fresh
///   timestamp.
/// - **Reloads are coalesced, failures included.** Concurrent misses share one
///   loader run and its outcome, so a backend outage costs one attempt per wave
///   instead of one attempt per caller.
/// - **A load is bounded.** [`Snapshot::with_load_timeout`] turns a hung loader
///   into a bounded failure instead of a lock everyone queues behind.
/// - **A failed reload keeps the previous version.** Only a successful load
///   replaces the stored one.
pub struct Snapshot<T, E> {
    inner: OnceMap<(), T, E>,
}

impl<T, E> std::fmt::Debug for Snapshot<T, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Snapshot")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<T, E> Default for Snapshot<T, E>
where
    T: CacheWeight,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<T, E> Snapshot<T, E>
where
    T: CacheWeight,
{
    pub fn new() -> Self {
        Self {
            inner: OnceMap::<(), T, E>::builder().build(),
        }
    }

    /// A snapshot whose loads fail after `timeout` instead of holding the
    /// reload slot forever.
    pub fn with_load_timeout(timeout: Duration) -> Self {
        Self {
            inner: OnceMap::<(), T, E>::builder().load_timeout(timeout).build(),
        }
    }

    /// The current version, whatever its age.
    pub fn peek(&self) -> Option<Arc<T>> {
        self.inner.peek_within(&(), Duration::MAX)
    }

    /// Replaces the version, as from a write path that already loaded it.
    ///
    /// A load that is still running is invalidated, so it cannot install the
    /// version it read before this call.
    pub fn store(&self, value: T) {
        self.inner.publish((), value, Duration::MAX);
    }

    /// Drops the version; the next read reloads it.
    pub fn invalidate(&self) {
        self.inner.invalidate(&());
    }
}

impl<T, E> Snapshot<T, E>
where
    T: CacheWeight + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    /// A version younger than `fresh_for`, or one reload shared by every caller
    /// that arrived while it ran.
    ///
    /// `fresh_for` decides freshness, not retention: the loaded version is kept
    /// until a write replaces it or the next reload after it goes stale. A
    /// caller that always wants a reload passes
    /// [`Duration::ZERO`](Duration::ZERO) and still gets a usable `peek`.
    pub async fn get_or_load<F, Fut>(
        &self,
        fresh_for: Duration,
        load: F,
    ) -> Result<Arc<T>, OnceMapError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.inner.peek_within(&(), fresh_for) {
            return Ok(value);
        }
        self.inner.reload((), Duration::MAX, load).await
    }
}

#[cfg(test)]
mod tests {
    use super::{OnceMapError, Snapshot};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    const FRESH_FOR: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn a_repeat_read_is_served_from_the_version() {
        let snapshot: Snapshot<u32, std::convert::Infallible> = Snapshot::new();
        let loads = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let loads = Arc::clone(&loads);
            let value = snapshot
                .get_or_load(FRESH_FOR, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(7_u32)
                })
                .await
                .unwrap();
            assert_eq!(7, *value);
        }

        assert_eq!(1, loads.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn concurrent_cold_reads_share_one_load() {
        let snapshot = Arc::new(Snapshot::<u32, std::convert::Infallible>::new());
        let loads = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..16 {
            let snapshot = Arc::clone(&snapshot);
            let loads = Arc::clone(&loads);
            tasks.push(tokio::spawn(async move {
                snapshot
                    .get_or_load(FRESH_FOR, || async move {
                        loads.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok::<u32, std::convert::Infallible>(1_u32)
                    })
                    .await
                    .unwrap()
            }));
        }
        for task in tasks {
            assert_eq!(1, *task.await.unwrap());
        }

        assert_eq!(1, loads.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_stale_version_is_reloaded() {
        let snapshot: Snapshot<u32, std::convert::Infallible> = Snapshot::new();
        snapshot.store(1);
        tokio::time::sleep(Duration::from_millis(2)).await;

        let value = snapshot
            .get_or_load(Duration::ZERO, || async {
                Ok::<u32, std::convert::Infallible>(2)
            })
            .await
            .unwrap();

        assert_eq!(2, *value);
        assert_eq!(2, *snapshot.peek().unwrap());
    }

    #[tokio::test]
    async fn invalidate_forces_a_reload() {
        let snapshot: Snapshot<u32, std::convert::Infallible> = Snapshot::new();
        snapshot.store(1);
        snapshot.invalidate();

        assert!(snapshot.peek().is_none());
        let value = snapshot
            .get_or_load(FRESH_FOR, || async {
                Ok::<u32, std::convert::Infallible>(2_u32)
            })
            .await
            .unwrap();
        assert_eq!(2, *value);
    }

    #[tokio::test]
    async fn a_failed_reload_keeps_the_previous_version() {
        let snapshot: Snapshot<u32, &'static str> = Snapshot::new();
        snapshot.store(1);

        let error = snapshot
            .get_or_load(Duration::ZERO, || async { Err::<u32, &str>("down") })
            .await
            .unwrap_err();
        assert_eq!(OnceMapError::Load("down"), error);
        assert_eq!(1, *snapshot.peek().unwrap());
    }

    /// A write that lands while a load runs must win: the load read the table
    /// before the write, so installing it would resurrect the old version.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_write_that_lands_during_a_load_wins() {
        let snapshot = Arc::new(Snapshot::<String, &'static str>::new());
        snapshot.store("v1".to_string());

        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(Semaphore::new(0));
        let load = {
            let snapshot = Arc::clone(&snapshot);
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                snapshot
                    .get_or_load(Duration::ZERO, || async move {
                        started.notify_one();
                        let _permit = gate.acquire().await.expect("gate is never closed");
                        Ok::<_, &str>("stale-read".to_string())
                    })
                    .await
            })
        };
        started.notified().await;

        snapshot.store("written".to_string());
        gate.add_permits(1);
        load.await.unwrap().unwrap();

        assert_eq!(
            Some("written"),
            snapshot.peek().map(|value| (*value).clone()).as_deref(),
            "the load started before the write and must not be installed"
        );
    }

    /// A failing load is shared with every waiter instead of being retried once
    /// per caller, which is what keeps an outage from turning into a queue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_load_is_shared_with_every_waiter() {
        let snapshot = Arc::new(Snapshot::<u32, &'static str>::new());
        let attempts = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let snapshot = Arc::clone(&snapshot);
            let attempts = Arc::clone(&attempts);
            tasks.push(tokio::spawn(async move {
                snapshot
                    .get_or_load(FRESH_FOR, || async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        Err::<u32, &'static str>("down")
                    })
                    .await
            }));
        }
        for task in tasks {
            assert_eq!(OnceMapError::Load("down"), task.await.unwrap().unwrap_err());
        }

        assert_eq!(1, attempts.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_slow_load_times_out() {
        let snapshot: Snapshot<u32, &'static str> =
            Snapshot::with_load_timeout(Duration::from_millis(20));

        let error = snapshot
            .get_or_load(FRESH_FOR, || async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok::<u32, &str>(1)
            })
            .await
            .unwrap_err();

        assert!(matches!(error, OnceMapError::LoadTimeout { .. }));
        assert!(snapshot.peek().is_none());
    }
}
