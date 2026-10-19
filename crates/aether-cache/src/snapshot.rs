//! Single-version snapshots with coalesced reloads.
//!
//! A [`Snapshot`] holds one whole-table read. Callers get an `Arc` to the
//! current version and a version is replaced as a unit, so a read can never see
//! half of one configuration and half of another. Reloads are coalesced and a
//! failed reload leaves the previous version in place.

use std::future::Future;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// One version of a configuration read.
pub struct Snapshot<T> {
    state: RwLock<Option<Version<T>>>,
    reload: tokio::sync::Mutex<()>,
}

struct Version<T> {
    value: Arc<T>,
    loaded_at: Instant,
}

impl<T> Default for Snapshot<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> std::fmt::Debug for Snapshot<T> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Report the age only, so no bound is needed on `T`.
        let age = match self.state.read() {
            Ok(state) => state.as_ref().map(|version| version.loaded_at.elapsed()),
            Err(_) => None,
        };
        formatter
            .debug_struct("Snapshot")
            .field("loaded", &age.is_some())
            .field("age", &age)
            .finish()
    }
}

impl<T> Snapshot<T> {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(None),
            reload: tokio::sync::Mutex::new(()),
        }
    }

    /// The current version, whatever its age.
    pub fn peek(&self) -> Option<Arc<T>> {
        match self.state.read() {
            Ok(state) => state.as_ref().map(|version| Arc::clone(&version.value)),
            Err(poisoned) => poisoned
                .into_inner()
                .as_ref()
                .map(|version| Arc::clone(&version.value)),
        }
    }

    /// Replaces the version, as from a write path that already loaded it.
    pub fn store(&self, value: T) {
        self.replace(Arc::new(value));
    }

    /// Drops the version; the next read reloads it.
    pub fn invalidate(&self) {
        match self.state.write() {
            Ok(mut state) => *state = None,
            Err(poisoned) => *poisoned.into_inner() = None,
        }
    }

    /// A version younger than `fresh_for`, or one reload shared by every caller
    /// that arrived while it ran.
    pub async fn get_or_load<F, Fut, E>(&self, fresh_for: Duration, load: F) -> Result<Arc<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if let Some(value) = self.fresh_version(fresh_for) {
            return Ok(value);
        }

        // One reload at a time; everyone else waits for its result instead of
        // starting a second whole-table read.
        let _reload = self.reload.lock().await;
        if let Some(value) = self.fresh_version(fresh_for) {
            return Ok(value);
        }

        let value = Arc::new(load().await?);
        self.replace(Arc::clone(&value));
        Ok(value)
    }

    fn fresh_version(&self, fresh_for: Duration) -> Option<Arc<T>> {
        let state = self.state.read().ok()?;
        let version = state.as_ref()?;
        (version.loaded_at.elapsed() <= fresh_for).then(|| Arc::clone(&version.value))
    }

    fn replace(&self, value: Arc<T>) {
        let version = Version {
            value,
            loaded_at: Instant::now(),
        };
        match self.state.write() {
            Ok(mut state) => *state = Some(version),
            Err(poisoned) => *poisoned.into_inner() = Some(version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Snapshot;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    const FRESH_FOR: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn a_repeat_read_is_served_from_the_version() {
        let snapshot: Snapshot<u32> = Snapshot::new();
        let loads = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let loads = Arc::clone(&loads);
            let value = snapshot
                .get_or_load(FRESH_FOR, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok::<u32, std::convert::Infallible>(7_u32)
                })
                .await
                .unwrap();
            assert_eq!(7, *value);
        }

        assert_eq!(1, loads.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn concurrent_cold_reads_share_one_load() {
        let snapshot = Arc::new(Snapshot::<u32>::new());
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
        let snapshot: Snapshot<u32> = Snapshot::new();
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
        let snapshot: Snapshot<u32> = Snapshot::new();
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
        let snapshot: Snapshot<u32> = Snapshot::new();
        snapshot.store(1);

        let error = snapshot
            .get_or_load(Duration::ZERO, || async { Err::<u32, &str>("down") })
            .await
            .unwrap_err();
        assert_eq!("down", error);
        assert_eq!(1, *snapshot.peek().unwrap());
    }
}
