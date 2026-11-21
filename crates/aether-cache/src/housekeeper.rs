//! Deadline-driven reclamation for every cache in the process.
//!
//! A [`OnceMap`](crate::OnceMap) reclaims on its own write path, which leaves one
//! hole: a cache that is only ever read — or not touched at all — never runs a
//! write, so entries that have passed their deadline stay resident and keep
//! their bytes charged to the shared budget. That is the state a gateway sits in
//! whenever traffic stops, and it is why a footprint measured after a burst does
//! not come back down.
//!
//! This closes it without giving every entry a timer. Each cache is indexed by
//! deadline, so it answers "when do I next have something to drop?" in
//! `O(log n)`; one task sleeps until the earliest of those answers and then asks
//! only the caches that are due. A process that is not caching anything parks in
//! a single `Notify::notified()` and costs nothing.
//!
//! Work is bounded per pass and never waits for a lock: a cache that is being
//! written is skipped and picked up on the next pass, and a cache with more
//! expired entries than one batch keeps its store lock for `REAP_BATCH` drops at
//! most.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tracing::{debug, warn};

/// Entries one pass may drop per cache, so a cache that fell far behind cannot
/// hold its store lock for a visible amount of time.
pub(crate) const REAP_BATCH: usize = 512;

/// Floor on the wait before retrying a cache that could not make progress: its
/// store lock was held, or an in-flight load pins a tombstone. Without it a pass
/// that achieves nothing would spin.
const MIN_RETRY: Duration = Duration::from_millis(50);

/// Ceiling on the sleep. No cache needs it, but a task that sleeps forever
/// because of one deadline it cannot act on is worse than a wakeup a minute.
const MAX_SLEEP: Duration = Duration::from_secs(60);

/// What one housekeeping attempt achieved.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Progress {
    /// Nothing is due any more.
    Done,
    /// The batch limit stopped this pass; more is already due.
    More,
    /// The cache was being written, so nothing was attempted.
    Busy,
}

/// One cache, as the housekeeper sees it.
pub(crate) trait Housekeeping: Send + Sync {
    /// The next instant this cache expects to have something to drop.
    ///
    /// `None` means "nothing scheduled" rather than "never": an invalidation or
    /// a write can arm one later.
    fn next_deadline(&self) -> Option<Instant>;

    /// Drops what is due right now.
    fn housekeep(&self, now: Instant) -> Progress;
}

static REGISTRY: LazyLock<Arc<Registry>> = LazyLock::new(|| Arc::new(Registry::default()));

#[derive(Default)]
struct Registry {
    caches: Mutex<Vec<Weak<dyn Housekeeping>>>,
    armed: Mutex<Armed>,
    wake: Notify,
    started: AtomicBool,
}

#[derive(Default)]
struct Armed {
    /// The instant the task is sleeping towards.
    sleeping: Option<Instant>,
    /// An earlier instant a write asked for while the task was between passes
    /// and could not yet compare against a sleep target.
    requested: Option<Instant>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Registers a cache with the process-wide housekeeper.
pub(crate) fn register(cache: Weak<dyn Housekeeping>) {
    REGISTRY.register(cache);
}

/// Tells the housekeeper that a cache now expects to have something to drop at
/// `at`.
///
/// Called from the insert path, which holds the store's write lock, so this only
/// ever takes the armed slot — the documented order is `store -> housekeeper`,
/// and the task never holds the armed slot while taking a store lock.
pub(crate) fn request_wake(at: Instant) {
    REGISTRY.request_wake(at);
}

/// Starts the housekeeper, once per process.
///
/// Idempotent. Does nothing when the caller has no Tokio runtime, so a plain
/// unit test can build caches without a background task appearing.
pub fn start_housekeeper() {
    if REGISTRY.started.swap(true, Ordering::AcqRel) {
        return;
    }
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        REGISTRY.started.store(false, Ordering::Release);
        debug!(
            event_name = "cache_housekeeper_not_started",
            log_type = "ops",
            "cache housekeeper not started: no tokio runtime available"
        );
        return;
    };
    debug!(
        event_name = "cache_housekeeper_started",
        log_type = "ops",
        "cache housekeeper started"
    );
    handle.spawn(run(Arc::clone(&REGISTRY)));
}

async fn run(registry: Arc<Registry>) {
    loop {
        let now = Instant::now();
        {
            // The task is running a pass, so a write that arms a deadline now
            // has no sleep target to compare against and is recorded instead.
            lock(&registry.armed).sleeping = None;
        }
        // A panic in one pass must not kill the only maintenance task: `started`
        // stays true forever afterwards, so nothing can restart it and every cache
        // silently stops reclaiming memory. Recover and keep going instead.
        let pass =
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| registry.pass(now))) {
                Ok(pass) => pass,
                Err(_) => {
                    warn!(
                        event_name = "cache_housekeeper_pass_panicked",
                        log_type = "ops",
                        "cache housekeeper pass panicked; continuing with the next pass"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    continue;
                }
            };
        let at = registry.arm(next_wake(now, &pass), now);
        if pass.more {
            // `next_wake` returns `now`, so the sleep branch would be ready
            // immediately and the loop would keep a worker busy draining the
            // backlog synchronously. Yield so other tasks on this worker run
            // between batches; the pass count is already bounded by REAP_BATCH.
            tokio::task::yield_now().await;
        }

        tokio::select! {
            _ = tokio::time::sleep_until(at.into()) => {}
            _ = registry.wake.notified() => {}
        }
    }
}

impl Registry {
    fn register(&self, cache: Weak<dyn Housekeeping>) {
        let mut caches = lock(&self.caches);
        // Building a cache is rare, so this is the cheapest place to drop the
        // weak references of caches that no longer exist.
        caches.retain(|cached| cached.strong_count() > 0);
        caches.push(cache);
    }

    /// One pass over every live cache. Never holds the registry lock while
    /// calling into a cache, so a cache's own locks cannot deadlock it.
    ///
    /// A cache is only asked to do work once its own deadline has arrived, which
    /// keeps the common case — dozens of caches, one of them due — down to a
    /// read lock per cache instead of a write lock.
    fn pass(&self, now: Instant) -> Pass {
        let mut pass = Pass::default();
        for cache in self.live() {
            let Some(at) = cache.next_deadline() else {
                continue;
            };
            if at > now {
                pass.note(at);
                continue;
            }

            if cache.housekeep(now) == Progress::More {
                pass.more = true;
            }
            // Re-read: a batch-limited cache still has its front in the past,
            // and one that finished has moved it forward.
            if let Some(at) = cache.next_deadline() {
                pass.note(at);
            }
        }
        pass
    }

    fn live(&self) -> Vec<Arc<dyn Housekeeping>> {
        let mut caches = lock(&self.caches);
        let mut live = Vec::with_capacity(caches.len());
        caches.retain(|cache| match cache.upgrade() {
            Some(cache) => {
                live.push(cache);
                true
            }
            None => false,
        });
        live
    }

    /// Fixes the instant to sleep towards and records it for `request_wake`.
    fn arm(&self, candidate: Instant, now: Instant) -> Instant {
        let mut armed = lock(&self.armed);
        let requested = armed.requested.take();
        let at = requested
            .map_or(candidate, |requested| requested.min(candidate))
            .max(now);
        armed.sleeping = Some(at);
        at
    }

    /// Records an earlier deadline a write wants the task to wake for.
    ///
    /// A hint that lands while the task is between passes is recorded rather
    /// than compared: the pass may already have read that cache's deadline, so
    /// the arm that follows is what has to pick it up.
    fn request_wake(&self, at: Instant) {
        let notify = {
            let mut armed = lock(&self.armed);
            if armed.sleeping.is_some_and(|sleeping| at >= sleeping) {
                // The sleep ends before this hint would matter.
                return;
            }
            armed.requested = Some(armed.requested.map_or(at, |current| current.min(at)));
            armed.sleeping
        };
        if notify.is_some() {
            self.wake.notify_one();
        }
    }
}

#[derive(Debug, Default)]
struct Pass {
    /// The earliest deadline any cache reported.
    next: Option<Instant>,
    /// A cache stopped at its batch limit.
    more: bool,
}

impl Pass {
    fn note(&mut self, at: Instant) {
        self.next = Some(self.next.map_or(at, |current| current.min(at)));
    }
}

fn next_wake(now: Instant, pass: &Pass) -> Instant {
    if pass.more {
        // The batch limit, not the clock, is what stopped the pass: come back
        // for the rest right away.
        return now;
    }
    let at = pass.next.unwrap_or(now + MAX_SLEEP).min(now + MAX_SLEEP);
    if at > now {
        return at;
    }
    // Something is due and the pass could not act on it — a store lock held by a
    // writer, or a tombstone pinned by an in-flight load. Back off rather than
    // spin on it.
    now + MIN_RETRY
}

/// Whether the process-wide registry holds this exact cache.
#[cfg(test)]
pub(crate) fn holds_for_tests<T: Housekeeping + 'static>(cache: &Arc<T>) -> bool {
    let target = Arc::as_ptr(cache) as *const ();
    REGISTRY
        .live()
        .iter()
        .any(|registered| Arc::as_ptr(registered) as *const () == target)
}

#[cfg(test)]
mod tests {
    use super::{next_wake, run, Housekeeping, Pass, Progress, Registry};
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, Weak};
    use std::time::{Duration, Instant};

    #[derive(Default)]
    struct StubCache {
        deadline: Mutex<Option<Instant>>,
        progress: AtomicU8,
        passes: AtomicUsize,
    }

    impl StubCache {
        fn reporting(deadline: Option<Instant>, progress: Progress) -> Arc<Self> {
            let cache = Self::default();
            *cache.deadline.lock().unwrap() = deadline;
            cache.progress.store(progress as u8, Ordering::Relaxed);
            Arc::new(cache)
        }
    }

    impl Housekeeping for StubCache {
        fn next_deadline(&self) -> Option<Instant> {
            *self.deadline.lock().unwrap()
        }

        fn housekeep(&self, _now: Instant) -> Progress {
            self.passes.fetch_add(1, Ordering::Relaxed);
            match self.progress.load(Ordering::Relaxed) {
                0 => Progress::Done,
                1 => Progress::More,
                _ => Progress::Busy,
            }
        }
    }

    fn register(registry: &Registry, cache: &Arc<StubCache>) -> Arc<StubCache> {
        registry.register(Arc::downgrade(cache) as Weak<dyn Housekeeping>);
        Arc::clone(cache)
    }

    /// End to end: the real loop, a real timer, and a cache nobody touches.
    ///
    /// It runs against its own registry so it neither depends on nor disturbs the
    /// process-wide one the other tests share.
    #[tokio::test]
    async fn the_loop_wakes_at_the_deadline_it_was_given() {
        let registry = Arc::new(Registry::default());
        let started = Instant::now();
        let cache = register(
            &registry,
            &StubCache::reporting(Some(started + Duration::from_millis(40)), Progress::Done),
        );

        let task = tokio::spawn(run(Arc::clone(&registry)));
        let mut worked = false;
        for _ in 0..200 {
            if cache.passes.load(Ordering::Relaxed) > 0 {
                worked = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        task.abort();

        assert!(worked, "the loop should have reached the due cache");
        assert!(
            started.elapsed() >= Duration::from_millis(30),
            "the loop must wait for the deadline instead of spinning"
        );
    }

    #[test]
    fn a_pass_reports_the_earliest_deadline() {
        let registry = Registry::default();
        let now = Instant::now();
        let late = register(
            &registry,
            &StubCache::reporting(Some(now + Duration::from_secs(30)), Progress::Done),
        );
        let early = register(
            &registry,
            &StubCache::reporting(Some(now + Duration::from_secs(1)), Progress::Done),
        );

        let pass = registry.pass(now);

        assert_eq!(Some(now + Duration::from_secs(1)), pass.next);
        assert!(!pass.more);
        // Neither is due, so neither store lock is touched.
        assert_eq!(0, late.passes.load(Ordering::Relaxed));
        assert_eq!(0, early.passes.load(Ordering::Relaxed));
    }

    #[test]
    fn a_due_cache_is_asked_to_work_and_the_rest_are_left_alone() {
        let registry = Registry::default();
        let now = Instant::now();
        let due = register(&registry, &StubCache::reporting(Some(now), Progress::Done));
        let later = register(
            &registry,
            &StubCache::reporting(Some(now + Duration::from_secs(5)), Progress::Done),
        );

        registry.pass(now);

        assert_eq!(1, due.passes.load(Ordering::Relaxed));
        assert_eq!(0, later.passes.load(Ordering::Relaxed));
    }

    #[test]
    fn a_cache_that_was_dropped_is_no_longer_visited() {
        let registry = Registry::default();
        let now = Instant::now();
        let cache = StubCache::reporting(None, Progress::Done);
        registry.register(Arc::downgrade(&cache) as std::sync::Weak<dyn Housekeeping>);
        drop(cache);

        registry.pass(now);

        assert_eq!(0, registry.live().len());
    }

    #[test]
    fn a_pass_reports_that_a_batch_limit_was_hit() {
        let registry = Registry::default();
        let now = Instant::now();
        let _cache = register(&registry, &StubCache::reporting(Some(now), Progress::More));

        let pass = registry.pass(now);

        assert!(pass.more);
        assert_eq!(now, next_wake(now, &pass));
    }

    #[test]
    fn a_hint_recorded_during_a_pass_is_picked_up_by_the_arm() {
        let registry = Registry::default();
        let now = Instant::now();
        let recorded = now + Duration::from_millis(5);

        // The task is between passes: there is no sleep target to compare with.
        registry.request_wake(recorded);
        let armed = registry.arm(now + Duration::from_secs(30), now);

        assert_eq!(recorded, armed);
    }

    #[test]
    fn a_hint_later_than_the_sleep_does_not_shorten_it() {
        let registry = Registry::default();
        let now = Instant::now();
        let sleeping = now + Duration::from_secs(1);
        registry.arm(sleeping, now);

        registry.request_wake(sleeping + Duration::from_secs(1));
        let armed = registry.arm(sleeping, now);

        assert_eq!(sleeping, armed);
    }

    #[test]
    fn the_arm_never_returns_an_instant_in_the_past() {
        let registry = Registry::default();
        let now = Instant::now();

        assert_eq!(now, registry.arm(now - Duration::from_secs(5), now));
    }

    #[test]
    fn a_pass_with_nothing_to_do_sleeps_to_the_horizon() {
        let now = Instant::now();
        let at = next_wake(now, &Pass::default());

        assert_eq!(now + super::MAX_SLEEP, at);
    }

    #[test]
    fn a_reported_deadline_is_what_the_task_waits_for() {
        let now = Instant::now();
        let deadline = now + Duration::from_millis(250);
        let pass = Pass {
            next: Some(deadline),
            ..Pass::default()
        };

        assert_eq!(deadline, next_wake(now, &pass));
    }

    #[test]
    fn a_batch_limit_wakes_the_task_again_immediately() {
        let now = Instant::now();
        let pass = Pass {
            next: Some(now + Duration::from_secs(30)),
            more: true,
        };

        assert_eq!(now, next_wake(now, &pass));
    }

    #[test]
    fn a_deadline_that_dropped_nothing_is_not_retried_in_a_tight_loop() {
        let now = Instant::now();
        let pass = Pass {
            next: Some(now - Duration::from_secs(1)),
            more: false,
        };

        assert_eq!(now + super::MIN_RETRY, next_wake(now, &pass));
    }

    #[test]
    fn a_far_deadline_is_capped_so_the_task_re_checks_occasionally() {
        let now = Instant::now();
        let pass = Pass {
            next: now.checked_add(Duration::from_secs(3600)),
            ..Pass::default()
        };

        assert_eq!(now + super::MAX_SLEEP, next_wake(now, &pass));
    }
}
