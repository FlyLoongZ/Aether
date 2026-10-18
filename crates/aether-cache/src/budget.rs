//! Process-wide byte ledger shared by the caches.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Ledger every cache shares unless it installs one of its own.
const DEFAULT_PROCESS_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const PROCESS_BUDGET_ENV: &str = "AETHER_CACHE_BUDGET_BYTES";

/// Shared retention budget, in bytes.
///
/// This is an **admission** ledger, not a reclaimer: a charge either fits or is
/// refused, and a cache whose charge is refused never asks another cache to
/// give bytes back. Keeping any single cache small is `capacity_bytes`' job;
/// the ledger only bounds their sum, so the process can answer "how much is the
/// cache layer holding" without summing guesses.
///
/// A cache that cannot charge a value still returns it to the caller — the
/// value simply does not become resident.
#[derive(Debug)]
pub struct CacheBudget {
    limit: usize,
    used: AtomicUsize,
    peak: AtomicUsize,
    rejections: AtomicUsize,
}

impl CacheBudget {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            used: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejections: AtomicUsize::new(0),
        }
    }

    /// The ledger used by caches that do not install their own, so the sum of
    /// every cache's retention is bounded by one declared number.
    ///
    /// The limit is read once from `AETHER_CACHE_BUDGET_BYTES`; a value that
    /// does not parse or is zero falls back to 256 MiB.
    pub fn process() -> Arc<CacheBudget> {
        static PROCESS: OnceLock<Arc<CacheBudget>> = OnceLock::new();
        Arc::clone(PROCESS.get_or_init(|| {
            let limit = std::env::var(PROCESS_BUDGET_ENV)
                .ok()
                .and_then(|value| value.trim().parse::<usize>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_PROCESS_BUDGET_BYTES);
            Arc::new(CacheBudget::new(limit))
        }))
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// How many values were refused retention because the ledger was full.
    ///
    /// Counted once per refused value, never once per attempt: making room for
    /// one insert can retry the charge many times.
    pub fn rejections(&self) -> usize {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Charges `bytes` if they fit.
    pub(crate) fn try_charge(&self, bytes: usize) -> bool {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(bytes);
            if next > self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak.fetch_max(next, Ordering::Relaxed);
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Records one value that was not retained because the ledger was full.
    pub(crate) fn record_rejection(&self) {
        self.rejections.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn uncharge(&self, bytes: usize) {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            // Every release must have a matching charge; a saturating subtraction
            // would hide a drifted ledger until it silently stopped bounding
            // anything.
            debug_assert!(
                current >= bytes,
                "releasing {bytes} bytes while only {current} are charged"
            );
            let next = current.saturating_sub(bytes);
            match self.used.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CacheBudget;

    #[test]
    fn a_charge_that_does_not_fit_is_refused() {
        let budget = CacheBudget::new(100);

        assert!(budget.try_charge(60));
        assert!(budget.try_charge(40));
        assert_eq!(100, budget.used());
        assert!(!budget.try_charge(1));
        assert_eq!(100, budget.peak());
        // The ledger reports what it holds, not why a caller gave up.
        assert_eq!(0, budget.rejections());
        assert_eq!(100, budget.used());
    }

    #[test]
    fn a_refused_value_is_counted_once() {
        let budget = CacheBudget::new(100);

        assert!(!budget.try_charge(200));
        budget.record_rejection();
        assert_eq!(1, budget.rejections());
    }

    #[test]
    fn releasing_makes_room_again() {
        let budget = CacheBudget::new(100);

        assert!(budget.try_charge(100));
        budget.uncharge(100);
        assert_eq!(0, budget.used());
        assert!(budget.try_charge(100));
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "releasing")]
    fn releasing_more_than_charged_is_a_bug_not_a_silent_zero() {
        let budget = CacheBudget::new(100);

        budget.uncharge(50);
    }
}
