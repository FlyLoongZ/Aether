//! Process-wide byte ledger shared by the caches.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Ledger every cache shares unless it installs one of its own.
const DEFAULT_PROCESS_BUDGET_BYTES: usize = 256 * 1024 * 1024;
const PROCESS_BUDGET_ENV: &str = "AETHER_CACHE_BUDGET_BYTES";

/// Shared retention budget, in bytes.
///
/// A per-cache `capacity_bytes` bounds one map; this bounds their sum. Caches
/// charge the weight of what they retain and release it when they evict, so the
/// process can answer "how much is the cache layer holding" without summing
/// guesses. A cache that cannot charge a value still returns it to the caller —
/// the value simply does not become resident.
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

    /// How many charges were refused because the ledger was full.
    pub fn rejections(&self) -> usize {
        self.rejections.load(Ordering::Relaxed)
    }

    pub fn over_limit(&self) -> bool {
        self.used() > self.limit
    }

    /// Charges `bytes` if they fit. Refusals are counted so a cache that keeps
    /// hitting the ceiling is visible.
    pub(crate) fn try_charge(&self, bytes: usize) -> bool {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let next = current.saturating_add(bytes);
            if next > self.limit {
                self.rejections.fetch_add(1, Ordering::Relaxed);
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

    pub(crate) fn uncharge(&self, bytes: usize) {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
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
    fn a_charge_that_does_not_fit_is_refused_and_counted() {
        let budget = CacheBudget::new(100);

        assert!(budget.try_charge(60));
        assert!(budget.try_charge(40));
        assert_eq!(100, budget.used());
        assert!(!budget.try_charge(1));
        assert_eq!(1, budget.rejections());
        assert_eq!(100, budget.peak());
        assert!(!budget.over_limit());
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
    fn releasing_more_than_charged_does_not_underflow() {
        let budget = CacheBudget::new(100);

        budget.uncharge(50);
        assert_eq!(0, budget.used());
    }
}
