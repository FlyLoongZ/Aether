//! Short-window throttling.

use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// "Do this at most once per window, per key."
///
/// The window starts when a key enters it; there is no explicit exit. An
/// entry's window is stored with it, so reclamation never depends on which
/// window a later caller happens to pass.
pub struct Gate<K, V = ()> {
    state: Mutex<State<K, V>>,
}

struct State<K, V> {
    entries: HashMap<K, Windowed<V>>,
    /// Insertion order as `(key, seq)`; `seq` identifies the live entry so a
    /// superseded queue slot can be dropped without comparing instants.
    order: VecDeque<(K, u64)>,
    seq: u64,
}

struct Windowed<V> {
    entered_at: Instant,
    window: Duration,
    seq: u64,
    value: V,
}

impl<K, V> std::fmt::Debug for Gate<K, V> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Print the size only, so no bound is needed on `K` or `V`.
        let len = self
            .state
            .lock()
            .map(|state| state.entries.len())
            .unwrap_or(0);
        formatter.debug_struct("Gate").field("len", &len).finish()
    }
}

impl<K, V> Default for Gate<K, V> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                entries: HashMap::new(),
                order: VecDeque::new(),
                seq: 0,
            }),
        }
    }
}

impl<K, V> Gate<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    /// Holds at most the keys entered within their own window.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enters the window if the key is not already inside it. A re-entry is a
    /// no-op that neither extends the window nor replaces the stored value.
    pub fn try_enter(&self, key: K, window: Duration, value: V) -> bool {
        self.enter_inner(key, window, value).is_some()
    }

    /// Reads the stored value without entering or restarting the window.
    ///
    /// Reclamation rides on the read, so an idle gate does not hold expired
    /// keys until the next write.
    pub fn peek<Q>(&self, key: &Q, window: Duration) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let now = Instant::now();
        let Ok(mut state) = self.state.lock() else {
            return None;
        };
        state.reap(now);
        let entry = state.entries.get(key)?;
        let age = now.saturating_duration_since(entry.entered_at);
        (age <= window.min(entry.window)).then(|| entry.value.clone())
    }

    /// Drops the key so the next `try_enter` succeeds immediately.
    pub fn release<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Ok(mut state) = self.state.lock() {
            state.entries.remove(key);
        }
    }

    pub fn clear(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.entries.clear();
            state.order.clear();
        }
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .map(|state| state.entries.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn enter_inner(&self, key: K, window: Duration, value: V) -> Option<Instant> {
        let now = Instant::now();
        let Ok(mut state) = self.state.lock() else {
            return None;
        };

        if let Some(entry) = state.entries.get(&key) {
            if now.saturating_duration_since(entry.entered_at) <= entry.window {
                return None;
            }
        }

        state.seq = state.seq.wrapping_add(1);
        let seq = state.seq;
        state.entries.insert(
            key.clone(),
            Windowed {
                entered_at: now,
                window,
                seq,
                value,
            },
        );
        state.order.push_back((key, seq));
        state.reap(now);
        Some(now)
    }
}

impl<K> Gate<K, ()>
where
    K: Clone + Eq + Hash,
{
    /// Convenience for value-less gates.
    pub fn enter(&self, key: K, window: Duration) -> bool {
        self.try_enter(key, window, ())
    }
}

impl<K, V> State<K, V>
where
    K: Clone + Eq + Hash,
{
    fn reap(&mut self, now: Instant) {
        while let Some((key, seq)) = self.order.front().cloned() {
            let Some(entry) = self.entries.get(&key) else {
                self.order.pop_front();
                continue;
            };
            if entry.seq != seq {
                self.order.pop_front();
                continue;
            }
            if now.saturating_duration_since(entry.entered_at) <= entry.window {
                break;
            }
            self.order.pop_front();
            self.entries.remove(&key);
        }

        // Rebuild when the queue accumulated stale slots.
        if self.entries.is_empty() {
            self.order.clear();
        } else if self.order.len() > self.entries.len().saturating_mul(2).saturating_add(64) {
            self.order = self
                .entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.seq))
                .collect();
            self.order
                .make_contiguous()
                .sort_unstable_by_key(|(_, seq)| *seq);
        }

        // Returning bucket capacity matters after a burst: without this the
        // map keeps its high-water mark for the process lifetime.
        if self.entries.capacity() > self.entries.len().saturating_mul(4) + 64 {
            self.entries.shrink_to_fit();
            self.order.shrink_to_fit();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Gate;
    use std::time::Duration;

    #[test]
    fn first_entry_wins_and_repeats_are_ignored() {
        let gate = Gate::new();
        let window = Duration::from_secs(60);

        assert!(gate.enter("a".to_string(), window));
        assert!(!gate.enter("a".to_string(), window));
        assert!(gate.enter("b".to_string(), window));
    }

    #[test]
    fn a_zero_window_never_blocks() {
        let gate = Gate::new();
        assert!(gate.enter("a".to_string(), Duration::ZERO));
        assert!(gate.enter("a".to_string(), Duration::ZERO));
    }

    #[test]
    fn peek_reads_without_entering() {
        let gate = Gate::new();
        let window = Duration::from_secs(60);

        assert_eq!(None, gate.peek(&"a".to_string(), window));
        gate.try_enter("a".to_string(), window, 7_u64);
        assert_eq!(Some(7), gate.peek(&"a".to_string(), window));
        assert_eq!(1, gate.len());
    }

    #[tokio::test]
    async fn a_narrower_read_window_does_not_see_the_entry() {
        let gate = Gate::new();
        gate.try_enter("a".to_string(), Duration::from_secs(60), 7_u64);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // The narrower window hides the entry, the writer's window keeps it.
        assert_eq!(None, gate.peek(&"a".to_string(), Duration::from_millis(1)));
        assert_eq!(
            Some(7),
            gate.peek(&"a".to_string(), Duration::from_secs(60))
        );
    }

    #[test]
    fn release_allows_the_next_entry() {
        let gate = Gate::new();
        let window = Duration::from_secs(60);

        assert!(gate.enter("a".to_string(), window));
        gate.release(&"a".to_string());
        assert!(gate.enter("a".to_string(), window));
    }

    #[tokio::test]
    async fn expired_keys_are_reclaimed_on_the_next_entry() {
        let gate = Gate::new();
        let window = Duration::from_millis(20);

        assert!(gate.enter("a".to_string(), window));
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(gate.enter("a".to_string(), window));
    }

    #[tokio::test]
    async fn reads_reclaim_expired_keys_too() {
        let gate = Gate::new();
        let window = Duration::from_millis(20);

        assert!(gate.enter("a".to_string(), window));
        assert_eq!(1, gate.len());
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(None, gate.peek(&"a".to_string(), window));
        assert_eq!(0, gate.len());
    }
}
