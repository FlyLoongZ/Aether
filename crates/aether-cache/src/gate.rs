//! Short-window throttling.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// "Do this at most once per window, per key."
///
/// The window starts when a key enters it; there is no explicit exit. An
/// entry's window is stored with it, so reclamation never depends on which
/// window a later caller happens to pass.
///
/// Expiry is indexed by deadline rather than by insertion order. A key with a
/// long window therefore cannot keep its already-expired neighbours resident:
/// [`Gate::peek`] and [`Gate::try_enter`] reclaim whatever is due, in
/// `O(expired * log n)`, and a gate with only short windows stays empty when it
/// is idle. The index doubles as the eviction order when a [`Gate`] is given a
/// [`max_entries`](Gate::with_max_entries) cap.
pub struct Gate<K, V = ()> {
    state: Mutex<State<K, V>>,
}

struct State<K, V> {
    entries: HashMap<K, Windowed<V>>,
    /// `(deadline, seq) -> key`. The `seq` makes a slot identify the entry that
    /// created it, so a replaced key never has its successor removed by a stale
    /// index node; both are retired together when the entry is.
    expiry: BTreeMap<(u128, u64), K>,
    /// Anchor for the relative deadlines, so `Instant + Duration` overflow can
    /// never enter the comparison.
    base: Instant,
    seq: u64,
    max_entries: Option<usize>,
}

struct Windowed<V> {
    entered_nanos: u128,
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
                expiry: BTreeMap::new(),
                base: Instant::now(),
                seq: 0,
                max_entries: None,
            }),
        }
    }
}

impl<K, V> Gate<K, V>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Caps how many keys the gate retains, evicting the earliest deadline
    /// first once the cap is reached.
    ///
    /// Without a cap the gate is bounded only by "distinct keys within the
    /// longest window anyone entered", which is fine for key spaces that come
    /// from configuration and not for ones that come from requests.
    pub fn with_max_entries(max_entries: usize) -> Self {
        let gate = Self::default();
        if let Ok(mut state) = gate.state.lock() {
            state.max_entries = Some(max_entries.max(1));
        }
        gate
    }

    /// Enters the window if the key is not already inside it. A re-entry is a
    /// no-op that neither extends the window nor replaces the stored value.
    ///
    /// A zero window never blocks and never retains anything.
    pub fn try_enter(&self, key: K, window: Duration, value: V) -> bool {
        self.enter_inner(key, window, value)
    }

    /// Reads the stored value without entering or restarting the window.
    ///
    /// The caller may narrow the window (an entry seen through a tighter window
    /// is hidden), but the entry's own window always applies. Reclamation rides
    /// on the read, so an idle gate does not hold expired keys until the next
    /// write.
    pub fn peek<Q>(&self, key: &Q, window: Duration) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = lock(&self.state);
        let now = now_nanos(state.base);
        state.reap(now);
        let entry = state.entries.get(key)?;
        let age = now.saturating_sub(entry.entered_nanos);
        (age <= window.min(entry.window).as_nanos()).then(|| entry.value.clone())
    }

    /// Reads the stored value using the window it was entered with.
    ///
    /// This is what a caller wants when it is validating *its own* earlier
    /// admission: a later configuration change to the window must not make a
    /// live admission look expired.
    pub fn peek_stored_window<Q>(&self, key: &Q) -> Option<V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = lock(&self.state);
        let now = now_nanos(state.base);
        state.reap(now);
        let entry = state.entries.get(key)?;
        Some(entry.value.clone())
    }

    /// Drops the key so the next `try_enter` succeeds immediately.
    pub fn release<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let mut state = lock(&self.state);
        if let Some(entry) = state.entries.remove(key) {
            state.expiry.remove(&(deadline_of(&entry), entry.seq));
        }
    }

    pub fn clear(&self) {
        let mut state = lock(&self.state);
        state.entries.clear();
        state.expiry.clear();
    }

    pub fn len(&self) -> usize {
        lock(&self.state).entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn enter_inner(&self, key: K, window: Duration, value: V) -> bool {
        let mut state = lock(&self.state);
        let now = now_nanos(state.base);

        // Make room for a key that is not already present before touching it,
        // so the cap counts the entry being replaced only once.
        let at_capacity = state
            .max_entries
            .is_some_and(|max| state.entries.len() >= max);
        if at_capacity && !state.entries.contains_key(&key) {
            state.evict_earliest();
        }

        if let Some(entry) = state.entries.get(&key) {
            if now < deadline_of(entry) {
                return false;
            }
        }
        if let Some(entry) = state.entries.remove(&key) {
            state.expiry.remove(&(deadline_of(&entry), entry.seq));
        }

        state.seq = state.seq.wrapping_add(1);
        let seq = state.seq;
        let entry = Windowed {
            entered_nanos: now,
            window,
            seq,
            value,
        };
        state.expiry.insert((deadline_of(&entry), seq), key.clone());
        state.entries.insert(key, entry);
        // A zero window is already due; retire it so it never becomes resident.
        state.reap(now);
        true
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
    /// Drops every entry whose deadline has passed.
    ///
    /// The index is deadline-ordered, so a long window at the front of the map
    /// cannot block reclamation of shorter ones behind it.
    fn reap(&mut self, now: u128) {
        loop {
            let due = self
                .expiry
                .keys()
                .next()
                .is_some_and(|(deadline, _)| *deadline <= now);
            if !due {
                break;
            }
            let ((_, seq), key) = self.expiry.pop_first().expect("checked above");
            if self.entries.get(&key).is_some_and(|entry| entry.seq == seq) {
                self.entries.remove(&key);
            }
        }
    }

    /// Evicts the entry with the earliest deadline, skipping stale index nodes.
    fn evict_earliest(&mut self) {
        while let Some(((_, seq), key)) = self.expiry.pop_first() {
            if self.entries.get(&key).is_some_and(|entry| entry.seq == seq) {
                self.entries.remove(&key);
                return;
            }
        }
    }
}

fn deadline_of<V>(entry: &Windowed<V>) -> u128 {
    entry.entered_nanos.saturating_add(entry.window.as_nanos())
}

/// Nanoseconds since the gate was created.
///
/// Relative deadlines keep `Instant` arithmetic out of the comparison: a window
/// long enough to overflow `Instant::checked_add` still orders correctly.
fn now_nanos(base: Instant) -> u128 {
    Instant::now().saturating_duration_since(base).as_nanos()
}

fn lock<K, V>(state: &Mutex<State<K, V>>) -> std::sync::MutexGuard<'_, State<K, V>> {
    match state.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
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
    fn a_zero_window_never_blocks_and_never_retains() {
        let gate = Gate::new();
        assert!(gate.enter("a".to_string(), Duration::ZERO));
        assert!(gate.enter("a".to_string(), Duration::ZERO));
        assert_eq!(0, gate.len());
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
        assert_eq!(
            Some(7),
            gate.peek_stored_window(&"a".to_string()),
            "the stored window is the one the entry was admitted with"
        );
    }

    #[test]
    fn release_allows_the_next_entry() {
        let gate = Gate::new();
        let window = Duration::from_secs(60);

        assert!(gate.enter("a".to_string(), window));
        gate.release(&"a".to_string());
        assert!(gate.enter("a".to_string(), window));
        assert_eq!(1, gate.len(), "release leaves no residue behind");
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

    /// A long-window key must not keep shorter-lived keys resident: that was the
    /// whole point of indexing by deadline instead of by insertion order.
    #[tokio::test]
    async fn a_long_window_does_not_pin_shorter_ones() {
        let gate = Gate::new();
        assert!(gate.enter("long".to_string(), Duration::from_secs(3600)));

        for index in 0..2000 {
            assert!(gate.enter(format!("short-{index}"), Duration::from_millis(1)));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Any read reclaims everything that is due, wherever it sits.
        assert_eq!(
            None,
            gate.peek(&"short-0".to_string(), Duration::from_millis(1))
        );
        assert_eq!(1, gate.len());
        assert_eq!(
            Some(()),
            gate.peek(&"long".to_string(), Duration::from_secs(3600))
        );
    }

    /// The optional cap evicts by deadline, so the gate stays bounded even when
    /// every key is still live.
    #[test]
    fn a_capped_gate_evicts_the_earliest_deadline() {
        let gate = Gate::with_max_entries(3);
        let long = Duration::from_secs(3600);
        let short = Duration::from_secs(1);

        assert!(gate.enter("a".to_string(), short));
        assert!(gate.enter("b".to_string(), long));
        assert!(gate.enter("c".to_string(), long));
        assert_eq!(3, gate.len());

        // A fourth entry pushes out the earliest deadline, and only that one.
        assert!(gate.enter("d".to_string(), long));
        assert_eq!(3, gate.len());
        assert_eq!(None, gate.peek(&"a".to_string(), short));
        assert!(gate.peek(&"b".to_string(), long).is_some());
        assert!(gate.peek(&"d".to_string(), long).is_some());
    }

    /// Re-entering an expired key must replace its index slot, not stack a
    /// second one, or the cap would evict live entries early.
    #[tokio::test]
    async fn re_entering_an_expired_key_replaces_its_slot() {
        let gate = Gate::with_max_entries(2);
        let short = Duration::from_millis(10);

        assert!(gate.enter("a".to_string(), short));
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(gate.enter("b".to_string(), Duration::from_secs(60)));
        assert!(gate.enter("a".to_string(), Duration::from_secs(60)));

        // Two live entries, two index slots: the expired slot for "a" was
        // retired rather than left behind.
        assert_eq!(2, gate.len());
        assert_eq!(
            2,
            gate.state.lock().expect("state should lock").expiry.len()
        );

        // The cap evicts the earliest deadline, which is "b" (entered first).
        assert!(gate.enter("c".to_string(), Duration::from_secs(60)));
        assert_eq!(2, gate.len());
        assert!(gate
            .peek(&"a".to_string(), Duration::from_secs(60))
            .is_some());
        assert!(gate
            .peek(&"b".to_string(), Duration::from_secs(60))
            .is_none());
    }
}
