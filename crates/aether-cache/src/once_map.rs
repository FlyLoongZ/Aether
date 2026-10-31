//! Keyed cache with request coalescing and write-through invalidation.

use crate::budget::CacheBudget;
use crate::weight::CacheWeight;
use std::borrow::Borrow;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Optional callbacks for cache traffic. Function pointers keep this free of
/// allocation and cheap to copy.
#[derive(Clone, Copy, Default)]
pub struct CacheObserver {
    on_hit: Option<fn()>,
    on_miss: Option<fn()>,
    on_load: Option<fn()>,
    on_follower_wait: Option<fn()>,
}

impl CacheObserver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn on_hit(mut self, callback: fn()) -> Self {
        self.on_hit = Some(callback);
        self
    }

    pub fn on_miss(mut self, callback: fn()) -> Self {
        self.on_miss = Some(callback);
        self
    }

    pub fn on_load(mut self, callback: fn()) -> Self {
        self.on_load = Some(callback);
        self
    }

    pub fn on_follower_wait(mut self, callback: fn()) -> Self {
        self.on_follower_wait = Some(callback);
        self
    }

    fn hit(self) {
        if let Some(callback) = self.on_hit {
            callback();
        }
    }

    fn miss(self) {
        if let Some(callback) = self.on_miss {
            callback();
        }
    }

    fn load(self) {
        if let Some(callback) = self.on_load {
            callback();
        }
    }

    fn follower_wait(self) {
        if let Some(callback) = self.on_follower_wait {
            callback();
        }
    }
}

/// What a caller does once `max_inflight` loads are already running.
///
/// `Bypass` keeps availability: the caller runs its own loader without taking a
/// flight slot, and the result is still published. `Reject` sheds load and
/// surfaces [`OnceMapError::AdmissionSaturated`], which call sites that already
/// have a fallback can opt into.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Admission {
    #[default]
    Bypass,
    Reject,
}

/// Failures the cache reports on its own behalf. Loader failures arrive as
/// [`OnceMapError::Load`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OnceMapError<E> {
    /// The loader exceeded `load_timeout`.
    LoadTimeout { elapsed: Duration },
    /// `max_inflight` loads were already running and admission is `Reject`.
    AdmissionSaturated { inflight: usize },
    /// Returned by the loader.
    Load(E),
}

impl<E> OnceMapError<E> {
    /// Collapses the cache's own failures into the caller's error type.
    ///
    /// The cache can describe the two failures it raises itself but cannot
    /// construct the caller's error, so those are handed to a closure.
    pub fn or_else_load(self, cache_failure: impl FnOnce(String) -> E) -> E {
        match self {
            Self::Load(error) => error,
            Self::LoadTimeout { elapsed } => {
                cache_failure(format!("cache load timed out after {elapsed:?}"))
            }
            Self::AdmissionSaturated { inflight } => cache_failure(format!(
                "cache admission saturated with {inflight} loads in flight"
            )),
        }
    }
}

/// Generation marker captured when a load starts.
///
/// A load may only publish if the key still carries the stamp it captured, so
/// an invalidation or an application write in between discards the result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Stamp {
    epoch: u64,
    key_epoch: u64,
}

/// Backoff applied to background refreshes that keep failing.
#[derive(Clone, Copy, Debug)]
struct RefreshPolicy {
    base: Duration,
    max: Duration,
}

impl Default for RefreshPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max: Duration::from_secs(30),
        }
    }
}

const DEFAULT_TOMBSTONE_TTL: Duration = Duration::from_secs(60);
/// Upper bound on evictions performed to make room for one insert.
const MAX_EVICTIONS_PER_INSERT: usize = 32;

/// Keyed cache with request coalescing and write-through invalidation.
///
/// A hit returns `Arc<V>`, so reads never clone the value. A miss is served by
/// exactly one caller running the loader; the rest wait and share the result.
///
/// Every operation takes its window from the call site, and an entry's own
/// window is a ceiling on its life: a read sees the entry while it is younger
/// than both the reader's window and the window it was written with. Retention
/// is additionally bounded by `capacity_bytes` and, when one is installed, by a
/// shared [`CacheBudget`]. Reclamation happens on write, so there is no
/// background task.
///
/// Flights are coalesced per `(key, load kind)`, never per key alone: a caller
/// only joins a load that promises the same thing it does. [`OnceMap::reload`]
/// therefore never inherits the result of a [`OnceMap::get_or_load`], and the
/// cold load's `None` can never stand in for an authoritative answer.
pub struct OnceMap<K, V, E> {
    inner: Arc<Inner<K, V, E>>,
}

impl<K, V, E> Clone for OnceMap<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K, V, E> std::fmt::Debug for OnceMap<K, V, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Report sizes only, so no bound is needed on `K`, `V` or `E`.
        let (entries, tombstones, weight) = match self.inner.store.read() {
            Ok(store) => (store.entries.len(), store.tombstones.len(), store.weight),
            Err(_) => (0, 0, 0),
        };
        let inflight = match self.inner.inflight.lock() {
            Ok(inflight) => inflight.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        };
        formatter
            .debug_struct("OnceMap")
            .field("entries", &entries)
            .field("tombstones", &tombstones)
            .field("inflight", &inflight)
            .field("weight", &weight)
            .finish()
    }
}

/// A cache that is only ever written through [`OnceMap::publish`], with no
/// loader path of its own.
pub type PublishedMap<K, V> = OnceMap<K, V, std::convert::Infallible>;

struct Inner<K, V, E> {
    store: RwLock<Store<K, V>>,
    inflight: Mutex<Inflight<K, V, E>>,
    limits: Limits,
    observer: CacheObserver,
    #[cfg(test)]
    stats: Stats,
}

struct Limits {
    max_inflight: usize,
    load_timeout: Option<Duration>,
    admission: Admission,
    refresh: RefreshPolicy,
}

struct Store<K, V> {
    entries: HashMap<K, Entry<V>>,
    /// Keys dropped by an application write or an invalidation.
    ///
    /// A tombstone keeps the key's generation alive while a load could still
    /// publish under it, so a load that started before the mutation cannot
    /// repopulate the key. Once no load is in flight the tombstone is swept.
    tombstones: HashMap<K, Tombstone>,
    /// Insertion order as `(key, seq)`. Drives both expiry and capacity
    /// eviction; `seq` identifies the entry that owns the slot.
    order: VecDeque<(K, u64)>,
    tomb_order: VecDeque<(K, u64)>,
    seq: u64,
    /// Global generation. `invalidate_all` bumps it instead of walking keys.
    epoch: u64,
    weight: usize,
    capacity_bytes: usize,
    tombstone_ttl: Duration,
    budget: Option<Arc<CacheBudget>>,
}

struct Entry<V> {
    value: Arc<V>,
    seq: u64,
    inserted_at: Instant,
    ttl: Duration,
    /// Per-key generation this value belongs to.
    epoch: u64,
    weight: usize,
    refresh_failures: u32,
    next_refresh_at: Option<Instant>,
}

struct Tombstone {
    epoch: u64,
    at: Instant,
    weight: usize,
}

struct Flight<V, E> {
    stamp: Stamp,
    notify: Notify,
    completion: OnceLock<Completion<V, E>>,
}

enum Completion<V, E> {
    Loaded(Arc<V>),
    Failed(E),
    TimedOut(Duration),
    Cancelled,
    Invalidated,
}

// Hand-written so cloning a completion never requires `V: Clone`; the payload
// is behind an `Arc`.
impl<V, E: Clone> Clone for Completion<V, E> {
    fn clone(&self) -> Self {
        match self {
            Self::Loaded(value) => Self::Loaded(Arc::clone(value)),
            Self::Failed(error) => Self::Failed(error.clone()),
            Self::TimedOut(elapsed) => Self::TimedOut(*elapsed),
            Self::Cancelled => Self::Cancelled,
            Self::Invalidated => Self::Invalidated,
        }
    }
}

/// Which loading contract a flight belongs to.
///
/// Flights are keyed by `(key, kind)`, so two callers only share a load when
/// they also share the post-condition that load has to satisfy. A cold load may
/// legitimately resolve to "nothing"; an authoritative reload must produce the
/// value it was asked for, and it may overwrite results from flights that
/// started before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LoadKind {
    /// A read that may be served from, and merged with, any other cold read.
    Cold,
    /// A read that must be answered by its own loader. It may coalesce with
    /// another authoritative reload, but never with a cold load.
    Authoritative,
}

impl LoadKind {
    const ALL: [LoadKind; 2] = [LoadKind::Cold, LoadKind::Authoritative];
}

/// The in-flight flights of one map, at most one per `(key, kind)`.
///
/// The fields are private on purpose: every access has an invariant to keep
/// (counting, slot matching, tombstone reclamation), and a new accessor that
/// forgets one would silently let a load that started before an invalidation
/// publish anyway.
struct Inflight<K, V, E> {
    by_key: HashMap<K, Flights<V, E>>,
    count: usize,
}

impl<K, V, E> Default for Inflight<K, V, E> {
    fn default() -> Self {
        Self {
            by_key: HashMap::new(),
            count: 0,
        }
    }
}

struct Flights<V, E> {
    cold: Option<Arc<Flight<V, E>>>,
    authoritative: Option<Arc<Flight<V, E>>>,
}

impl<V, E> Default for Flights<V, E> {
    fn default() -> Self {
        Self {
            cold: None,
            authoritative: None,
        }
    }
}

impl<V, E> Flights<V, E> {
    fn slot(&self, kind: LoadKind) -> Option<&Arc<Flight<V, E>>> {
        match kind {
            LoadKind::Cold => self.cold.as_ref(),
            LoadKind::Authoritative => self.authoritative.as_ref(),
        }
    }

    fn slot_mut(&mut self, kind: LoadKind) -> &mut Option<Arc<Flight<V, E>>> {
        match kind {
            LoadKind::Cold => &mut self.cold,
            LoadKind::Authoritative => &mut self.authoritative,
        }
    }

    fn take(&mut self, kind: LoadKind) -> Option<Arc<Flight<V, E>>> {
        self.slot_mut(kind).take()
    }

    fn is_empty(&self) -> bool {
        self.cold.is_none() && self.authoritative.is_none()
    }
}

impl<K, V, E> Inflight<K, V, E> {
    fn len(&self) -> usize {
        self.count
    }
}

impl<K, V, E> Inflight<K, V, E>
where
    K: Clone + Eq + Hash,
{
    fn get(&self, key: &K, kind: LoadKind) -> Option<&Arc<Flight<V, E>>> {
        self.by_key.get(key).and_then(|flights| flights.slot(kind))
    }

    /// Whether any flight, of either kind, holds this key.
    fn has(&self, key: &K) -> bool {
        self.by_key
            .get(key)
            .is_some_and(|flights| !flights.is_empty())
    }

    fn insert(&mut self, key: K, kind: LoadKind, flight: Arc<Flight<V, E>>) {
        let flights = self.by_key.entry(key).or_default();
        let slot = flights.slot_mut(kind);
        debug_assert!(
            slot.is_none(),
            "a flight for this key and kind already exists"
        );
        *slot = Some(flight);
        self.count += 1;
    }

    /// Removes `flight` only if it still owns the slot, so a superseded leader
    /// cannot take its replacement's slot.
    fn remove(&mut self, key: &K, kind: LoadKind, flight: &Arc<Flight<V, E>>) {
        let Self { by_key, count } = self;
        let Some(flights) = by_key.get_mut(key) else {
            return;
        };
        let owns_slot = flights
            .slot(kind)
            .is_some_and(|current| Arc::ptr_eq(current, flight));
        if owns_slot {
            flights.take(kind);
            *count -= 1;
        }
        let empty = flights.is_empty();
        if empty {
            by_key.remove(key);
        }
    }

    /// Takes every flight for `key` that captured `stamp`, whichever kind it is.
    fn take_matching<Q>(&mut self, key: &Q, stamp: Stamp) -> Vec<Arc<Flight<V, E>>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Self { by_key, count } = self;
        let Some(flights) = by_key.get_mut(key) else {
            return Vec::new();
        };
        let mut taken = Vec::new();
        for kind in LoadKind::ALL {
            if flights
                .slot(kind)
                .is_some_and(|flight| flight.stamp == stamp)
            {
                if let Some(flight) = flights.take(kind) {
                    taken.push(flight);
                    *count -= 1;
                }
            }
        }
        let empty = flights.is_empty();
        if empty {
            by_key.remove(key);
        }
        taken
    }

    /// Takes every flight registered before `epoch`, of either kind.
    fn drain_before(&mut self, epoch: u64) -> Vec<Arc<Flight<V, E>>> {
        let Self { by_key, count } = self;
        let mut stale = Vec::new();
        by_key.retain(|_, flights| {
            for kind in LoadKind::ALL {
                if flights
                    .slot(kind)
                    .is_some_and(|flight| flight.stamp.epoch == epoch)
                {
                    if let Some(flight) = flights.take(kind) {
                        stale.push(flight);
                        *count -= 1;
                    }
                }
            }
            !flights.is_empty()
        });
        stale
    }
}

/// Snapshot of cache traffic, for tests.
#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub loads: u64,
    pub follower_waits: u64,
}

#[cfg(test)]
#[derive(Default)]
struct Stats {
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
    loads: std::sync::atomic::AtomicU64,
    follower_waits: std::sync::atomic::AtomicU64,
}

impl<K, V, E> OnceMap<K, V, E>
where
    K: Clone + Eq + Hash + CacheWeight,
    V: CacheWeight,
{
    pub fn builder() -> OnceMapBuilder<K, V, E> {
        OnceMapBuilder::new()
    }

    /// Hit within `window`, which every caller states explicitly: the cache
    /// holds entries for as long as they are useful to somebody, and how old is
    /// too old is the caller's question, not the map's.
    pub fn peek_within<Q>(&self, key: &Q, window: Duration) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.lookup(key, window)
    }

    /// `peek_within` for callers that decide freshness from the entry's age.
    pub fn peek_within_age<Q>(&self, key: &Q, window: Duration) -> Option<(Arc<V>, Duration)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.lookup_with_age(key, window)
    }

    /// Application write: stores a value and invalidates any load in flight
    /// for that key, so a slower stale load cannot overwrite it.
    pub fn publish(&self, key: K, value: V, window: Duration) {
        let key_weight = key.cache_weight();
        let now = Instant::now();
        let stale = match self.inner.store.write() {
            Ok(mut store) => {
                let stale = store.stamp(&key);
                store.invalidate(&key, key_weight, now);
                store.insert(
                    key.clone(),
                    Arc::new(value),
                    window,
                    stale.key_epoch.wrapping_add(1),
                    key_weight,
                    now,
                );
                self.inner.reap(&mut store, now);
                stale
            }
            Err(poisoned) => {
                let mut store = poisoned.into_inner();
                let stale = store.stamp(&key);
                store.invalidate(&key, key_weight, now);
                store.insert(
                    key.clone(),
                    Arc::new(value),
                    window,
                    stale.key_epoch.wrapping_add(1),
                    key_weight,
                    now,
                );
                stale
            }
        };
        self.abort_flights_if(&key, stale);
        self.inner.maintain();
    }

    /// Drops the cached value and returns it. A load in flight for the key is
    /// aborted so its result is not published.
    pub fn remove<Q>(&self, key: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        let key_weight = key.to_owned().cache_weight();
        let now = Instant::now();
        let (removed, stale) = match self.inner.store.write() {
            Ok(mut store) => {
                let stale = store.stamp(key);
                let removed = store.invalidate(key, key_weight, now);
                (removed, stale)
            }
            Err(poisoned) => {
                let mut store = poisoned.into_inner();
                let stale = store.stamp(key);
                let removed = store.invalidate(key, key_weight, now);
                (removed, stale)
            }
        };
        self.abort_flights_if(key, stale);
        removed
    }

    /// Drops the cached value. A load in flight for the key is aborted so its
    /// result is not published.
    pub fn invalidate<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        self.remove(key);
    }

    /// Drops everything. Used when the underlying table is rewritten.
    pub fn invalidate_all(&self) {
        let stale_epoch = {
            let store = self.inner.store.write();
            match store {
                Ok(mut store) => {
                    let epoch = store.epoch;
                    store.clear();
                    epoch
                }
                Err(poisoned) => {
                    let mut store = poisoned.into_inner();
                    let epoch = store.epoch;
                    store.clear();
                    epoch
                }
            }
        };

        let stale_flights = match self.inner.inflight.lock() {
            Ok(mut inflight) => inflight.drain_before(stale_epoch),
            Err(poisoned) => poisoned.into_inner().drain_before(stale_epoch),
        };
        for flight in stale_flights {
            flight.complete(Completion::Invalidated);
        }
        // `clear` only empties the containers; returning their capacity is
        // structural maintenance and belongs outside the write section, like
        // every other compaction in this file.
        self.inner.maintain();
    }

    /// Entries currently resident, expired ones included: reclamation happens
    /// on write, so a read can still see a key whose window has passed. Use
    /// [`OnceMap::for_each_fresh`] to count what is still readable.
    pub fn len(&self) -> usize {
        self.inner
            .store
            .read()
            .map(|store| store.entries.len())
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retained tombstones. Bounded by `tombstone_ttl` and by in-flight loads.
    pub fn tombstones(&self) -> usize {
        self.inner
            .store
            .read()
            .map(|store| store.tombstones.len())
            .unwrap_or(0)
    }

    /// Accounted bytes: keys, values and tombstones.
    pub fn weight(&self) -> usize {
        self.inner
            .store
            .read()
            .map(|store| store.weight)
            .unwrap_or(0)
    }

    /// Visits the entries still inside their own window.
    ///
    /// The visitor runs under the store's read lock, so it must not call back
    /// into this map (a write from inside would deadlock). Scanning without
    /// cloning every entry is the point: callers filter first and clone only
    /// what they keep.
    pub fn for_each_fresh<F>(&self, mut visit: F)
    where
        F: FnMut(&K, &Arc<V>, Duration),
    {
        let now = Instant::now();
        let Ok(store) = self.inner.store.read() else {
            return;
        };
        for (key, entry) in &store.entries {
            let age = now.saturating_duration_since(entry.inserted_at);
            if age <= entry.ttl {
                visit(key, &entry.value, age);
            }
        }
    }

    /// Collects the entries still inside their own window.
    ///
    /// A convenience wrapper over [`OnceMap::for_each_fresh`] for callers that
    /// genuinely want an owned copy of everything.
    pub fn iter_fresh(&self) -> Vec<(K, Arc<V>, Duration)> {
        let mut fresh = Vec::new();
        self.for_each_fresh(|key, value, age| {
            fresh.push((key.clone(), Arc::clone(value), age));
        });
        fresh
    }

    fn lookup<Q>(&self, key: &Q, max_age: Duration) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.lookup_with_age(key, max_age).map(|(value, _)| value)
    }

    fn lookup_with_age<Q>(&self, key: &Q, max_age: Duration) -> Option<(Arc<V>, Duration)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let store = self.inner.store.read().ok()?;
        let entry = store.entries.get(key)?;
        let age = Instant::now().saturating_duration_since(entry.inserted_at);
        // The narrower of the reader's window and the writer's window wins.
        (age <= max_age.min(entry.ttl)).then(|| (Arc::clone(&entry.value), age))
    }

    /// Aborts every flight for the key that captured `stale`; a load that
    /// registered after the mutation belongs to the new generation and must be
    /// left alone. Both load kinds are covered: a mutation outdates a cold read
    /// and an authoritative reload alike.
    fn abort_flights_if<Q>(&self, key: &Q, stale: Stamp)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let flights = match self.inner.inflight.lock() {
            Ok(mut inflight) => inflight.take_matching(key, stale),
            Err(poisoned) => poisoned.into_inner().take_matching(key, stale),
        };
        for flight in flights {
            flight.complete(Completion::Invalidated);
        }
    }

    fn inflight_len(&self) -> usize {
        match self.inner.inflight.lock() {
            Ok(inflight) => inflight.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }

    /// Whether any load, of either kind, currently owns this key.
    fn has_flight(&self, key: &K) -> bool {
        match self.inner.inflight.lock() {
            Ok(inflight) => inflight.has(key),
            Err(poisoned) => poisoned.into_inner().has(key),
        }
    }

    fn refresh_due<Q>(&self, key: &Q, now: Instant) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Ok(store) = self.inner.store.read() else {
            return false;
        };
        store
            .entries
            .get(key)
            .and_then(|entry| entry.next_refresh_at)
            .is_none_or(|next| now >= next)
    }

    fn note_hit(&self) {
        self.inner.observer.hit();
        #[cfg(test)]
        self.bump(|stats| &stats.hits);
    }

    fn note_miss(&self) {
        self.inner.observer.miss();
        #[cfg(test)]
        self.bump(|stats| &stats.misses);
    }

    fn note_load(&self) {
        self.inner.observer.load();
        #[cfg(test)]
        self.bump(|stats| &stats.loads);
    }

    fn note_follower_wait(&self) {
        self.inner.observer.follower_wait();
        #[cfg(test)]
        self.bump(|stats| &stats.follower_waits);
    }

    #[cfg(test)]
    fn bump(&self, pick: impl Fn(&Stats) -> &std::sync::atomic::AtomicU64) {
        pick(&self.inner.stats).fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    pub fn stats(&self) -> CacheStats {
        use std::sync::atomic::Ordering;
        CacheStats {
            hits: self.inner.stats.hits.load(Ordering::Relaxed),
            misses: self.inner.stats.misses.load(Ordering::Relaxed),
            loads: self.inner.stats.loads.load(Ordering::Relaxed),
            follower_waits: self.inner.stats.follower_waits.load(Ordering::Relaxed),
        }
    }
}

// `E` is only needed by the paths that actually run a loader.
impl<K, V, E> OnceMap<K, V, E>
where
    K: Clone + Eq + Hash + CacheWeight + Send + Sync + 'static,
    V: CacheWeight + Send + Sync + 'static,
    E: Clone + Send + Sync + 'static,
{
    /// Serves an entry younger than `window`, or loads once on behalf of every
    /// concurrent caller.
    ///
    /// `window` is also how long the loaded value stays readable, so a caller
    /// never has to reconcile a per-call window with a per-cache one.
    pub async fn get_or_load<F, Fut>(
        &self,
        key: K,
        window: Duration,
        load: F,
    ) -> Result<Arc<V>, OnceMapError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        if let Some(value) = self.peek_within(&key, window) {
            self.note_hit();
            return Ok(value);
        }
        self.drive(key, window, LoadKind::Cold, load).await
    }

    /// Runs the loader even when a value is cached, coalescing only with other
    /// reloads of the same key.
    ///
    /// Callers that must not act on a stale value at all — authorization, for
    /// one — use this instead of lowering the window and losing the ability to
    /// serve the stale window at all. It never joins a [`OnceMap::get_or_load`]
    /// in flight: that load is allowed to resolve to "nothing", while this one
    /// is not, so inheriting its answer would silently skip the reload.
    pub async fn reload<F, Fut>(
        &self,
        key: K,
        window: Duration,
        load: F,
    ) -> Result<Arc<V>, OnceMapError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        self.drive(key, window, LoadKind::Authoritative, load).await
    }

    /// Returns a stale value immediately and revalidates in the background.
    ///
    /// A value within `fresh` is returned as is. A value within `stale` is
    /// returned while one background refresh runs. Anything older behaves like
    /// [`OnceMap::get_or_load`].
    pub async fn get_or_load_revalidating<C, CF, B, BF>(
        &self,
        key: K,
        fresh: Duration,
        stale: Duration,
        cold_load: C,
        background_refresh: B,
    ) -> Result<Arc<V>, OnceMapError<E>>
    where
        C: FnOnce() -> CF,
        CF: Future<Output = Result<V, E>>,
        B: FnOnce() -> BF,
        BF: Future<Output = Result<V, E>> + Send + 'static,
    {
        if let Some((value, age)) = self.lookup_with_age(&key, stale) {
            self.note_hit();
            if age > fresh
                && self.refresh_due(&key, Instant::now())
                && self.try_spawn_refresh(key, stale, background_refresh)
            {
                self.note_load();
            }
            return Ok(value);
        }
        // Past the stale window the entry is not a candidate, so the load must
        // not consult it again on the way in.
        self.drive(key, stale, LoadKind::Cold, cold_load).await
    }

    /// Starts a background refresh unless one is already in flight, or the last
    /// one failed recently enough to still be backing off.
    ///
    /// Returns whether this caller won the refresh slot. The refresh future is
    /// spawned, so it must be `'static`; build it lazily to keep the caller's
    /// inputs from being cloned on the fast path.
    pub fn try_spawn_refresh<B, BF>(&self, key: K, window: Duration, refresh: B) -> bool
    where
        B: FnOnce() -> BF,
        BF: Future<Output = Result<V, E>> + Send + 'static,
    {
        // A refresh must not add a second load while an authoritative reload is
        // already running for this key: that reload is the fresher read and it
        // is not something a background refresh can improve on.
        if self.has_flight(&key) {
            return false;
        }
        let mut guard = match self.register(&key, LoadKind::Cold) {
            Registration::Leader(guard) => guard,
            Registration::Follower(_) | Registration::Saturated(_) => return false,
        };
        let refresh = refresh();
        let load_timeout = self.inner.limits.load_timeout;
        tokio::spawn(async move {
            let result = match load_timeout {
                Some(timeout) => match tokio::time::timeout(timeout, refresh).await {
                    Ok(result) => result,
                    Err(_) => {
                        guard.note_refresh_failure();
                        guard.timed_out(timeout);
                        return;
                    }
                },
                None => refresh.await,
            };
            match result {
                Ok(value) => {
                    guard.publish(value, window);
                }
                Err(error) => {
                    guard.note_refresh_failure();
                    guard.fail(error);
                }
            }
        });
        true
    }

    async fn drive<F, Fut>(
        &self,
        key: K,
        window: Duration,
        kind: LoadKind,
        load: F,
    ) -> Result<Arc<V>, OnceMapError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        let mut load = Some(load);
        self.note_miss();

        // The caller already decided there is nothing within its window, so the
        // loop never re-consults the store on the way in.
        loop {
            match self.register(&key, kind) {
                Registration::Saturated(stamp) => {
                    if self.inner.limits.admission == Admission::Reject {
                        return Err(OnceMapError::AdmissionSaturated {
                            inflight: self.inflight_len(),
                        });
                    }
                    // Bypass: keep the request alive and let it load for
                    // itself. The result is still published; it just does not
                    // own a flight slot.
                    self.note_load();
                    let load = load.take().expect("a caller runs the loader at most once");
                    return match self.run_load(load).await {
                        Ok(value) => Ok(self.publish_bypass(key, value, window, stamp)),
                        Err(error) => Err(error),
                    };
                }
                Registration::Follower(flight) => {
                    self.note_follower_wait();
                    match flight.wait().await {
                        Completion::Loaded(value) => {
                            self.note_hit();
                            return Ok(value);
                        }
                        Completion::Failed(error) => return Err(OnceMapError::Load(error)),
                        Completion::TimedOut(elapsed) => {
                            return Err(OnceMapError::LoadTimeout { elapsed })
                        }
                        Completion::Cancelled | Completion::Invalidated => continue,
                    }
                }
                Registration::Leader(mut guard) => {
                    self.note_load();
                    let load = load.take().expect("a leader runs the loader once");
                    return match self.run_load(load).await {
                        Ok(value) => Ok(guard.publish(value, window)),
                        Err(OnceMapError::Load(error)) => {
                            guard.fail(error.clone());
                            Err(OnceMapError::Load(error))
                        }
                        Err(OnceMapError::LoadTimeout { elapsed }) => {
                            guard.timed_out(elapsed);
                            Err(OnceMapError::LoadTimeout { elapsed })
                        }
                        Err(OnceMapError::AdmissionSaturated { inflight }) => {
                            debug_assert!(false, "a leader never runs under admission saturation");
                            Err(OnceMapError::AdmissionSaturated { inflight })
                        }
                    };
                }
            }
        }
    }

    async fn run_load<F, Fut>(&self, load: F) -> Result<V, OnceMapError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        match self.inner.limits.load_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, load()).await {
                Ok(result) => result.map_err(OnceMapError::Load),
                Err(_) => Err(OnceMapError::LoadTimeout { elapsed: timeout }),
            },
            None => load().await.map_err(OnceMapError::Load),
        }
    }

    /// Publishes an unregistered load's result (admission bypass).
    ///
    /// The result belongs to the generation the load started under: if the key
    /// was written or invalidated while it ran, the result is handed to the
    /// caller but not stored, so a slow bypass cannot overwrite a newer value.
    fn publish_bypass(&self, key: K, value: V, window: Duration, stamp: Stamp) -> Arc<V> {
        let key_weight = key.cache_weight();
        let value = Arc::new(value);
        let now = Instant::now();
        if let Ok(mut store) = self.inner.store.write() {
            if store.stamp(&key) == stamp {
                store.insert(
                    key,
                    Arc::clone(&value),
                    window,
                    stamp.key_epoch,
                    key_weight,
                    now,
                );
                self.inner.reap(&mut store, now);
            }
        }
        self.inner.maintain();
        value
    }

    fn register(&self, key: &K, kind: LoadKind) -> Registration<K, V, E> {
        let stamp = match self.inner.store.read() {
            Ok(store) => store.stamp(key),
            Err(poisoned) => poisoned.into_inner().stamp(key),
        };

        let mut inflight = match self.inner.inflight.lock() {
            Ok(inflight) => inflight,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(flight) = inflight.get(key, kind) {
            return Registration::Follower(Arc::clone(flight));
        }
        if inflight.len() >= self.inner.limits.max_inflight {
            return Registration::Saturated(stamp);
        }

        let flight = Arc::new(Flight::new(stamp));
        inflight.insert(key.clone(), kind, Arc::clone(&flight));
        Registration::Leader(FlightGuard {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            flight,
            stamp,
            kind,
            done: false,
        })
    }
}

enum Registration<K, V, E>
where
    K: Clone + Eq + Hash,
    V: CacheWeight,
{
    Leader(FlightGuard<K, V, E>),
    Follower(Arc<Flight<V, E>>),
    /// No slot was available; the stamp the caller would have registered with
    /// is carried out so a bypass load can still publish under its generation.
    Saturated(Stamp),
}

/// Owns the leader slot for one key and one load kind. Dropping it without
/// finishing means the leader went away, so waiters are woken to elect a
/// replacement.
///
/// The bounds live on the struct so `Drop` can clean up the in-flight map
/// without knowing anything else about the call site.
struct FlightGuard<K, V, E>
where
    K: Clone + Eq + Hash,
    V: CacheWeight,
{
    inner: Arc<Inner<K, V, E>>,
    key: K,
    flight: Arc<Flight<V, E>>,
    stamp: Stamp,
    kind: LoadKind,
    done: bool,
}

impl<K, V, E> FlightGuard<K, V, E>
where
    K: Clone + Eq + Hash + CacheWeight,
    V: CacheWeight,
{
    /// Stores the loaded value unless the key was mutated while this load ran,
    /// then hands it to every waiter regardless.
    ///
    /// An authoritative result is stored like an application write: it advances
    /// the key's generation, so every flight that started before it can no
    /// longer publish over it.
    fn publish(&mut self, value: V, window: Duration) -> Arc<V> {
        let key_weight = self.key.cache_weight();
        let value = Arc::new(value);
        let now = Instant::now();
        if let Ok(mut store) = self.inner.store.write() {
            if store.stamp(&self.key) == self.stamp {
                let epoch = if self.kind == LoadKind::Authoritative {
                    // Leaves a tombstone for the new generation, so the bump
                    // survives even if the value itself cannot be retained.
                    store.invalidate(&self.key, key_weight, now);
                    self.stamp.key_epoch.wrapping_add(1)
                } else {
                    self.stamp.key_epoch
                };
                store.insert(
                    self.key.clone(),
                    Arc::clone(&value),
                    window,
                    epoch,
                    key_weight,
                    now,
                );
                self.inner.reap(&mut store, now);
            }
        }
        self.inner.maintain();

        self.retire(Completion::Loaded(Arc::clone(&value)));
        value
    }

    fn note_refresh_failure(&mut self) {
        let policy = self.inner.limits.refresh;
        if let Ok(mut store) = self.inner.store.write() {
            store.note_refresh_failure(&self.key, Instant::now(), policy);
        }
    }
}

impl<K, V, E> FlightGuard<K, V, E>
where
    K: Clone + Eq + Hash,
    V: CacheWeight,
{
    fn fail(&mut self, error: E) {
        self.retire(Completion::Failed(error));
    }

    fn timed_out(&mut self, elapsed: Duration) {
        self.retire(Completion::TimedOut(elapsed));
    }

    fn retire(&mut self, completion: Completion<V, E>) {
        self.release();
        self.flight.complete(completion);
    }

    fn release(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        // A newer flight for this key may have replaced ours; only the owner of
        // the slot may remove it, and only for its own load kind.
        match self.inner.inflight.lock() {
            Ok(mut inflight) => inflight.remove(&self.key, self.kind, &self.flight),
            Err(poisoned) => poisoned
                .into_inner()
                .remove(&self.key, self.kind, &self.flight),
        }
    }
}

impl<K, V, E> Drop for FlightGuard<K, V, E>
where
    K: Clone + Eq + Hash,
    V: CacheWeight,
{
    fn drop(&mut self) {
        if self.done {
            return;
        }
        self.retire(Completion::Cancelled);
    }
}

impl<V, E> Flight<V, E> {
    fn new(stamp: Stamp) -> Self {
        Self {
            stamp,
            notify: Notify::new(),
            completion: OnceLock::new(),
        }
    }

    fn complete(&self, completion: Completion<V, E>) {
        if self.completion.set(completion).is_ok() {
            self.notify.notify_waiters();
        }
    }
}

impl<V, E: Clone> Flight<V, E> {
    async fn wait(&self) -> Completion<V, E> {
        loop {
            if let Some(completion) = self.completion.get() {
                return completion.clone();
            }
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Register before re-checking so a completion landing in between
            // is not lost.
            notified.as_mut().enable();
            if let Some(completion) = self.completion.get() {
                return completion.clone();
            }
            notified.await;
        }
    }
}

impl<K, V, E> Inner<K, V, E>
where
    K: Clone + Eq + Hash,
    V: CacheWeight,
{
    /// Reclaims entries and tombstones. Holding the store lock while taking the
    /// in-flight lock is the documented order (`store -> inflight`).
    ///
    /// Only the tombstone sweep needs to know which keys are in flight, so the
    /// in-flight lock is released before entries are reclaimed, compacted or
    /// shrunk: those are the expensive parts, and holding `inflight` through
    /// them would stall every concurrent lookup and flight registration.
    fn reap(&self, store: &mut Store<K, V>, now: Instant) {
        {
            let inflight = match self.inflight.lock() {
                Ok(inflight) => inflight,
                Err(poisoned) => poisoned.into_inner(),
            };
            store.reap_tombstones(now, &|key: &K| inflight.has(key));
        }
        store.reap_entries(now);
    }

    /// Runs the structural reclamation the write path has only *flagged*.
    ///
    /// Compaction rebuilds and re-sorts the insertion queue and shrinking
    /// rehashes the maps; both are O(n) and both used to run inside the same
    /// write section as the insert that triggered them, so one publish could
    /// hold the map for a full-table walk. They now run once, in their own
    /// short section, and only if nobody else has already done the work.
    fn maintain(&self) {
        let Ok(mut store) = self.store.write() else {
            return;
        };
        if !store.maintenance_due() {
            return;
        }
        store.compact();
        store.shrink();
    }
}

impl<K, V> Store<K, V>
where
    K: Clone + Eq + Hash,
    V: CacheWeight,
{
    fn stamp<Q>(&self, key: &Q) -> Stamp
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let key_epoch = self
            .entries
            .get(key)
            .map(|entry| entry.epoch)
            .or_else(|| self.tombstones.get(key).map(|tombstone| tombstone.epoch))
            .unwrap_or(0);
        Stamp {
            epoch: self.epoch,
            key_epoch,
        }
    }

    /// Bumps the key's generation and leaves a tombstone so a load that is
    /// still in flight cannot publish. Returns the value it displaced.
    fn invalidate<Q>(&mut self, key: &Q, key_weight: usize, now: Instant) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ToOwned<Owned = K> + ?Sized,
    {
        // Read the generation before dropping the entry, so it only ever grows.
        let epoch = self.stamp(key).key_epoch.wrapping_add(1);
        let previous = self.take_entry(key);
        if let Some(previous_tombstone) = self.tombstones.remove(key) {
            self.weight = self.weight.saturating_sub(previous_tombstone.weight);
        }
        self.tombstones.insert(
            key.to_owned(),
            Tombstone {
                epoch,
                at: now,
                weight: key_weight,
            },
        );
        self.weight = self.weight.saturating_add(key_weight);
        self.tomb_order.push_back((key.to_owned(), epoch));
        previous
    }

    fn take_entry<Q>(&mut self, key: &Q) -> Option<Arc<V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let entry = self.entries.remove(key)?;
        self.weight = self.weight.saturating_sub(entry.weight);
        if let Some(budget) = &self.budget {
            budget.uncharge(entry.weight);
        }
        Some(entry.value)
    }

    fn insert(
        &mut self,
        key: K,
        value: Arc<V>,
        ttl: Duration,
        epoch: u64,
        key_weight: usize,
        now: Instant,
    ) {
        let weight = key_weight
            .saturating_add(value.cache_weight())
            .saturating_add(entry_overhead::<K, V>());
        self.take_entry(&key);
        if let Some(previous_tombstone) = self.tombstones.remove(&key) {
            self.weight = self.weight.saturating_sub(previous_tombstone.weight);
        }

        // A value larger than this cache's own cap can never be resident: it
        // would be the first thing `reap_entries` drops. Refuse it up front so
        // a hot path does not pay for an insert that is immediately undone.
        if weight > self.capacity_bytes {
            return;
        }

        if let Some(budget) = self.budget.clone() {
            if !budget.try_charge(weight) {
                // Expired entries are already dead weight in the ledger.
                // Release them before the eviction loop starts sacrificing
                // live rows.
                self.reclaim_expired(now);
                let mut evictions = 0;
                while !budget.try_charge(weight) {
                    if evictions >= MAX_EVICTIONS_PER_INSERT || !self.evict_oldest() {
                        // The ledger is full and this cache has nothing left
                        // to give: the caller still gets its value, it just is
                        // not retained.
                        budget.record_rejection();
                        return;
                    }
                    evictions += 1;
                }
            }
        }

        self.seq = self.seq.wrapping_add(1);
        let seq = self.seq;
        self.entries.insert(
            key.clone(),
            Entry {
                value,
                seq,
                inserted_at: now,
                ttl,
                epoch,
                weight,
                refresh_failures: 0,
                next_refresh_at: None,
            },
        );
        self.weight = self.weight.saturating_add(weight);
        self.order.push_back((key, seq));
    }

    /// Drops every entry whose own window has passed, wherever it sits in the
    /// insertion queue.
    ///
    /// `reap_entries` stops at the first live entry because it runs on the
    /// write path and must stay cheap. That rule cannot free an expired entry
    /// that was inserted before a long-lived one, which is exactly the state
    /// that fills the ledger with dead weight — so the budget-refusal path pays
    /// for one full sweep instead.
    fn reclaim_expired(&mut self, now: Instant) {
        let mut kept = VecDeque::with_capacity(self.order.len());
        let mut expired = Vec::new();
        while let Some((key, seq)) = self.order.pop_front() {
            match self.entries.get(&key) {
                // Slot superseded by a newer insert, or already gone.
                None => {}
                Some(entry) if entry.seq != seq => {}
                Some(entry) if now.saturating_duration_since(entry.inserted_at) > entry.ttl => {
                    expired.push(key);
                }
                Some(_) => kept.push_back((key, seq)),
            }
        }
        self.order = kept;
        for key in expired {
            self.take_entry(&key);
        }
    }

    /// Evicts the oldest live entry. Returns whether anything was evicted.
    fn evict_oldest(&mut self) -> bool {
        while let Some((key, seq)) = self.order.front().cloned() {
            self.order.pop_front();
            let owns_slot = self.entries.get(&key).is_some_and(|entry| entry.seq == seq);
            if !owns_slot {
                continue;
            }
            self.take_entry(&key);
            return true;
        }
        false
    }

    fn note_refresh_failure<Q>(&mut self, key: &Q, now: Instant, policy: RefreshPolicy)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        entry.refresh_failures = entry.refresh_failures.saturating_add(1);
        let exponent = entry.refresh_failures.min(6);
        let backoff = policy.base.saturating_mul(1u32 << exponent).min(policy.max);
        entry.next_refresh_at = Some(now + backoff);
    }

    fn clear(&mut self) {
        for entry in self.entries.values() {
            if let Some(budget) = &self.budget {
                budget.uncharge(entry.weight);
            }
        }
        self.entries.clear();
        self.tombstones.clear();
        self.order.clear();
        self.tomb_order.clear();
        self.weight = 0;
        self.epoch = self.epoch.wrapping_add(1);
    }

    /// Drops expired entries and, once the byte budget is exceeded, the oldest
    /// survivors. The tombstone sweep runs separately because it is the only
    /// part that needs the in-flight set.
    fn reap_entries(&mut self, now: Instant) {
        while let Some((key, seq)) = self.order.front().cloned() {
            let Some(entry) = self.entries.get(&key) else {
                self.order.pop_front();
                continue;
            };
            // Queue entries superseded by a newer insert are discarded.
            if entry.seq != seq {
                self.order.pop_front();
                continue;
            }

            // The ledger only admits or refuses; it never reclaims. Anything
            // that is over `capacity_bytes` is evicted here, and the charge
            // that put it there was already refused by the budget.
            let expired = now.saturating_duration_since(entry.inserted_at) > entry.ttl;
            let over_capacity = self.weight > self.capacity_bytes;
            if !expired && !over_capacity {
                break;
            }
            self.order.pop_front();
            self.take_entry(&key);
        }
    }

    /// Tombstones only have to outlive the loads that could republish under
    /// them, so a key with no in-flight load can be swept before its TTL.
    fn reap_tombstones(&mut self, now: Instant, is_inflight: &dyn Fn(&K) -> bool) {
        while let Some((key, epoch)) = self.tomb_order.front().cloned() {
            let Some(tombstone) = self.tombstones.get(&key) else {
                self.tomb_order.pop_front();
                continue;
            };
            if tombstone.epoch != epoch {
                self.tomb_order.pop_front();
                continue;
            }
            let expired = now.saturating_duration_since(tombstone.at) > self.tombstone_ttl;
            if !expired || is_inflight(&key) {
                break;
            }
            self.tomb_order.pop_front();
            if let Some(removed) = self.tombstones.remove(&key) {
                self.weight = self.weight.saturating_sub(removed.weight);
            }
        }
    }

    /// Whether the queues or the buckets have grown far beyond the live set.
    ///
    /// Bucket capacity is part of the footprint, so it is part of the trigger:
    /// without this a burst's high-water mark is kept for the process lifetime.
    fn maintenance_due(&self) -> bool {
        let order_bloated =
            self.order.len() > self.entries.len().saturating_mul(2).saturating_add(64);
        let tombs_bloated =
            self.tomb_order.len() > self.tombstones.len().saturating_mul(2).saturating_add(64);
        let entries_bloated =
            self.entries.capacity() > self.entries.len().saturating_mul(4).saturating_add(64);
        let tombs_capacity_bloated =
            self.tombstones.capacity() > self.tombstones.len().saturating_mul(4).saturating_add(64);
        order_bloated || tombs_bloated || entries_bloated || tombs_capacity_bloated
    }

    fn compact(&mut self) {
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
        if self.tombstones.is_empty() {
            self.tomb_order.clear();
        } else if self.tomb_order.len() > self.tombstones.len().saturating_mul(2).saturating_add(64)
        {
            self.tomb_order = self
                .tombstones
                .iter()
                .map(|(key, tombstone)| (key.clone(), tombstone.epoch))
                .collect();
        }
    }

    /// Bucket capacity is part of the footprint, so release it once it is far
    /// larger than the live set. Without this the maps keep a burst's
    /// high-water mark for the process lifetime.
    fn shrink(&mut self) {
        if self.entries.capacity() > self.entries.len().saturating_mul(4) + 64 {
            self.entries.shrink_to_fit();
            self.order.shrink_to_fit();
        }
        if self.tombstones.capacity() > self.tombstones.len().saturating_mul(4) + 64 {
            self.tombstones.shrink_to_fit();
            self.tomb_order.shrink_to_fit();
        }
    }
}

/// Bytes the map retains per entry besides the key and the value: the entry
/// header, the entry's slot in the insertion queue (which holds a second copy
/// of the key) and its share of the table's buckets.
///
/// Charging them is what makes `capacity_bytes` a bound on memory rather than
/// on value bytes: without it a cache of small values holds several times its
/// declared cap.
fn entry_overhead<K, V>() -> usize {
    std::mem::size_of::<Entry<V>>()
        .saturating_add(std::mem::size_of::<(K, u64)>())
        .saturating_add(16)
}

/// Keeps the builder's type parameters without implying ownership of them, so
/// the builder does not inherit `Send`/`Sync` from `K`, `V` or `E`.
type BuilderMarker<K, V, E> = std::marker::PhantomData<fn() -> (K, V, E)>;

pub struct OnceMapBuilder<K, V, E> {
    capacity_bytes: usize,
    max_inflight: usize,
    load_timeout: Option<Duration>,
    admission: Admission,
    tombstone_ttl: Duration,
    refresh: RefreshPolicy,
    budget: Option<Arc<CacheBudget>>,
    observer: CacheObserver,
    marker: BuilderMarker<K, V, E>,
}

impl<K, V, E> Default for OnceMapBuilder<K, V, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, E> OnceMapBuilder<K, V, E> {
    pub fn new() -> Self {
        Self {
            capacity_bytes: usize::MAX,
            max_inflight: 1024,
            load_timeout: None,
            admission: Admission::Bypass,
            tombstone_ttl: DEFAULT_TOMBSTONE_TTL,
            refresh: RefreshPolicy::default(),
            // Every cache shares one ledger by default, so their sum is
            // bounded even though each one also has its own soft cap.
            budget: Some(CacheBudget::process()),
            observer: CacheObserver::default(),
            marker: std::marker::PhantomData,
        }
    }

    /// Soft cap for this cache alone, computed from [`CacheWeight`].
    /// Defaults to unbounded, which leaves expiry as the only bound.
    pub fn capacity_bytes(mut self, bytes: usize) -> Self {
        self.capacity_bytes = bytes;
        self
    }

    /// Concurrent loads allowed before new callers are admitted as followers,
    /// bypassed or rejected.
    pub fn max_inflight(mut self, permits: usize) -> Self {
        self.max_inflight = permits.max(1);
        self
    }

    /// Deadline for one loader invocation, foreground and background alike.
    pub fn load_timeout(mut self, timeout: Duration) -> Self {
        self.load_timeout = Some(timeout);
        self
    }

    /// What to do once `max_inflight` loads are running. Defaults to
    /// [`Admission::Bypass`].
    pub fn admission(mut self, admission: Admission) -> Self {
        self.admission = admission;
        self
    }

    /// How long a tombstone is retained once no load is in flight for its key.
    pub fn tombstone_ttl(mut self, ttl: Duration) -> Self {
        self.tombstone_ttl = ttl;
        self
    }

    /// Backoff applied to background refreshes that keep failing.
    pub fn refresh_backoff(mut self, base: Duration, max: Duration) -> Self {
        self.refresh = RefreshPolicy { base, max };
        self
    }

    /// Shared byte ledger. Values that do not fit are still returned to the
    /// caller, they just are not retained.
    pub fn budget(mut self, budget: Arc<CacheBudget>) -> Self {
        self.budget = Some(budget);
        self
    }

    pub fn observer(mut self, observer: CacheObserver) -> Self {
        self.observer = observer;
        self
    }

    pub fn build(self) -> OnceMap<K, V, E> {
        OnceMap {
            inner: Arc::new(Inner {
                store: RwLock::new(Store {
                    entries: HashMap::new(),
                    tombstones: HashMap::new(),
                    order: VecDeque::new(),
                    tomb_order: VecDeque::new(),
                    seq: 0,
                    epoch: 0,
                    weight: 0,
                    capacity_bytes: self.capacity_bytes,
                    tombstone_ttl: self.tombstone_ttl,
                    budget: self.budget,
                }),
                inflight: Mutex::new(Inflight::default()),
                limits: Limits {
                    max_inflight: self.max_inflight,
                    load_timeout: self.load_timeout,
                    admission: self.admission,
                    refresh: self.refresh,
                },
                observer: self.observer,
                #[cfg(test)]
                stats: Stats::default(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Admission, OnceMap, OnceMapError};
    use crate::budget::CacheBudget;
    use crate::weight::CacheWeight;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Semaphore;

    const WINDOW: Duration = Duration::from_secs(60);

    fn cache() -> OnceMap<String, String, String> {
        OnceMap::builder().max_inflight(64).build()
    }

    fn value(bytes: usize) -> String {
        "x".repeat(bytes)
    }

    /// Weight of one entry with the given value size, measured on a map with no
    /// limits.
    ///
    /// The tests size their budgets relative to this instead of hard-coding the
    /// per-entry bookkeeping that `capacity_bytes` now charges.
    fn entry_weight(value_bytes: usize) -> usize {
        let probe: OnceMap<String, String, String> = OnceMap::builder().build();
        probe.publish("k".to_string(), value(value_bytes), WINDOW);
        probe.weight()
    }

    #[tokio::test]
    async fn a_fresh_entry_is_served_without_loading() {
        let map = cache();
        let calls = Arc::new(AtomicUsize::new(0));

        for _ in 0..3 {
            let calls = Arc::clone(&calls);
            let loaded = map
                .get_or_load("k".to_string(), WINDOW, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok("v".to_string())
                })
                .await
                .unwrap();
            assert_eq!("v", loaded.as_str());
        }

        assert_eq!(1, calls.load(Ordering::SeqCst));
        assert_eq!(1, map.stats().loads);
        assert_eq!(2, map.stats().hits);
    }

    #[tokio::test]
    async fn concurrent_misses_coalesce_into_one_load() {
        let map = cache();
        let calls = Arc::new(AtomicUsize::new(0));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let map = map.clone();
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                map.get_or_load("k".to_string(), WINDOW, || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok("v".to_string())
                })
                .await
            }));
        }
        for task in tasks {
            assert_eq!("v", task.await.unwrap().unwrap().as_str());
        }

        assert_eq!(1, calls.load(Ordering::SeqCst));
        let stats = map.stats();
        assert_eq!(1, stats.loads);
        assert_eq!(7, stats.follower_waits);
    }

    #[tokio::test]
    async fn a_cancelled_leader_hands_over_to_a_waiter() {
        let map = cache();
        let slow = {
            let map = map.clone();
            tokio::spawn(async move {
                map.get_or_load("k".to_string(), WINDOW, || async {
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    Ok("stale".to_string())
                })
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;

        // This caller joins the flight as a follower, then the leader goes away.
        let waiter = {
            let map = map.clone();
            tokio::spawn(async move {
                map.get_or_load("k".to_string(), WINDOW, || async {
                    Ok("fresh".to_string())
                })
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        slow.abort();

        let loaded = waiter.await.unwrap().unwrap();
        assert_eq!("fresh", loaded.as_str());
        assert_eq!(
            "fresh",
            map.peek_within(&"k".to_string(), WINDOW).unwrap().as_str()
        );
    }

    #[tokio::test]
    async fn invalidation_discards_an_inflight_publication() {
        let map = cache();
        let barrier = Arc::new(tokio::sync::Notify::new());
        let started = Arc::clone(&barrier);
        let map_for_task = map.clone();

        let task = tokio::spawn(async move {
            map_for_task
                .get_or_load("k".to_string(), WINDOW, || async move {
                    started.notify_one();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Ok("late".to_string())
                })
                .await
        });

        barrier.notified().await;
        map.invalidate(&"k".to_string());
        let loaded = task.await.unwrap().unwrap();

        // The caller keeps the value it loaded, but nothing is cached.
        assert_eq!("late", loaded.as_str());
        assert_eq!(None, map.peek_within(&"k".to_string(), WINDOW));
        assert!(map.stats().loads >= 1);
    }

    /// A leader that was invalidated while it ran must not take the slot of the
    /// flight that replaced it: the replacement's followers still coalesce.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stale_leader_does_not_steal_the_new_flight() {
        let map = cache();
        let key = "k".to_string();

        let first_started = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        let second_started = Arc::new(tokio::sync::Notify::new());
        let second_release = Arc::new(tokio::sync::Notify::new());
        let extra_loads = Arc::new(AtomicUsize::new(0));

        let first = {
            let map = map.clone();
            let key = key.clone();
            let started = Arc::clone(&first_started);
            let release = Arc::clone(&first_release);
            tokio::spawn(async move {
                map.get_or_load(key, WINDOW, || async move {
                    started.notify_one();
                    release.notified().await;
                    Ok("stale".to_string())
                })
                .await
            })
        };
        first_started.notified().await;
        map.invalidate(&key);

        let second = {
            let map = map.clone();
            let key = key.clone();
            let started = Arc::clone(&second_started);
            let release = Arc::clone(&second_release);
            tokio::spawn(async move {
                map.get_or_load(key, WINDOW, || async move {
                    started.notify_one();
                    release.notified().await;
                    Ok("fresh".to_string())
                })
                .await
            })
        };
        second_started.notified().await;

        // Let the stale leader finish first; it must leave the new flight alone.
        first_release.notify_one();
        assert_eq!("stale", first.await.unwrap().unwrap().as_str());
        tokio::time::sleep(Duration::from_millis(20)).await;

        let extra_loads_for_caller = Arc::clone(&extra_loads);
        let bystander = {
            let map = map.clone();
            let key = key.clone();
            tokio::spawn(async move {
                map.get_or_load(key, WINDOW, || async move {
                    extra_loads_for_caller.fetch_add(1, Ordering::SeqCst);
                    Ok("third".to_string())
                })
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(0, extra_loads.load(Ordering::SeqCst));
        assert!(!bystander.is_finished());

        second_release.notify_one();
        assert_eq!("fresh", second.await.unwrap().unwrap().as_str());
        assert_eq!("fresh", bystander.await.unwrap().unwrap().as_str());
        assert_eq!(0, extra_loads.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn publish_supersedes_an_older_application_value() {
        let map = cache();
        map.publish("k".to_string(), "first".to_string(), WINDOW);
        map.publish("k".to_string(), "second".to_string(), WINDOW);

        assert_eq!(
            "second",
            map.peek_within(&"k".to_string(), WINDOW).unwrap().as_str()
        );
    }

    #[tokio::test]
    async fn remove_returns_the_value_and_drops_it() {
        let map = cache();
        map.publish("k".to_string(), "v".to_string(), WINDOW);

        assert_eq!("v", map.remove(&"k".to_string()).unwrap().as_str());
        assert_eq!(None, map.peek_within(&"k".to_string(), WINDOW));
        assert_eq!(None, map.remove(&"k".to_string()));
    }

    #[tokio::test]
    async fn entries_expire() {
        let map = cache();
        let short = Duration::from_millis(30);
        map.get_or_load("k".to_string(), short, || async { Ok("v".to_string()) })
            .await
            .unwrap();
        assert!(map.peek_within(&"k".to_string(), short).is_some());

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(map.peek_within(&"k".to_string(), short).is_none());
    }

    #[tokio::test]
    async fn a_published_value_lives_for_its_own_window() {
        let map = cache();
        map.publish("k".to_string(), "v".to_string(), Duration::from_millis(30));
        assert!(map.peek_within(&"k".to_string(), WINDOW).is_some());

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(map
            .peek_within(&"k".to_string(), Duration::from_secs(600))
            .is_none());
    }

    #[tokio::test]
    async fn the_byte_budget_evicts_the_oldest_entries() {
        let map: OnceMap<String, String, String> = OnceMap::builder().capacity_bytes(256).build();

        for index in 0..16 {
            map.publish(format!("k{index}"), value(64), WINDOW);
        }

        assert!(map.len() < 16);
        assert_eq!(None, map.peek_within(&"k0".to_string(), WINDOW));
    }

    #[tokio::test]
    async fn keys_and_tombstones_are_accounted() {
        let map: OnceMap<String, String, String> = OnceMap::builder().build();
        let key = "k".repeat(64);
        let baseline = map.weight();

        map.publish(key.clone(), value(8), WINDOW);
        let with_entry = map.weight();
        assert!(
            with_entry >= baseline + key.capacity(),
            "the key must count: {baseline} -> {with_entry}"
        );

        map.invalidate(&key);
        assert_eq!(1, map.tombstones());
        assert!(map.weight() > baseline);
    }

    #[tokio::test]
    async fn tombstones_are_swept_when_no_load_is_in_flight() {
        let map: OnceMap<String, String, String> = OnceMap::builder()
            .tombstone_ttl(Duration::from_millis(20))
            .build();
        map.publish("k".to_string(), "v".to_string(), WINDOW);
        map.invalidate(&"k".to_string());
        assert_eq!(1, map.tombstones());

        tokio::time::sleep(Duration::from_millis(40)).await;
        map.publish("other".to_string(), "v".to_string(), WINDOW);
        assert_eq!(0, map.tombstones());
    }

    #[tokio::test]
    async fn the_shared_budget_bounds_retention() {
        let budget = Arc::new(CacheBudget::new(entry_weight(64) * 4));
        let map: OnceMap<String, String, String> =
            OnceMap::builder().budget(Arc::clone(&budget)).build();

        for index in 0..16 {
            map.publish(format!("k{index}"), value(64), WINDOW);
        }

        assert!(map.len() < 16, "the ledger must bound retention");
        assert!(budget.used() <= budget.limit());
        assert_eq!(
            0,
            budget.rejections(),
            "making room by evicting is not a refusal"
        );
    }

    /// A value that cannot fit the whole ledger is refused exactly once, and
    /// the caller still gets it.
    #[tokio::test]
    async fn a_value_larger_than_the_ledger_is_refused_and_counted() {
        let budget = Arc::new(CacheBudget::new(entry_weight(0)));
        let map: OnceMap<String, String, String> =
            OnceMap::builder().budget(Arc::clone(&budget)).build();

        let value = map
            .get_or_load("k".to_string(), WINDOW, || async { Ok(value(256)) })
            .await
            .unwrap();
        assert_eq!(256, value.len());
        assert_eq!(1, budget.rejections());
        assert_eq!(None, map.peek_within(&"k".to_string(), WINDOW));
    }

    #[tokio::test]
    async fn eviction_releases_bucket_capacity() {
        let map: OnceMap<String, String, String> = OnceMap::builder().build();

        let short = Duration::from_millis(20);
        for index in 0..512 {
            map.publish(format!("k{index}"), value(16), short);
        }
        let peak = map.inner.store.read().unwrap().entries.capacity();
        assert!(peak >= 500, "the map should have grown: {peak}");

        tokio::time::sleep(Duration::from_millis(120)).await;
        map.publish("fresh".to_string(), value(8), WINDOW);

        let after = map.inner.store.read().unwrap().entries.capacity();
        assert!(
            after < peak,
            "bucket capacity should shrink: {peak} -> {after}"
        );
    }

    #[tokio::test]
    async fn invalidate_all_clears_values_and_inflight_loads() {
        let map = cache();
        map.publish("a".to_string(), "1".to_string(), WINDOW);
        map.publish("b".to_string(), "2".to_string(), WINDOW);

        map.invalidate_all();

        assert!(map.is_empty());
        assert_eq!(None, map.peek_within(&"a".to_string(), WINDOW));
    }

    #[tokio::test]
    async fn admission_is_capped() {
        let map: OnceMap<String, String, String> = OnceMap::builder()
            .max_inflight(1)
            .admission(Admission::Reject)
            .build();

        let map_for_task = map.clone();
        let task = tokio::spawn(async move {
            map_for_task
                .get_or_load("k".to_string(), WINDOW, || async {
                    tokio::time::sleep(Duration::from_millis(80)).await;
                    Ok("v".to_string())
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let error = map
            .get_or_load("other".to_string(), WINDOW, || async {
                Ok("w".to_string())
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            OnceMapError::AdmissionSaturated { inflight: 1 }
        ));
        task.await.unwrap().unwrap();
    }

    /// The default policy must keep callers alive when admission is full.
    #[tokio::test]
    async fn admission_bypass_runs_the_loader_inline() {
        let map: OnceMap<String, String, String> = OnceMap::builder().max_inflight(1).build();

        let map_for_task = map.clone();
        let task = tokio::spawn(async move {
            map_for_task
                .get_or_load("k".to_string(), WINDOW, || async {
                    tokio::time::sleep(Duration::from_millis(60)).await;
                    Ok("v".to_string())
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let value = map
            .get_or_load("other".to_string(), WINDOW, || async {
                Ok("w".to_string())
            })
            .await
            .unwrap();
        assert_eq!("w", value.as_str());
        assert_eq!(
            Some("w".to_string()),
            map.peek_within(&"other".to_string(), WINDOW)
                .map(|value| (*value).clone())
        );
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_slow_loader_times_out() {
        let map: OnceMap<String, String, String> = OnceMap::builder()
            .load_timeout(Duration::from_millis(20))
            .build();

        let error = map
            .get_or_load("k".to_string(), WINDOW, || async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok("v".to_string())
            })
            .await
            .unwrap_err();

        assert!(matches!(error, OnceMapError::LoadTimeout { .. }));
        assert_eq!(None, map.peek_within(&"k".to_string(), WINDOW));
    }

    #[tokio::test]
    async fn revalidating_returns_stale_while_refreshing_once() {
        let map: OnceMap<String, String, String> = OnceMap::builder().build();
        let refreshes = Arc::new(AtomicUsize::new(0));

        map.publish("k".to_string(), "old".to_string(), WINDOW);
        tokio::time::sleep(Duration::from_millis(30)).await;

        for _ in 0..4 {
            let refreshes = Arc::clone(&refreshes);
            let value = map
                .get_or_load_revalidating(
                    "k".to_string(),
                    Duration::from_millis(10),
                    Duration::from_secs(600),
                    || async { Ok("cold".to_string()) },
                    || async move {
                        refreshes.fetch_add(1, Ordering::SeqCst);
                        Ok("new".to_string())
                    },
                )
                .await
                .unwrap();
            assert_eq!("old", value.as_str());
        }

        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(1, refreshes.load(Ordering::SeqCst));
        assert_eq!(
            "new",
            map.peek_within(&"k".to_string(), WINDOW).unwrap().as_str()
        );
    }

    /// A failing refresh must back off instead of hammering the backend.
    #[tokio::test]
    async fn a_failing_refresh_backs_off() {
        let map: OnceMap<String, String, String> = OnceMap::builder()
            .refresh_backoff(Duration::from_secs(60), Duration::from_secs(60))
            .build();
        let attempts = Arc::new(AtomicUsize::new(0));

        map.publish("k".to_string(), "old".to_string(), WINDOW);
        tokio::time::sleep(Duration::from_millis(30)).await;

        for _ in 0..3 {
            let attempts = Arc::clone(&attempts);
            map.get_or_load_revalidating(
                "k".to_string(),
                Duration::from_millis(10),
                Duration::from_secs(600),
                || async { Ok("cold".to_string()) },
                move || async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err("down".to_string())
                },
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        assert_eq!(1, attempts.load(Ordering::SeqCst));
        assert_eq!(
            "old",
            map.peek_within(&"k".to_string(), WINDOW).unwrap().as_str()
        );
    }

    #[test]
    fn a_string_weights_by_capacity() {
        let text = value(100);
        assert!(text.cache_weight() >= 100);
    }

    /// A full ledger must release what has already expired before it starts
    /// evicting rows that are still inside their window.
    #[tokio::test]
    async fn an_expired_entry_is_released_before_a_live_one_is_evicted() {
        // Room for two entries and a bit, which is one short of three.
        let budget = Arc::new(CacheBudget::new(entry_weight(60) * 5 / 2));
        let map: OnceMap<String, String, String> = OnceMap::builder()
            .budget(Arc::clone(&budget))
            .capacity_bytes(usize::MAX)
            .build();

        map.publish("live".to_string(), value(60), WINDOW);
        map.publish("expiring".to_string(), value(60), Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(30)).await;

        map.publish("incoming".to_string(), value(60), WINDOW);

        assert_eq!(
            Some("x".repeat(60)),
            map.peek_within(&"live".to_string(), WINDOW)
                .map(|value| (*value).clone()),
            "the resident entry was dropped although an expired one held the bytes"
        );
        assert!(
            map.peek_within(&"expiring".to_string(), WINDOW).is_none(),
            "the expired entry should have been the one reclaimed"
        );
        assert!(budget.used() <= budget.limit());
    }

    /// A value larger than the cache's own cap can never stay resident, so it
    /// must not displace anything on its way in.
    #[tokio::test]
    async fn a_value_larger_than_the_capacity_is_not_retained() {
        // One small entry plus half again: room for the small one, not the big.
        let capacity = entry_weight(0) * 3 / 2;
        let map: OnceMap<String, String, String> =
            OnceMap::builder().capacity_bytes(capacity).build();

        map.publish("small".to_string(), String::new(), WINDOW);
        assert!(map.peek_within(&"small".to_string(), WINDOW).is_some());

        map.publish("large".to_string(), value(200), WINDOW);

        assert!(
            map.peek_within(&"large".to_string(), WINDOW).is_none(),
            "a value above the cache cap must not be retained"
        );
        assert!(
            map.peek_within(&"small".to_string(), WINDOW).is_some(),
            "the resident entry was evicted for a value that cannot be kept"
        );
    }

    /// A reload must run its own loader even while a cold load for the same key
    /// is in flight: the cold load is allowed to answer "nothing", so
    /// inheriting it would silently skip the reload.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_reload_does_not_inherit_a_cold_loads_answer() {
        let map: OnceMap<String, Option<String>, String> = OnceMap::builder().build();
        let key = "k".to_string();
        let cold_started = Arc::new(tokio::sync::Notify::new());
        let cold_gate = Arc::new(Semaphore::new(0));
        let authoritative_loads = Arc::new(AtomicUsize::new(0));

        let cold = {
            let map = map.clone();
            let key = key.clone();
            let started = Arc::clone(&cold_started);
            let gate = Arc::clone(&cold_gate);
            tokio::spawn(async move {
                map.get_or_load(key, WINDOW, || async move {
                    started.notify_one();
                    let _permit = gate.acquire().await.expect("gate is never closed");
                    Ok::<_, String>(None)
                })
                .await
            })
        };
        cold_started.notified().await;

        let reload = {
            let map = map.clone();
            let key = key.clone();
            let loads = Arc::clone(&authoritative_loads);
            tokio::spawn(async move {
                map.reload(key, WINDOW, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>(Some("fresh".to_string()))
                })
                .await
            })
        };

        // The reload answers from its own loader instead of waiting for the
        // cold flight; the cold flight is still parked on its gate.
        let reloaded = tokio::time::timeout(Duration::from_secs(5), reload)
            .await
            .expect("the reload must not wait for the cold load")
            .expect("reload task should join")
            .expect("reload should succeed");
        assert_eq!(Some("fresh".to_string()), *reloaded);
        assert_eq!(1, authoritative_loads.load(Ordering::SeqCst));

        cold_gate.add_permits(1);
        assert_eq!(
            None,
            *cold
                .await
                .expect("cold task should join")
                .expect("cold read")
        );
    }

    /// Two authoritative reloads still coalesce into a single loader run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reloads_coalesce() {
        let map: OnceMap<String, String, String> = OnceMap::builder().build();
        let loads = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let gate = Arc::new(Semaphore::new(0));

        let leader = {
            let map = map.clone();
            let loads = Arc::clone(&loads);
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                map.reload("k".to_string(), WINDOW, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    started.notify_one();
                    let _permit = gate.acquire().await.expect("gate is never closed");
                    Ok::<_, String>("value".to_string())
                })
                .await
            })
        };
        started.notified().await;

        let follower = {
            let map = map.clone();
            let loads = Arc::clone(&loads);
            tokio::spawn(async move {
                map.reload("k".to_string(), WINDOW, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>("second".to_string())
                })
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(1, loads.load(Ordering::SeqCst));

        gate.add_permits(1);
        assert_eq!("value", leader.await.unwrap().unwrap().as_str());
        assert_eq!("value", follower.await.unwrap().unwrap().as_str());
        assert_eq!(1, loads.load(Ordering::SeqCst));
    }

    /// An authoritative result is stored like an application write: a cold load
    /// that started before it must not overwrite it when it finishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_authoritative_result_survives_an_earlier_cold_load() {
        let map = cache();
        let key = "k".to_string();
        let cold_started = Arc::new(tokio::sync::Notify::new());
        let cold_gate = Arc::new(Semaphore::new(0));

        let cold = {
            let map = map.clone();
            let key = key.clone();
            let started = Arc::clone(&cold_started);
            let gate = Arc::clone(&cold_gate);
            tokio::spawn(async move {
                map.get_or_load(key, WINDOW, || async move {
                    started.notify_one();
                    let _permit = gate.acquire().await.expect("gate is never closed");
                    Ok::<_, String>("cold".to_string())
                })
                .await
            })
        };
        cold_started.notified().await;

        map.reload(key.clone(), WINDOW, || async {
            Ok::<_, String>("authoritative".to_string())
        })
        .await
        .expect("authoritative reload should succeed");

        cold_gate.add_permits(1);
        assert_eq!("cold", cold.await.unwrap().unwrap().as_str());
        assert_eq!(
            "authoritative",
            map.peek_within(&key, WINDOW).unwrap().as_str(),
            "the cold load started first and must not win"
        );
    }

    /// `invalidate_all` has to wake waiters of both load kinds, otherwise a
    /// reload waiting on a cold flight would sit there through the clear.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invalidate_all_aborts_both_load_kinds() {
        let map: OnceMap<String, Option<String>, String> = OnceMap::builder().build();
        let started = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));

        let cold = {
            let map = map.clone();
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                map.get_or_load("cold".to_string(), WINDOW, || async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let _permit = gate.acquire().await.expect("gate is never closed");
                    Ok::<_, String>(None)
                })
                .await
            })
        };
        let authoritative = {
            let map = map.clone();
            let started = Arc::clone(&started);
            let gate = Arc::clone(&gate);
            tokio::spawn(async move {
                map.reload("authoritative".to_string(), WINDOW, || async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let _permit = gate.acquire().await.expect("gate is never closed");
                    Ok::<_, String>(Some("auth".to_string()))
                })
                .await
            })
        };

        while started.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(2, map.inflight_len());

        map.invalidate_all();
        assert_eq!(0, map.inflight_len());

        gate.add_permits(2);
        cold.await.unwrap().unwrap();
        authoritative.await.unwrap().unwrap();
        // Neither load may repopulate a key the clear already dropped.
        assert!(map.is_empty());
    }

    /// A key can hold a cold flight and an authoritative flight at once, so an
    /// authoritative reload never waits on a cold load for the same key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_key_can_hold_one_flight_per_kind() {
        let map: OnceMap<String, Option<String>, String> =
            OnceMap::builder().max_inflight(2).build();
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let authoritative_loads = Arc::new(AtomicUsize::new(0));

        let cold = {
            let map = map.clone();
            let gate = Arc::clone(&gate);
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                map.get_or_load("k".to_string(), WINDOW, || async move {
                    started.notify_one();
                    let _permit = gate.acquire().await.expect("gate is never closed");
                    Ok::<_, String>(None)
                })
                .await
            })
        };
        started.notified().await;
        assert_eq!(1, map.inflight_len());

        let reloaded = {
            let map = map.clone();
            let loads = Arc::clone(&authoritative_loads);
            tokio::spawn(async move {
                map.reload("k".to_string(), WINDOW, || async move {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, String>(Some("auth".to_string()))
                })
                .await
            })
        };

        let reloaded = tokio::time::timeout(Duration::from_secs(5), reloaded)
            .await
            .expect("the reload must not wait on the cold flight")
            .expect("reload task should join")
            .expect("a reload owns its own slot");
        assert_eq!(Some("auth".to_string()), *reloaded);
        assert_eq!(1, authoritative_loads.load(Ordering::SeqCst));

        gate.add_permits(1);
        cold.await.unwrap().unwrap();
    }
}
