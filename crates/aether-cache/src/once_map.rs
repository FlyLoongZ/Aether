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
/// window is a floor on its life. Retention is additionally bounded by
/// `capacity_bytes` and, when one is installed, by a shared [`CacheBudget`].
/// Reclamation happens on write, so there is no background task.
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
    inflight: Mutex<HashMap<K, Arc<Flight<V, E>>>>,
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
    /// When `invalidate_all` last ran. Tombstones are cleared with the values,
    /// so derived writes need this to know a clear happened after their read.
    cleared_at: Option<Instant>,
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
        self.abort_flight_if(&key, stale);
    }

    /// Stores a value derived from a read that began at `read_started_at`,
    /// unless the key was invalidated after that point.
    ///
    /// Derived writes (mirrors, warm-ups) use this instead of a generation
    /// token: it can be repeated for the same read, and it still refuses to
    /// resurrect what a mutation dropped in the meantime.
    pub fn publish_if_not_invalidated_since(
        &self,
        key: K,
        value: V,
        window: Duration,
        read_started_at: Instant,
    ) -> bool {
        let key_weight = key.cache_weight();
        let now = Instant::now();
        let stale = match self.inner.store.write() {
            Ok(mut store) => {
                let invalidated_after_read = store
                    .tombstones
                    .get(&key)
                    .is_some_and(|tombstone| tombstone.at > read_started_at)
                    || store
                        .cleared_at
                        .is_some_and(|cleared_at| cleared_at > read_started_at);
                if invalidated_after_read {
                    return false;
                }
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
            Err(_) => return false,
        };
        // The write bumped the generation, so an in-flight load is already
        // obsolete; no flight has to be aborted for correctness here.
        let _ = stale;
        true
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
        self.abort_flight_if(key, stale);
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

        let mut stale_flights = Vec::new();
        let mut inflight = match self.inner.inflight.lock() {
            Ok(inflight) => inflight,
            Err(poisoned) => poisoned.into_inner(),
        };
        let keys = inflight.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let is_stale = inflight
                .get(&key)
                .is_some_and(|flight| flight.stamp.epoch == stale_epoch);
            if is_stale {
                if let Some(flight) = inflight.remove(&key) {
                    stale_flights.push(flight);
                }
            }
        }
        drop(inflight);
        for flight in stale_flights {
            flight.complete(Completion::Invalidated);
        }
    }

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

    /// Iterates the entries still inside their own window.
    pub fn iter_fresh(&self) -> Vec<(K, Arc<V>, Duration)> {
        let now = Instant::now();
        let Ok(store) = self.inner.store.read() else {
            return Vec::new();
        };
        store
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                let age = now.saturating_duration_since(entry.inserted_at);
                (age <= entry.ttl).then(|| (key.clone(), Arc::clone(&entry.value), age))
            })
            .collect()
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

    /// Aborts the flight only if it captured `stale`; a load that registered
    /// after the mutation belongs to the new generation and must be left alone.
    fn abort_flight_if<Q>(&self, key: &Q, stale: Stamp)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let flight = match self.inner.inflight.lock() {
            Ok(mut inflight) => match inflight.get(key) {
                Some(flight) if flight.stamp == stale => inflight.remove(key),
                _ => None,
            },
            Err(poisoned) => {
                let mut inflight = poisoned.into_inner();
                match inflight.get(key) {
                    Some(flight) if flight.stamp == stale => inflight.remove(key),
                    _ => None,
                }
            }
        };
        if let Some(flight) = flight {
            flight.complete(Completion::Invalidated);
        }
    }

    fn inflight_len(&self) -> usize {
        match self.inner.inflight.lock() {
            Ok(inflight) => inflight.len(),
            Err(poisoned) => poisoned.into_inner().len(),
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
        self.drive(key, window, load).await
    }

    /// Runs the loader even when a value is cached, still only once per key.
    ///
    /// Callers that must not act on a stale value at all — authorization, for
    /// one — use this instead of lowering the window and losing the ability to
    /// serve the stale window at all.
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
        self.drive(key, window, load).await
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
        self.drive(key, stale, cold_load).await
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
        let mut guard = match self.register(&key) {
            Registration::Leader(guard) => guard,
            Registration::Follower(_) | Registration::Saturated => return false,
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
            match self.register(&key) {
                Registration::Saturated => {
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
                        Ok(value) => Ok(self.publish_bypass(key, value, window)),
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

    /// Publishes an unregistered load's result (admission bypass). It is a
    /// fresh read, so it takes the next generation like `publish` does.
    fn publish_bypass(&self, key: K, value: V, window: Duration) -> Arc<V> {
        let key_weight = key.cache_weight();
        let value = Arc::new(value);
        let now = Instant::now();
        if let Ok(mut store) = self.inner.store.write() {
            let stale = store.stamp(&key);
            store.invalidate(&key, key_weight, now);
            store.insert(
                key,
                Arc::clone(&value),
                window,
                stale.key_epoch.wrapping_add(1),
                key_weight,
                now,
            );
            self.inner.reap(&mut store, now);
        }
        value
    }

    fn register(&self, key: &K) -> Registration<K, V, E> {
        let stamp = match self.inner.store.read() {
            Ok(store) => store.stamp(key),
            Err(poisoned) => poisoned.into_inner().stamp(key),
        };

        let mut inflight = match self.inner.inflight.lock() {
            Ok(inflight) => inflight,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(flight) = inflight.get(key) {
            return Registration::Follower(Arc::clone(flight));
        }
        if inflight.len() >= self.inner.limits.max_inflight {
            return Registration::Saturated;
        }

        let flight = Arc::new(Flight::new(stamp));
        inflight.insert(key.clone(), Arc::clone(&flight));
        Registration::Leader(FlightGuard {
            inner: Arc::clone(&self.inner),
            key: key.clone(),
            flight,
            stamp,
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
    Saturated,
}

/// Owns the leader slot for one key. Dropping it without finishing means the
/// leader went away, so waiters are woken to elect a replacement.
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
    done: bool,
}

impl<K, V, E> FlightGuard<K, V, E>
where
    K: Clone + Eq + Hash + CacheWeight,
    V: CacheWeight,
{
    /// Stores the loaded value unless the key was mutated while this load ran,
    /// then hands it to every waiter regardless.
    fn publish(&mut self, value: V, window: Duration) -> Arc<V> {
        let key_weight = self.key.cache_weight();
        let value = Arc::new(value);
        let now = Instant::now();
        if let Ok(mut store) = self.inner.store.write() {
            if store.stamp(&self.key) == self.stamp {
                store.insert(
                    self.key.clone(),
                    Arc::clone(&value),
                    window,
                    self.stamp.key_epoch,
                    key_weight,
                    now,
                );
                self.inner.reap(&mut store, now);
            }
        }

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
        match self.inner.inflight.lock() {
            Ok(mut inflight) => {
                // A newer flight for this key may have replaced ours; only the
                // owner may remove the slot.
                if inflight
                    .get(&self.key)
                    .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
                {
                    inflight.remove(&self.key);
                }
            }
            Err(poisoned) => {
                let mut inflight = poisoned.into_inner();
                if inflight
                    .get(&self.key)
                    .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
                {
                    inflight.remove(&self.key);
                }
            }
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
    fn reap(&self, store: &mut Store<K, V>, now: Instant) {
        let inflight = match self.inflight.lock() {
            Ok(inflight) => inflight,
            Err(poisoned) => poisoned.into_inner(),
        };
        store.reap(now, &|key: &K| inflight.contains_key(key));
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
        let weight = key_weight.saturating_add(value.cache_weight());
        self.take_entry(&key);
        if let Some(previous_tombstone) = self.tombstones.remove(&key) {
            self.weight = self.weight.saturating_sub(previous_tombstone.weight);
        }

        if let Some(budget) = self.budget.clone() {
            let mut evictions = 0;
            while !budget.try_charge(weight) {
                if evictions >= MAX_EVICTIONS_PER_INSERT || !self.evict_oldest() {
                    // The ledger is full and this cache has nothing left to
                    // give: the caller still gets its value, it just is not
                    // retained.
                    return;
                }
                evictions += 1;
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
        self.cleared_at = Some(Instant::now());
        self.shrink();
    }

    /// Drops expired entries, expired tombstones and, once the byte budget is
    /// exceeded, the oldest survivors. Both operations pop their own queue.
    fn reap(&mut self, now: Instant, is_inflight: &dyn Fn(&K) -> bool) {
        self.reap_tombstones(now, is_inflight);

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

            let expired = now.saturating_duration_since(entry.inserted_at) > entry.ttl;
            let over_capacity = self.weight > self.capacity_bytes;
            let over_budget = self
                .budget
                .as_ref()
                .is_some_and(|budget| budget.over_limit());
            if !expired && !over_capacity && !over_budget {
                break;
            }
            self.order.pop_front();
            self.take_entry(&key);
        }

        self.compact();
        self.shrink();
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
                    cleared_at: None,
                    weight: 0,
                    capacity_bytes: self.capacity_bytes,
                    tombstone_ttl: self.tombstone_ttl,
                    budget: self.budget,
                }),
                inflight: Mutex::new(HashMap::new()),
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
    use std::time::{Duration, Instant};

    const WINDOW: Duration = Duration::from_secs(60);

    fn cache() -> OnceMap<String, String, String> {
        OnceMap::builder().max_inflight(64).build()
    }

    fn value(bytes: usize) -> String {
        "x".repeat(bytes)
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
    async fn a_derived_write_is_repeatable_and_refuses_to_resurrect() {
        let map = cache();
        let key = "k".to_string();
        let read_started_at = Instant::now();

        assert!(map.publish_if_not_invalidated_since(
            key.clone(),
            "first".to_string(),
            WINDOW,
            read_started_at
        ));
        // Same read, second mirror target: still allowed.
        assert!(map.publish_if_not_invalidated_since(
            key.clone(),
            "second".to_string(),
            WINDOW,
            read_started_at
        ));
        assert_eq!("second", map.peek_within(&key, WINDOW).unwrap().as_str());

        // A mutation after the read began must win over a late derived write.
        tokio::time::sleep(Duration::from_millis(5)).await;
        map.invalidate(&key);
        assert!(!map.publish_if_not_invalidated_since(
            key.clone(),
            "resurrected".to_string(),
            WINDOW,
            read_started_at
        ));
        assert_eq!(None, map.peek_within(&key, WINDOW));
    }

    #[tokio::test]
    async fn clearing_the_cache_also_blocks_derived_writes() {
        let map = cache();
        let key = "k".to_string();
        let read_started_at = Instant::now();
        tokio::time::sleep(Duration::from_millis(2)).await;

        map.invalidate_all();

        assert!(
            !map.publish_if_not_invalidated_since(
                key.clone(),
                "late".to_string(),
                WINDOW,
                read_started_at
            ),
            "a whole-cache clear must win over a mirror of the previous read"
        );
        assert_eq!(None, map.peek_within(&key, WINDOW));
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
    async fn the_shared_budget_refuses_what_does_not_fit() {
        let budget = Arc::new(CacheBudget::new(256));
        let map: OnceMap<String, String, String> =
            OnceMap::builder().budget(Arc::clone(&budget)).build();

        for index in 0..16 {
            map.publish(format!("k{index}"), value(64), WINDOW);
        }

        assert!(budget.rejections() > 0);
        assert!(budget.used() <= budget.limit());
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
}
