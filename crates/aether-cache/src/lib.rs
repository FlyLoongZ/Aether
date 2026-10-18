//! Shared in-memory cache primitives.
//!
//! Three primitives with deliberately different semantics; none substitutes for
//! another.
//!
//! - [`OnceMap`] — keyed cache with request coalescing and invalidation.
//! - [`Gate`] — "at most once per window, per key".
//! - [`Snapshot`] — one whole-table version, replaced as a unit.
//!
//! [`CacheWeight`] is the only input to a cache's byte budget: values say how
//! large they are, so capacity is expressed in bytes rather than in a guessed
//! number of entries. Caches can share one [`CacheBudget`], which turns a set of
//! individual caps into a process-wide ledger of what the layer retains; the
//! ledger only admits or refuses, it never reclaims for another cache. Windows
//! are per call, never per cache — how old is too old is the caller's question.
//!
//! Caches built on these primitives charge what they retain, keys and per-entry
//! bookkeeping included, so `capacity_bytes` is a bound on memory rather than on
//! the size of the values alone.

mod budget;
mod gate;
mod namespace;
mod once_map;
mod snapshot;
mod weight;

pub use budget::CacheBudget;
pub use gate::Gate;
pub use namespace::CacheKeyNamespace;
pub use once_map::{Admission, CacheObserver, OnceMap, OnceMapBuilder, OnceMapError, PublishedMap};
pub use snapshot::Snapshot;
pub use weight::{optional_strings_weight, value_weight, vec_weight, CacheWeight};
