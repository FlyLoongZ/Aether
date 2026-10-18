//! Shared in-memory cache primitives.
//!
//! Two primitives with deliberately different semantics; neither substitutes
//! for the other.
//!
//! - [`OnceMap`] — keyed cache with request coalescing and invalidation.
//! - [`Gate`] — "at most once per window, per key".
//!
//! [`CacheWeight`] is the only input to a cache's byte budget: values say how
//! large they are, so capacity is expressed in bytes rather than in a guessed
//! number of entries. Caches can share one [`CacheBudget`], which turns a set of
//! individual caps into a process-wide ledger. Windows are per call, never per
//! cache — how old is too old is the caller's question.

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

#[cfg(test)]
pub use once_map::CacheStats;
