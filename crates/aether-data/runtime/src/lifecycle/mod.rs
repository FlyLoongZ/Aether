//! Database lifecycle workflows.
//!
//! Runtime request paths should not depend on this module directly except at
//! process startup or explicit maintenance commands.

pub mod backfill;
#[cfg(feature = "postgres")]
pub(crate) mod bootstrap;
pub mod migrate;
#[cfg(all(test, feature = "postgres"))]
mod postgres_test_support;
