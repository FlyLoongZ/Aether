//! Database lifecycle workflows.
//!
//! Runtime request paths should not depend on this module directly except at
//! process startup or explicit maintenance commands.

pub mod backfill;
pub(crate) mod bootstrap;
pub mod migrate;
#[cfg(test)]
mod postgres_test_support;
