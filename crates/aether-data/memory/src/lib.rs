//! In-memory repository implementations for Aether tests.
//!
//! This crate is intentionally test-only. It depends on the repository
//! contracts but not on the production data runtime, so tests can use the
//! same fakes without pulling `aether-data` or `aether-data-postgres` into
//! their dependency graph.

pub mod announcements;
pub mod audit;
pub mod auth;
pub mod auth_modules;
pub mod background_tasks;
pub mod billing;
pub mod candidate_selection;
pub mod candidates;
pub mod gemini_file_mappings;
pub mod global_models;
pub mod management_tokens;
pub mod oauth_providers;
pub mod pool_scores;
pub mod provider_catalog;
pub mod proxy_nodes;
pub mod quota;
pub mod routing_profiles;
pub mod settlement;
pub mod usage;
pub mod users;
pub mod wallet;
