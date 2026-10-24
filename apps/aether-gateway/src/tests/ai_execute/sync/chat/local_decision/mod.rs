use super::{
    any, build_router_with_state, build_state_with_execution_runtime_override,
    encrypt_python_fernet_plaintext, json, run_async_test_on_large_stack, start_server, to_bytes,
    AppState, Arc, Body, Digest, InMemoryAuthApiKeySnapshotRepository,
    InMemoryMinimalCandidateSelectionReadRepository, InMemoryProviderCatalogReadRepository,
    InMemoryRequestCandidateRepository, Json, Mutex, Request, RequestCandidateReadRepository,
    RequestCandidateStatus, Router, Sha256, StatusCode, StoredAuthApiKeySnapshot,
    StoredMinimalCandidateSelectionRow, StoredProviderCatalogEndpoint, StoredProviderCatalogKey,
    StoredProviderCatalogProvider, StoredProviderModelMapping, DEVELOPMENT_ENCRYPTION_KEY,
    EXECUTION_PATH_EXECUTION_RUNTIME_SYNC, EXECUTION_PATH_HEADER, TRACE_ID_HEADER,
};
fn hash_api_key(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn sample_auth_snapshot(api_key_id: &str, user_id: &str) -> StoredAuthApiKeySnapshot {
    StoredAuthApiKeySnapshot::new(
        user_id.to_string(),
        "alice".to_string(),
        Some("alice@example.com".to_string()),
        "user".to_string(),
        "local".to_string(),
        true,
        false,
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-5"])),
        api_key_id.to_string(),
        Some("default".to_string()),
        true,
        false,
        false,
        Some(60),
        Some(5),
        Some(4_102_444_800),
        Some(serde_json::json!(["openai"])),
        Some(serde_json::json!(["openai:chat"])),
        Some(serde_json::json!(["gpt-5"])),
    )
    .expect("auth snapshot should build")
}

mod cross_format;
mod pii;
mod sync;
