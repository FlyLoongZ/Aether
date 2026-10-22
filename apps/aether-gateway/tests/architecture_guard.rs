//! Executable architecture guardrails.
//!
//! The previous guard was a 14k-line tree of source-string assertions. This
//! file keeps only the checks the compiler cannot express: SQL ownership,
//! sensitive log fields, removed legacy components, and crate dependency tiers.
//! Each check is intentionally small and fails with the offending path.

use std::fs;
use std::path::{Path, PathBuf};

const SKIP_DIRS: &[&str] = &["target", ".git", "node_modules", "dist"];

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve")
}

fn gateway_src(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative)
}

fn read_workspace_file(relative: &str) -> String {
    fs::read_to_string(workspace_root().join(relative))
        .unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

fn workspace_file_exists(relative: &str) -> bool {
    workspace_root().join(relative).exists()
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries {
        let path = entry.expect("directory entry should be readable").path();
        if path.is_dir() {
            let name = path.file_name().and_then(|value| value.to_str());
            if name.is_some_and(|name| SKIP_DIRS.contains(&name)) {
                continue;
            }
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn source_files(root: &Path) -> Vec<(String, String)> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files.sort();
    let workspace = workspace_root();
    files
        .into_iter()
        .map(|path| {
            let relative = path
                .strip_prefix(&workspace)
                .unwrap_or(&path)
                .display()
                .to_string();
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
            (relative, source)
        })
        .collect()
}

fn assert_no_patterns(root: &Path, patterns: &[&str], label: &str) {
    let violations = source_files(root)
        .into_iter()
        .filter_map(|(path, source)| {
            let hits = patterns
                .iter()
                .filter(|pattern| source.contains(**pattern))
                .copied()
                .collect::<Vec<_>>();
            (!hits.is_empty()).then(|| format!("{path} -> {}", hits.join(", ")))
        })
        .collect::<Vec<_>>();

    assert!(violations.is_empty(), "{label}:\n{}", violations.join("\n"));
}

fn contains_identifier(source: &str, needle: &str) -> bool {
    source
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|identifier| identifier == needle)
}

fn assert_no_sqlx_queries(root: &Path) {
    let patterns = [
        "sqlx::query(",
        "sqlx::query_scalar",
        "sqlx::postgres::PgRow",
        "sqlx::Row",
        "PostgresPoolFactory",
        "query_scalar::<",
        "QueryBuilder<",
    ];
    let violations = source_files(root)
        .into_iter()
        .filter_map(|(path, source)| {
            let mut hits = patterns
                .iter()
                .filter(|pattern| source.contains(**pattern))
                .copied()
                .collect::<Vec<_>>();
            if contains_identifier(&source, "PostgresPool") {
                hits.push("PostgresPool");
            }
            (!hits.is_empty()).then(|| format!("{path} -> {}", hits.join(", ")))
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "disallowed SQL ownership violations:\n{}",
        violations.join("\n")
    );
}

fn assert_manifest_excludes(path: &str, forbidden: &[&str]) {
    let manifest = read_workspace_file(path);
    let violations = forbidden
        .iter()
        .filter(|dependency| manifest.contains(**dependency))
        .copied()
        .collect::<Vec<_>>();
    assert!(
        violations.is_empty(),
        "{path} crosses its dependency tier through: {}",
        violations.join(", ")
    );
}

#[test]
fn no_disabled_test_modules() {
    for root in ["apps", "crates"] {
        let violations = source_files(&workspace_root().join(root))
            .into_iter()
            .filter(|(path, _)| !path.ends_with("tests/architecture_guard.rs"))
            .filter_map(|(path, source)| {
                source
                    .contains("cfg(any())")
                    .then_some(format!("{path} -> cfg(any())"))
            })
            .collect::<Vec<_>>();
        assert!(
            violations.is_empty(),
            "disabled test modules are not allowed:\n{}",
            violations.join("\n")
        );
    }
}

#[test]
fn handlers_do_not_inline_sql_queries() {
    assert_no_sqlx_queries(&gateway_src("handlers"));
}

#[test]
fn gateway_runtime_does_not_inline_sql_queries() {
    assert_no_sqlx_queries(&gateway_src("state/runtime"));
}

#[test]
fn hotspot_modules_do_not_log_sensitive_payload_like_fields() {
    let patterns = [
        "report_context = ?",
        "payload = ?",
        "headers = ?",
        "original_request_body = ?",
        "provider_request_body = ?",
        "request_body = ?",
        "response_body = ?",
    ];
    for root in [
        gateway_src("ai_serving"),
        gateway_src("execution_runtime"),
        gateway_src("usage"),
        gateway_src("async_task"),
    ] {
        assert_no_patterns(
            &root,
            &patterns,
            "hotspot modules must not log payload-like fields",
        );
    }
}

#[test]
fn gateway_does_not_log_raw_request_queries_or_credential_bearing_urls() {
    let patterns = [
        "request_query_string = %",
        "request_query_string = ?",
        "request_path_and_query = %request_context.request_path_and_query()",
        "request_path_and_query = ?request_context.request_path_and_query()",
        "upstream_url = %",
        "upstream_url = ?",
        "plan_url = %",
        "plan_url = ?",
        "token_url = %",
        "token_url = ?",
        "endpoint_base_url = %",
        "endpoint_base_url = ?",
        "url = %plan.url",
        "url = ?plan.url",
    ];
    for root in [
        gateway_src("ai_serving"),
        gateway_src("execution_runtime"),
        gateway_src("handlers"),
        gateway_src("maintenance"),
        gateway_src("oauth"),
        gateway_src("state"),
        workspace_root().join("crates/aether-provider/transport/src"),
    ] {
        assert_no_patterns(
            &root,
            &patterns,
            "raw request queries and credential-bearing URLs must not be logged",
        );
    }
}

#[test]
fn identity_oauth_does_not_persist_raw_userinfo_claims() {
    let source = fs::read_to_string(gateway_src("oauth/identity_repo.rs"))
        .expect("identity OAuth repository should be readable");
    assert!(
        !source.contains("claims.raw"),
        "identity OAuth must persist only dedicated claim fields, never raw userinfo JSON"
    );
}

#[test]
fn handlers_do_not_depend_on_raw_state_records() {
    assert_no_patterns(
        &gateway_src("handlers"),
        &[
            "StoredUserSessionRecord",
            "StoredUserPreferenceRecord",
            "AdminPaymentCallbackRecord",
        ],
        "handlers must use handler-facing state views instead of raw stored records",
    );
}

#[test]
fn pure_policy_crates_do_not_depend_on_runtime_adapters() {
    assert_manifest_excludes(
        "crates/aether-task/core/Cargo.toml",
        &[
            "axum",
            "sqlx",
            "redis",
            "reqwest",
            "wreq",
            "tokio",
            "aether-data =",
            "aether-gateway",
        ],
    );
}

#[test]
fn database_adapters_are_independent_driver_boundaries() {
    let path = "crates/aether-data/adapters/postgres/Cargo.toml";
    let manifest = read_workspace_file(path);
    assert!(manifest.contains("aether-data-contracts.workspace = true"));
    assert!(manifest.contains("features = [\"postgres\""));
    assert_manifest_excludes(path, &["aether-gateway", "axum"]);
}

#[test]
fn gateway_runtime_components_keep_focused_dependency_surfaces() {
    assert_manifest_excludes(
        "crates/aether-gateway/frontdoor/Cargo.toml",
        &[
            "aether-data",
            "aether-provider-transport",
            "aether-gateway-workers",
            "sqlx",
            "redis",
        ],
    );
    assert_manifest_excludes(
        "crates/aether-gateway/workers/Cargo.toml",
        &[
            "axum",
            "aether-gateway-frontdoor",
            "aether-provider-transport",
        ],
    );
    assert_manifest_excludes(
        "crates/aether-gateway/execution/Cargo.toml",
        &[
            "axum",
            "sqlx",
            "redis",
            "aether-data",
            "aether-gateway-frontdoor",
            "aether-gateway-workers",
        ],
    );
    assert_manifest_excludes(
        "crates/aether-gateway/control/Cargo.toml",
        &[
            "sqlx",
            "redis",
            "reqwest",
            "aether-data",
            "aether-provider-transport",
            "aether-gateway-workers",
        ],
    );
    assert_manifest_excludes(
        "crates/aether-testing/loadtools/Cargo.toml",
        &[
            "aether-gateway",
            "aether-testkit",
            "aether-data",
            "axum",
            "redis",
        ],
    );
}

#[test]
fn ai_serving_crate_owns_attempt_loop_without_gateway_runtime_deps() {
    assert_manifest_excludes(
        "crates/aether-ai/serving/Cargo.toml",
        &["axum", "sqlx", "redis", "aether-gateway"],
    );
}

#[test]
fn data_query_helpers_belong_to_adapters_not_the_runtime_facade() {
    let data_manifest = read_workspace_file("crates/aether-data/runtime/Cargo.toml");
    assert!(
        !data_manifest.contains("aether-data-query.workspace = true"),
        "aether-data should not keep a direct query-helper dependency after SQL repositories move to adapters"
    );
    let adapter_manifest = read_workspace_file("crates/aether-data/adapters/postgres/Cargo.toml");
    assert!(adapter_manifest.contains("aether-data-query.workspace = true"));
}

#[test]
fn data_facade_defaults_to_postgres_and_gateway_selects_all_drivers_explicitly() {
    let data_manifest = read_workspace_file("crates/aether-data/runtime/Cargo.toml");
    assert!(data_manifest.contains("default = [\"postgres\"]"));
    assert!(data_manifest.contains("all-drivers = [\"postgres\"]"));
    assert!(data_manifest.contains("aether-data-postgres = { workspace = true, optional = true }"));

    let gateway_manifest = read_workspace_file("apps/aether-gateway/Cargo.toml");
    assert!(gateway_manifest
        .contains("aether-data = { workspace = true, features = [\"all-drivers\"] }"));
}

#[test]
fn testkit_gateway_harness_is_opt_in() {
    let manifest = read_workspace_file("crates/aether-testing/testkit/Cargo.toml");
    assert!(manifest.contains("default = []"));
    assert!(manifest.contains("gateway = [\"dep:aether-gateway\", \"dep:aether-runtime-state\"]"));
    assert!(manifest.contains("postgres = [\"dep:aether-data\", \"dep:sqlx\"]"));
    assert!(manifest.contains(
        "aether-gateway = { workspace = true, features = [\"testkit\"], optional = true }"
    ));

    let lib = read_workspace_file("crates/aether-testing/testkit/src/lib.rs").replace("\r\n", "\n");
    for module in ["execution_runtime", "gateway"] {
        assert!(
            lib.contains(&format!("#[cfg(feature = \"gateway\")]\nmod {module};")),
            "aether-testkit::{module} should be feature-gated"
        );
    }
    assert!(
        lib.contains("#[cfg(feature = \"postgres\")]\nmod postgres;"),
        "the Postgres helper should be feature-gated"
    );

    let integration_manifest = read_workspace_file("crates/aether-testing/integration/Cargo.toml");
    assert!(integration_manifest
        .contains("aether-testkit = { workspace = true, features = [\"gateway\", \"postgres\"] }"));
}

#[test]
fn tunnel_feature_is_removed() {
    for removed_path in [
        "apps/aether-tunnel",
        "crates/aether-gateway/tunnel",
        "apps/aether-gateway/src/tunnel",
    ] {
        assert!(
            !workspace_file_exists(removed_path),
            "removed tunnel component {removed_path} must not reappear"
        );
    }

    let workspace = read_workspace_file("Cargo.toml");
    let gateway_manifest = read_workspace_file("apps/aether-gateway/Cargo.toml");
    for forbidden in [
        "apps/aether-tunnel",
        "aether-tunnel",
        "aether_gateway_tunnel",
    ] {
        assert!(
            !workspace.contains(forbidden) && !gateway_manifest.contains(forbidden),
            "workspace must not reference removed tunnel component {forbidden}"
        );
    }

    let gateway_lib = read_workspace_file("apps/aether-gateway/src/lib.rs");
    assert!(
        !gateway_lib.contains("mod tunnel") && !gateway_lib.contains("crate::tunnel"),
        "gateway must not declare a tunnel module"
    );

    let env_example = read_workspace_file(".env.example");
    assert!(
        !env_example.contains("AETHER_TUNNEL_"),
        ".env.example must not expose removed AETHER_TUNNEL_* variables"
    );
}

#[test]
fn legacy_ai_pipeline_identifiers_are_removed() {
    let forbidden = [
        "aether-ai-pipeline",
        "aether_ai_pipeline",
        "ai_pipeline::",
        "ai_pipeline_api",
        "PipelineFinalizeError",
    ];
    let violations = [
        "apps/aether-gateway/src",
        "crates/aether-ai/serving/src",
        "crates/aether-ai/formats/src",
    ]
    .into_iter()
    .flat_map(|root| source_files(&workspace_root().join(root)))
    .filter(|(path, _)| !path.contains("/tests/"))
    .filter_map(|(path, source)| {
        let hits = forbidden
            .iter()
            .filter(|pattern| source.contains(**pattern))
            .copied()
            .collect::<Vec<_>>();
        (!hits.is_empty()).then(|| format!("{path} -> {}", hits.join(", ")))
    })
    .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "legacy AI pipeline identifiers must not remain in active code:\n{}",
        violations.join("\n")
    );

    let workspace = read_workspace_file("Cargo.toml");
    let gateway_manifest = read_workspace_file("apps/aether-gateway/Cargo.toml");
    assert!(!workspace.contains("aether-ai-pipeline"));
    assert!(!gateway_manifest.contains("aether-ai-pipeline"));
}
