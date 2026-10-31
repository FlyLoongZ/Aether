use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use aether_contracts::{
    ExecutionPlan, ExecutionResponseBodyMode, ExecutionTimeouts, ProxySnapshot, RequestBody,
    ResolvedTransportProfile, EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER,
    EXECUTION_RESPONSE_BODY_MODE_HEADER, PROXY_NODE_GENERATION_EXTRA_KEY,
    TRANSPORT_BACKEND_BROWSER_WREQ, TRANSPORT_BACKEND_REQWEST_RUSTLS, TRANSPORT_HTTP_MODE_AUTO,
    TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE, TRANSPORT_HTTP_MODE_HTTP1_ONLY,
};

use aether_data::repository::proxy_nodes::{
    InMemoryProxyNodeRepository, ProxyNodeReadRepository, StoredProxyNode,
};
use axum::body::{Body, Bytes};

use axum::http::HeaderMap as AxumHeaderMap;
use axum::routing::{any, post};
use axum::{Json, Router};
use base64::Engine as _;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{
    append_upstream_response_body_chunk_with_limit, build_browser_wreq_client, build_client,
    build_execution_response_body, build_request_headers, decode_base64_body_with_limit,
    decode_response_body_bytes_with_limit, effective_response_body_limit_bytes,
    execution_plan_response_body_limit_bytes, execution_response_body_mode,
    execution_result_envelope_limit_bytes, http_uris_have_same_origin,
    json_value_fits_serialized_limit, maximum_base64_len_for_decoded_limit,
    record_manual_proxy_request_failure, record_manual_proxy_request_outcome,
    record_manual_proxy_request_success, record_manual_proxy_stream_error,
    reqwest_urls_have_same_origin, resolve_non_stream_total_timeout, resolve_proxy_url,
    safe_redirect_decision, validate_execution_upstream_url, with_upstream_response_body_limit,
    DirectSyncExecutionRuntime, ExecutionRuntimeTransportError, ExecutionTransportControls,
    SafeRedirectDecision, UpstreamResponseBodyPhase, DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
    EXECUTION_RESPONSE_BODY_LIMIT_HEADER, MAX_SAFE_REDIRECTS, MAX_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
    MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
};
use crate::frontdoor_loop_guard::{
    frontdoor_self_loop_public_ai_path, gateway_frontdoor_self_loop_guard_error_with_port,
    gateway_frontdoor_self_loop_guard_matches_with_port,
};
use crate::AppState;

const LOCAL_HTTP_SUCCESS_TIMEOUT_MS: u64 = 15_000;

#[test]
fn execution_upstream_url_accepts_http_and_https_with_safe_targets() {
    for allowed in [
        "https://api.example.test/v1/responses?api-version=1",
        "http://api.example.test:8080/v1/responses?api-version=1",
        "http://8.8.8.8:8080/v1/responses",
        "https://8.8.8.8/v1/responses",
        "http://[2606:4700:4700::1111]:8080/v1/responses",
        "http://localhost:8080/v1/responses",
        "http://127.42.0.1:8080/v1/responses",
        "http://[::1]:8080/v1/responses",
    ] {
        assert!(
            validate_execution_upstream_url(allowed).is_ok(),
            "URL should be accepted: {allowed}"
        );
    }

    for rejected in [
        "http://10.0.0.1/v1/responses",
        "http://0.0.0.0:8080/v1/responses",
        "http://[::ffff:127.0.0.1]:8080/v1/responses",
        "https://127.0.0.1:8443/v1/responses",
        "https://10.0.0.1:8443/v1/responses",
        "https://token@example.test/v1/responses",
        "https://example.test/v1/responses#secret",
        "http://token@example.test/v1/responses",
        "http://example.test/v1/responses#secret",
        "ftp://localhost/resource",
    ] {
        assert!(
            validate_execution_upstream_url(rejected).is_err(),
            "URL should be rejected: {rejected}"
        );
    }
}

#[test]
fn invalid_execution_upstream_url_error_does_not_echo_credentials() {
    let error = validate_execution_upstream_url(
        "https://sensitive-user:sensitive-password@example.test/v1/responses",
    )
    .expect_err("URL userinfo should be rejected")
    .to_string();

    assert!(!error.contains("sensitive-user"));
    assert!(!error.contains("sensitive-password"));
}

#[test]
fn execution_dns_answers_allow_private_and_fake_ip_provider_addresses() {
    let addresses = vec![
        "198.18.78.41:443".parse().unwrap(),
        "10.0.0.8:443".parse().unwrap(),
        "127.0.0.1:443".parse().unwrap(),
        "169.254.169.254:443".parse().unwrap(),
        "[fd00::1]:443".parse().unwrap(),
        "93.184.216.34:443".parse().unwrap(),
    ];
    assert_eq!(
        super::validate_resolved_execution_addresses(addresses.clone())
            .expect("provider DNS answers should pass through"),
        addresses
    );
}

#[tokio::test]
async fn execution_dns_resolves_literal_ipv6_provider_targets() {
    for (host, expected) in [("[::1]", "[::1]:8443"), ("[fd00::1]", "[fd00::1]:8443")] {
        let addresses = super::resolve_execution_target_addresses(host, 8443)
            .await
            .expect("literal IPv6 provider addresses should resolve without DNS");
        assert_eq!(
            addresses,
            vec![expected.parse::<std::net::SocketAddr>().unwrap()]
        );
    }
}

#[tokio::test]
async fn execution_dns_resolvers_preserve_provider_fake_ip_answers() {
    for host in ["198.18.78.41", "198.19.1.2"] {
        let expected = vec![format!("{host}:0").parse::<std::net::SocketAddr>().unwrap()];
        let reqwest_addresses =
            reqwest::dns::Resolve::resolve(&super::ExecutionSafeDnsResolver, host.parse().unwrap())
                .await
                .expect("HTTP provider DNS must accept Fake-IP answers")
                .collect::<Vec<_>>();
        let wreq_addresses =
            wreq::dns::Resolve::resolve(&super::ExecutionSafeDnsResolver, host.into())
                .await
                .expect("WebSocket provider DNS must accept Fake-IP answers")
                .collect::<Vec<_>>();

        assert_eq!(reqwest_addresses, expected);
        assert_eq!(wreq_addresses, expected);
    }
}

#[test]
fn execution_dns_rejects_empty_answers() {
    assert_eq!(
        super::validate_resolved_execution_addresses(Vec::new())
            .expect_err("empty DNS answers must fail")
            .kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn execution_proxy_url_policy_rejects_non_origin_components() {
    for rejected in [
        "mailto:proxy@example.test",
        "http://proxy.example.test/path",
        "http://proxy.example.test?token=secret",
        "http://proxy.example.test#fragment",
    ] {
        assert!(
            super::validate_execution_proxy_url(rejected).is_err(),
            "proxy URL should be rejected: {rejected}"
        );
    }
    assert!(
        super::validate_execution_proxy_url("http://alice:password@proxy.example.test:8080")
            .is_ok()
    );
}

#[test]
fn execution_proxy_url_normalizes_local_socks_dns_to_remote_dns() {
    assert_eq!(
        super::normalize_execution_proxy_url("socks5://alice:password@proxy.example.test:1080")
            .expect("socks5 URL should normalize"),
        "socks5h://alice:password@proxy.example.test:1080"
    );
    assert_eq!(
        super::normalize_execution_proxy_url("socks5h://proxy.example.test:1080")
            .expect("socks5h URL should remain valid"),
        "socks5h://proxy.example.test:1080"
    );
    assert_eq!(
        super::normalize_execution_proxy_url("https://proxy.example.test:8443")
            .expect("https URL should remain valid"),
        "https://proxy.example.test:8443/"
    );
}

#[test]
fn enabled_proxy_without_a_usable_target_is_rejected() {
    let proxy = ProxySnapshot {
        enabled: Some(true),
        mode: Some("unavailable".to_string()),
        ..ProxySnapshot::default()
    };

    assert!(matches!(
        resolve_proxy_url(Some(&proxy)),
        Err(ExecutionRuntimeTransportError::ProxyUnsupported)
    ));
    assert_eq!(
        resolve_proxy_url(Some(&ProxySnapshot {
            enabled: Some(false),
            ..ProxySnapshot::default()
        }))
        .expect("disabled proxy should be accepted"),
        None
    );
}

#[test]
fn redirect_origin_checks_scheme_host_and_effective_port() {
    let reqwest_base =
        reqwest::Url::parse("https://api.example.com/v1").expect("base URL should parse");
    for same_origin in [
        "https://api.example.com/v2",
        "https://API.EXAMPLE.COM:443/v2",
    ] {
        let next = reqwest::Url::parse(same_origin).expect("same-origin URL should parse");
        assert!(reqwest_urls_have_same_origin(&reqwest_base, &next));
    }
    for cross_origin in [
        "http://api.example.com/v2",
        "https://other.example.com/v2",
        "https://api.example.com:444/v2",
    ] {
        let next = reqwest::Url::parse(cross_origin).expect("cross-origin URL should parse");
        assert!(!reqwest_urls_have_same_origin(&reqwest_base, &next));
    }

    let wreq_base: http::Uri = "https://api.example.com/v1"
        .parse()
        .expect("base URI should parse");
    for same_origin in [
        "https://api.example.com/v2",
        "https://API.EXAMPLE.COM:443/v2",
    ] {
        let next: http::Uri = same_origin.parse().expect("same-origin URI should parse");
        assert!(http_uris_have_same_origin(&wreq_base, &next));
    }
    for cross_origin in [
        "http://api.example.com/v2",
        "https://other.example.com/v2",
        "https://api.example.com:444/v2",
    ] {
        let next: http::Uri = cross_origin.parse().expect("cross-origin URI should parse");
        assert!(!http_uris_have_same_origin(&wreq_base, &next));
    }
}

#[test]
fn safe_redirect_decision_preserves_the_ten_hop_limit() {
    assert_eq!(
        safe_redirect_decision(MAX_SAFE_REDIRECTS, true),
        SafeRedirectDecision::Follow
    );
    assert_eq!(
        safe_redirect_decision(MAX_SAFE_REDIRECTS + 1, true),
        SafeRedirectDecision::TooMany
    );
    assert_eq!(safe_redirect_decision(1, false), SafeRedirectDecision::Stop);
}

#[test]
fn upstream_error_url_sanitization_removes_secrets_everywhere() {
    let upstream_url =
        "https://upstream-user:upstream-password@api.example.test/v1/messages?key=query-secret&alt=sse#fragment-secret";
    let detail =
        format!("error sending request for url ({upstream_url}); source repeated {upstream_url}");

    let (sanitized_detail, sanitized_url) =
        super::sanitize_upstream_request_error_detail(&detail, upstream_url);

    assert_eq!(sanitized_url, "https://api.example.test/v1/messages");
    assert_eq!(
        sanitized_detail,
        "error sending request for url (https://api.example.test/v1/messages); source repeated https://api.example.test/v1/messages"
    );
    assert!(!sanitized_detail.contains("query-secret"));
    assert!(!sanitized_detail.contains("fragment-secret"));
    assert!(!sanitized_detail.contains("upstream-user"));
    assert!(!sanitized_detail.contains("upstream-password"));
}

#[test]
fn upstream_error_detail_redacts_embedded_proxy_urls_and_is_bounded() {
    let detail = format!(
        "proxy=https://proxy-user:proxy-password@10.0.0.8:8443/connect?token=secret#fragment {}",
        "diagnostic ".repeat(400)
    );
    let sanitized = super::sanitize_error_detail(&detail);

    assert!(!sanitized.contains("proxy-password"));
    assert!(!sanitized.contains("token=secret"));
    assert!(!sanitized.contains("10.0.0.8"));
    assert!(sanitized.len() <= super::MAX_UPSTREAM_ERROR_DETAIL_BYTES + 3);
}

#[test]
fn transport_error_debug_sanitizes_dynamic_url_details() {
    let secret_url = "https://upstream-user:upstream-password@127.0.0.1:8443/path?token=query-secret#fragment-secret";
    let upstream = ExecutionRuntimeTransportError::UpstreamRequest(format!(
        "request failed for url={secret_url}"
    ));
    let upstream_debug = format!("{upstream:?}");
    assert!(!upstream_debug.contains("upstream-user"));
    assert!(!upstream_debug.contains("upstream-password"));
    assert!(!upstream_debug.contains("query-secret"));
    assert!(!upstream_debug.contains("fragment-secret"));
    assert!(!upstream_debug.contains("127.0.0.1"));
    assert!(upstream_debug.contains("redacted.invalid"));
    let upstream_message = super::safe_transport_error_message(&upstream);
    assert!(!upstream_message.contains("upstream-user"));
    assert!(!upstream_message.contains("upstream-password"));
    assert!(!upstream_message.contains("query-secret"));
    assert!(!upstream_message.contains("fragment-secret"));
    assert!(!upstream_message.contains("127.0.0.1"));
    assert!(upstream_message.contains("redacted.invalid"));
    // Display is used by a few legacy error/logging boundaries.  Keep it
    // safe as well, so a missed `?error`/`safe_transport_error_message`
    // conversion cannot reintroduce URL credential leakage.
    let upstream_display = format!("{upstream}");
    assert!(!upstream_display.contains("upstream-user"));
    assert!(!upstream_display.contains("upstream-password"));
    assert!(!upstream_display.contains("query-secret"));
    assert!(!upstream_display.contains("fragment-secret"));
    assert!(!upstream_display.contains("127.0.0.1"));
    assert!(upstream_display.contains("redacted.invalid"));

    let status = ExecutionRuntimeTransportError::UpstreamHttpStatus {
        status_code: 502,
        message: secret_url.to_string(),
    };
    let status_debug = format!("{status:?}");
    assert!(!status_debug.contains("upstream-password"));
    assert!(!status_debug.contains("query-secret"));
    assert!(!status_debug.contains("127.0.0.1"));
    let status_display = format!("{status}");
    assert!(!status_display.contains("upstream-user"));
    assert!(!status_display.contains("upstream-password"));
    assert!(!status_display.contains("query-secret"));
    assert!(!status_display.contains("fragment-secret"));
    assert!(!status_display.contains("127.0.0.1"));

    let decode = ExecutionRuntimeTransportError::UpstreamResponseDecode {
        encoding: "gzip".to_string(),
        message: format!("decode failed for {secret_url}"),
    };
    let decode_display = format!("{decode}");
    assert!(!decode_display.contains("upstream-user"));
    assert!(!decode_display.contains("upstream-password"));
    assert!(!decode_display.contains("query-secret"));
    assert!(!decode_display.contains("fragment-secret"));
    assert!(!decode_display.contains("127.0.0.1"));

    let relay = ExecutionRuntimeTransportError::RelayError(format!(
        "relay failed while contacting {secret_url}"
    ));
    let relay_display = format!("{relay}");
    assert!(!relay_display.contains("upstream-user"));
    assert!(!relay_display.contains("upstream-password"));
    assert!(!relay_display.contains("query-secret"));
    assert!(!relay_display.contains("fragment-secret"));
    assert!(!relay_display.contains("127.0.0.1"));

    let source = reqwest::Proxy::all("http://[")
        .expect_err("malformed proxy should produce a reqwest error")
        .with_url(reqwest::Url::parse(secret_url).expect("test URL should parse"));
    let invalid_proxy = ExecutionRuntimeTransportError::InvalidProxy(source);
    let invalid_proxy_debug = format!("{invalid_proxy:?}");
    assert!(!invalid_proxy_debug.contains("upstream-user"));
    assert!(!invalid_proxy_debug.contains("upstream-password"));
    assert!(!invalid_proxy_debug.contains("query-secret"));
    assert!(!invalid_proxy_debug.contains("127.0.0.1"));
}

#[test]
fn request_header_materialization_strips_all_aether_internal_headers() {
    let headers = BTreeMap::from([
        ("authorization".to_string(), "Bearer upstream".to_string()),
        ("x-aether-grok-runtime".to_string(), "1".to_string()),
        ("x-aether-future-control".to_string(), "private".to_string()),
    ]);

    let materialized = build_request_headers(&headers, None, false)
        .expect("provider request headers should materialize");

    assert_eq!(
        materialized
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer upstream")
    );
    assert!(!materialized.contains_key("x-aether-grok-runtime"));
    assert!(!materialized.contains_key("x-aether-future-control"));
}

#[test]
fn scoped_response_body_limit_injection_preserves_transport_profile_and_extra() {
    let mut plan = sample_execution_plan(false);
    let original_profile = ResolvedTransportProfile {
        profile_id: "existing-profile".into(),
        backend: TRANSPORT_BACKEND_BROWSER_WREQ.into(),
        http_mode: TRANSPORT_HTTP_MODE_HTTP1_ONLY.into(),
        pool_scope: "provider".into(),
        header_fingerprint: Some(json!({"user_agent": "existing"})),
        extra: Some(json!({"existing": {"nested": true}})),
    };
    plan.transport_profile = Some(original_profile.clone());

    let bounded_plan =
        with_upstream_response_body_limit(&plan, DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES);

    assert_eq!(plan.transport_profile, Some(original_profile.clone()));
    assert_eq!(bounded_plan.transport_profile, Some(original_profile));
    assert_eq!(
        bounded_plan
            .headers
            .get(EXECUTION_RESPONSE_BODY_LIMIT_HEADER)
            .and_then(|value| value.parse::<usize>().ok()),
        Some(DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES)
    );
    assert_eq!(
        execution_plan_response_body_limit_bytes(&bounded_plan),
        DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES
    );

    let unprofiled_plan = sample_execution_plan(false);
    let bounded_unprofiled_plan = with_upstream_response_body_limit(
        &unprofiled_plan,
        DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
    );
    assert!(unprofiled_plan.transport_profile.is_none());
    assert!(bounded_unprofiled_plan.transport_profile.is_none());
    assert_eq!(
        execution_plan_response_body_limit_bytes(&bounded_unprofiled_plan),
        DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES
    );

    let mut shadowed_plan = sample_execution_plan(false);
    shadowed_plan.headers.insert(
        EXECUTION_RESPONSE_BODY_LIMIT_HEADER.to_ascii_uppercase(),
        "65536".to_string(),
    );
    let bounded_shadowed_plan =
        with_upstream_response_body_limit(&shadowed_plan, DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES);
    assert_eq!(
        bounded_shadowed_plan
            .headers
            .keys()
            .filter(|name| name.eq_ignore_ascii_case(EXECUTION_RESPONSE_BODY_LIMIT_HEADER))
            .count(),
        1
    );
}

#[test]
fn scoped_response_body_limit_parsing_rejects_invalid_values_and_clamps_bounds() {
    let scoped_plan = |raw_limit: &str| {
        let mut plan = sample_execution_plan(false);
        plan.headers.insert(
            EXECUTION_RESPONSE_BODY_LIMIT_HEADER.to_string(),
            raw_limit.to_string(),
        );
        plan
    };

    for invalid in ["0", "-1", "1.5", "", "invalid"] {
        assert_eq!(
            execution_plan_response_body_limit_bytes(&scoped_plan(invalid)),
            DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES
        );
    }
    assert_eq!(
        execution_plan_response_body_limit_bytes(&scoped_plan("1")),
        MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES
    );
    assert_eq!(
        execution_plan_response_body_limit_bytes(&scoped_plan(
            &(MAX_SCOPED_RESPONSE_BODY_LIMIT_BYTES as u64 + 1).to_string()
        )),
        MAX_SCOPED_RESPONSE_BODY_LIMIT_BYTES
    );
    assert_eq!(
        execution_plan_response_body_limit_bytes(&scoped_plan("1048576")),
        1_048_576
    );
    assert_eq!(
        effective_response_body_limit_bytes(
            Some(&(DEFAULT_SCOPED_RESPONSE_BODY_LIMIT_BYTES * 2).to_string()),
            1024 * 1024,
        ),
        1024 * 1024,
        "a scoped limit must never raise the operator's global cap"
    );
}

#[test]
fn bounded_execution_body_base64_checks_encoded_and_decoded_sizes() {
    let exact = base64::engine::general_purpose::STANDARD.encode([1_u8, 2, 3]);
    assert_eq!(
        decode_base64_body_with_limit(&exact, 3).expect("exact decoded limit should pass"),
        vec![1, 2, 3]
    );

    let encoded_too_large = base64::engine::general_purpose::STANDARD.encode([1_u8, 2, 3, 4]);
    assert!(matches!(
        decode_base64_body_with_limit(&encoded_too_large, 2),
        Err(ExecutionRuntimeTransportError::BodyTooLarge { limit_bytes: 2 })
    ));

    // Eight encoded bytes are within the limit for a four-byte bound, but
    // this valid value decodes to six bytes and must fail the second check.
    let decoded_too_large = "YWJjZGVm";
    assert!(matches!(
        decode_base64_body_with_limit(decoded_too_large, 4),
        Err(ExecutionRuntimeTransportError::BodyTooLarge { limit_bytes: 4 })
    ));

    assert!(matches!(
        decode_base64_body_with_limit("!!!!", 3),
        Err(ExecutionRuntimeTransportError::BodyDecode(_))
    ));
}

#[test]
fn serialized_json_limit_is_inclusive_and_does_not_allocate_an_encoded_copy() {
    let value = json!({"value": "abc"});
    let encoded_len = serde_json::to_vec(&value).unwrap().len();

    assert!(json_value_fits_serialized_limit(&value, encoded_len));
    assert!(!json_value_fits_serialized_limit(&value, encoded_len - 1));
}

#[test]
fn request_body_rejects_ambiguous_json_and_base64_representations() {
    let mut plan = sample_execution_plan(false);
    plan.body = RequestBody {
        json_body: Some(json!({"json": true})),
        body_bytes_b64: Some("e30=".to_string()),
        body_ref: None,
    };

    assert!(matches!(
        super::build_request_body(&plan),
        Err(ExecutionRuntimeTransportError::RequestBodyAmbiguous)
    ));
}

#[test]
fn execution_result_envelope_limit_accounts_for_base64_expansion() {
    let raw_limit = 64 * 1024 * 1024;
    let envelope_limit = execution_result_envelope_limit_bytes(raw_limit);
    assert!(envelope_limit > maximum_base64_len_for_decoded_limit(raw_limit));
    assert!(envelope_limit <= 256 * 1024 * 1024);
    assert_eq!(
        execution_result_envelope_limit_bytes(usize::MAX),
        256 * 1024 * 1024
    );
}

#[test]
fn scoped_response_body_wire_limit_rejects_overflow() {
    let bounded_plan = with_upstream_response_body_limit(
        &sample_execution_plan(false),
        MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
    );
    let limit_bytes = execution_plan_response_body_limit_bytes(&bounded_plan);
    let mut body = vec![b'x'; limit_bytes];

    let error = append_upstream_response_body_chunk_with_limit(&mut body, b"overflow", limit_bytes)
        .expect_err("wire body above the plan-scoped limit should fail");

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Wire,
            limit_bytes: MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
        }
    ));
}

#[test]
fn scoped_response_body_limit_rejects_gzip_bomb_after_wire_check() {
    let bounded_plan = with_upstream_response_body_limit(
        &sample_execution_plan(false),
        MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
    );
    let limit_bytes = execution_plan_response_body_limit_bytes(&bounded_plan);
    let payload = vec![b'x'; limit_bytes + 1];
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(&payload)
        .expect("gzip payload should encode");
    let encoded = encoder.finish().expect("gzip payload should finish");
    assert!(encoded.len() < limit_bytes);

    let mut wire_body = Vec::new();
    append_upstream_response_body_chunk_with_limit(&mut wire_body, &encoded, limit_bytes)
        .expect("compressed wire body should fit within the plan-scoped limit");
    let headers = BTreeMap::from([("content-encoding".to_string(), "gzip".to_string())]);

    let error = decode_response_body_bytes_with_limit(&headers, &wire_body, limit_bytes)
        .expect_err("decoded body above the plan-scoped limit should fail");

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Decoded,
            limit_bytes: MIN_SCOPED_RESPONSE_BODY_LIMIT_BYTES,
        }
    ));
}

#[test]
fn upstream_response_wire_limit_allows_exact_body_and_rejects_next_byte() {
    let mut body = Vec::new();
    append_upstream_response_body_chunk_with_limit(&mut body, b"1234", 5)
        .expect("chunk below limit should append");
    append_upstream_response_body_chunk_with_limit(&mut body, b"5", 5)
        .expect("body exactly at limit should append");

    let error = append_upstream_response_body_chunk_with_limit(&mut body, b"6", 5)
        .expect_err("body above limit should fail");

    assert_eq!(body, b"12345");
    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Wire,
            limit_bytes: 5,
        }
    ));
}

#[test]
fn upstream_response_gzip_decode_limit_rejects_decompression_bomb() {
    let payload = vec![b'x'; 9];
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(&payload)
        .expect("gzip payload should encode");
    let encoded = encoder.finish().expect("gzip payload should finish");
    let headers = BTreeMap::from([("content-encoding".to_string(), "gzip".to_string())]);

    let error = decode_response_body_bytes_with_limit(&headers, &encoded, 8)
        .expect_err("decoded body above limit should fail");

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Decoded,
            limit_bytes: 8,
        }
    ));
}

#[test]
fn upstream_response_gzip_decode_limit_allows_exact_body() {
    let payload = b"12345678";
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(payload)
        .expect("gzip payload should encode");
    let encoded = encoder.finish().expect("gzip payload should finish");
    let headers = BTreeMap::from([("content-encoding".to_string(), "gzip".to_string())]);

    let decoded = decode_response_body_bytes_with_limit(&headers, &encoded, payload.len())
        .expect("decoded body exactly at limit should pass");

    assert_eq!(decoded.as_ref(), payload);
}

#[test]
fn gateway_frontdoor_self_loop_guard_matches_loopback_public_ai_route() {
    assert!(gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://127.0.0.1:8084/v1/messages"
    ));
    assert!(gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://127.42.0.1:8084/v1/messages"
    ));
    assert!(gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://localhost:8084/v1/responses"
    ));
    assert!(gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://[::ffff:127.0.0.1]:8084/v1/responses"
    ));
    assert!(gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://0.0.0.0:8084/v1/responses"
    ));
    assert!(gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://localhost:8084/v1internal:streamGenerateContent?alt=sse"
    ));
}

#[test]
fn gateway_frontdoor_self_loop_guard_ignores_non_ai_routes() {
    assert!(!gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://127.0.0.1:8084/_gateway/health"
    ));
    assert!(!frontdoor_self_loop_public_ai_path("/_gateway/health"));
}

#[test]
fn gateway_frontdoor_self_loop_guard_ignores_different_ports() {
    assert!(!gateway_frontdoor_self_loop_guard_matches_with_port(
        8084,
        "http://127.0.0.1:9999/v1/messages"
    ));
}

#[test]
fn gateway_frontdoor_self_loop_guard_reports_clear_error() {
    assert_eq!(
        gateway_frontdoor_self_loop_guard_error_with_port(
            8084,
            "http://localhost:8084/v1/responses"
        ),
        Some(
            "upstream execution target resolves back to the local aether-gateway frontdoor"
                .to_string()
        )
    );
}

#[test]
fn gateway_frontdoor_self_loop_guard_does_not_echo_target_secrets() {
    let error = gateway_frontdoor_self_loop_guard_error_with_port(
        8084,
        "http://user:password@localhost:8084/v1/responses?api_key=query-secret#fragment-secret",
    )
    .expect("frontdoor self-loop should be rejected");
    assert!(!error.contains("password"));
    assert!(!error.contains("query-secret"));
    assert!(!error.contains("fragment-secret"));
    assert!(!error.contains("localhost:8084"));
}

#[test]
fn direct_sync_execution_runtime_builds_clients_for_socks_proxy_urls() {
    let timeouts = ExecutionTimeouts {
        connect_ms: Some(5_000),
        total_ms: Some(5_000),
        ..ExecutionTimeouts::default()
    };

    for proxy_url in ["socks5://127.0.0.1:1080", "socks5h://127.0.0.1:1080"] {
        build_client(
            "https://api.example.test/v1/chat/completions",
            "key-test",
            Some(&timeouts),
            Some(&aether_contracts::ProxySnapshot {
                enabled: Some(true),
                mode: Some("socks".into()),
                node_id: None,
                label: Some("manual-proxy".into()),
                url: Some(proxy_url.to_string()),
                extra: None,
            }),
            None,
            ExecutionTransportControls::default(),
        )
        .unwrap_or_else(|err| panic!("client should build for {proxy_url}: {err}"));
    }
}

struct TestEnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl Drop for TestEnvVarGuard {
    fn drop(&mut self) {
        match self.previous.as_ref() {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

fn set_test_env_var(key: &'static str, value: &str) -> TestEnvVarGuard {
    let previous = std::env::var(key).ok();
    std::env::set_var(key, value);
    TestEnvVarGuard { key, previous }
}

#[cfg(test)]
fn direct_reqwest_env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    &LOCK
}

#[test]
fn direct_reqwest_client_cache_key_includes_transport_profile() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let timeouts = ExecutionTimeouts {
        connect_ms: Some(5_000),
        ..ExecutionTimeouts::default()
    };
    let h2c_profile = ResolvedTransportProfile {
        profile_id: "mock-h2c".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: Some(json!({"pool": "a"})),
    };
    let same_h2c_profile = ResolvedTransportProfile {
        extra: Some(json!({"pool": "a"})),
        ..h2c_profile.clone()
    };
    let http1_profile = ResolvedTransportProfile {
        http_mode: TRANSPORT_HTTP_MODE_HTTP1_ONLY.into(),
        ..h2c_profile.clone()
    };

    let left = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        Some(&timeouts),
        None,
        Some(&h2c_profile),
        ExecutionTransportControls::default(),
    );
    let right = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/responses",
        "key-1",
        Some(&timeouts),
        None,
        Some(&same_h2c_profile),
        ExecutionTransportControls::default(),
    );
    let different_mode = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        Some(&timeouts),
        None,
        Some(&http1_profile),
        ExecutionTransportControls::default(),
    );
    let different_proxy = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        Some(&timeouts),
        Some("http://127.0.0.1:8080".into()),
        Some(&h2c_profile),
        ExecutionTransportControls::default(),
    );
    assert_eq!(left, right);
    assert_ne!(left, different_mode);
    assert_ne!(left, different_proxy);
    assert!(super::direct_reqwest_client_cache_key_uses_http2(&left));
    assert!(!super::direct_reqwest_client_cache_key_uses_http2(
        &different_mode
    ));
}

#[test]
fn direct_reqwest_proxy_cache_identity_is_digest_only() {
    let proxy_url = "http://alice:proxy-password@proxy.example.test:8080";
    let rotated_proxy_url = "http://alice:rotated-password@proxy.example.test:8080";
    let cache_key = super::direct_reqwest_client_cache_key(
        "https://api.example.test/v1/messages",
        "key-1",
        None,
        Some(proxy_url.to_string()),
        None,
        ExecutionTransportControls::default(),
    );
    let rotated = super::direct_reqwest_client_cache_key(
        "https://api.example.test/v1/messages",
        "key-1",
        None,
        Some(rotated_proxy_url.to_string()),
        None,
        ExecutionTransportControls::default(),
    );

    assert_ne!(cache_key, rotated);
    assert_eq!(cache_key.proxy_digest.as_deref().map(str::len), Some(64));
    let debug = format!("{cache_key:?}");
    assert!(!debug.contains("alice"));
    assert!(!debug.contains("proxy-password"));
    assert!(!debug.contains("proxy.example.test"));
    super::build_direct_reqwest_client_from_cache_key(&cache_key, Some(proxy_url))
        .expect("authenticated proxy client should build from transient URL material");
}

#[test]
fn direct_upstream_origin_brackets_ipv6_literals() {
    assert_eq!(
        super::direct_reqwest_upstream_origin("https://[::1]:8443/v1/messages").as_deref(),
        Some("https://[::1]:8443")
    );
}

#[test]
fn direct_reqwest_client_cache_evicts_least_recently_used_entry_at_capacity() {
    let cache_key = |suffix| {
        super::direct_reqwest_client_cache_key(
            "https://api.example.test/v1/messages",
            "key-1",
            None,
            Some(format!("http://proxy-{suffix}.example.test:8080")),
            None,
            ExecutionTransportControls::default(),
        )
    };
    let oldest = cache_key("oldest");
    let recent = cache_key("recent");
    let incoming = cache_key("incoming");
    let mut cache = std::collections::HashMap::new();
    let mut oldest_entry = super::DirectReqwestClientCacheEntry::new(Vec::new(), 1, false);
    oldest_entry.last_used = 1;
    let mut recent_entry = super::DirectReqwestClientCacheEntry::new(Vec::new(), 1, false);
    recent_entry.last_used = 2;
    cache.insert(oldest.clone(), oldest_entry);
    cache.insert(recent.clone(), recent_entry);

    super::evict_direct_reqwest_client_cache_for_insert(&mut cache, &incoming, 2);

    assert_eq!(cache.len(), 1);
    assert!(!cache.contains_key(&oldest));
    assert!(cache.contains_key(&recent));
}

#[test]
fn direct_reqwest_client_cache_key_partitions_key_scoped_pools_by_hashed_key_id() {
    let profile = ResolvedTransportProfile {
        profile_id: "key-scoped-profile".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_AUTO.into(),
        pool_scope: " key ".into(),
        header_fingerprint: None,
        extra: None,
    };
    let first_key_id = "plain-key-identity-alpha";
    let second_key_id = "plain-key-identity-beta";
    let cache_key = |key_id| {
        super::direct_reqwest_client_cache_key(
            "https://api.example.test/v1/messages",
            key_id,
            None,
            None,
            Some(&profile),
            ExecutionTransportControls::default(),
        )
    };

    let first = cache_key(first_key_id);
    let first_key_id_with_whitespace = format!("  {first_key_id}  ");
    let first_with_whitespace = cache_key(&first_key_id_with_whitespace);
    let second = cache_key(second_key_id);
    let empty = cache_key("   ");

    assert_eq!(first, first_with_whitespace);
    assert_ne!(first, second);
    assert_eq!(first.pool_partition.as_deref().map(str::len), Some(64));
    assert!(empty.pool_partition.is_none());
    let debug = format!("{first:?} {second:?}");
    assert!(!debug.contains(first_key_id));
    assert!(!debug.contains(second_key_id));
}

#[test]
fn direct_reqwest_client_cache_key_shares_non_key_scoped_pools() {
    let profile = ResolvedTransportProfile {
        profile_id: "provider-scoped-profile".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_AUTO.into(),
        pool_scope: "provider".into(),
        header_fingerprint: None,
        extra: None,
    };
    let cache_key = |key_id| {
        super::direct_reqwest_client_cache_key(
            "https://api.example.test/v1/messages",
            key_id,
            None,
            None,
            Some(&profile),
            ExecutionTransportControls::default(),
        )
    };

    let first = cache_key("plain-key-identity-alpha");
    let second = cache_key("plain-key-identity-beta");

    assert_eq!(first, second);
    assert!(first.pool_partition.is_none());
}

#[test]
fn direct_reqwest_client_cache_key_splits_origin_only_when_enabled() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let profile = ResolvedTransportProfile {
        profile_id: "mock-h2c-origin".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };

    let shared_left = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        None,
        None,
        Some(&profile),
        ExecutionTransportControls::default(),
    );
    let shared_right = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18185/v1/chat/completions",
        "key-1",
        None,
        None,
        Some(&profile),
        ExecutionTransportControls::default(),
    );
    assert_eq!(shared_left, shared_right);

    let _per_origin = set_test_env_var(super::DIRECT_REQWEST_CACHE_PER_ORIGIN_ENV, "true");
    let split_left = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        None,
        None,
        Some(&profile),
        ExecutionTransportControls::default(),
    );
    let split_right = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18185/v1/chat/completions",
        "key-1",
        None,
        None,
        Some(&profile),
        ExecutionTransportControls::default(),
    );
    assert_ne!(split_left, split_right);
}

#[test]
fn direct_reqwest_auto_profile_is_not_classified_as_h2() {
    let auto_profile = ResolvedTransportProfile {
        profile_id: "auto-profile".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_AUTO.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };
    let h2c_profile = ResolvedTransportProfile {
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        ..auto_profile.clone()
    };

    let auto_key = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        None,
        None,
        Some(&auto_profile),
        ExecutionTransportControls::default(),
    );
    let h2c_key = super::direct_reqwest_client_cache_key(
        "http://127.0.0.1:18184/v1/chat/completions",
        "key-1",
        None,
        None,
        Some(&h2c_profile),
        ExecutionTransportControls::default(),
    );

    assert!(!super::direct_reqwest_client_cache_key_uses_http2(
        &auto_key
    ));
    assert!(super::direct_reqwest_client_cache_key_uses_http2(&h2c_key));
}

#[test]
fn direct_reqwest_stream_http_mode_parser_defaults_to_http1() {
    assert_eq!(
        super::parse_direct_reqwest_stream_http_mode(""),
        super::DirectReqwestStreamHttpMode::Http1
    );
    assert_eq!(
        super::parse_direct_reqwest_stream_http_mode("http1_only"),
        super::DirectReqwestStreamHttpMode::Http1
    );
    assert_eq!(
        super::parse_direct_reqwest_stream_http_mode("auto"),
        super::DirectReqwestStreamHttpMode::Auto
    );
}

#[test]
fn direct_reqwest_stream_http1_default_preserves_explicit_h2c_profile() {
    let mut plan = ExecutionPlan {
        request_id: "req-h2c-controls".into(),
        candidate_id: None,
        provider_name: Some("mock".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "http://127.0.0.1:18184/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(ResolvedTransportProfile {
            profile_id: "mock-h2c".into(),
            backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
            http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
            pool_scope: "key".into(),
            header_fingerprint: None,
            extra: None,
        }),
        timeouts: None,
    };

    let controls = super::direct_reqwest_effective_transport_controls(
        &plan,
        ExecutionTransportControls::default(),
    );
    assert!(!controls.http1_only);

    plan.transport_profile = None;
    if super::direct_reqwest_stream_http_mode() == super::DirectReqwestStreamHttpMode::Http1 {
        let controls = super::direct_reqwest_effective_transport_controls(
            &plan,
            ExecutionTransportControls::default(),
        );
        assert!(controls.http1_only);
    }
}

#[test]
fn direct_reqwest_h2_client_shards_scale_from_target_gate() {
    assert_eq!(
        super::direct_reqwest_h2_client_shards_from_config(None, 12_000, 64),
        188
    );
    assert_eq!(
        super::direct_reqwest_h2_client_shards_from_config(None, 2_000, 64),
        32
    );
    assert_eq!(
        super::direct_reqwest_h2_client_shards_from_config(Some(4), 12_000, 64),
        4
    );
    assert_eq!(
        super::direct_reqwest_h2_client_shards_from_config(None, 200_000, 100),
        2_000
    );
}

#[test]
fn direct_reqwest_http1_client_shards_scale_from_target_gate() {
    assert_eq!(
        super::direct_reqwest_client_shards_from_config(None, 10_000, 512),
        20
    );
    assert_eq!(
        super::direct_reqwest_client_shards_from_config(None, 2_000, 512),
        4
    );
    assert_eq!(
        super::direct_reqwest_client_shards_from_config(Some(8), 10_000, 512),
        8
    );
}

#[test]
fn direct_h2c_client_shards_respect_explicit_env() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _shards = set_test_env_var(super::DIRECT_H2C_CLIENT_SHARDS_ENV, "7");
    assert_eq!(super::direct_h2c_client_shard_count(), 7);
}

#[test]
fn direct_h2c_adaptive_window_respects_explicit_env() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    {
        let _adaptive = set_test_env_var(super::DIRECT_H2C_ADAPTIVE_WINDOW_ENV, "0");
        assert!(!super::direct_h2c_adaptive_window_enabled());
    }
    let _adaptive = set_test_env_var(super::DIRECT_H2C_ADAPTIVE_WINDOW_ENV, "true");
    assert!(super::direct_h2c_adaptive_window_enabled());
}

#[test]
fn direct_h2c_driver_runtime_threads_are_opt_in_and_bounded() {
    assert_eq!(super::parse_direct_h2c_driver_runtime_threads(None), None);
    assert_eq!(
        super::parse_direct_h2c_driver_runtime_threads(Some("")),
        None
    );
    assert_eq!(
        super::parse_direct_h2c_driver_runtime_threads(Some("invalid")),
        None
    );
    assert_eq!(
        super::parse_direct_h2c_driver_runtime_threads(Some("0")),
        None
    );
    assert_eq!(
        super::parse_direct_h2c_driver_runtime_threads(Some(" 1 ")),
        Some(1)
    );
    assert_eq!(
        super::parse_direct_h2c_driver_runtime_threads(Some("16")),
        Some(16)
    );
    assert_eq!(
        super::parse_direct_h2c_driver_runtime_threads(Some("128")),
        Some(super::MAX_DIRECT_H2C_DRIVER_RUNTIME_THREADS)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_h2c_driver_task_defaults_to_current_runtime_and_can_use_dedicated_runtime() {
    let current_runtime_id = tokio::runtime::Handle::current().id();
    let default_runtime_id =
        super::spawn_direct_h2c_driver_task(None, async { tokio::runtime::Handle::current().id() })
            .await
            .expect("default direct H2C driver task should join");
    assert_eq!(default_runtime_id, current_runtime_id);

    let driver_runtime = super::direct_h2c_driver_runtime(1)
        .expect("dedicated direct H2C driver runtime should build");
    let (dedicated_runtime_id, thread_name) =
        super::spawn_direct_h2c_driver_task(Some(driver_runtime), async {
            (
                tokio::runtime::Handle::current().id(),
                std::thread::current().name().map(ToOwned::to_owned),
            )
        })
        .await
        .expect("dedicated direct H2C driver task should join");
    assert_ne!(dedicated_runtime_id, current_runtime_id);
    assert_eq!(
        thread_name.as_deref(),
        Some(super::DIRECT_H2C_DRIVER_RUNTIME_THREAD_NAME)
    );
}

#[test]
fn direct_h2c_prewarm_urls_parse_env_list() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _urls = set_test_env_var(
        super::DIRECT_H2C_PREWARM_URLS_ENV,
        " http://127.0.0.1:18184/v1/chat/completions,;http://127.0.0.1:18185/v1/chat/completions\nhttp://127.0.0.1:18186/v1/chat/completions ",
    );

    assert_eq!(
        super::direct_h2c_prewarm_urls_from_env(),
        vec![
            "http://127.0.0.1:18184/v1/chat/completions".to_string(),
            "http://127.0.0.1:18185/v1/chat/completions".to_string(),
            "http://127.0.0.1:18186/v1/chat/completions".to_string(),
        ]
    );
}

#[test]
fn direct_h2c_prewarm_cache_keys_dedup_by_origin() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let urls = vec![
        "http://127.0.0.1:18184/v1/chat/completions".to_string(),
        "http://127.0.0.1:18184/v1/responses".to_string(),
        "http://127.0.0.1:18185/v1/chat/completions".to_string(),
        "not-a-url".to_string(),
    ];

    let (keys, failures, first_error) = super::direct_h2c_sender_prewarm_cache_keys(&urls, None);

    assert_eq!(failures, 1);
    assert!(first_error
        .as_deref()
        .is_some_and(|err| err.contains("invalid h2c upstream origin")));
    assert_eq!(keys.len(), 2);
    assert!(keys
        .iter()
        .any(|key| key.upstream_origin == "http://127.0.0.1:18184"));
    assert!(keys
        .iter()
        .any(|key| key.upstream_origin == "http://127.0.0.1:18185"));
}

#[test]
fn direct_h2c_client_cache_splits_by_origin_and_shards() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _shards = set_test_env_var(super::DIRECT_H2C_CLIENT_SHARDS_ENV, "3");
    super::DIRECT_H2C_CLIENT_CACHE
        .lock()
        .expect("h2c cache lock")
        .clear();

    let left = super::cached_direct_h2c_client("http://127.0.0.1:18184/v1/chat/completions", None)
        .expect("left client");
    let right = super::cached_direct_h2c_client("http://127.0.0.1:18185/v1/chat/completions", None)
        .expect("right client");
    drop((left, right));

    let cache = super::DIRECT_H2C_CLIENT_CACHE
        .lock()
        .expect("h2c cache lock");
    assert_eq!(cache.len(), 2);
    assert!(cache.values().all(|entry| entry.len() == 3));
    assert!(cache.values().all(|entry| entry.target_len == 3));
}

#[test]
fn direct_reqwest_initial_client_shards_are_bounded_by_target() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    assert_eq!(super::direct_reqwest_initial_client_shard_count(1), 1);
    assert_eq!(super::direct_reqwest_initial_client_shard_count(2), 2);
    assert_eq!(
        super::direct_reqwest_initial_client_shard_count(21),
        super::DEFAULT_DIRECT_REQWEST_SYNC_WARM_CLIENTS
    );
}

#[test]
fn direct_reqwest_initial_client_shards_cap_large_sync_env() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _sync = set_test_env_var(super::DIRECT_REQWEST_SYNC_WARM_CLIENTS_ENV, "128");
    assert_eq!(
        super::direct_reqwest_initial_client_shard_count(128),
        super::MAX_DIRECT_REQWEST_SYNC_WARM_CLIENTS
    );
}

#[test]
fn direct_reqwest_prewarm_client_shards_default_to_initial() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    assert_eq!(super::direct_reqwest_prewarm_client_shard_count(1), 1);
    assert_eq!(
        super::direct_reqwest_prewarm_client_shard_count(96),
        super::direct_reqwest_initial_client_shard_count(96)
    );
}

#[test]
fn direct_reqwest_prewarm_client_shards_do_not_exceed_request_path_cap() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _sync = set_test_env_var(super::DIRECT_REQWEST_SYNC_WARM_CLIENTS_ENV, "4");
    let _prewarm = set_test_env_var(super::DIRECT_REQWEST_PREWARM_SYNC_CLIENTS_ENV, "128");

    assert_eq!(super::direct_reqwest_prewarm_client_shard_count(128), 4);
}

#[test]
fn direct_reqwest_prewarm_populates_cache_for_plan() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _shards = set_test_env_var(super::DIRECT_REQWEST_H2_CLIENT_SHARDS_ENV, "4");
    let profile = ResolvedTransportProfile {
        profile_id: "mock-h2c-prewarm".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };
    let plan = ExecutionPlan {
        request_id: "req-prewarm".into(),
        candidate_id: Some("candidate-prewarm".into()),
        provider_name: Some("mock".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "http://127.0.0.1:18184/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(profile.clone()),
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };

    assert!(
        super::try_prewarm_direct_reqwest_client_cache_for_plan(&plan)
            .expect("prewarm should succeed")
    );

    let cache_key = super::direct_reqwest_client_cache_key(
        &plan.url,
        &plan.key_id,
        plan.timeouts.as_ref(),
        None,
        Some(&profile),
        super::ExecutionTransportControls::default(),
    );
    let target_len = super::direct_reqwest_client_shard_count(&cache_key);
    let cache = super::DIRECT_REQWEST_CLIENT_CACHE
        .lock()
        .expect("cache lock");
    let entry = cache.get(&cache_key).expect("cache entry");
    assert_eq!(
        entry.len(),
        super::direct_reqwest_prewarm_client_shard_count(target_len)
    );
    assert_eq!(entry.target_len, target_len);
}

#[test]
fn direct_reqwest_prewarm_plan_keeps_large_sync_env_off_request_path() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _shards = set_test_env_var(super::DIRECT_REQWEST_H2_CLIENT_SHARDS_ENV, "128");
    let _sync = set_test_env_var(super::DIRECT_REQWEST_SYNC_WARM_CLIENTS_ENV, "4");
    let _prewarm = set_test_env_var(super::DIRECT_REQWEST_PREWARM_SYNC_CLIENTS_ENV, "128");
    let profile = ResolvedTransportProfile {
        profile_id: "mock-h2c-large-prewarm".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };
    let plan = ExecutionPlan {
        request_id: "req-large-prewarm".into(),
        candidate_id: Some("candidate-large-prewarm".into()),
        provider_name: Some("mock".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-large-prewarm".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "http://127.0.0.1:18184/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(profile.clone()),
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            ..ExecutionTimeouts::default()
        }),
    };

    assert!(
        super::try_prewarm_direct_reqwest_client_cache_for_plan(&plan)
            .expect("prewarm should succeed")
    );

    let cache_key = super::direct_reqwest_client_cache_key(
        &plan.url,
        &plan.key_id,
        plan.timeouts.as_ref(),
        None,
        Some(&profile),
        super::ExecutionTransportControls::default(),
    );
    let cache = super::DIRECT_REQWEST_CLIENT_CACHE
        .lock()
        .expect("cache lock");
    let entry = cache.get(&cache_key).expect("cache entry");
    assert_eq!(entry.len(), 4);
    assert_eq!(entry.target_len, 128);
}

#[test]
fn direct_reqwest_prewarm_skips_h2c_fast_path() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _fast_path = set_test_env_var(super::DIRECT_H2C_FAST_PATH_ENV, "1");
    let profile = ResolvedTransportProfile {
        profile_id: "mock-h2c-fast-path-prewarm-skip".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };
    let plan = ExecutionPlan {
        request_id: "req-h2c-fast-path-prewarm-skip".into(),
        candidate_id: Some("candidate-h2c-fast-path-prewarm-skip".into()),
        provider_name: Some("mock".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-h2c-fast-path-prewarm-skip".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "http://127.0.0.1:18184/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(profile.clone()),
        timeouts: None,
    };

    assert!(
        !super::try_prewarm_direct_reqwest_client_cache_for_plan(&plan)
            .expect("prewarm skip should succeed")
    );

    let cache_key = super::direct_reqwest_client_cache_key(
        &plan.url,
        &plan.key_id,
        plan.timeouts.as_ref(),
        None,
        Some(&profile),
        super::ExecutionTransportControls::default(),
    );
    let cache = super::DIRECT_REQWEST_CLIENT_CACHE
        .lock()
        .expect("cache lock");
    assert!(!cache.contains_key(&cache_key));
}

#[test]
fn direct_reqwest_cache_metrics_expose_ready_state() {
    let _guard = direct_reqwest_env_lock().blocking_lock();
    let _shards = set_test_env_var(super::DIRECT_REQWEST_H2_CLIENT_SHARDS_ENV, "1");
    let profile = ResolvedTransportProfile {
        profile_id: "mock-h2c-ready-metrics".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };
    let plan = ExecutionPlan {
        request_id: "req-ready-metrics".into(),
        candidate_id: Some("candidate-ready-metrics".into()),
        provider_name: Some("mock".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-ready-metrics".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "http://127.0.0.1:18184/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(profile),
        timeouts: None,
    };

    super::try_prewarm_direct_reqwest_client_cache_for_plan(&plan).expect("prewarm should succeed");

    let samples = super::direct_reqwest_client_cache_metric_samples();
    assert!(samples
        .iter()
        .any(|sample| sample.name == "direct_reqwest_client_cache_ready_entries"));
    assert!(samples
        .iter()
        .any(|sample| sample.name == "direct_reqwest_client_cache_pending_clients"));
    assert!(samples
        .iter()
        .any(|sample| sample.name == "direct_reqwest_client_cache_warming_entries"));
}

#[test]
fn direct_reqwest_prewarm_skips_browser_transport() {
    let plan = ExecutionPlan {
        request_id: "req-browser".into(),
        candidate_id: None,
        provider_name: Some("browser".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/v1/chat/completions".into(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"stream": true})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(ResolvedTransportProfile {
            profile_id: "chrome_136".into(),
            backend: TRANSPORT_BACKEND_BROWSER_WREQ.into(),
            http_mode: "auto".into(),
            pool_scope: "key".into(),
            header_fingerprint: None,
            extra: None,
        }),
        timeouts: None,
    };

    assert!(
        !super::try_prewarm_direct_reqwest_client_cache_for_plan(&plan)
            .expect("browser transport should skip prewarm")
    );
}

#[test]
fn response_body_mode_control_header_is_never_forwarded_upstream() {
    let headers = BTreeMap::from([
        ("content-type".into(), "application/json".into()),
        (
            EXECUTION_RESPONSE_BODY_MODE_HEADER.into(),
            ExecutionResponseBodyMode::PreserveBytes
                .as_str()
                .to_string(),
        ),
    ]);

    let forwarded = build_request_headers(&headers, None, true)
        .expect("headers should build after stripping internal controls");

    assert!(forwarded.get("content-type").is_some());
    assert!(forwarded.get(EXECUTION_RESPONSE_BODY_MODE_HEADER).is_none());
}

#[test]
fn response_body_mode_requires_same_format_raw_sync_plan() {
    let mut plan = sample_execution_plan(false);
    plan.headers.insert(
        EXECUTION_RESPONSE_BODY_MODE_HEADER.to_string(),
        ExecutionResponseBodyMode::PreserveBytes
            .as_str()
            .to_string(),
    );

    assert_eq!(
        execution_response_body_mode(&plan),
        ExecutionResponseBodyMode::StructuredJson
    );

    plan.body = RequestBody {
        json_body: None,
        body_bytes_b64: Some("e30=".to_string()),
        body_ref: None,
    };
    assert_eq!(
        execution_response_body_mode(&plan),
        ExecutionResponseBodyMode::PreserveBytes
    );

    plan.provider_api_format = "claude:messages".to_string();
    assert_eq!(
        execution_response_body_mode(&plan),
        ExecutionResponseBodyMode::StructuredJson
    );
}

#[test]
fn non_stream_total_timeout_defaults_to_provider_request_timeout() {
    let mut plan = sample_execution_plan(false);
    plan.timeouts = None;

    let timeout = resolve_non_stream_total_timeout(&plan)
        .expect("non-stream plans should have a default total timeout");

    assert_eq!(timeout, std::time::Duration::from_secs(300));
}

fn sample_execution_plan(stream: bool) -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-timeout".into(),
        candidate_id: None,
        provider_name: Some("provider".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/chat".into(),
        headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
        stream,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("gpt-4.1".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(ExecutionTimeouts {
            total_ms: Some(90_000),
            first_byte_ms: Some(12_345),
            ..ExecutionTimeouts::default()
        }),
    }
}

fn direct_timeout_plan(url: String, stream: bool, timeouts: ExecutionTimeouts) -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-direct-timeout".into(),
        candidate_id: None,
        provider_name: Some("provider".into()),
        provider_id: "prov-direct-timeout".into(),
        endpoint_id: "ep-direct-timeout".into(),
        key_id: "key-direct-timeout".into(),
        method: "POST".into(),
        url,
        headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
        stream,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("gpt-4.1".into()),
        proxy: None,
        transport_profile: None,
        timeouts: Some(timeouts),
    }
}

#[test]
fn limited_json_body_serialization_rejects_before_growing_to_full_body() {
    let body = json!({"payload": "x".repeat(1024)});
    assert!(matches!(
        super::serialize_json_body_with_limit(&body, 32),
        Err(ExecutionRuntimeTransportError::BodyTooLarge { limit_bytes: 32 })
    ));
    let encoded = super::serialize_json_body_with_limit(&json!({"ok": true}), 32)
        .expect("small JSON body should pass");
    assert_eq!(encoded, br#"{"ok":true}"#);
}

fn manual_proxy_snapshot(node_id: &str) -> ProxySnapshot {
    ProxySnapshot {
        enabled: Some(true),
        mode: Some("http".into()),
        node_id: Some(node_id.to_string()),
        label: Some("manual-proxy".into()),
        url: Some("http://127.0.0.1:1".into()),
        extra: Some(json!({
            PROXY_NODE_GENERATION_EXTRA_KEY: "test-manual-generation"
        })),
    }
}

fn sample_manual_proxy_node(node_id: &str) -> StoredProxyNode {
    StoredProxyNode::new(
        node_id.to_string(),
        "manual-proxy".to_string(),
        "127.0.0.1".to_string(),
        1,
        "online".to_string(),
    )
    .expect("manual proxy node should build")
    .with_node_generation("test-manual-generation".into())
    .with_manual_proxy_fields(Some("http://127.0.0.1:1".into()), None, None)
}

#[test]
fn manual_proxy_binding_requires_incarnation_generation() {
    let proxy = ProxySnapshot {
        enabled: Some(true),
        mode: Some("http".into()),
        node_id: Some("manual-node".into()),
        label: None,
        url: Some("http://127.0.0.1:1".into()),
        extra: None,
    };
    assert!(super::manual_proxy_node_binding(Some(&proxy)).is_none());
}

#[cfg(test)]
#[tokio::test]
async fn direct_sync_execution_runtime_preserves_upstream_status_and_json_body() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|_headers: AxumHeaderMap| async move {
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error": {"message": "slow down"}})),
            )
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution_runtime = DirectSyncExecutionRuntime::new();
    let result = execution_runtime
        .execute_sync(&ExecutionPlan {
            request_id: "req-1".into(),
            candidate_id: Some("cand-1".into()),
            provider_name: Some("openai".into()),
            provider_id: "prov-1".into(),
            endpoint_id: "ep-1".into(),
            key_id: "key-1".into(),
            method: "POST".into(),
            url: format!("http://{addr}/chat"),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
            stream: false,
            client_api_format: "openai:chat".into(),
            provider_api_format: "openai:chat".into(),
            model_name: Some("gpt-4.1".into()),
            proxy: None,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("sync execution should succeed");

    server.abort();

    assert_eq!(result.status_code, 429);
    assert_eq!(
        result.body.and_then(|body| body.json_body),
        Some(json!({"error": {"message": "slow down"}}))
    );
}

#[tokio::test]
async fn direct_sync_execution_runtime_preserves_gemini_tool_config_on_wire() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let captured_body = Arc::new(Mutex::new(None));
    let captured_body_for_handler = Arc::clone(&captured_body);
    let app = Router::new().route(
        "/generate",
        post(move |body: Bytes| {
            let captured_body = Arc::clone(&captured_body_for_handler);
            async move {
                *captured_body
                    .lock()
                    .expect("capture lock should not be poisoned") = Some(body.to_vec());
                Json(json!({"ok": true}))
            }
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let result = DirectSyncExecutionRuntime::new()
        .execute_sync(&ExecutionPlan {
            request_id: "req-gemini-tool-config-wire".into(),
            candidate_id: Some("cand-gemini-tool-config-wire".into()),
            provider_name: Some("google".into()),
            provider_id: "prov-gemini-tool-config-wire".into(),
            endpoint_id: "ep-gemini-tool-config-wire".into(),
            key_id: "key-gemini-tool-config-wire".into(),
            method: "POST".into(),
            url: format!("http://{addr}/generate"),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({
                "model": "gemini-3-flash-preview",
                "contents": [{
                    "role": "user",
                    "parts": [{"text": "Search, then save the result."}]
                }],
                "tools": [
                    {"googleSearch": {}},
                    {"functionDeclarations": [{
                        "name": "save_result",
                        "parameters": {
                            "type": "OBJECT",
                            "properties": {"result": {"type": "STRING"}}
                        }
                    }]}
                ],
                "toolConfig": {
                    "includeServerSideToolInvocations": true,
                    "functionCallingConfig": {"mode": "ANY"}
                }
            })),
            stream: false,
            client_api_format: "openai:responses".into(),
            provider_api_format: "gemini:generate_content".into(),
            model_name: Some("gemini-3-flash-preview".into()),
            proxy: None,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("sync execution should succeed");

    server.abort();

    assert_eq!(result.status_code, 200);
    let body = captured_body
        .lock()
        .expect("capture lock should not be poisoned")
        .take()
        .and_then(|body| serde_json::from_slice::<serde_json::Value>(&body).ok())
        .expect("upstream should receive a JSON body");
    assert_eq!(
        body["toolConfig"]["includeServerSideToolInvocations"],
        json!(true)
    );
    assert_eq!(body["toolConfig"]["functionCallingConfig"]["mode"], "ANY");
    assert!(body["toolConfig"]
        .get("include_server_side_tool_invocations")
        .is_none());
}

#[tokio::test]
async fn direct_sync_execution_runtime_applies_non_stream_total_timeout_to_body() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            let body = Body::from_stream(async_stream::stream! {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(br#"{"ok":true}"#));
            });
            axum::response::Response::builder()
                .status(http::StatusCode::OK)
                .header("content-type", "application/json")
                .body(body)
                .expect("response should build")
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let result = DirectSyncExecutionRuntime::new()
        .execute_sync(&direct_timeout_plan(
            format!("http://{addr}/chat"),
            false,
            ExecutionTimeouts {
                total_ms: Some(50),
                ..ExecutionTimeouts::default()
            },
        ))
        .await;

    server.abort();

    let error = match result {
        Ok(_) => panic!("non-stream body should hit total timeout"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("provider non-stream request total timeout after 50 ms"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn direct_sync_execution_runtime_applies_stream_first_byte_timeout_to_body_after_headers() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("client should connect");
        let mut request = [0_u8; 1024];
        let _ = socket
            .read(&mut request)
            .await
            .expect("request should read");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
            )
            .await
            .expect("headers should write");
        socket.flush().await.expect("headers should flush");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        let _ = socket.write_all(b"d\r\ndata: hello\n\n\r\n0\r\n\r\n").await;
    });

    let result = DirectSyncExecutionRuntime::new()
        .execute_sync(&direct_timeout_plan(
            format!("http://{addr}/chat"),
            true,
            ExecutionTimeouts {
                first_byte_ms: Some(50),
                total_ms: Some(5_000),
                ..ExecutionTimeouts::default()
            },
        ))
        .await;

    server.abort();

    let error = match result {
        Ok(_) => panic!("stream sync body should hit first-byte timeout"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("provider stream first byte timeout after 50 ms"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn direct_sync_execution_runtime_does_not_apply_total_timeout_after_stream_body_starts() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("client should connect");
        let mut request = [0_u8; 1024];
        let _ = socket
            .read(&mut request)
            .await
            .expect("request should read");
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\n\r\n",
            )
            .await
            .expect("headers should write");
        socket.flush().await.expect("headers should flush");
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        socket
            .write_all(b"b\r\ndata: one\n\n\r\n")
            .await
            .expect("first chunk should write");
        socket.flush().await.expect("first chunk should flush");
        tokio::time::sleep(std::time::Duration::from_millis(120)).await;
        socket
            .write_all(b"b\r\ndata: two\n\n\r\n0\r\n\r\n")
            .await
            .expect("second chunk should write");
    });

    let result = DirectSyncExecutionRuntime::new()
        .execute_sync(&direct_timeout_plan(
            format!("http://{addr}/chat"),
            true,
            ExecutionTimeouts {
                first_byte_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                total_ms: Some(25),
                ..ExecutionTimeouts::default()
            },
        ))
        .await
        .expect("stream body should not use total timeout after first chunk");

    server.abort();

    let body = result
        .body
        .clone()
        .and_then(|body| body.body_bytes_b64)
        .and_then(|body| base64::engine::general_purpose::STANDARD.decode(body).ok())
        .expect("stream body should be captured as bytes");
    let body = String::from_utf8(body).expect("stream body should be utf8");
    assert!(body.contains("data: one"));
    assert!(body.contains("data: two"));
    let observation = result
        .response_observation
        .expect("stream sync execution should preserve header observation");
    let telemetry = result
        .telemetry
        .expect("stream sync execution should include telemetry");
    let ttfb_ms = telemetry
        .ttfb_ms
        .expect("stream sync execution should measure the first body byte");
    assert!(
        observation.response_headers_observed_at_unix_ms >= observation.request_started_at_unix_ms
    );
    assert!(
        observation
            .response_headers_observed_at_unix_ms
            .saturating_sub(observation.request_started_at_unix_ms)
            < ttfb_ms,
        "header observation must not be derived from body-byte ttfb"
    );
    assert!(!observation.request_order_id.is_empty());
}

#[tokio::test]
async fn direct_stream_execution_runtime_applies_first_byte_timeout() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            axum::response::Response::builder()
                .status(http::StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(Bytes::from_static(b"data: {}\n\n")))
                .expect("response should build")
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let result = DirectSyncExecutionRuntime::new()
        .execute_stream(&direct_timeout_plan(
            format!("http://{addr}/chat"),
            true,
            ExecutionTimeouts {
                first_byte_ms: Some(50),
                total_ms: Some(5_000),
                ..ExecutionTimeouts::default()
            },
        ))
        .await;

    server.abort();

    let error = match result {
        Ok(_) => panic!("stream should hit first-byte timeout"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("provider stream first byte timeout after 50 ms"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn direct_stream_execution_runtime_prefers_first_byte_timeout_over_total_timeout() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
            axum::response::Response::builder()
                .status(http::StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(Bytes::from_static(b"data: {}\n\n")))
                .expect("response should build")
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution = DirectSyncExecutionRuntime::new()
        .execute_stream(&direct_timeout_plan(
            format!("http://{addr}/chat"),
            true,
            ExecutionTimeouts {
                first_byte_ms: Some(250),
                total_ms: Some(25),
                ..ExecutionTimeouts::default()
            },
        ))
        .await
        .expect("stream should use first-byte timeout instead of total timeout");

    server.abort();

    assert_eq!(execution.status_code, http::StatusCode::OK.as_u16());
}

#[tokio::test]
async fn direct_stream_execution_runtime_ignores_total_timeout_when_first_byte_unset() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            axum::response::Response::builder()
                .status(http::StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(Bytes::from_static(b"data: {}\n\n")))
                .expect("response should build")
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution = DirectSyncExecutionRuntime::new()
        .execute_stream(&direct_timeout_plan(
            format!("http://{addr}/chat"),
            true,
            ExecutionTimeouts {
                total_ms: Some(5),
                ..ExecutionTimeouts::default()
            },
        ))
        .await
        .expect("stream should ignore total_ms and use the first-byte default");

    server.abort();

    assert_eq!(execution.status_code, http::StatusCode::OK.as_u16());
}

#[tokio::test]
async fn browser_wreq_stream_execution_ignores_total_timeout_when_first_byte_unset() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(15)).await;
            axum::response::Response::builder()
                .status(http::StatusCode::OK)
                .header("content-type", "text/event-stream")
                .body(Body::from(Bytes::from_static(b"data: {}\n\n")))
                .expect("response should build")
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });
    let mut plan = direct_timeout_plan(
        format!("http://{addr}/chat"),
        true,
        ExecutionTimeouts {
            total_ms: Some(5),
            ..ExecutionTimeouts::default()
        },
    );
    plan.transport_profile = Some(ResolvedTransportProfile {
        profile_id: "chrome136".into(),
        backend: TRANSPORT_BACKEND_BROWSER_WREQ.into(),
        http_mode: "auto".into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: Some(json!({
            "browser_profile": "chrome136"
        })),
    });

    let execution = DirectSyncExecutionRuntime::new()
        .execute_stream(&plan)
        .await
        .expect("browser-wreq stream should ignore total_ms and use the first-byte default");

    server.abort();

    assert_eq!(execution.status_code, http::StatusCode::OK.as_u16());
}

#[tokio::test]
async fn direct_sync_execution_runtime_routes_browser_wreq_transport_in_process() {
    async fn browser_upstream(headers: AxumHeaderMap, body: Bytes) -> axum::response::Response {
        assert_eq!(
            headers
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        assert!(
            headers
                .get(EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER)
                .is_none(),
            "internal execution control headers must not leak upstream"
        );
        assert_eq!(body.as_ref(), br#"{"modelName":"auto"}"#);
        axum::response::Response::builder()
            .status(http::StatusCode::ACCEPTED)
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "ok": true,
                    "via": "browser_wreq"
                })
                .to_string(),
            ))
            .expect("response should build")
    }

    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route("/request", any(browser_upstream));
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let plan = ExecutionPlan {
        request_id: "req-browser-wreq".into(),
        candidate_id: None,
        provider_name: Some("example".into()),
        provider_id: "provider-1".into(),
        endpoint_id: "endpoint-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: format!("http://{addr}/request"),
        headers: BTreeMap::from([
            ("content-type".into(), "application/json".into()),
            (
                EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER.into(),
                "true".into(),
            ),
        ]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"modelName":"auto"})),
        stream: false,
        client_api_format: "openai:responses".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("example-model".into()),
        proxy: None,
        transport_profile: Some(ResolvedTransportProfile {
            profile_id: "chrome136".into(),
            backend: TRANSPORT_BACKEND_BROWSER_WREQ.into(),
            http_mode: "auto".into(),
            pool_scope: "key".into(),
            header_fingerprint: None,
            extra: Some(json!({
                "browser_profile": "chrome136"
            })),
        }),
        timeouts: Some(ExecutionTimeouts {
            total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
            ..ExecutionTimeouts::default()
        }),
    };

    let result = DirectSyncExecutionRuntime::new()
        .execute_sync(&plan)
        .await
        .expect("browser wreq transport plan should execute in-process");

    server.abort();

    assert_eq!(result.status_code, http::StatusCode::ACCEPTED.as_u16());
    assert_eq!(
        result
            .body
            .and_then(|body| body.json_body)
            .and_then(|body| body.get("via").cloned()),
        Some(json!("browser_wreq"))
    );
}

#[test]
fn browser_wreq_transport_rejects_unknown_profile() {
    let profile = ResolvedTransportProfile {
        profile_id: "firefox999".into(),
        backend: TRANSPORT_BACKEND_BROWSER_WREQ.into(),
        http_mode: "auto".into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };

    let error = match build_browser_wreq_client(
        None,
        None,
        &profile,
        ExecutionTransportControls::default(),
        true,
    ) {
        Ok(_) => panic!("unknown browser profile should fail loudly"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UnsupportedTransportProfile(backend)
            if backend == "browser_wreq:firefox999"
    ));
}

#[tokio::test]
async fn execute_sync_plan_records_manual_proxy_success() {
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        sample_manual_proxy_node("manual-node-1"),
    ]));
    let data = crate::data::GatewayDataState::with_proxy_node_repository_for_tests(Arc::clone(
        &repository,
    ));
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data);
    let plan = ExecutionPlan {
        request_id: "req-manual-proxy-success".into(),
        candidate_id: None,
        provider_name: None,
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/chat".into(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody::from_json(json!({})),
        stream: false,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: None,
        proxy: Some(manual_proxy_snapshot("manual-node-1")),
        transport_profile: None,
        timeouts: None,
    };

    record_manual_proxy_request_success(&state, &plan).await;

    let node = repository
        .find_proxy_node("manual-node-1")
        .await
        .expect("proxy node lookup should succeed")
        .expect("manual proxy node should exist");
    assert_eq!(node.total_requests, 1);
    assert_eq!(node.failed_requests, 0);
}

#[tokio::test]
async fn execute_sync_plan_records_manual_proxy_failure() {
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        sample_manual_proxy_node("manual-node-1"),
    ]));
    let data = crate::data::GatewayDataState::with_proxy_node_repository_for_tests(Arc::clone(
        &repository,
    ));
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data);
    let plan = ExecutionPlan {
        request_id: "req-manual-proxy-failure".into(),
        candidate_id: None,
        provider_name: None,
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/chat".into(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody::from_json(json!({})),
        stream: false,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: None,
        proxy: Some(manual_proxy_snapshot("manual-node-1")),
        transport_profile: None,
        timeouts: None,
    };

    record_manual_proxy_request_failure(&state, &plan).await;

    let node = repository
        .find_proxy_node("manual-node-1")
        .await
        .expect("proxy node lookup should succeed")
        .expect("manual proxy node should exist");
    assert_eq!(node.total_requests, 1);
    assert_eq!(node.failed_requests, 1);
}

#[tokio::test]
async fn execute_sync_plan_records_manual_proxy_http_error_as_failure() {
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        sample_manual_proxy_node("manual-node-1"),
    ]));
    let data = crate::data::GatewayDataState::with_proxy_node_repository_for_tests(Arc::clone(
        &repository,
    ));
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data);
    let plan = ExecutionPlan {
        request_id: "req-manual-proxy-http-error".into(),
        candidate_id: None,
        provider_name: None,
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/chat".into(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody::from_json(json!({})),
        stream: false,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: None,
        proxy: Some(manual_proxy_snapshot("manual-node-1")),
        transport_profile: None,
        timeouts: None,
    };

    record_manual_proxy_request_outcome(&state, &plan, 429).await;

    let node = repository
        .find_proxy_node("manual-node-1")
        .await
        .expect("proxy node lookup should succeed")
        .expect("manual proxy node should exist");
    assert_eq!(node.total_requests, 1);
    assert_eq!(node.failed_requests, 1);
}

#[tokio::test]
async fn execute_sync_plan_records_manual_proxy_http_success_without_failure() {
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        sample_manual_proxy_node("manual-node-1"),
    ]));
    let data = crate::data::GatewayDataState::with_proxy_node_repository_for_tests(Arc::clone(
        &repository,
    ));
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data);
    let plan = ExecutionPlan {
        request_id: "req-manual-proxy-http-success".into(),
        candidate_id: None,
        provider_name: None,
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/chat".into(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody::from_json(json!({})),
        stream: false,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: None,
        proxy: Some(manual_proxy_snapshot("manual-node-1")),
        transport_profile: None,
        timeouts: None,
    };

    record_manual_proxy_request_outcome(&state, &plan, 200).await;

    let node = repository
        .find_proxy_node("manual-node-1")
        .await
        .expect("proxy node lookup should succeed")
        .expect("manual proxy node should exist");
    assert_eq!(node.total_requests, 1);
    assert_eq!(node.failed_requests, 0);
}

#[tokio::test]
async fn execute_sync_plan_records_manual_proxy_stream_error_without_extra_request_count() {
    let repository = Arc::new(InMemoryProxyNodeRepository::seed(vec![
        sample_manual_proxy_node("manual-node-1"),
    ]));
    let data = crate::data::GatewayDataState::with_proxy_node_repository_for_tests(Arc::clone(
        &repository,
    ));
    let state = AppState::new()
        .expect("app state should build")
        .with_data_state_for_tests(data);
    let plan = ExecutionPlan {
        request_id: "req-manual-proxy-stream-error".into(),
        candidate_id: None,
        provider_name: None,
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "https://example.com/chat".into(),
        headers: BTreeMap::new(),
        content_type: None,
        content_encoding: None,
        body: RequestBody::from_json(json!({})),
        stream: true,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: None,
        proxy: Some(manual_proxy_snapshot("manual-node-1")),
        transport_profile: None,
        timeouts: None,
    };

    record_manual_proxy_request_success(&state, &plan).await;
    record_manual_proxy_stream_error(&state, &plan).await;

    let node = repository
        .find_proxy_node("manual-node-1")
        .await
        .expect("proxy node lookup should succeed")
        .expect("manual proxy node should exist");
    assert_eq!(node.total_requests, 1);
    assert_eq!(node.failed_requests, 0);
    assert_eq!(node.stream_errors, 1);
}

#[tokio::test]
async fn direct_sync_execution_runtime_disables_redirects_by_default() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new()
        .route(
            "/redirect",
            post(|| async {
                (
                    axum::http::StatusCode::TEMPORARY_REDIRECT,
                    [(
                        axum::http::header::LOCATION,
                        axum::http::HeaderValue::from_static("/final"),
                    )],
                )
            }),
        )
        .route(
            "/final",
            post(|| async {
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"redirected": true})),
                )
            }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution_runtime = DirectSyncExecutionRuntime::new();
    let result = execution_runtime
        .execute_sync(&ExecutionPlan {
            request_id: "req-redirect-1".into(),
            candidate_id: None,
            provider_name: Some("provider_ops".into()),
            provider_id: "prov-1".into(),
            endpoint_id: "ep-1".into(),
            key_id: "key-1".into(),
            method: "POST".into(),
            url: format!("http://{addr}/redirect"),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
            stream: false,
            client_api_format: "provider_ops:verify".into(),
            provider_api_format: "provider_ops:verify".into(),
            model_name: Some("verify-auth".into()),
            proxy: None,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("sync execution should succeed");

    server.abort();

    assert_eq!(result.status_code, 307);
    assert_eq!(
        result.headers.get("location").map(String::as_str),
        Some("/final")
    );
}

#[tokio::test]
async fn direct_sync_execution_runtime_follows_redirects_when_explicitly_enabled() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new()
        .route(
            "/redirect",
            post(|| async {
                (
                    axum::http::StatusCode::TEMPORARY_REDIRECT,
                    [(
                        axum::http::header::LOCATION,
                        axum::http::HeaderValue::from_static("/final"),
                    )],
                )
            }),
        )
        .route(
            "/final",
            post(|| async {
                (
                    axum::http::StatusCode::OK,
                    Json(json!({"redirected": true})),
                )
            }),
        );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution_runtime = DirectSyncExecutionRuntime::new();
    let result = execution_runtime
        .execute_sync(&ExecutionPlan {
            request_id: "req-redirect-2".into(),
            candidate_id: None,
            provider_name: Some("provider_oauth".into()),
            provider_id: "prov-1".into(),
            endpoint_id: "ep-1".into(),
            key_id: "key-1".into(),
            method: "POST".into(),
            url: format!("http://{addr}/redirect"),
            headers: BTreeMap::from([
                ("content-type".into(), "application/json".into()),
                (
                    EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER.into(),
                    "true".into(),
                ),
            ]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
            stream: false,
            client_api_format: "provider_oauth:exchange".into(),
            provider_api_format: "provider_oauth:exchange".into(),
            model_name: Some("oauth-exchange".into()),
            proxy: None,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("sync execution should succeed");

    server.abort();

    assert_eq!(result.status_code, 200);
    assert_eq!(
        result.body.and_then(|body| body.json_body),
        Some(json!({"redirected": true}))
    );
}

#[tokio::test]
async fn direct_sync_execution_runtime_allows_transport_profile_best_effort() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            (
                axum::http::StatusCode::OK,
                Json(json!({"transport_profile": true})),
            )
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution_runtime = DirectSyncExecutionRuntime::new();
    let result = execution_runtime
        .execute_sync(&ExecutionPlan {
            request_id: "req-tls-1".into(),
            candidate_id: Some("cand-1".into()),
            provider_name: Some("claude".into()),
            provider_id: "prov-1".into(),
            endpoint_id: "ep-1".into(),
            key_id: "key-1".into(),
            method: "POST".into(),
            url: format!("http://{addr}/chat"),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "claude-3.7-sonnet"})),
            stream: false,
            client_api_format: "claude:messages".into(),
            provider_api_format: "claude:messages".into(),
            model_name: Some("claude-3.7-sonnet".into()),
            proxy: None,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("sync execution with transport profile should succeed");

    server.abort();

    assert_eq!(result.status_code, 200);
    assert_eq!(
        result.body.and_then(|body| body.json_body),
        Some(json!({"transport_profile": true}))
    );
}

#[test]
fn direct_sync_execution_runtime_prepares_h2c_prior_knowledge_profile() {
    let profile = ResolvedTransportProfile {
        profile_id: "mock-h2c".into(),
        backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
        http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };
    let plan = ExecutionPlan {
        request_id: "req-h2c-1".into(),
        candidate_id: Some("cand-h2c-1".into()),
        provider_name: Some("mock".into()),
        provider_id: "prov-1".into(),
        endpoint_id: "ep-1".into(),
        key_id: "key-1".into(),
        method: "POST".into(),
        url: "http://127.0.0.1:18184/chat".into(),
        headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
        content_type: Some("application/json".into()),
        content_encoding: None,
        body: RequestBody::from_json(json!({"model": "mock-model"})),
        stream: false,
        client_api_format: "openai:chat".into(),
        provider_api_format: "openai:chat".into(),
        model_name: Some("mock-model".into()),
        proxy: None,
        transport_profile: Some(profile.clone()),
        timeouts: Some(ExecutionTimeouts {
            connect_ms: Some(5_000),
            total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
            ..ExecutionTimeouts::default()
        }),
    };

    let transport_controls = super::direct_reqwest_effective_transport_controls(
        &plan,
        super::ExecutionTransportControls::default(),
    );
    let cache_key = super::direct_reqwest_client_cache_key(
        &plan.url,
        &plan.key_id,
        plan.timeouts.as_ref(),
        None,
        Some(&profile),
        transport_controls,
    );

    assert!(!transport_controls.http1_only);
    assert!(!super::direct_h2c_fast_path_applies(
        &plan,
        transport_controls
    ));
    assert!(super::direct_reqwest_client_cache_key_uses_http2(
        &cache_key
    ));
    assert!(super::direct_reqwest_client_cache_key_uses_h2c_prior_knowledge(&cache_key));
    assert_eq!(
        cache_key
            .transport_profile
            .as_ref()
            .map(|profile| profile.http_mode.as_str()),
        Some(TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE)
    );
    super::build_direct_reqwest_client_from_cache_key(&cache_key, None)
        .expect("h2c prior-knowledge client should build");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_sync_execution_runtime_uses_h2c_prior_knowledge_on_wire() {
    let _guard = direct_reqwest_env_lock().lock().await;
    let _shards = set_test_env_var(super::DIRECT_REQWEST_H2_CLIENT_SHARDS_ENV, "1");
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("server should accept");
        let service = hyper::service::service_fn(
            |request: hyper::Request<hyper::body::Incoming>| async move {
                let body = if request.version() == hyper::Version::HTTP_2 {
                    Bytes::from_static(br#"{"http_version":"h2c"}"#)
                } else {
                    Bytes::from_static(br#"{"http_version":"unexpected"}"#)
                };
                Ok::<_, std::convert::Infallible>(
                    hyper::Response::builder()
                        .header(hyper::header::CONTENT_TYPE, "application/json")
                        .body(http_body_util::Full::new(body))
                        .expect("response should build"),
                )
            },
        );
        hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
            .await
            .expect("H2C server connection should run");
    });

    let result = DirectSyncExecutionRuntime::new()
        .execute_sync(&ExecutionPlan {
            request_id: "req-h2c-wire-1".into(),
            candidate_id: Some("cand-h2c-wire-1".into()),
            provider_name: Some("mock".into()),
            provider_id: "prov-h2c-wire".into(),
            endpoint_id: "ep-h2c-wire".into(),
            key_id: "key-h2c-wire".into(),
            method: "POST".into(),
            url: format!("http://{addr}/chat"),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "mock-model"})),
            stream: false,
            client_api_format: "openai:chat".into(),
            provider_api_format: "openai:chat".into(),
            model_name: Some("mock-model".into()),
            proxy: None,
            transport_profile: Some(ResolvedTransportProfile {
                profile_id: "mock-h2c-wire".into(),
                backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.into(),
                http_mode: TRANSPORT_HTTP_MODE_H2C_PRIOR_KNOWLEDGE.into(),
                pool_scope: "key".into(),
                header_fingerprint: None,
                extra: None,
            }),
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("H2C prior-knowledge request should succeed");

    server.abort();
    let _ = server.await;

    assert_eq!(result.status_code, 200);
    assert_eq!(
        result.body.and_then(|body| body.json_body),
        Some(json!({"http_version": "h2c"}))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_h2c_connection_driver_can_run_on_dedicated_runtime() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("server should accept");
        let service = hyper::service::service_fn(
            |request: hyper::Request<hyper::body::Incoming>| async move {
                assert_eq!(request.version(), hyper::Version::HTTP_2);
                Ok::<_, std::convert::Infallible>(
                    hyper::Response::builder()
                        .header("x-aether-driver-runtime", "dedicated")
                        .body(http_body_util::Full::new(Bytes::from_static(b"ok")))
                        .expect("response should build"),
                )
            },
        );
        hyper::server::conn::http2::Builder::new(hyper_util::rt::TokioExecutor::new())
            .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
            .await
            .expect("H2C server connection should run");
    });

    let driver_runtime = super::direct_h2c_driver_runtime(1)
        .expect("dedicated direct H2C driver runtime should build");
    let cache_key = super::DirectHyperH2cClientCacheKey {
        upstream_origin: format!("http://{addr}"),
        connect_timeout_ms: Some(5_000),
        pool_max_idle_per_host: 1,
    };
    let sender = super::connect_direct_h2c_sender_on_runtime(&cache_key, Some(driver_runtime))
        .await
        .expect("dedicated-runtime H2C sender should connect");
    let slot = super::DirectHyperH2cSenderSlot::new(sender);
    let request = hyper::Request::builder()
        .method(hyper::Method::POST)
        .uri(format!("http://{addr}/chat"))
        .header(hyper::header::HOST, addr.to_string())
        .body(http_body_util::Full::new(Bytes::from_static(b"{}")))
        .expect("request should build");
    let response = super::send_hyper_h2c_request(
        slot.acquire(),
        request,
        Some(std::time::Duration::from_secs(5)),
    )
    .await
    .expect("dedicated-runtime H2C request should succeed");

    assert_eq!(response.status(), hyper::StatusCode::OK);
    assert_eq!(response.version(), hyper::Version::HTTP_2);
    assert_eq!(
        response
            .headers()
            .get("x-aether-driver-runtime")
            .and_then(|value| value.to_str().ok()),
        Some("dedicated")
    );
    drop(response);
    drop(slot);
    server.abort();
    let _ = server.await;
}

#[test]
fn direct_sync_execution_runtime_rejects_unsupported_transport_backend() {
    let profile = ResolvedTransportProfile {
        profile_id: "chrome-120".into(),
        backend: "utls".into(),
        http_mode: "auto".into(),
        pool_scope: "key".into(),
        header_fingerprint: None,
        extra: None,
    };

    let error = match build_client(
        "https://api.example.test/v1/chat/completions",
        "key-test",
        None,
        None,
        Some(&profile),
        ExecutionTransportControls::default(),
    ) {
        Ok(_) => panic!("unsupported backend should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UnsupportedTransportProfile(backend)
            if backend == "utls"
    ));
}

#[test]
fn structured_json_response_does_not_duplicate_body_bytes() {
    let headers = BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let body_bytes = br#"{ "unknown": true, "ok": true }"#;

    let body = build_execution_response_body(
        &headers,
        body_bytes,
        body_bytes,
        false,
        ExecutionResponseBodyMode::StructuredJson,
    )
    .expect("body should build")
    .expect("body should be present");

    assert!(body.json_body.is_some());
    assert!(body.body_bytes_b64.is_none());
}

#[test]
fn preserve_bytes_json_response_keeps_parsed_and_wire_representations() {
    let headers = BTreeMap::from([("content-type".to_string(), "application/json".to_string())]);
    let body_bytes = br#"{ "unknown": true, "ok": true }"#;

    let body = build_execution_response_body(
        &headers,
        body_bytes,
        body_bytes,
        false,
        ExecutionResponseBodyMode::PreserveBytes,
    )
    .expect("body should build")
    .expect("body should be present");

    assert_eq!(body.json_body, Some(json!({"unknown": true, "ok": true})));
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(body.body_bytes_b64.expect("wire bytes should be present"))
            .expect("wire body should decode"),
        body_bytes
    );
}

#[tokio::test]
async fn direct_sync_execution_runtime_compresses_json_body_when_requested() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|headers: axum::http::HeaderMap, body: Bytes| async move {
            let header_encoding = headers
                .get(axum::http::header::CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let decoded = match header_encoding.as_str() {
                "gzip" => {
                    let mut decoder = flate2::read::GzDecoder::new(body.as_ref());
                    let mut decoded = Vec::new();
                    decoder
                        .read_to_end(&mut decoded)
                        .expect("gzip body should decode");
                    decoded
                }
                "zstd" => zstd::stream::decode_all(std::io::Cursor::new(body.as_ref()))
                    .expect("zstd body should decode"),
                encoding => panic!("unexpected content encoding: {encoding}"),
            };
            let decoded_json: serde_json::Value =
                serde_json::from_slice(&decoded).expect("decoded json should parse");
            (
                axum::http::StatusCode::OK,
                Json(json!({
                    "content_encoding": header_encoding,
                    "body": decoded_json,
                })),
            )
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution_runtime = DirectSyncExecutionRuntime::new();
    for encoding in ["gzip", "zstd"] {
        let result = execution_runtime
            .execute_sync(&ExecutionPlan {
                request_id: format!("req-{encoding}-1"),
                candidate_id: Some("cand-1".into()),
                provider_name: Some("openai".into()),
                provider_id: "prov-1".into(),
                endpoint_id: "ep-1".into(),
                key_id: "key-1".into(),
                method: "POST".into(),
                url: format!("http://{addr}/chat"),
                headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
                content_type: Some("application/json".into()),
                content_encoding: Some(encoding.into()),
                body: RequestBody::from_json(json!({
                    "model": "gpt-4.1",
                    "service_tier": "ultrafast"
                })),
                stream: false,
                client_api_format: "openai:chat".into(),
                provider_api_format: "openai:chat".into(),
                model_name: Some("gpt-4.1".into()),
                proxy: None,
                transport_profile: None,
                timeouts: Some(ExecutionTimeouts {
                    connect_ms: Some(5_000),
                    total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                    ..ExecutionTimeouts::default()
                }),
            })
            .await
            .expect("compressed sync execution should succeed");

        assert_eq!(result.status_code, 200);
        assert_eq!(
            result.body.and_then(|body| body.json_body),
            Some(json!({
                "content_encoding": encoding,
                "body": {"model": "gpt-4.1", "service_tier": "ultrafast"},
            }))
        );
    }

    server.abort();
}

#[tokio::test]
async fn direct_sync_execution_runtime_reports_ttfb_once_upstream_response_starts() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().route(
        "/chat",
        post(|| async {
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            (axum::http::StatusCode::OK, Json(json!({"ok": true})))
        }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let execution_runtime = DirectSyncExecutionRuntime::new();
    let result = execution_runtime
        .execute_sync(&ExecutionPlan {
            request_id: "req-ttfb-1".into(),
            candidate_id: Some("cand-1".into()),
            provider_name: Some("openai".into()),
            provider_id: "prov-1".into(),
            endpoint_id: "ep-1".into(),
            key_id: "key-1".into(),
            method: "POST".into(),
            url: format!("http://{addr}/chat"),
            headers: BTreeMap::from([("content-type".into(), "application/json".into())]),
            content_type: Some("application/json".into()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "gpt-4.1"})),
            stream: false,
            client_api_format: "openai:chat".into(),
            provider_api_format: "openai:chat".into(),
            model_name: Some("gpt-4.1".into()),
            proxy: None,
            transport_profile: None,
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(5_000),
                total_ms: Some(LOCAL_HTTP_SUCCESS_TIMEOUT_MS),
                ..ExecutionTimeouts::default()
            }),
        })
        .await
        .expect("sync execution should succeed");

    server.abort();

    let telemetry = result
        .telemetry
        .expect("sync execution should include telemetry");
    let ttfb_ms = telemetry
        .ttfb_ms
        .expect("sync execution should include ttfb");
    let elapsed_ms = telemetry
        .elapsed_ms
        .expect("sync execution should include elapsed time");
    assert!(ttfb_ms > 0);
    assert!(elapsed_ms >= ttfb_ms);
}
