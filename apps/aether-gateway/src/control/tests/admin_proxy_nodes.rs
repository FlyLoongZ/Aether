use http::Uri;

use super::{classify_control_route, headers};

fn assert_proxy_nodes_admin_route(method: http::Method, path: &str, route_kind: &str) {
    let headers = headers(&[]);
    let uri: Uri = path.parse().expect("uri should parse");
    let decision = classify_control_route(&method, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("proxy_nodes_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some(route_kind));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:proxy_nodes")
    );
    assert!(!decision.is_execution_runtime_candidate());
}

#[test]
fn classifies_supported_manual_proxy_node_routes() {
    for (method, path, route_kind) in [
        (http::Method::GET, "/api/admin/proxy-nodes", "list_nodes"),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/manual",
            "create_manual_node",
        ),
        (
            http::Method::PATCH,
            "/api/admin/proxy-nodes/node-1",
            "update_manual_node",
        ),
        (
            http::Method::GET,
            "/api/admin/proxy-nodes/node-1",
            "get_node",
        ),
        (
            http::Method::DELETE,
            "/api/admin/proxy-nodes/node-1",
            "delete_node",
        ),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/node-1/test",
            "test_node",
        ),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/test-url",
            "test_proxy_url",
        ),
    ] {
        for suffix in ["", "/", "?skip=10&limit=20", "/?limit=20"] {
            assert_proxy_nodes_admin_route(method.clone(), &format!("{path}{suffix}"), route_kind);
        }
    }
}

#[test]
fn removed_automatic_proxy_node_routes_do_not_classify() {
    let headers = headers(&[]);
    for (method, path) in [
        (http::Method::POST, "/api/admin/proxy-nodes/register"),
        (http::Method::POST, "/api/admin/proxy-nodes/heartbeat"),
        (http::Method::POST, "/api/admin/proxy-nodes/unregister"),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/install-sessions",
        ),
        (http::Method::GET, "/api/admin/proxy-nodes/node-1/metrics"),
        (http::Method::GET, "/api/admin/proxy-nodes/metrics/fleet"),
        (http::Method::PUT, "/api/admin/proxy-nodes/node-1/config"),
        (http::Method::GET, "/api/admin/proxy-nodes/node-1/events"),
        (http::Method::POST, "/api/admin/proxy-nodes/upgrade"),
        (http::Method::POST, "/api/admin/proxy-nodes/upgrade/cancel"),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/upgrade/clear-conflicts",
        ),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/upgrade/restore-skipped",
        ),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/node-1/upgrade/skip",
        ),
        (
            http::Method::POST,
            "/api/admin/proxy-nodes/node-1/upgrade/retry",
        ),
    ] {
        for suffix in ["", "/", "?limit=50", "/?limit=50"] {
            let uri: Uri = format!("{path}{suffix}").parse().expect("uri should parse");
            assert!(
                classify_control_route(&method, &uri, &headers).is_none(),
                "removed route {method} {uri} must not classify"
            );
        }
    }
}

#[test]
fn proxy_node_routes_reject_unknown_subresources_and_reserved_operation_names() {
    let headers = headers(&[]);
    for suffix in [
        "register",
        "heartbeat",
        "unregister",
        "install-sessions",
        "upgrade",
        "metrics",
        "manual",
        "test-url",
        "node-1/unknown",
        "node-1/config",
        "node-1/events",
        "node-1/test/extra",
        "node-1/upgrade/skip",
        "/node-1",
    ] {
        for method in [http::Method::GET, http::Method::PATCH, http::Method::DELETE] {
            let uri: Uri = format!("/api/admin/proxy-nodes/{suffix}")
                .parse()
                .expect("uri should parse");
            assert!(
                classify_control_route(&method, &uri, &headers).is_none(),
                "unsupported route {method} {uri} must not classify"
            );
        }
    }
    for (method, suffix) in [
        (http::Method::POST, "node-1"),
        (http::Method::PUT, "node-1"),
        (http::Method::POST, "node-1/unknown/test"),
        (http::Method::POST, "register/test"),
    ] {
        let uri: Uri = format!("/api/admin/proxy-nodes/{suffix}")
            .parse()
            .expect("uri should parse");
        assert!(classify_control_route(&method, &uri, &headers).is_none());
    }
}
