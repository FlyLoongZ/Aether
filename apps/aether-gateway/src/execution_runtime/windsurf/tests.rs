use std::collections::{BTreeMap, HashMap, HashSet};

use aether_contracts::{
    ExecutionErrorKind, ExecutionPhase, ExecutionPlan, RequestBody, StreamFrame, StreamFramePayload,
};
use base64::Engine as _;
use serde_json::{json, Value};

use aether_provider_transport::windsurf::cascade::{
    parse_trajectory_steps, CascadeStep, CascadeUsage,
};

use super::ExecutionRuntimeTransportError;
use super::{
    build_openai_chat_sse_body, detect_windsurf_request, emit_windsurf_step_text_deltas,
    is_windsurf_cascade_transport_error, is_windsurf_send_retryable_error, new_windsurf_csrf_token,
    windsurf_connect_http_error, windsurf_language_server_exited_before_ready_error,
    windsurf_language_server_ready_timeout_error, WindsurfToolCall, WindsurfToolDefinition,
};

#[test]
fn windsurf_language_server_csrf_tokens_are_per_process_secrets() {
    let first = new_windsurf_csrf_token();
    let second = new_windsurf_csrf_token();

    assert!(first.starts_with("aether-"));
    assert_eq!(first.len(), "aether-".len() + 64);
    assert_ne!(first, second);
    assert_ne!(first, "windsurf-api-csrf-fixed-token");
}

#[cfg(unix)]
#[test]
fn windsurf_language_server_binary_must_be_trusted_and_already_executable() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = std::env::current_dir()
        .expect("current directory")
        .join(format!(
            ".aether-windsurf-binary-test-{}",
            uuid::Uuid::new_v4()
        ));
    std::fs::create_dir(&root).expect("test directory should be created");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
        .expect("test directory mode should be private");

    let binary = root.join("language-server");
    std::fs::write(&binary, b"test binary").expect("binary fixture should be written");
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))
        .expect("binary fixture should be executable");
    assert_eq!(
        super::validate_language_server_binary_path(&binary)
            .expect("trusted executable should validate"),
        std::fs::canonicalize(&binary).expect("binary fixture should canonicalize")
    );

    let linked = root.join("language-server-link");
    symlink(&binary, &linked).expect("binary symlink should be created");
    assert!(super::validate_language_server_binary_path(&linked).is_err());

    let hard_linked = root.join("language-server-hard-link");
    std::fs::hard_link(&binary, &hard_linked).expect("binary hard link should be created");
    assert!(super::validate_language_server_binary_path(&binary).is_err());
    assert!(super::validate_language_server_binary_path(&hard_linked).is_err());
    std::fs::remove_file(&hard_linked).expect("binary hard link should be removed");

    assert!(super::validate_language_server_binary_path(&root).is_err());

    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o720))
        .expect("binary fixture should become group writable");
    assert!(super::validate_language_server_binary_path(&binary).is_err());

    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o600))
        .expect("binary fixture should become non-executable");
    assert!(super::validate_language_server_binary_path(&binary).is_err());
    assert_eq!(
        std::fs::metadata(&binary)
            .expect("binary fixture metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600,
        "validation must not grant execute permission"
    );

    let unsafe_parent = root.join("group-writable");
    std::fs::create_dir(&unsafe_parent).expect("unsafe parent fixture should be created");
    std::fs::set_permissions(&unsafe_parent, std::fs::Permissions::from_mode(0o770))
        .expect("unsafe parent should be group writable");
    let nested_binary = unsafe_parent.join("language-server");
    std::fs::write(&nested_binary, b"test binary")
        .expect("nested binary fixture should be written");
    std::fs::set_permissions(&nested_binary, std::fs::Permissions::from_mode(0o700))
        .expect("nested binary fixture should be executable");
    assert!(super::validate_language_server_binary_path(&nested_binary).is_err());

    std::fs::remove_dir_all(root).expect("test directory should be removed");
}

#[cfg(unix)]
#[test]
fn windsurf_runtime_files_are_private_and_reject_links() {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let root = std::env::temp_dir().join(format!(
        "aether-windsurf-private-test-{}",
        uuid::Uuid::new_v4()
    ));
    super::ensure_private_directory(&root).expect("private root should be created");
    let workspace = root.join("workspace");
    super::ensure_workspace_dir(&workspace).expect("private workspace should be created");

    assert_eq!(
        std::fs::metadata(&root)
            .expect("private root metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(workspace.join("package.json"))
            .expect("private workspace file metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let log_path = root.join("language-server.stderr.log");
    drop(super::open_private_append_file(&log_path).expect("private log should open"));
    assert_eq!(
        std::fs::metadata(&log_path)
            .expect("private log metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let target = root.join("target.log");
    super::write_private_file(&target, b"target").expect("target fixture should be written");
    let link = root.join("linked.log");
    symlink(&target, &link).expect("symlink fixture should be created");
    assert!(super::open_private_append_file(&link).is_err());

    let hard_link = root.join("hard-linked.log");
    std::fs::hard_link(&target, &hard_link).expect("hard-link fixture should be created");
    assert!(super::open_private_append_file(&target).is_err());

    std::fs::remove_dir_all(root).expect("private runtime fixture should be removed");
}

#[test]
fn windsurf_delta_buffer_rejects_overflow_without_mutation() {
    let mut content = "1234".to_string();

    let error = super::append_windsurf_delta_with_limit(&mut content, "56", 5)
        .expect_err("decoded content above the limit must fail");

    assert_eq!(content, "1234");
    assert!(matches!(
        error,
        ExecutionRuntimeTransportError::UpstreamResponseTooLarge { limit_bytes: 5, .. }
    ));
}

#[test]
fn windsurf_auxiliary_errors_omit_response_and_local_diagnostics() {
    let sensitive_detail = "Bearer secret-windsurf-error-body stderr tail /private/path";
    let connect_message = windsurf_connect_http_error("GetUserStatus", 502).to_string();

    assert!(connect_message.contains("Windsurf Connect GetUserStatus returned HTTP 502"));
    assert!(!connect_message.contains(sensitive_detail));

    let exited_message = windsurf_language_server_exited_before_ready_error().to_string();
    let timeout_message = windsurf_language_server_ready_timeout_error().to_string();
    assert_eq!(
        exited_message,
        "failed to execute upstream request: Windsurf language server exited before becoming ready"
    );
    assert_eq!(
        timeout_message,
        "failed to execute upstream request: Windsurf language server was not ready before timeout"
    );
    assert!(!exited_message.contains(sensitive_detail));
    assert!(!timeout_message.contains(sensitive_detail));

    let raw_error = ExecutionRuntimeTransportError::UpstreamRequest(format!(
        "request failed: {sensitive_detail}"
    ));
    let public_error = super::windsurf_execution_error_from_transport_error(
        &raw_error,
        ExecutionPhase::StreamRead,
    );
    let trajectory_error =
        super::windsurf_trajectory_error(&format!("rate limit: {sensitive_detail}"));
    assert_eq!(public_error.message, "Windsurf request failed");
    assert!(trajectory_error.to_string().contains("rate limit"));
    assert!(!public_error.message.contains("secret-windsurf-error-body"));
    assert!(!trajectory_error
        .to_string()
        .contains("secret-windsurf-error-body"));
}

fn windsurf_plan() -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-windsurf".to_string(),
        candidate_id: Some("cand-windsurf".to_string()),
        provider_name: Some("Windsurf".to_string()),
        provider_id: "provider-windsurf".to_string(),
        endpoint_id: "endpoint-windsurf".to_string(),
        key_id: "key-windsurf".to_string(),
        method: "POST".to_string(),
        url: "https://server.codeium.com/exa.api_server_pb.ApiServerService/GetChatMessage"
            .to_string(),
        headers: BTreeMap::from([(
            "authorization".to_string(),
            "Bearer windsurf-api-key".to_string(),
        )]),
        content_type: Some("application/connect+json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(json!({
            "metadata": {"apiKey": "windsurf-api-key"},
            "model": "gpt-5-5-low",
            "modelName": "gpt-5-5-low",
            "message": "hello from test",
            "messages": [{"role": "user", "content": "hello from test"}],
            "stream": true
        })),
        stream: true,
        client_api_format: "openai:chat".to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("gpt-5-5-low".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

#[test]
fn detects_legacy_windsurf_envelope_plan_as_native_request() {
    let plan = windsurf_plan();
    let report_context = json!({
        "envelope_name": aether_provider_transport::windsurf::WINDSURF_ENVELOPE_NAME,
    });

    let detected =
        detect_windsurf_request(&plan, Some(&report_context)).expect("request should match");

    assert_eq!(detected.api_key, "windsurf-api-key");
    assert_eq!(detected.model, "gpt-5-5-low");
    assert_eq!(detected.message, "hello from test");
}

#[test]
fn detects_latest_user_image_parts_for_native_cascade_field_six() {
    let mut plan = windsurf_plan();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "gpt-5-5-low",
        "message": "describe this",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "describe this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,aW1hZ2U="}}
            ]
        }],
        "stream": true
    }));

    let detected = detect_windsurf_request(&plan, None).expect("request should match");

    assert_eq!(detected.message, "describe this");
    assert_eq!(detected.images.len(), 1);
    assert_eq!(detected.images[0].mime_type, "image/png");
    assert_eq!(detected.images[0].base64_data, "aW1hZ2U=");
}

#[test]
fn detects_declared_tools_and_builds_proto_tool_preamble() {
    let mut plan = windsurf_plan();
    plan.client_api_format = "openai:responses".to_string();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "gpt-5-5-low",
        "messages": [{"role": "user", "content": "read Cargo.toml"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "Read",
                "description": "Read a local file",
                "parameters": {
                    "type": "object",
                    "properties": {"file_path": {"type": "string"}},
                    "required": ["file_path"]
                }
            }
        }],
        "tool_choice": "required"
    }));

    let detected = detect_windsurf_request(&plan, None).expect("request should match");

    assert_eq!(detected.tools.len(), 1);
    let preamble = detected
        .tool_preamble
        .as_deref()
        .expect("tools should build a proto preamble");
    assert!(preamble.contains("Available functions"));
    assert!(preamble.contains("### Read"));
    assert!(preamble.contains("function_call"));
    assert!(preamble.contains("MUST call at least one function"));
    assert!(detected.message.contains("Tools available this turn"));
    assert!(detected.message.contains("read Cargo.toml"));
}

#[test]
fn web_search_tool_normalizes_to_function_tool() {
    let tool = super::normalize_windsurf_tool(&json!({
        "type": "web_search_preview",
        "description": "Search live web"
    }))
    .expect("web search should normalize");

    assert_eq!(tool.name, "web_search");
    assert_eq!(tool.description.as_deref(), Some("Search live web"));
    assert_eq!(
        tool.parameters
            .as_ref()
            .and_then(|value| value.pointer("/properties/query/type"))
            .and_then(Value::as_str),
        Some("string")
    );
}

#[test]
fn converted_claude_builtin_web_search_synthesizes_tool_from_tool_choice() {
    let mut plan = windsurf_plan();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "claude-sonnet-4-6",
        "messages": [{
            "role": "user",
            "content": "Perform a web search for the query: 2026年5月最新科技新闻"
        }],
        "tool_choice": {
            "type": "function",
            "function": {"name": "web_search"}
        },
        "stream": true
    }));

    let detected = detect_windsurf_request(&plan, None).expect("request should match");

    assert_eq!(detected.tools.len(), 1);
    assert_eq!(detected.tools[0].name, "web_search");
    assert!(detected
        .tool_preamble
        .as_deref()
        .expect("synthetic web search should build a preamble")
        .contains("### web_search"));
}

#[test]
fn converted_claude_builtin_web_search_enters_native_bridge_when_enabled() {
    let mut plan = windsurf_plan();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "claude-sonnet-4-6",
        "messages": [{
            "role": "user",
            "content": "Perform a web search for the query: 2026年5月最新科技新闻"
        }],
        "tool_choice": {
            "type": "function",
            "function": {"name": "web_search"}
        },
        "stream": true
    }));

    let detected = super::detect_windsurf_request_with_native_bridge_flags(
        &plan,
        None,
        super::WindsurfNativeBridgeFlags {
            explicit_on: true,
            explicit_off: false,
        },
    )
    .expect("request should match");

    let bridge = detected.native_bridge.as_ref().expect("native bridge");
    assert_eq!(bridge.native_allowlist, vec!["search_web"]);
    assert!(bridge.emulation_tools.is_empty());
    assert!(detected.tool_preamble.is_none());
}

#[test]
fn codex_toolset_partitions_mapped_and_unmapped_tools() {
    let tools = [
        "shell_command",
        "update_plan",
        "apply_patch",
        "web_search",
        "view_image",
    ]
    .into_iter()
    .map(|name| WindsurfToolDefinition {
        name: name.to_string(),
        description: None,
        parameters: None,
    })
    .collect::<Vec<_>>();

    let partition = super::partition_windsurf_tools(&tools);

    assert!(partition.has_any);
    assert_eq!(
        partition
            .mapped
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["shell_command", "web_search"]
    );
    assert_eq!(
        partition
            .unmapped
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        vec!["update_plan", "apply_patch", "view_image"]
    );
}

#[test]
fn mixed_native_and_emulated_tools_preserve_tool_preamble_for_unmapped_tools() {
    let mut plan = windsurf_plan();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "claude-sonnet-4.6",
        "messages": [{"role": "user", "content": "search and patch"}],
        "tools": [
            {"type": "function", "function": {"name": "WebSearch", "description": "Search web"}},
            {"type": "function", "function": {"name": "apply_patch", "description": "Patch files"}}
        ]
    }));

    let detected = super::detect_windsurf_request_with_native_bridge_flags(
        &plan,
        None,
        super::WindsurfNativeBridgeFlags {
            explicit_on: true,
            explicit_off: false,
        },
    )
    .expect("request should match");

    let bridge = detected.native_bridge.as_ref().expect("native bridge");
    assert_eq!(bridge.native_allowlist, vec!["search_web"]);
    assert_eq!(bridge.emulation_tools[0].name, "apply_patch");
    assert!(detected
        .tool_preamble
        .as_deref()
        .expect("unmapped preamble")
        .contains("### apply_patch"));
    assert!(!detected
        .tool_preamble
        .as_deref()
        .expect("unmapped preamble")
        .contains("### WebSearch"));
}

#[test]
fn windsurf_native_bridge_flags_match_windsurfapi_default_off() {
    let tools = vec![WindsurfToolDefinition {
        name: "WebSearch".to_string(),
        description: None,
        parameters: None,
    }];

    assert!(!super::should_use_windsurf_native_tool_bridge_with_flags(
        &tools,
        super::WindsurfNativeBridgeFlags {
            explicit_on: false,
            explicit_off: false,
        },
    ));
    assert!(super::should_use_windsurf_native_tool_bridge_with_flags(
        &tools,
        super::WindsurfNativeBridgeFlags {
            explicit_on: true,
            explicit_off: false,
        },
    ));
    assert!(!super::should_use_windsurf_native_tool_bridge_with_flags(
        &tools,
        super::WindsurfNativeBridgeFlags {
            explicit_on: true,
            explicit_off: true,
        },
    ));
}

#[test]
fn native_trajectory_step_restores_declared_tool_name() {
    let tools = vec![WindsurfToolDefinition {
        name: "WebSearch".to_string(),
        description: None,
        parameters: None,
    }];
    let step_bytes = aether_provider_transport::windsurf::cascade::build_additional_step(
        "search_web",
        &json!({"query": "today tech", "domain": "example.com"}),
    )
    .expect("step should encode");
    let response = aether_provider_transport::windsurf::proto::write_message_field(1, &step_bytes);
    let steps = parse_trajectory_steps(&response).expect("steps should parse");

    let call = super::native_cascade_step_to_windsurf_tool_call(&steps[0], &tools, 0)
        .expect("native step should map");

    assert_eq!(call.name, "WebSearch");
    assert_eq!(
        serde_json::from_str::<Value>(&call.arguments_json).expect("arguments json"),
        json!({"query": "today tech", "domains": ["example.com"]})
    );
}

#[test]
fn native_tool_collection_tracks_steps_once() {
    let tools = vec![WindsurfToolDefinition {
        name: "WebSearch".to_string(),
        description: None,
        parameters: None,
    }];
    let bridge = super::WindsurfNativeBridgeInput {
        native_allowlist: vec!["search_web".to_string()],
        additional_steps: Vec::new(),
        mapped_tools: tools,
        emulation_tools: Vec::new(),
    };
    let step_bytes = aether_provider_transport::windsurf::cascade::build_additional_step(
        "search_web",
        &json!({"query": "today tech"}),
    )
    .expect("step should encode");
    let response = aether_provider_transport::windsurf::proto::write_message_field(1, &step_bytes);
    let steps = parse_trajectory_steps(&response).expect("steps should parse");
    let mut seen = HashSet::new();
    let mut calls = Vec::new();

    assert!(super::collect_windsurf_native_tool_calls(
        &steps,
        Some(&bridge),
        &mut seen,
        &mut calls,
    ));
    assert!(!super::collect_windsurf_native_tool_calls(
        &steps,
        Some(&bridge),
        &mut seen,
        &mut calls,
    ));
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name, "WebSearch");
}

#[test]
fn native_tool_single_frame_uses_supplied_stream_index() {
    let tool_call = WindsurfToolCall {
        id: "call_windsurf_native_7".to_string(),
        name: "web_search".to_string(),
        arguments_json: r#"{"query":"today tech"}"#.to_string(),
    };

    let frame = super::sse_tool_call_frame("req-1", "gpt-5-5-low", 3, &tool_call);
    let text = decode_data_frame_text(&frame);

    assert!(text.contains(r#""index":3"#));
    assert!(text.contains(r#""name":"web_search""#));
    assert!(text.contains(r#""arguments":"{\"query\":\"today tech\"}""#));
}

#[test]
fn native_tool_stream_finish_reason_stays_tool_calls_after_live_delta() {
    assert_eq!(
        super::windsurf_stream_finish_reason(true, false),
        "tool_calls"
    );
    assert_eq!(
        super::windsurf_stream_finish_reason(false, true),
        "tool_calls"
    );
    assert_eq!(super::windsurf_stream_finish_reason(false, false), "stop");
}

#[test]
fn windsurf_rate_limit_errors_classify_as_upstream_429() {
    let err = ExecutionRuntimeTransportError::UpstreamRequest(
        "Reached message rate limit for this model. Please try again later. Resets in: 2h58m56s"
            .to_string(),
    );

    let execution_error =
        super::windsurf_execution_error_from_transport_error(&err, ExecutionPhase::StreamRead);

    assert_eq!(execution_error.kind, ExecutionErrorKind::Upstream4xx);
    assert_eq!(execution_error.upstream_status, Some(429));
    assert!(execution_error.retryable);
    assert!(execution_error.failover_recommended);
}

#[test]
fn windsurf_transport_errors_do_not_synthesize_provider_status() {
    let cases = [
        (
            "tcp connect error: connection refused",
            ExecutionErrorKind::ProtocolError,
            ExecutionPhase::Connect,
        ),
        (
            "dns lookup failed: no such host",
            ExecutionErrorKind::ProtocolError,
            ExecutionPhase::Connect,
        ),
        (
            "TLS certificate handshake failed",
            ExecutionErrorKind::TlsError,
            ExecutionPhase::Handshake,
        ),
        (
            "proxy connection failed",
            ExecutionErrorKind::ProxyError,
            ExecutionPhase::Connect,
        ),
        (
            "cascade polling timed out",
            ExecutionErrorKind::ReadTimeout,
            ExecutionPhase::StreamRead,
        ),
        (
            "connection reset by peer",
            ExecutionErrorKind::ProtocolError,
            ExecutionPhase::StreamRead,
        ),
    ];

    for (message, expected_kind, expected_phase) in cases {
        let err = ExecutionRuntimeTransportError::UpstreamRequest(message.to_string());
        let execution_error =
            super::windsurf_execution_error_from_transport_error(&err, ExecutionPhase::StreamRead);

        assert_eq!(execution_error.kind, expected_kind, "{message}");
        assert_eq!(execution_error.phase, expected_phase, "{message}");
        assert_eq!(execution_error.upstream_status, None, "{message}");
        assert!(execution_error.retryable, "{message}");
        assert!(execution_error.failover_recommended, "{message}");
    }
}

#[test]
fn windsurf_sanitizer_redacts_workspace_paths_in_text_and_tool_args() {
    let text = super::sanitize_windsurf_text(
        "read /tmp/windsurf-workspace/a.txt under /Volumes/ext/GitHub/Aether and /home/user/projects/workspace-abc/main.rs",
    );
    assert!(!text.contains("/tmp/windsurf-workspace"));
    assert!(!text.contains("/Volumes/ext/GitHub/Aether"));
    assert!(!text.contains("/home/user/projects/workspace-abc"));
    assert!(text.contains("<workspace>"));

    let call = super::sanitize_windsurf_tool_call(WindsurfToolCall {
        id: "call_1".to_string(),
        name: "Read".to_string(),
        arguments_json: r#"{"file_path":"/tmp/windsurf-workspace/a.txt"}"#.to_string(),
    });
    assert_eq!(call.arguments_json, r#"{"file_path":"<workspace>/a.txt"}"#);
}

#[test]
fn emulated_parser_recovers_declared_function_style_tool_call() {
    let input = super::WindsurfRequestInput {
        api_key: "windsurf-api-key".to_string(),
        model: "gpt-5-5-low".to_string(),
        message: String::new(),
        images: Vec::new(),
        tools: vec![WindsurfToolDefinition {
            name: "WebSearch".to_string(),
            description: None,
            parameters: None,
        }],
        tool_preamble: None,
        tool_dialect: super::ToolDialect::OpenAiJsonXml,
        native_bridge: None,
    };

    let input_debug = format!("{input:?}");
    assert!(input_debug.contains("[REDACTED]"));
    assert!(!input_debug.contains("windsurf-api-key"));

    let parsed = super::parse_and_filter_windsurf_tool_calls(
        r#"I will use WebSearch(query="today tech", domain="example.com")."#,
        &input,
    );

    assert_eq!(parsed.tool_calls.len(), 1);
    assert_eq!(parsed.tool_calls[0].name, "WebSearch");
    assert_eq!(
        serde_json::from_str::<Value>(&parsed.tool_calls[0].arguments_json).unwrap(),
        json!({"query": "today tech", "domain": "example.com"})
    );
}

#[test]
fn native_bridge_filters_emulated_tool_calls_to_unmapped_tools() {
    let mapped = WindsurfToolDefinition {
        name: "WebSearch".to_string(),
        description: None,
        parameters: None,
    };
    let unmapped = WindsurfToolDefinition {
        name: "apply_patch".to_string(),
        description: None,
        parameters: None,
    };
    let input = super::WindsurfRequestInput {
        api_key: "windsurf-api-key".to_string(),
        model: "gpt-5-5-low".to_string(),
        message: String::new(),
        images: Vec::new(),
        tools: vec![mapped.clone(), unmapped.clone()],
        tool_preamble: None,
        tool_dialect: super::ToolDialect::OpenAiJsonXml,
        native_bridge: Some(super::WindsurfNativeBridgeInput {
            native_allowlist: vec!["search_web".to_string()],
            additional_steps: Vec::new(),
            mapped_tools: vec![mapped],
            emulation_tools: vec![unmapped],
        }),
    };
    let parsed = super::parse_and_filter_windsurf_tool_calls(
        r#"
        <tool_call>{"name":"WebSearch","arguments":{"query":"today tech"}}</tool_call>
        <tool_call>{"name":"apply_patch","arguments":{"patch":"*** Begin Patch\n*** End Patch"}}</tool_call>
        "#,
        &input,
    );

    assert_eq!(parsed.tool_calls.len(), 1);
    assert_eq!(parsed.tool_calls[0].name, "apply_patch");
}

#[test]
fn chat_route_gpt_uses_xml_tool_dialect_unless_forced() {
    let mut plan = windsurf_plan();
    plan.client_api_format = "openai:chat".to_string();
    plan.provider_api_format = "openai:chat".to_string();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "gpt-5-5-low",
        "messages": [{"role": "user", "content": "read Cargo.toml"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "Read",
                "description": "Read a local file",
                "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}}}
            }
        }]
    }));

    let detected = detect_windsurf_request(&plan, None).expect("request should match");

    if !super::env_flag("WINDSURFAPI_FORCE_GPT_NATIVE_DIALECT")
        && !super::env_flag("AETHER_WINDSURF_FORCE_GPT_NATIVE_DIALECT")
    {
        assert_eq!(detected.tool_dialect, super::ToolDialect::OpenAiJsonXml);
        assert!(detected
            .tool_preamble
            .as_deref()
            .expect("tool preamble")
            .contains("<tool_call>"));
    }
}

#[test]
fn tool_preamble_lifts_caller_environment() {
    let mut plan = windsurf_plan();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "claude-sonnet-4.6",
        "messages": [
            {"role": "system", "content": "<env>\nWorking directory: /Users/me/project\nIs directory a git repo: yes\nPlatform: macos\n</env>"},
            {"role": "user", "content": "read package.json"}
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "Read",
                "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}}}
            }
        }]
    }));

    let detected = detect_windsurf_request(&plan, None).expect("request should match");
    let preamble = detected.tool_preamble.as_deref().expect("tool preamble");
    assert!(preamble.contains("## Environment facts"));
    assert!(preamble.contains("- Working directory: /Users/me/project"));
    assert!(preamble.contains("placeholder directory"));
}

#[test]
fn cascade_message_preserves_tool_history_for_tool_emulation() {
    let body = json!({
        "messages": [
            {"role": "user", "content": "read Cargo.toml"},
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "Read", "arguments": "{\"file_path\":\"Cargo.toml\"}"}
                }]
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "workspace Cargo.toml content"},
            {"role": "user", "content": "summarize it"}
        ]
    });

    let text = super::build_cascade_message_text_with_dialect(&body, super::ToolDialect::GptNative)
        .expect("message text should build");

    assert!(text.contains(r#""function_call":{"name":"Read""#));
    assert!(text.contains(r#"<tool_result tool_call_id="call_1">"#));
    assert!(text.contains("workspace Cargo.toml content"));
}

#[test]
fn detect_prefers_messages_history_over_flat_message_snapshot_for_tool_result_turn() {
    let mut plan = windsurf_plan();
    plan.body = RequestBody::from_json(json!({
        "metadata": {"apiKey": "windsurf-api-key"},
        "model": "gpt-5-5-low",
        "modelName": "gpt-5-5-low",
        "message": "read Cargo.toml",
        "messages": [
            {"role": "user", "content": "read Cargo.toml"},
            {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "Read", "arguments": "{\"file_path\":\"Cargo.toml\"}"}
                }]
            },
            {"role": "tool", "tool_call_id": "call_1", "content": "workspace Cargo.toml content"}
        ],
        "tools": [{
            "type": "function",
            "function": {
                "name": "Read",
                "parameters": {"type": "object", "properties": {"file_path": {"type": "string"}}}
            }
        }]
    }));

    let detected = detect_windsurf_request(&plan, None).expect("request should match");

    assert_ne!(detected.message, "read Cargo.toml");
    assert!(detected
        .message
        .contains(r#"<tool_result tool_call_id="call_1">"#));
    assert!(detected.message.contains("workspace Cargo.toml content"));
    assert!(
        !detected.message.contains("Tools available this turn"),
        "synthetic tool_result turns should not receive the user-message fallback preamble"
    );
}

#[test]
fn parses_emulated_tool_calls_out_of_model_text() {
    let parsed = super::parse_windsurf_tool_calls_from_text(
        r#"before <tool_call>{"name":"Read","arguments":{"file_path":"Cargo.toml"}}</tool_call> after"#,
        super::ToolDialect::OpenAiJsonXml,
    );

    assert_eq!(parsed.text, "before  after");
    assert_eq!(parsed.tool_calls.len(), 1);
    assert_eq!(parsed.tool_calls[0].name, "Read");
    assert_eq!(
        parsed.tool_calls[0].arguments_json,
        r#"{"file_path":"Cargo.toml"}"#
    );

    let gpt = super::parse_windsurf_tool_calls_from_text(
        r#"{"function_call":{"name":"Bash","arguments":{"command":"pwd"}}}"#,
        super::ToolDialect::GptNative,
    );
    assert!(gpt.text.is_empty());
    assert_eq!(gpt.tool_calls[0].name, "Bash");

    let lenient = super::parse_windsurf_tool_calls_from_text(
        r#"<tool_call>{"name":"Read","arguments":{"file_path":"Cargo.toml"}}}</tool_call>"#,
        super::ToolDialect::OpenAiJsonXml,
    );
    assert_eq!(lenient.tool_calls[0].name, "Read");
}

#[test]
fn ignores_non_windsurf_openai_chat_plan() {
    let mut plan = windsurf_plan();
    plan.url = "https://api.openai.com/v1/chat/completions".to_string();
    plan.body = RequestBody::from_json(json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "hello"}],
        "stream": true
    }));

    assert!(detect_windsurf_request(&plan, None).is_none());
}

fn decode_data_frame_text(frame: &StreamFrame) -> String {
    let StreamFramePayload::Data { chunk_b64, .. } = &frame.payload else {
        panic!("expected data frame");
    };
    let encoded = chunk_b64.as_deref().expect("base64 payload");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("base64 should decode");
    String::from_utf8(bytes).expect("frame should be utf8")
}

#[test]
fn builds_openai_sse_chunks_for_windsurf_deltas() {
    let body = build_openai_chat_sse_body(
        "req-windsurf",
        "gpt-5-5-low",
        &["hello".to_string(), " world".to_string()],
    )
    .expect("sse should encode");
    let text = String::from_utf8(body).expect("sse should be utf8");

    assert!(text.contains(r#""object":"chat.completion.chunk""#));
    assert!(text.contains(r#""content":"hello""#));
    assert!(text.contains(r#""content":" world""#));
    assert!(text.contains(r#""finish_reason":"stop""#));
    assert!(text.ends_with("data: [DONE]\n\n"));
}

#[test]
fn maps_windsurf_generator_usage_to_openai_usage_and_terminal_summary() {
    let usage = CascadeUsage {
        input_tokens: 10,
        output_tokens: 20,
        cache_write_tokens: 30,
        cache_read_tokens: 40,
        entry_count: 2,
    };

    let openai_usage = super::windsurf_openai_usage_json(&usage);
    assert_eq!(openai_usage["prompt_tokens"], json!(50));
    assert_eq!(openai_usage["completion_tokens"], json!(20));
    assert_eq!(openai_usage["total_tokens"], json!(100));
    assert_eq!(openai_usage["cache_creation_input_tokens"], json!(30));
    assert_eq!(openai_usage["cache_read_input_tokens"], json!(40));
    assert_eq!(
        openai_usage["cascade_breakdown"]["generator_entry_count"],
        json!(2)
    );

    let summary = super::windsurf_terminal_summary(Some(usage), Some("gpt-5.5-low"), Some("stop"))
        .expect("summary should be present");
    let standardized = summary
        .standardized_usage
        .expect("standardized usage should be present");
    assert_eq!(standardized.input_tokens, 50);
    assert_eq!(standardized.output_tokens, 20);
    assert_eq!(standardized.cache_creation_tokens, 30);
    assert_eq!(standardized.cache_creation_ephemeral_5m_tokens, 30);
    assert_eq!(standardized.cache_read_tokens, 40);
    assert_eq!(
        standardized.dimensions["windsurf_generator_entry_count"],
        json!(2)
    );
}

#[test]
fn sums_per_step_usage_for_generator_metadata_fallback() {
    let usage_by_step = HashMap::from([
        (
            0usize,
            CascadeUsage {
                input_tokens: 10,
                output_tokens: 20,
                cache_write_tokens: 30,
                cache_read_tokens: 40,
                entry_count: 1,
            },
        ),
        (
            1usize,
            CascadeUsage {
                input_tokens: 1,
                output_tokens: 2,
                cache_write_tokens: 3,
                cache_read_tokens: 4,
                entry_count: 1,
            },
        ),
    ]);

    let usage = super::sum_windsurf_step_usage(&usage_by_step).expect("usage should sum");

    assert_eq!(usage.input_tokens, 11);
    assert_eq!(usage.output_tokens, 22);
    assert_eq!(usage.cache_write_tokens, 33);
    assert_eq!(usage.cache_read_tokens, 44);
    assert_eq!(usage.entry_count, 2);
}

#[test]
fn resolves_windsurf_model_from_key_upstream_metadata_when_static_catalog_misses() {
    assert!(
        aether_provider_transport::windsurf::models::resolve_windsurf_model("deepseek-v4")
            .is_none()
    );
    let upstream_metadata = json!({
        "windsurf": {
            "models": [{
                "model_uid": "deepseek-v4",
                "label": "DeepSeek V4",
                "provider": "MODEL_PROVIDER_DEEPSEEK",
                "credit_multiplier": 3.0
            }]
        }
    });

    let model = super::resolve_windsurf_execution_model("deepseek-v4", Some(&upstream_metadata))
        .expect("live Windsurf metadata model should resolve");

    assert_eq!(model.canonical_name, "deepseek-v4");
    assert_eq!(model.enum_value, 0);
    assert_eq!(model.model_uid.as_deref(), Some("deepseek-v4"));
}

#[test]
fn cascade_message_text_neutralizes_system_identity_for_user_channel() {
    let body = json!({
        "messages": [
            {"role": "system", "content": "You are Codex, a coding agent.\nx-anthropic-billing-header: secret"},
            {"role": "user", "content": "hello"}
        ]
    });

    let text = super::build_cascade_message_text(&body).expect("message text should build");

    assert!(text.contains("The assistant is"));
    assert!(!text.contains("You are Codex"));
    assert!(!text.contains("x-anthropic-billing-header"));
}

#[test]
fn long_claude_code_system_prompt_is_compacted_before_user_channel() {
    let long_system = format!(
        "Anthropic's official CLI for Claude\n<env>\nWorking directory: /Users/me/project\nPlatform: macos\n</env>\n{}\ncontent_block tool_use",
        "tool protocol details\n".repeat(260)
    );
    let body = json!({
        "messages": [
            {"role": "system", "content": long_system},
            {"role": "user", "content": "hello"}
        ]
    });

    let text = super::build_cascade_message_text(&body).expect("message text should build");

    assert!(text.contains("local coding CLI request"));
    assert!(text.contains("- Working directory: /Users/me/project"));
    assert!(!text.contains("content_block tool_use"));
}

#[test]
fn final_sweep_tops_up_response_and_modified_text_extensions() {
    let mut yielded_by_step = std::collections::HashMap::from([(0usize, 5usize)]);
    let steps = vec![CascadeStep {
        step_type: 15,
        status: 3,
        text: "hello world!".to_string(),
        response_text: "hello world".to_string(),
        modified_text: "hello world!".to_string(),
        thinking: String::new(),
        error_text: String::new(),
        native_tool: None,
        usage: None,
    }];
    let mut deltas = Vec::new();

    let grew = emit_windsurf_step_text_deltas(&steps, &mut yielded_by_step, true, |delta| {
        deltas.push(delta);
        Ok(())
    })
    .expect("deltas should emit");

    assert!(grew);
    assert_eq!(deltas, vec![" world".to_string(), "!".to_string()]);
    assert_eq!(yielded_by_step.get(&0), Some(&12));
}

#[test]
fn text_delta_cursor_resets_on_non_char_boundary() {
    assert_eq!(
        super::windsurf_text_delta_from_cursor("aé", 2).as_deref(),
        Some("aé")
    );
    assert_eq!(
        super::windsurf_text_delta_from_cursor("hello", 2).as_deref(),
        Some("llo")
    );
}

#[test]
fn windsurf_warmup_treats_http_404_as_non_transport_error() {
    let err = ExecutionRuntimeTransportError::UpstreamRequest(
        "Windsurf gRPC UpdateWorkspaceTrust returned HTTP 404 Not Found: 404 page not found"
            .to_string(),
    );

    assert!(!is_windsurf_cascade_transport_error(&err));
}

#[test]
fn windsurf_warmup_treats_panel_state_errors_as_transport_errors() {
    let err = ExecutionRuntimeTransportError::UpstreamRequest(
        "Windsurf gRPC StartCascade returned panel state not found".to_string(),
    );

    assert!(is_windsurf_cascade_transport_error(&err));
}

#[test]
fn windsurf_treats_local_ls_connection_refused_as_transport_error() {
    let err = ExecutionRuntimeTransportError::UpstreamRequest(
        "Windsurf gRPC GetCascadeTrajectorySteps request failed: error sending request for url (http://127.0.0.1:42102/exa.language_server_pb.LanguageServerService/GetCascadeTrajectorySteps): client error (Connect): tcp connect error: Connection refused (os error 61) [kind=connect,request]"
            .to_string(),
    );

    assert!(is_windsurf_cascade_transport_error(&err));
    assert!(is_windsurf_send_retryable_error(&err));
}

#[test]
fn windsurf_send_retry_classifies_panel_untrusted_and_expired_cascade_errors() {
    for message in [
        "panel state not found",
        "SendUserCascadeMessage returned untrusted workspace",
        "not_found: cascade not found",
        "unknown trajectory",
    ] {
        let err = ExecutionRuntimeTransportError::UpstreamRequest(message.to_string());

        assert!(is_windsurf_send_retryable_error(&err), "{message}");
    }

    let quota = ExecutionRuntimeTransportError::UpstreamRequest(
        "resource_exhausted: quota exhausted".to_string(),
    );
    assert!(!is_windsurf_send_retryable_error(&quota));
}
