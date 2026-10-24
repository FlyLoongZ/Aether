use super::{
    apply_codex_openai_responses_chat_body_edits, apply_codex_openai_responses_compact_body_edits,
    apply_codex_openai_responses_lite_header_for_request_body_with_capabilities,
    apply_codex_openai_responses_lite_header_with_capabilities,
    apply_codex_openai_responses_special_body_edits,
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities,
    apply_codex_openai_responses_websocket_continuation_body_edits_with_source_model_and_capabilities,
    apply_codex_openai_special_headers, apply_openai_responses_compact_special_body_edits,
    build_codex_model_catalog_metadata, bundled_codex_model_cards, effective_codex_model_cards,
    parse_codex_auth_identity, project_codex_catalog_model_card,
    resolve_codex_responses_model_capabilities,
    validate_codex_openai_responses_compact_request_contract, CODEX_OPENAI_IMAGE_INTERNAL_MODEL,
    CODEX_OPENAI_RESPONSES_UNSUPPORTED_BODY_FIELDS, CODEX_RESPONSES_LITE_HEADER,
};
use serde_json::{json, Value};

#[test]
fn codex_client_user_agent_matches_originator_and_version() {
    let profile = crate::codex_client_profile();
    assert!(profile.user_agent.starts_with(&format!(
        "{}/{} (",
        profile.originator, profile.codex_version
    )));
    assert!(profile.user_agent.ends_with(") unknown"));
}

#[test]
fn bundled_latest_cli_models_preserve_capabilities_without_account_data() {
    for model in ["gpt-6-astra", "gpt-6.1-sol"] {
        let card = bundled_codex_model_cards()
            .iter()
            .find(|card| card["slug"] == model)
            .unwrap();
        assert_eq!(card["context_window"], 272_000);
        assert_eq!(card["max_context_window"], 872_000);
        assert_eq!(card["comp_hash"], "3000");
        assert_eq!(card["minimal_client_version"], "0.153.0");
        assert_eq!(
            card["supported_reasoning_levels"]
                .as_array()
                .unwrap()
                .iter()
                .map(|level| level["effort"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["low", "medium", "high", "xhigh", "max", "ultra"]
        );
        for private in [
            "account_id",
            "user_id",
            "installation_id",
            "cookie",
            "authorization",
        ] {
            assert!(
                card.get(private).is_none(),
                "unexpected account field: {private}"
            );
        }
    }
}

#[test]
fn model_card_drives_unknown_codex_model_body_and_header_contracts() {
    let card = json!({
        "id": "gpt-future-agent",
        "slug": "gpt-future-agent",
        "use_responses_lite": true,
        "default_reasoning_level": "low",
        "default_reasoning_summary": "auto",
        "supports_parallel_tool_calls": true,
        "support_verbosity": true,
        "default_verbosity": "low",
        "service_tiers": [{"id": "priority"}],
        "supported_reasoning_levels": [
            {"effort": "low"},
            {"effort": "max"},
            {"effort": "ultra"}
        ],
        "base_instructions": "large prompt",
        "model_messages": {"instructions_template": "large template"},
        "available_in_plans": ["pro"],
        "future_capability": {"mode": "native"}
    });
    let metadata = build_codex_model_catalog_metadata(&[card]);
    assert_eq!(
        metadata["codex_models"]["cards"]["gpt-future-agent"]["future_capability"]["mode"],
        "native"
    );
    let stored_card = &metadata["codex_models"]["cards"]["gpt-future-agent"];
    assert!(stored_card.get("base_instructions").is_none());
    assert!(stored_card.get("model_messages").is_none());
    assert!(stored_card.get("available_in_plans").is_none());
    let capabilities = resolve_codex_responses_model_capabilities(
        "gpt-future-agent",
        "gpt-future-agent",
        Some(&metadata),
    );
    assert!(capabilities.supports_reasoning_summary_parameter);
    assert_eq!(
        capabilities.default_reasoning_effort.as_deref(),
        Some("low")
    );
    assert_eq!(
        capabilities.default_reasoning_summary.as_deref(),
        Some("auto")
    );

    let mut body = json!({
        "model": "gpt-future-agent",
        "instructions": "Use the tools.",
        "input": [{"id": "msg-1", "type": "message", "role": "user", "content": []}],
        "tools": [{"type": "function", "name": "lookup"}],
        "reasoning": {"effort": "max"},
        "parallel_tool_calls": true,
        "service_tier": "priority",
        "text": {"format": {"type": "json_schema"}},
        "include": ["file_search_call.results"]
    });
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        &mut body,
        "codex",
        "openai:responses",
        "gpt-future-agent",
        "gpt-future-agent",
        Some(&capabilities),
        None,
    );
    assert_eq!(body["reasoning"]["effort"], "max");
    assert_eq!(body["reasoning"]["summary"], "auto");
    assert_eq!(body["reasoning"]["context"], "all_turns");
    assert!(body.get("instructions").is_none());
    assert!(body.get("tools").is_none());
    assert_eq!(body["input"][0]["type"], "additional_tools");
    assert!(body["input"][1].get("id").is_none());
    assert_eq!(body["input"][2]["id"], "msg-1");
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["service_tier"], "priority");
    assert_eq!(body["text"]["verbosity"], "low");
    assert_eq!(body["text"]["format"]["type"], "json_schema");
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));

    let mut headers = std::collections::BTreeMap::new();
    apply_codex_openai_responses_lite_header_with_capabilities(
        &mut headers,
        "codex",
        "openai:responses",
        "gpt-future-agent",
        "gpt-future-agent",
        Some(&capabilities),
    );
    assert_eq!(
        headers.get("x-openai-internal-codex-responses-lite"),
        Some(&"true".to_string())
    );
}

#[test]
fn projects_current_codex_catalog_card_as_opaque_json() {
    let card = json!({
        "id": "upstream-internal-id",
        "slug": "gpt-future-dynamic",
        "api_formats": ["openai:responses"],
        "model_messages": {
            "instructions_template": "Current instructions for {{ personality }}"
        },
        "available_in_plans": ["plus", "pro"],
        "future_capability": {
            "mode": "native",
            "unknown_nested_field": [1, 2, 3]
        }
    });

    let projected =
        project_codex_catalog_model_card(&[card], "gpt-future-dynamic", "future-model-alias")
            .expect("current Codex card should project");

    assert_eq!(projected["slug"], "future-model-alias");
    assert!(projected.get("id").is_none());
    assert!(projected.get("api_formats").is_none());
    assert_eq!(
        projected["model_messages"]["instructions_template"],
        "Current instructions for {{ personality }}"
    );
    assert_eq!(projected["available_in_plans"], json!(["plus", "pro"]));
    assert_eq!(
        projected["future_capability"],
        json!({
            "mode": "native",
            "unknown_nested_field": [1, 2, 3]
        })
    );
}

#[test]
fn projects_legacy_codex_catalog_card_by_id_without_known_schema_fields() {
    let card = json!({
        "id": "gpt-future-dynamic",
        "base_instructions": "Legacy instructions",
        "available_in_plans": ["team"],
        "future_capability": true
    });

    let projected = project_codex_catalog_model_card(
        &[json!("not an object"), card],
        "gpt-future-dynamic",
        "legacy-model-alias",
    )
    .expect("legacy Codex card should project by id");

    assert_eq!(
        projected,
        json!({
            "slug": "legacy-model-alias",
            "base_instructions": "Legacy instructions",
            "available_in_plans": ["team"],
            "future_capability": true
        })
    );
}

#[test]
fn codex_catalog_projection_requires_a_non_empty_exact_identity() {
    let cards = [
        json!(null),
        json!({}),
        json!({"id": "", "slug": "   "}),
        json!({"slug": "gpt-future-dynamic-preview"}),
    ];

    assert!(
        project_codex_catalog_model_card(&cards, "gpt-future-dynamic", "future-model-alias")
            .is_none()
    );
    assert!(project_codex_catalog_model_card(
        &[json!({"slug": "gpt-future-dynamic"})],
        "",
        "future-model-alias"
    )
    .is_none());
    assert!(project_codex_catalog_model_card(
        &[json!({"slug": "gpt-future-dynamic"})],
        " gpt-future-dynamic ",
        "future-model-alias"
    )
    .is_none());
    assert!(project_codex_catalog_model_card(
        &[json!({"slug": "gpt-future-dynamic"})],
        "gpt-future-dynamic",
        "   "
    )
    .is_none());
}

#[test]
fn codex_responses_lite_header_is_omitted_for_server_side_compaction() {
    let mut headers = std::collections::BTreeMap::from([(
        CODEX_RESPONSES_LITE_HEADER.to_string(),
        "true".to_string(),
    )]);
    let body = json!({
        "model": "gpt-5.6-sol",
        "context_management": [{
            "type": "compaction",
            "compact_threshold": 128000
        }]
    });

    apply_codex_openai_responses_lite_header_for_request_body_with_capabilities(
        &mut headers,
        Some(&body),
        "codex",
        "openai:responses",
        "gpt-5.6-sol",
        "gpt-5.6-sol",
        None,
    );

    assert!(!headers.contains_key(CODEX_RESPONSES_LITE_HEADER));
}

#[test]
fn model_card_without_summary_parameter_support_keeps_reasoning_and_ids() {
    let metadata = build_codex_model_catalog_metadata(&[json!({
        "slug": "gpt-no-summary",
        "supports_reasoning_summary_parameter": false,
        "default_reasoning_level": "high",
        "default_reasoning_summary": "detailed",
        "supported_reasoning_levels": [{"effort": "high"}],
        "supports_parallel_tool_calls": true
    })]);
    let capabilities = resolve_codex_responses_model_capabilities(
        "gpt-no-summary",
        "gpt-no-summary",
        Some(&metadata),
    );
    assert!(!capabilities.supports_reasoning_summary_parameter);

    let mut body = json!({
        "model": "gpt-no-summary",
        "input": [{
            "id": "msg-client",
            "type": "message",
            "role": "user",
            "content": []
        }],
        "reasoning": {"effort": "high", "summary": "detailed"},
        "stream_options": {
            "reasoning_summary_delivery": "sequential_cutoff",
            "future_option": true
        }
    });
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        &mut body,
        "codex",
        "openai:responses",
        "gpt-no-summary",
        "gpt-no-summary",
        Some(&capabilities),
        None,
    );

    assert_eq!(body["reasoning"], json!({"effort": "high"}));
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert_eq!(body["stream_options"], json!({"future_option": true}));
    assert_eq!(body["input"][0]["id"], "msg-client");
}

#[test]
fn codex_request_normalizes_the_reasoning_envelope_without_model_defaults() {
    let capabilities =
        resolve_codex_responses_model_capabilities("gpt-future-agent", "gpt-future-agent", None);
    for initial_reasoning in [None, Some(Value::Null)] {
        let mut body = json!({
            "model": "gpt-future-agent",
            "input": [],
            "stream_options": {
                "reasoning_summary_delivery": "sequential_cutoff"
            }
        });
        if let Some(initial_reasoning) = initial_reasoning {
            body["reasoning"] = initial_reasoning;
        }

        apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
            &mut body,
            "codex",
            "openai:responses",
            "gpt-future-agent",
            "gpt-future-agent",
            Some(&capabilities),
            None,
        );

        assert_eq!(body["reasoning"], json!({}));
        assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
        assert!(body.get("stream_options").is_none());
    }

    let mut compact = json!({
        "model": "gpt-future-agent",
        "input": [],
        "reasoning": null,
        "include": ["reasoning.encrypted_content"],
        "store": true
    });
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        &mut compact,
        "codex",
        "openai:responses:compact",
        "gpt-future-agent",
        "gpt-future-agent",
        Some(&capabilities),
        None,
    );

    assert_eq!(compact["reasoning"], json!({}));
    assert!(compact.get("include").is_none());
    assert!(compact.get("store").is_none());
}

#[test]
fn empty_codex_model_catalog_clears_the_card_map() {
    assert_eq!(
        build_codex_model_catalog_metadata(&[])["codex_models"]["cards"],
        json!({})
    );
}

#[test]
fn special_body_edits_do_not_rewrite_explicit_ultra_effort() {
    for provider_api_format in ["openai:responses", "openai:responses:compact"] {
        let mut body = json!({
            "model": "gpt-5.6-luna",
            "input": [],
            "reasoning": {"effort": "ultra"}
        });
        apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
            &mut body,
            "codex",
            provider_api_format,
            "gpt-5.6-luna",
            "gpt-5.6-luna",
            None,
            None,
        );
        assert_eq!(body["reasoning"]["effort"], "ultra");
    }

    let mut custom = json!({
        "model": "gpt-5.6-luna",
        "input": [],
        "reasoning": {"effort": " ultra "}
    });
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        &mut custom,
        "codex",
        "openai:responses",
        "gpt-5.6-luna",
        "gpt-5.6-luna",
        None,
        None,
    );
    assert_eq!(custom["reasoning"]["effort"], " ultra ");
}

#[test]
fn populated_remote_catalog_is_authoritative_and_matches_model_variants() {
    let metadata = build_codex_model_catalog_metadata(&[json!({
        "slug": "gpt-future-agent",
        "supports_reasoning_summary_parameter": true,
        "default_reasoning_level": "high",
        "supported_reasoning_levels": [{"effort": "high"}],
        "supports_parallel_tool_calls": true
    })]);

    let variant = resolve_codex_responses_model_capabilities(
        "gpt-future-agent-2026-07-10",
        "gpt-future-agent-2026-07-10",
        Some(&metadata),
    );
    assert_eq!(variant.default_reasoning_effort.as_deref(), Some("high"));
    assert!(variant.supports_parallel_tool_calls);

    for model in [
        "gpt-future-agentpreview",
        "provider_1/gpt-future-agent-2026-07-10",
    ] {
        let capabilities =
            resolve_codex_responses_model_capabilities(model, model, Some(&metadata));
        assert_eq!(
            capabilities.default_reasoning_effort.as_deref(),
            Some("high"),
            "model: {model}"
        );
    }

    for model in [
        "GPT-FUTURE-AGENT",
        "gpt_future_agent",
        "org/team/gpt-future-agent",
        "provider!/gpt-future-agent",
    ] {
        let capabilities =
            resolve_codex_responses_model_capabilities(model, model, Some(&metadata));
        assert_eq!(
            capabilities.default_reasoning_effort, None,
            "model: {model}"
        );
    }

    let missing =
        resolve_codex_responses_model_capabilities("gpt-5.6-sol", "gpt-5.6-sol", Some(&metadata));
    assert!(!missing.use_responses_lite);
    assert!(missing.supports_reasoning_summary_parameter);
    assert!(!missing.supports_parallel_tool_calls);
    assert_eq!(missing.default_reasoning_effort, None);

    let mut body = json!({
        "model": "gpt-5.6-sol",
        "input": [],
        "reasoning": {"effort": "high"},
        "parallel_tool_calls": true,
        "service_tier": "priority",
        "text": {
            "verbosity": "high",
            "format": {"type": "json_schema"}
        }
    });
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        &mut body,
        "codex",
        "openai:responses",
        "gpt-5.6-sol",
        "gpt-5.6-sol",
        Some(&missing),
        None,
    );
    assert_eq!(body["reasoning"]["effort"], "high");
    assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
    assert_eq!(body["parallel_tool_calls"], false);
    assert_eq!(body["service_tier"], "priority");
    assert!(body["text"].get("verbosity").is_none());
    assert_eq!(body["text"]["format"]["type"], "json_schema");

    let mut compact = json!({
        "model": "gpt-5.6-sol",
        "input": [],
        "reasoning": {"effort": "high"}
    });
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        &mut compact,
        "codex",
        "openai:responses:compact",
        "gpt-5.6-sol",
        "gpt-5.6-sol",
        Some(&missing),
        None,
    );
    assert_eq!(compact["reasoning"]["effort"], "high");
}

#[test]
fn model_catalog_keys_and_hidden_card_overlays_use_exact_slugs() {
    let metadata = build_codex_model_catalog_metadata(&[
        json!({"slug": "gpt-card"}),
        json!({"slug": "GPT-CARD"}),
        json!({"slug": "gpt_card"}),
    ]);
    let cards = metadata["codex_models"]["cards"]
        .as_object()
        .expect("card map");
    assert_eq!(cards.len(), 3);
    assert!(cards.contains_key("gpt-card"));
    assert!(cards.contains_key("GPT-CARD"));
    assert!(cards.contains_key("gpt_card"));

    let effective = effective_codex_model_cards(&[json!({
        "slug": "GPT-5.6-SOL",
        "visibility": "hide"
    })]);
    assert!(effective.iter().any(|card| card["slug"] == "gpt-5.6-sol"));
    assert!(effective.iter().any(|card| card["slug"] == "GPT-5.6-SOL"));

    let effective = effective_codex_model_cards(&[json!({
        "id": "gpt-5.6-sol",
        "default_reasoning_level": "high",
        "future_capability": {"mode": "native"}
    })]);
    let sol = effective
        .iter()
        .find(|card| card["slug"] == "gpt-5.6-sol")
        .expect("merged Sol card");
    assert_eq!(sol["default_reasoning_level"], "high");
    assert_eq!(sol["future_capability"]["mode"], "native");
    assert_eq!(sol["use_responses_lite"], true);
    assert_eq!(sol["default_reasoning_summary"], "none");
    assert!(sol["supported_reasoning_levels"]
        .as_array()
        .is_some_and(|levels| levels.iter().any(|level| level["effort"] == "ultra")));
}

#[test]
fn bare_gpt_5_6_requires_an_explicit_model_card() {
    let conservative = resolve_codex_responses_model_capabilities("gpt-5.6", "gpt-5.6", None);
    assert!(!conservative.use_responses_lite);
    assert!(conservative.supports_reasoning_summary_parameter);
    assert!(!conservative.supports_parallel_tool_calls);
    assert_eq!(conservative.default_reasoning_effort, None);
    assert_eq!(conservative.default_verbosity, None);
    assert!(conservative.supported_service_tiers.is_empty());

    let metadata = build_codex_model_catalog_metadata(&[json!({
        "slug": "gpt-5.6",
        "use_responses_lite": true,
        "supports_reasoning_summary_parameter": true,
        "default_reasoning_level": "medium",
        "supported_reasoning_levels": [{"effort": "medium"}],
        "supports_parallel_tool_calls": true,
        "support_verbosity": true,
        "default_verbosity": "low",
        "service_tiers": [{"id": "priority"}]
    })]);
    let explicit =
        resolve_codex_responses_model_capabilities("gpt-5.6", "gpt-5.6", Some(&metadata));
    assert!(explicit.use_responses_lite);
    assert_eq!(explicit.default_reasoning_effort.as_deref(), Some("medium"));
    assert_eq!(explicit.default_verbosity.as_deref(), Some("low"));
    assert_eq!(explicit.supported_service_tiers, vec!["priority"]);

    let opaque =
        resolve_codex_responses_model_capabilities("deployment-production", "gpt-5.6-sol", None);
    assert!(opaque.use_responses_lite);
    assert_eq!(opaque.default_reasoning_effort.as_deref(), Some("low"));

    for provider_model in ["GPT-5.6-SOL", "gpt_5.6_sol", "org/team/gpt-5.6-sol"] {
        let capabilities =
            resolve_codex_responses_model_capabilities(provider_model, "gpt-5.6-sol", None);
        assert!(!capabilities.use_responses_lite, "model: {provider_model}");
    }
}

#[test]
fn bundled_gpt_5_4_mini_supports_official_reasoning_efforts() {
    let capabilities =
        resolve_codex_responses_model_capabilities("gpt-5.4-mini", "gpt-5.4-mini", None);

    assert_eq!(
        capabilities.default_reasoning_effort.as_deref(),
        Some("none")
    );
    assert_eq!(
        capabilities.supported_reasoning_efforts,
        vec!["none", "low", "medium", "high", "xhigh"]
    );
    assert!(capabilities.supports_reasoning_effort("none"));

    let metadata = build_codex_model_catalog_metadata(&[json!({
        "slug": "gpt-5.4-mini",
        "default_reasoning_level": "medium",
        "supported_reasoning_levels": [
            {"effort": "low"},
            {"effort": "medium"},
            {"effort": "high"},
            {"effort": "xhigh"}
        ]
    })]);
    let cached_capabilities =
        resolve_codex_responses_model_capabilities("gpt-5.4-mini", "gpt-5.4-mini", Some(&metadata));
    assert_eq!(
        cached_capabilities.default_reasoning_effort.as_deref(),
        Some("none")
    );
    assert_eq!(
        cached_capabilities.supported_reasoning_efforts,
        vec!["none", "low", "medium", "high", "xhigh"]
    );
}

#[test]
fn model_card_preserves_custom_reasoning_effort_values() {
    let metadata = build_codex_model_catalog_metadata(&[json!({
        "slug": "codex-custom",
        "supports_reasoning_summary_parameter": true,
        "default_reasoning_level": "VendorEffortX",
        "supported_reasoning_levels": [
            {"effort": "VendorEffortX"},
            {"effort": "MAX"}
        ]
    })]);
    let capabilities =
        resolve_codex_responses_model_capabilities("codex-custom", "codex-custom", Some(&metadata));
    assert_eq!(
        capabilities.default_reasoning_effort.as_deref(),
        Some("VendorEffortX")
    );
    assert_eq!(
        capabilities.supported_reasoning_efforts,
        vec!["VendorEffortX", "MAX"]
    );
    assert!(capabilities.supports_reasoning_effort("VendorEffortX"));
    assert!(!capabilities.supports_reasoning_effort("vendoreffortx"));
    assert!(!capabilities.supports_reasoning_effort("max"));
}

#[test]
fn bundled_auto_review_card_matches_the_codex_request_profile() {
    let card = bundled_codex_model_cards()
        .iter()
        .find(|card| card["slug"] == "codex-auto-review")
        .expect("Codex auto review card");
    assert_eq!(card["visibility"], "hide");
    assert_eq!(card["supported_in_api"], true);
    assert_eq!(card["priority"], 43);

    let capabilities =
        resolve_codex_responses_model_capabilities("codex-auto-review", "codex-auto-review", None);
    assert!(capabilities.use_responses_lite);
    assert_eq!(
        capabilities.default_reasoning_effort.as_deref(),
        Some("medium")
    );
    assert_eq!(capabilities.default_reasoning_summary, None);
    assert!(capabilities.supports_parallel_tool_calls);
    assert_eq!(capabilities.default_verbosity.as_deref(), Some("low"));
    assert_eq!(capabilities.supported_service_tiers, vec!["priority"]);
}

#[test]
fn codex_auth_identity_parses_member_claims_and_persisted_fingerprint() {
    let identity = parse_codex_auth_identity(Some(
        &json!({
            "provider_type": "codex",
            "accountId": "workspace-1",
            "codexIdentityFingerprint": "codex-persisted-fingerprint:v1:stable",
            "https://api.openai.com/auth": {
                "chatgpt_account_user_id": "workspace-member-1",
                "chatgpt_user_id": "user-1"
            },
            "https://api.openai.com/profile": {
                "email": "Alice@Example.com"
            }
        })
        .to_string(),
    ));

    assert_eq!(identity.account_id.as_deref(), Some("workspace-1"));
    assert_eq!(
        identity.account_user_id.as_deref(),
        Some("workspace-member-1")
    );
    assert_eq!(identity.user_id.as_deref(), Some("user-1"));
    assert_eq!(identity.email.as_deref(), Some("Alice@Example.com"));
    assert_eq!(
        identity.codex_identity_fingerprint.as_deref(),
        Some("codex-persisted-fingerprint:v1:stable")
    );
    assert!(identity.uses_codex_backend);

    let sub_fallback = parse_codex_auth_identity(Some(r#"{"sub":"legacy-user-1"}"#));
    assert_eq!(sub_fallback.user_id.as_deref(), Some("legacy-user-1"));
}

#[test]
fn codex_identity_headers_are_derived_only_from_auth_config() {
    let mut headers = std::collections::BTreeMap::from([
        ("chatgpt-account-id".to_string(), "spoofed".to_string()),
        ("x-openai-fedramp".to_string(), "true".to_string()),
    ]);

    apply_codex_openai_special_headers(
        &mut headers,
        &json!({"model": "gpt-5.6-sol", "input": []}),
        &http::HeaderMap::new(),
        "codex",
        "openai:responses",
        Some("request-1"),
        Some(r#"{"is_fedramp":false}"#),
    );

    assert!(!headers.contains_key("chatgpt-account-id"));
    assert!(!headers.contains_key("x-openai-fedramp"));
    assert_eq!(
        headers
            .get("x-openai-internal-codex-responses-lite")
            .map(String::as_str),
        Some("true")
    );
}

#[test]
fn codex_identity_headers_support_nested_agent_identity_export() {
    let mut headers = std::collections::BTreeMap::new();

    apply_codex_openai_special_headers(
        &mut headers,
        &json!({"model": "gpt-5.6-sol", "input": []}),
        &http::HeaderMap::new(),
        "codex",
        "openai:responses",
        Some("request-agent-identity"),
        Some(
            r#"{
                "auth_mode": "agentIdentity",
                "agent_identity": {
                    "accountId": "account-agent",
                    "chatgptAccountIsFedramp": true
                }
            }"#,
        ),
    );

    assert_eq!(
        headers.get("chatgpt-account-id").map(String::as_str),
        Some("account-agent")
    );
    assert_eq!(
        headers.get("x-openai-fedramp").map(String::as_str),
        Some("true")
    );
}

#[test]
fn codex_search_uses_identity_headers_without_responses_protocol_headers() {
    let mut headers = std::collections::BTreeMap::from([
        (
            "x-openai-internal-codex-responses-lite".to_string(),
            "true".to_string(),
        ),
        ("openai-beta".to_string(), "responses=v1".to_string()),
        (
            "accept".to_string(),
            "application/json, Text/Event-Stream; q=0.9".to_string(),
        ),
    ]);

    apply_codex_openai_special_headers(
        &mut headers,
        &json!({"id": "session-1", "model": "gpt-5.6-luna"}),
        &http::HeaderMap::new(),
        "codex",
        "openai:search",
        Some("request-search"),
        Some(r#"{"account_id":"account-1","is_fedramp":true}"#),
    );

    assert_eq!(
        headers.get("chatgpt-account-id").map(String::as_str),
        Some("account-1")
    );
    assert_eq!(
        headers.get("x-openai-fedramp").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        headers.get("user-agent").map(String::as_str),
        Some(crate::codex_client_user_agent().as_str())
    );
    assert_eq!(
        headers.get("originator").map(String::as_str),
        Some(crate::codex_client_originator().as_str())
    );
    assert!(!headers.contains_key(CODEX_RESPONSES_LITE_HEADER));
    assert!(!headers.contains_key("openai-beta"));
    assert!(!headers.contains_key("accept"));
}

#[test]
fn codex_live_uses_account_identity_without_responses_lite_headers() {
    let mut headers = std::collections::BTreeMap::from([(
        CODEX_RESPONSES_LITE_HEADER.to_string(),
        "true".to_string(),
    )]);

    apply_codex_openai_special_headers(
        &mut headers,
        &json!({"model": "gpt-live"}),
        &http::HeaderMap::new(),
        "codex",
        "codex:live",
        Some("request-live"),
        Some(r#"{"account_id":"account-live","is_fedramp":true}"#),
    );

    assert_eq!(
        headers.get("chatgpt-account-id").map(String::as_str),
        Some("account-live")
    );
    assert_eq!(
        headers.get("x-openai-fedramp").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        headers.get("user-agent").map(String::as_str),
        Some(crate::codex_client_user_agent().as_str())
    );
    assert_eq!(
        headers.get("originator").map(String::as_str),
        Some(crate::codex_client_originator().as_str())
    );
    assert!(!headers.contains_key(CODEX_RESPONSES_LITE_HEADER));
}

#[test]
fn standard_codex_models_do_not_send_the_responses_lite_header() {
    let mut headers = std::collections::BTreeMap::from([(
        "x-openai-internal-codex-responses-lite".to_string(),
        "true".to_string(),
    )]);

    apply_codex_openai_special_headers(
        &mut headers,
        &json!({"model": "gpt-5.4", "input": []}),
        &http::HeaderMap::new(),
        "codex",
        "openai:responses",
        Some("request-1"),
        None,
    );

    assert!(!headers.contains_key("x-openai-internal-codex-responses-lite"));
}

#[test]
fn codex_compact_headers_preserve_session_identity_and_remove_stream_headers() {
    let mut headers = std::collections::BTreeMap::from([
        ("session-id".to_string(), "session-1".to_string()),
        ("thread-id".to_string(), "thread-1".to_string()),
        ("x-client-request-id".to_string(), "thread-1".to_string()),
        ("accept".to_string(), "text/event-stream".to_string()),
    ]);

    apply_codex_openai_special_headers(
        &mut headers,
        &json!({"model": "gpt-5.6", "input": []}),
        &http::HeaderMap::new(),
        "codex",
        "openai:responses:compact",
        Some("request-1"),
        None,
    );

    assert_eq!(
        headers.get("session-id").map(String::as_str),
        Some("session-1")
    );
    assert_eq!(
        headers.get("thread-id").map(String::as_str),
        Some("thread-1")
    );
    assert!(!headers.contains_key("x-client-request-id"));
    assert!(!headers.contains_key("accept"));
}

#[test]
fn codex_responses_body_edits_inject_passthrough_fields_without_reasoning_summary() {
    let mut provider_request_body = json!( {
        "input": [{
            "role": "user",
            "content": "hello"
        }],
        "model": "gpt-5.4",
        "stream": true
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(provider_request_body["reasoning"]["effort"], "medium");
    assert!(provider_request_body["reasoning"].get("summary").is_none());
    assert_eq!(
        provider_request_body["include"],
        json!(["reasoning.encrypted_content"])
    );
    assert_eq!(provider_request_body["parallel_tool_calls"], json!(true));
    assert_eq!(provider_request_body["tool_choice"], json!("auto"));
    assert!(provider_request_body.get("instructions").is_none());
}

#[test]
fn codex_responses_body_edits_omit_disabled_reasoning_summary_and_delivery() {
    let mut provider_request_body = json!({
        "input": [{"role": "user", "content": "hello"}],
        "model": "gpt-5.6-sol",
        "stream": true,
        "reasoning": {"effort": "high", "summary": "none"},
        "stream_options": {
            "reasoning_summary_delivery": "sequential_cutoff",
            "future_option": true
        }
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert!(provider_request_body["reasoning"].get("summary").is_none());
    assert_eq!(
        provider_request_body["stream_options"],
        json!({"future_option": true})
    );
}

#[test]
fn codex_responses_body_edits_project_include_and_preserve_disabled_parallel_calls() {
    let mut provider_request_body = json!( {
        "input": [],
        "model": "gpt-5.4",
        "include": [
            "file_search_call.results",
            "web_search_call.results",
            "web_search_call.action.sources",
            "message.input_image.image_url",
            "computer_call_output.output.image_url",
            "code_interpreter_call.outputs",
            "message.output_text.logprobs"
        ],
        "reasoning": {"effort": "high", "summary": "detailed"},
        "parallel_tool_calls": false
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(provider_request_body["reasoning"]["effort"], json!("high"));
    assert_eq!(
        provider_request_body["reasoning"]["summary"],
        json!("detailed")
    );
    assert_eq!(
        provider_request_body["include"],
        json!(["reasoning.encrypted_content"])
    );
    assert_eq!(provider_request_body["parallel_tool_calls"], json!(false));
}

#[test]
fn codex_responses_body_edits_wrap_string_input_for_backend() {
    let mut provider_request_body = json!({
        "input": "hello",
        "model": "gpt-5.4"
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(
        provider_request_body["input"],
        json!([{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "hello"
            }]
        }])
    );
}

#[test]
fn codex_responses_body_edits_preserve_explicit_system_messages() {
    let explicit_system_message = json!({
        "type": "message",
        "role": "system",
        "content": [{
            "type": "input_text",
            "text": "Keep this caller-supplied role."
        }]
    });

    let mut standard_body = json!({
        "model": "gpt-5.4",
        "instructions": "Standard base instructions.",
        "input": [explicit_system_message.clone()]
    });
    apply_codex_openai_responses_special_body_edits(
        &mut standard_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(standard_body["instructions"], "Standard base instructions.");
    assert_eq!(standard_body["input"], json!([explicit_system_message]));

    let mut lite_body = json!({
        "model": "gpt-5.6-sol",
        "instructions": "Lite base instructions.",
        "input": [explicit_system_message.clone()]
    });
    apply_codex_openai_responses_special_body_edits(
        &mut lite_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert!(lite_body.get("instructions").is_none());
    assert_eq!(lite_body["input"][0]["type"], "additional_tools");
    assert_eq!(lite_body["input"][1]["role"], "developer");
    assert_eq!(
        lite_body["input"][1]["content"][0]["text"],
        "Lite base instructions."
    );
    assert_eq!(lite_body["input"][2], explicit_system_message);
}

#[test]
fn codex_responses_body_edits_preserve_function_tools_for_codex_backend() {
    let mut provider_request_body = json!({
        "input": [],
        "model": "gpt-5.4",
        "tools": [{
            "type": "function",
            "name": "lookup_account",
            "description": "Lookup an account by id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "account_id": {
                        "type": "string"
                    }
                },
                "required": ["account_id"],
                "additionalProperties": false
            },
            "strict": true
        }],
        "tool_choice": {
            "type": "function",
            "name": "lookup_account"
        }
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(
        provider_request_body["tools"][0]["name"],
        json!("lookup_account")
    );
    assert_eq!(
        provider_request_body["tools"][0]["parameters"]["properties"]["account_id"]["type"],
        json!("string")
    );
    assert_eq!(
        provider_request_body["tool_choice"]["name"],
        json!("lookup_account")
    );
    assert!(provider_request_body["tools"][0].get("function").is_none());
}

#[test]
fn codex_responses_body_edits_apply_backend_request_contract() {
    let mut provider_request_body = json!({
        "input": [{
            "id": "msg-1",
            "type": "message",
            "role": "user",
            "content": [{"id": "content-1", "type": "input_text", "text": "hello"}]
        }],
        "model": "gpt-5.4",
        "max_output_tokens": 1024,
        "max_completion_tokens": 1024,
        "temperature": 0.2,
        "top_p": 0.8,
        "frequency_penalty": 0.1,
        "presence_penalty": 0.1,
        "user": "user-123",
        "metadata": {"client": "cursor"},
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"},
        "prompt_cache_retention": "24h",
        "safety_identifier": "safe-user-123",
        "stream_options": {"reasoning_summary_delivery": "sequential_cutoff"},
        "previous_response_id": "resp_123"
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    for field in CODEX_OPENAI_RESPONSES_UNSUPPORTED_BODY_FIELDS {
        assert!(
            provider_request_body.get(*field).is_none(),
            "{field} must be stripped"
        );
    }
    assert!(provider_request_body.get("stream_options").is_none());
    assert_eq!(provider_request_body["input"][0]["id"], "msg-1");
    assert_eq!(
        provider_request_body["input"][0]["content"][0]["id"],
        json!("content-1")
    );
}

#[test]
fn codex_responses_body_edits_preserve_client_selected_input_item_ids() {
    let body_rules = json!([{"action":"set","path":"store","value":true}]);
    let mut provider_request_body = json!({
        "model": "gpt-5.4",
        "store": true,
        "input": [{
            "id": "msg-1",
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        Some(&body_rules),
        None,
    );

    assert_eq!(provider_request_body["store"], true);
    assert_eq!(provider_request_body["input"][0]["id"], "msg-1");
}

#[test]
fn codex_responses_lite_additional_tools_contain_only_client_executed_specs() {
    let mut provider_request_body = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "Use a tool"}]
        }],
        "tools": [
            {"type": "function", "name": "lookup", "parameters": {}},
            {"type": "custom", "name": "apply_patch", "description": "Apply a patch", "format": {}},
            {"type": "namespace", "name": "web", "description": "Web tools", "tools": []},
            {"type": "web_search"},
            {"type": "image_generation"},
            {"type": "tool_search", "execution": "client"},
            {"type": "tool_search", "execution": "server"},
            {"type": "tool_search"},
            {"type": "future_tool"},
            {"name": "missing_type"}
        ]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert!(provider_request_body.get("tools").is_none());
    assert_eq!(
        provider_request_body["input"][0]["tools"],
        json!([
            {"type": "function", "name": "lookup", "parameters": {}},
            {"type": "custom", "name": "apply_patch", "description": "Apply a patch", "format": {}},
            {"type": "namespace", "name": "web", "description": "Web tools", "tools": []},
            {"type": "tool_search", "execution": "client"}
        ])
    );

    let mut existing_additional_tools = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "additional_tools",
            "role": "developer",
            "tools": [
                {"type": "function", "name": "shell", "parameters": {}},
                {"type": "web_search"},
                {"type": "image_generation"}
            ]
        }]
    });
    apply_codex_openai_responses_special_body_edits(
        &mut existing_additional_tools,
        "codex",
        "openai:responses:compact",
        None,
        None,
    );
    assert_eq!(
        existing_additional_tools["input"][0]["tools"],
        json!([{"type": "function", "name": "shell", "parameters": {}}])
    );
}

#[test]
fn codex_responses_lite_websocket_continuations_do_not_reappend_static_config() {
    let incremental_input = json!([
        {
            "type": "reasoning",
            "id": "rs_provider_123",
            "content": [{
                "type": "reasoning_text",
                "text": "Pass the provider reasoning state through unchanged."
            }],
            "encrypted_content": "opaque-provider-state"
        },
        {
            "type": "function_call_output",
            "call_id": "call_123",
            "output": "small result"
        }
    ]);
    let mut provider_request_body = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "previous_response_id": "resp_123",
        "instructions": "Static developer instructions.",
        "tools": [{
            "type": "function",
            "name": "lookup",
            "parameters": {"type": "object"}
        }],
        "input": incremental_input.clone()
    });

    apply_codex_openai_responses_websocket_continuation_body_edits_with_source_model_and_capabilities(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        "gpt-5.6-sol",
        "gpt-5.6-sol",
        None,
        None,
    );

    assert!(provider_request_body.get("previous_response_id").is_none());
    assert!(provider_request_body.get("instructions").is_none());
    assert!(provider_request_body.get("tools").is_none());
    assert_eq!(provider_request_body["input"], incremental_input);
    assert_eq!(provider_request_body["parallel_tool_calls"], false);

    let once = provider_request_body.clone();
    apply_codex_openai_responses_websocket_continuation_body_edits_with_source_model_and_capabilities(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        "gpt-5.6-sol",
        "gpt-5.6-sol",
        None,
        None,
    );
    assert_eq!(provider_request_body, once);
}

#[test]
fn codex_responses_lite_websocket_continuation_removes_only_the_static_prefix() {
    let later_developer_message = json!({
        "type": "message",
        "role": "developer",
        "content": [{"type": "input_text", "text": "genuine incremental input"}]
    });
    let mut provider_request_body = json!({
        "model": "gpt-5.6-sol",
        "input": [
            {
                "type": "additional_tools",
                "role": "developer",
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}]
            },
            {
                "type": "message",
                "role": "developer",
                "content": [{"type": "input_text", "text": "static instructions"}]
            },
            {"type": "function_call_output", "call_id": "call_1", "output": "ok"},
            later_developer_message.clone()
        ]
    });

    apply_codex_openai_responses_websocket_continuation_body_edits_with_source_model_and_capabilities(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        "gpt-5.6-sol",
        "gpt-5.6-sol",
        None,
        None,
    );

    let input = provider_request_body["input"]
        .as_array()
        .expect("continuation input");
    assert_eq!(input.len(), 2);
    assert_eq!(input[0]["type"], "function_call_output");
    assert_eq!(input[1], later_developer_message);
}

#[test]
fn codex_responses_lite_http_body_cannot_opt_into_websocket_continuation_edits() {
    let mut provider_request_body = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "previous_response_id": "resp_123",
        "instructions": "Static developer instructions.",
        "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
        "input": [{"type": "message", "role": "user", "content": []}]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert!(provider_request_body.get("previous_response_id").is_none());
    assert_eq!(
        provider_request_body["input"][0]["type"],
        "additional_tools"
    );
    assert_eq!(provider_request_body["input"][1]["role"], "developer");
}

#[test]
fn codex_responses_lite_null_previous_id_is_an_independent_turn() {
    let mut provider_request_body = json!({
        "type": "response.create",
        "model": "gpt-5.6-sol",
        "previous_response_id": null,
        "instructions": "Fresh instructions.",
        "tools": [{"type": "function", "name": "lookup", "parameters": {}}],
        "input": [{"type": "message", "role": "user", "content": []}]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert!(provider_request_body.get("previous_response_id").is_none());
    assert_eq!(
        provider_request_body["input"][0]["type"],
        "additional_tools"
    );
    assert_eq!(provider_request_body["input"][1]["role"], "developer");
    assert_eq!(
        provider_request_body["input"][1]["content"][0]["text"],
        "Fresh instructions."
    );
}

#[test]
fn codex_responses_body_edits_strip_name_from_hosted_web_search_tool() {
    let mut provider_request_body = json!({
        "input": [],
        "model": "gpt-5.4",
        "tools": [{
            "type": "web_search",
            "name": "web_search"
        }],
        "tool_choice": {
            "type": "web_search",
            "name": "web_search"
        }
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert!(provider_request_body["tools"][0].get("name").is_none());
    assert!(provider_request_body["tool_choice"].get("name").is_none());
    assert_eq!(
        provider_request_body["tool_choice"]["type"],
        json!("web_search")
    );
}

#[test]
fn codex_responses_body_edits_do_not_derive_prompt_cache_key_from_metadata() {
    let mut body = json!({
        "input": [{"role": "user", "content": "hello"}],
        "model": "gpt-5.4",
        "metadata": {
            "user_id": "{\"session_id\":\"session-a\",\"device_id\":\"device-a\"}"
        }
    });

    apply_codex_openai_responses_special_body_edits(
        &mut body,
        "codex",
        "openai:responses",
        None,
        Some("key-123"),
    );

    assert!(body.get("prompt_cache_key").is_none());
    assert!(body.get("metadata").is_none());
}

#[test]
fn codex_responses_body_edits_do_not_treat_client_metadata_as_a_cache_key_source() {
    let mut body = json!({
        "model": "gpt-5.6-sol",
        "input": [],
        "client_metadata": {
            "session_id": "session-123",
            "thread_id": "thread-123"
        }
    });

    apply_codex_openai_responses_special_body_edits(
        &mut body,
        "codex",
        "openai:responses",
        None,
        Some("key-123"),
    );

    assert!(body.get("prompt_cache_key").is_none());
    assert_eq!(body["client_metadata"]["thread_id"], "thread-123");
}

#[test]
fn codex_responses_body_edits_strip_cache_control_without_deriving_a_cache_key() {
    let mut body = json!({
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "stable project brief",
                "cache_control": {"type": "ephemeral"}
            }]
        }, {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "new turn A"}]
        }],
        "model": "gpt-5.4"
    });

    apply_codex_openai_responses_special_body_edits(
        &mut body,
        "codex",
        "openai:responses",
        None,
        Some("key-a"),
    );

    assert!(body.get("prompt_cache_key").is_none());
    assert!(!body.to_string().contains("\"cache_control\""));
}

#[test]
fn codex_responses_body_edits_strip_developer_cache_control_before_upstream() {
    let mut provider_request_body = json!({
        "input": [{
            "type": "message",
            "role": "developer",
            "content": [{
                "type": "input_text",
                "text": "stable system brief",
                "cache_control": {"type": "ephemeral"}
            }]
        }, {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "new turn"}]
        }],
        "model": "gpt-5.4"
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        Some("key-a"),
    );

    assert!(provider_request_body.get("prompt_cache_key").is_none());
    assert!(!provider_request_body
        .to_string()
        .contains("\"cache_control\""));
    assert_eq!(
        provider_request_body["input"][0]["content"][0]["text"],
        json!("stable system brief")
    );
}

#[test]
fn codex_responses_body_edits_preserve_cache_control_named_tool_schema_property() {
    let mut provider_request_body = json!({
        "model": "gpt-5.4",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "inspect the payload",
                "cache_control": {"type": "ephemeral"}
            }]
        }],
        "tools": [{
            "type": "function",
            "name": "inspect",
            "cache_control": {"type": "ephemeral"},
            "parameters": {
                "type": "object",
                "properties": {
                    "cache_control": {"type": "string"}
                }
            }
        }]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        Some("key-a"),
    );

    assert!(provider_request_body["input"][0]["content"][0]
        .get("cache_control")
        .is_none());
    assert!(provider_request_body["tools"][0]
        .get("cache_control")
        .is_none());
    assert_eq!(
        provider_request_body["tools"][0]["parameters"]["properties"]["cache_control"]["type"],
        "string"
    );
}

#[test]
fn codex_responses_lite_compact_preserves_cache_control_named_tool_schema_property() {
    let mut provider_request_body = json!({
        "model": "gpt-5.6-sol",
        "instructions": "Use the inspection tool.",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": "inspect the payload",
                "cache_control": {"type": "ephemeral"}
            }]
        }],
        "tools": [{
            "type": "function",
            "name": "inspect",
            "cache_control": {"type": "ephemeral"},
            "parameters": {
                "type": "object",
                "properties": {
                    "cache_control": {"type": "string"}
                }
            }
        }]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses:compact",
        None,
        Some("key-a"),
    );

    assert!(provider_request_body.get("tools").is_none());
    assert_eq!(
        provider_request_body["input"][0]["type"],
        "additional_tools"
    );
    assert!(provider_request_body["input"][0]["tools"][0]
        .get("cache_control")
        .is_none());
    assert_eq!(
        provider_request_body["input"][0]["tools"][0]["parameters"]["properties"]["cache_control"]
            ["type"],
        "string"
    );
    assert!(provider_request_body["input"][2]["content"][0]
        .get("cache_control")
        .is_none());
}

#[test]
fn codex_responses_body_edits_do_not_synthesize_a_cache_key_from_request_content() {
    let mut body = json!({
        "input": [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "open workspace"}]
        }, {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "new turn A"}]
        }],
        "model": "gpt-5.4",
        "instructions": "Be concise.",
        "tools": [{
            "type": "function",
            "name": "shell",
            "parameters": {"type": "object", "properties": {}}
        }],
        "reasoning": {"effort": "medium"}
    });
    apply_codex_openai_responses_special_body_edits(
        &mut body,
        "codex",
        "openai:responses",
        None,
        Some("key-a"),
    );

    assert!(body.get("prompt_cache_key").is_none());
}

#[test]
fn compact_body_edits_apply_the_codex_request_projection() {
    let mut provider_request_body = json!({
        "input": [],
        "model": "gpt-5.4",
        "client_metadata": {"origin": "codex"},
        "include": ["reasoning.encrypted_content"],
        "store": true,
        "stream": true,
        "stream_options": {"reasoning_summary_delivery": "sequential_cutoff"},
        "tool_choice": "auto",
        "parallel_tool_calls": true,
        "reasoning": {"effort": "high"},
        "text": {"verbosity": "medium"},
        "tools": [{"type": "function", "name": "lookup"}],
        "prompt_cache_key": "session:compact",
    });

    apply_openai_responses_compact_special_body_edits(
        &mut provider_request_body,
        "openai:responses:compact",
    );

    for field in [
        "client_metadata",
        "include",
        "store",
        "stream",
        "stream_options",
        "tool_choice",
    ] {
        assert!(provider_request_body.get(field).is_none());
    }
    assert_eq!(provider_request_body["model"], json!("gpt-5.4"));
    assert_eq!(provider_request_body["input"], json!([]));
    assert_eq!(provider_request_body["parallel_tool_calls"], json!(true));
    assert_eq!(provider_request_body["reasoning"]["effort"], json!("high"));
    assert_eq!(provider_request_body["text"]["verbosity"], json!("medium"));
    assert_eq!(provider_request_body["tools"][0]["name"], json!("lookup"));
    assert_eq!(
        provider_request_body["prompt_cache_key"],
        json!("session:compact")
    );
}

#[test]
fn codex_responses_body_edits_omit_empty_instructions() {
    for instructions in [None, Some(Value::Null), Some(json!(""))] {
        for api_format in ["openai:responses", "openai:responses:compact"] {
            let mut provider_request_body = json!({
                "model": "gpt-5.6-sol",
                "input": []
            });
            if let Some(instructions) = instructions.clone() {
                provider_request_body["instructions"] = instructions;
            }

            apply_codex_openai_responses_special_body_edits(
                &mut provider_request_body,
                "codex",
                api_format,
                None,
                None,
            );

            assert!(provider_request_body.get("instructions").is_none());
        }
    }

    let mut whitespace = json!({
        "model": "gpt-5.6-sol",
        "input": [],
        "instructions": " "
    });
    apply_codex_openai_responses_special_body_edits(
        &mut whitespace,
        "codex",
        "openai:responses",
        None,
        None,
    );
    assert!(whitespace.get("instructions").is_none());
    assert_eq!(whitespace["input"][0]["type"], "additional_tools");
    assert_eq!(whitespace["input"][1]["role"], "developer");
    assert_eq!(whitespace["input"][1]["content"][0]["text"], " ");
}

#[test]
fn codex_compact_body_matches_the_typed_client_payload() {
    let mut provider_request_body = json!({
        "model": "gpt-5.6-sol",
        "input": [
            {
                "id": "msg-1",
                "type": "message",
                "role": "user",
                "content": [{
                    "id": "content-id",
                    "type": "input_text",
                    "text": "hello",
                    "cache_control": {"type": "ephemeral"}
                }]
            },
            {"id": "call-1", "type": "function_call", "name": "lookup"},
            {"id": "future-1", "type": "future_item", "value": true}
        ],
        "top_logprobs": 5,
        "max_output_tokens": 100,
        "previous_response_id": "resp_123",
        "prompt_cache_options": {"ttl": "30m"},
        "custom_extension": true,
        "service_tier": "priority",
        "text": {"verbosity": "medium"}
    });

    apply_codex_openai_responses_compact_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses:compact",
    );

    assert_eq!(
        provider_request_body,
        json!({
            "model": "gpt-5.6-sol",
            "input": [
                {
                    "id": "msg-1",
                    "type": "message",
                    "role": "user",
                    "content": [{
                        "id": "content-id",
                        "type": "input_text",
                        "text": "hello"
                    }]
                },
                {"id": "call-1", "type": "function_call", "name": "lookup"},
                {"id": "future-1", "type": "future_item", "value": true}
            ],
            "parallel_tool_calls": true,
            "service_tier": "priority",
            "text": {"verbosity": "medium"}
        })
    );
    validate_codex_openai_responses_compact_request_contract(
        &provider_request_body,
        "codex",
        "openai:responses:compact",
    )
    .expect("projected Compact request should match the typed contract");
}

#[test]
fn codex_compact_ignores_image_generation_tool_choice_before_projection() {
    let base = json!({
        "model": "gpt-5.4",
        "input": [{"role": "user", "content": "generate image"}],
        "tools": [{"type": "image_generation"}]
    });
    let mut without_tool_choice = base.clone();
    let mut with_tool_choice = base;
    with_tool_choice["tool_choice"] = json!({"type": "image_generation"});

    for body in [&mut without_tool_choice, &mut with_tool_choice] {
        apply_codex_openai_responses_special_body_edits(
            body,
            "codex",
            "openai:responses:compact",
            None,
            None,
        );
    }

    assert_eq!(with_tool_choice, without_tool_choice);
    assert_eq!(with_tool_choice["model"], json!("gpt-5.4"));
    assert!(with_tool_choice.get("tool_choice").is_none());
}

#[test]
fn codex_compact_contract_rejects_invalid_routing_mutations() {
    for (field, value) in [
        ("input", json!("hello")),
        ("parallel_tool_calls", json!("true")),
        ("reasoning", json!("high")),
        ("text", json!("medium")),
    ] {
        let mut body = json!({
            "model": "gpt-5.6-sol",
            "input": [],
            "parallel_tool_calls": true
        });
        body[field] = value;
        let violation = validate_codex_openai_responses_compact_request_contract(
            &body,
            "codex",
            "openai:responses:compact",
        )
        .expect_err("invalid Compact field type should be rejected");
        assert_eq!(violation.field, field);
    }
}

#[test]
fn codex_chat_body_edits_apply_model_reasoning_defaults() {
    for (model, effort, summary) in [
        ("gpt-5.6-sol", "low", None),
        ("gpt-5.6-terra", "medium", None),
        ("gpt-5.6-luna", "medium", None),
        ("gpt-5.4", "medium", None),
        ("gpt-5.2", "medium", Some("auto")),
    ] {
        let mut provider_request_body = json!({
            "input": [],
            "model": model
        });

        apply_codex_openai_responses_chat_body_edits(
            &mut provider_request_body,
            "codex",
            "openai:responses",
            None,
            None,
        );

        assert_eq!(provider_request_body["reasoning"]["effort"], effort);
        assert_eq!(
            provider_request_body["reasoning"]
                .get("summary")
                .and_then(Value::as_str),
            summary
        );
        assert_eq!(
            provider_request_body["include"],
            json!(["reasoning.encrypted_content"])
        );
        let uses_responses_lite = model.starts_with("gpt-5.6");
        assert_eq!(
            provider_request_body["parallel_tool_calls"],
            json!(!uses_responses_lite)
        );
        assert_eq!(
            provider_request_body["reasoning"]
                .get("context")
                .and_then(Value::as_str),
            uses_responses_lite.then_some("all_turns")
        );
    }
}

#[test]
fn codex_chat_body_edits_preserve_existing_reasoning_effort() {
    let mut provider_request_body = json!({
        "input": [],
        "model": "gpt-5.4",
        "reasoning": {"effort": "low"}
    });

    apply_codex_openai_responses_chat_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(provider_request_body["reasoning"]["effort"], json!("low"));
    assert!(provider_request_body["reasoning"].get("summary").is_none());
}

#[test]
fn codex_gpt_5_6_public_reasoning_modes_default_to_medium_effort() {
    for mode in ["standard", "pro"] {
        let mut provider_request_body = json!({
            "input": [],
            "model": "gpt-5.6-sol",
            "reasoning": {"mode": mode}
        });

        apply_codex_openai_responses_chat_body_edits(
            &mut provider_request_body,
            "codex",
            "openai:responses",
            None,
            None,
        );

        assert_eq!(provider_request_body["reasoning"]["effort"], "medium");
        assert_eq!(provider_request_body["reasoning"]["mode"], mode);
    }
}

#[test]
fn codex_responses_image_tool_edits_force_internal_model_and_tool_defaults() {
    let mut provider_request_body = json!({
        "model": "gpt-image-2",
        "input": "generate image",
        "tools": [{
            "type": "image_generation"
        }],
        "tool_choice": {"type": "image_generation"}
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(
        provider_request_body["model"],
        json!(CODEX_OPENAI_IMAGE_INTERNAL_MODEL)
    );
    assert_eq!(provider_request_body["stream"], json!(true));
    assert_eq!(
        provider_request_body["tools"][0]["type"],
        json!("image_generation")
    );
    assert_eq!(
        provider_request_body["tool_choice"]["type"],
        json!("image_generation")
    );
}

#[test]
fn codex_responses_image_tool_edits_triggered_by_string_tool_choice() {
    let mut provider_request_body = json!({
        "model": "gpt-image-2",
        "input": "generate image",
        "tools": [{"type": "image_generation"}],
        "tool_choice": "image_generation"
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(
        provider_request_body["model"],
        json!(CODEX_OPENAI_IMAGE_INTERNAL_MODEL)
    );
    assert_eq!(
        provider_request_body["tool_choice"]["type"],
        json!("image_generation")
    );
}

#[test]
fn codex_responses_image_tool_edits_skipped_when_tool_choice_is_auto() {
    let original_model = "gpt-5.5";
    let mut provider_request_body = json!({
        "model": original_model,
        "input": [{"role": "user", "content": "hi"}],
        "tools": [
            {"type": "function", "name": "shell"},
            {"type": "image_generation"},
            {"type": "web_search"}
        ],
        "tool_choice": "auto"
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(provider_request_body["model"], json!(original_model));
    assert_eq!(provider_request_body["tool_choice"], json!("auto"));
    assert_eq!(
        provider_request_body["tools"]
            .as_array()
            .map(Vec::len)
            .unwrap_or_default(),
        3,
        "tools array should be preserved when tool_choice is auto"
    );
}

#[test]
fn codex_responses_image_tool_edits_skipped_when_tool_choice_absent() {
    let original_model = "gpt-5.5";
    let mut provider_request_body = json!({
        "model": original_model,
        "input": [{"role": "user", "content": "hi"}],
        "tools": [{"type": "image_generation"}]
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(provider_request_body["model"], json!(original_model));
    assert_eq!(provider_request_body["tool_choice"], json!("auto"));
    assert_eq!(
        provider_request_body["tools"][0]["type"],
        json!("image_generation")
    );
    assert_eq!(
        provider_request_body["tools"]
            .as_array()
            .map(Vec::len)
            .unwrap_or_default(),
        1,
        "tools array should be preserved verbatim when tool_choice is absent"
    );
}

#[test]
fn codex_responses_image_tool_edits_skipped_when_tool_choice_targets_other_tool() {
    let original_model = "gpt-5.5";
    let mut provider_request_body = json!({
        "model": original_model,
        "input": [{"role": "user", "content": "hi"}],
        "tools": [
            {"type": "function", "name": "shell"},
            {"type": "image_generation"}
        ],
        "tool_choice": {"type": "function", "name": "shell"}
    });

    apply_codex_openai_responses_special_body_edits(
        &mut provider_request_body,
        "codex",
        "openai:responses",
        None,
        None,
    );

    assert_eq!(provider_request_body["model"], json!(original_model));
    assert_eq!(
        provider_request_body["tool_choice"],
        json!({"type": "function", "name": "shell"})
    );
}
