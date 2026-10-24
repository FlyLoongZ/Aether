use serde_json::json;

use super::{
    convert_request, convert_request_pure, convert_request_pure_with_context, convert_response,
    convert_response_pure, FormatContext, FormatError,
};
use crate::formats::id::FormatId;

#[test]
fn openai_cli_alias_is_not_a_primary_format() {
    assert_eq!(FormatId::parse("openai:cli"), None);
}

#[test]
fn converts_openai_chat_to_responses_via_registry() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}]
    });
    let ctx = FormatContext::default().with_mapped_model("gpt-target");

    let converted = convert_request("openai:chat", "openai:responses", &body, &ctx)
        .expect("request conversion should succeed");

    assert_eq!(converted["model"], "gpt-target");
    assert_eq!(converted["input"][0]["type"], "message");
    assert_eq!(converted["input"][0]["content"][0]["type"], "input_text");
}

#[test]
fn claude_request_to_responses_accepts_context_management() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 128,
        "context_management": {
            "edits": [{"type": "clear_tool_uses_20250919"}]
        }
    });

    let converted = convert_request_pure("claude:messages", "openai:responses", &body)
        .expect("context_management is audited for Claude cross-format requests")
        .value;

    assert_eq!(converted["model"], "claude-sonnet");
    assert!(converted.get("context_management").is_none());
}

#[test]
fn pure_request_conversion_does_not_apply_model_or_stream_edits() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}]
    });
    let ctx = FormatContext::default()
        .with_mapped_model("gpt-target")
        .with_upstream_stream(true);

    let converted =
        convert_request_pure_with_context("openai:chat", "openai:responses", &body, &ctx)
            .expect("pure conversion should succeed");

    assert_eq!(converted.value["model"], "gpt-source");
    assert!(converted.value.get("stream").is_none());
    assert!(converted
        .report
        .fields
        .iter()
        .any(|field| field.field == "messages"));
}

/// Gemini runs `googleSearch` server-side, so the only trace of the search
/// is `groundingMetadata`. Clients on the other formats have to receive it
/// as their own native citations or the answer arrives unverifiable.
#[test]
fn gemini_grounding_reaches_every_cross_format_client_as_citations() {
    let gemini = grounded_gemini_response();

    for target in ["openai:chat", "openai:responses"] {
        let converted =
            convert_response_pure("gemini:generate_content", target, &gemini).expect(target);
        let body = serde_json::to_string(&converted.value).expect("serialize");
        let annotations = find_first_array(&converted.value, "annotations")
            .unwrap_or_else(|| panic!("{target} dropped the grounding metadata: {body}"));
        assert_eq!(
            annotations,
            &json!([{
                "type": "url_citation",
                "url": "https://time.gov/",
                "title": "time.gov",
                "start_index": 0,
                "end_index": 9,
            }]),
            "{target} annotations"
        );
    }

    let converted = convert_response_pure("gemini:generate_content", "claude:messages", &gemini)
        .expect("claude:messages");
    let body = serde_json::to_string(&converted.value).expect("serialize");
    let citations = find_first_array(&converted.value, "citations")
        .unwrap_or_else(|| panic!("claude:messages dropped the grounding metadata: {body}"));
    assert_eq!(
        citations,
        &json!([{
            "type": "web_search_result_location",
            "url": "https://time.gov/",
            "title": "time.gov",
            "cited_text": "今天是 2026",
        }])
    );
}

/// The grounded span is reported in UTF-8 bytes but every target counts
/// characters, so a multi-byte answer must not shift the citation.
#[test]
fn gemini_grounding_offsets_are_converted_from_bytes_to_characters() {
    let converted = convert_response_pure(
        "gemini:generate_content",
        "openai:chat",
        &grounded_gemini_response(),
    )
    .expect("convert");
    let annotation = &find_first_array(&converted.value, "annotations").expect("annotations")[0];

    // "今天是 2026 " is 15 bytes but 9 characters.
    assert_eq!(annotation["end_index"], json!(9));
}

fn grounded_gemini_response() -> serde_json::Value {
    json!({
        "responseId": "resp_grounded",
        "modelVersion": "gemini-3.8-flash",
        "candidates": [{
            "index": 0,
            "finishReason": "STOP",
            "groundingMetadata": {
                "webSearchQueries": ["current UTC date"],
                "groundingChunks": [{
                    "web": {"uri": "https://time.gov/", "title": "time.gov"}
                }],
                "groundingSupports": [{
                    "segment": {"startIndex": 0, "endIndex": 15},
                    "groundingChunkIndices": [0]
                }]
            },
            "content": {"parts": [{"text": "今天是 2026 年"}]}
        }]
    })
}

fn find_first_array<'a>(value: &'a serde_json::Value, key: &str) -> Option<&'a serde_json::Value> {
    match value {
        serde_json::Value::Object(object) => {
            if let Some(found) = object.get(key).filter(|found| found.is_array()) {
                return Some(found);
            }
            object
                .values()
                .find_map(|value| find_first_array(value, key))
        }
        serde_json::Value::Array(items) => {
            items.iter().find_map(|item| find_first_array(item, key))
        }
        _ => None,
    }
}

#[test]
fn runtime_responses_to_gemini_rejects_mixed_tools_for_gemini_two() {
    let body = json!({
        "model": "gpt-5",
        "input": "Search, then save the result.",
        "tools": [
            {"type": "web_search_preview"},
            {
                "type": "function",
                "name": "save_result",
                "parameters": {"type": "object"}
            }
        ]
    });
    let context = FormatContext::default().with_mapped_model("gemini-2.5-pro");

    let error = convert_request(
        "openai:responses",
        "gemini:generate_content",
        &body,
        &context,
    )
    .expect_err("Gemini 2.5 mixed tools should fail before reaching the provider");

    assert!(matches!(
        error,
        FormatError::InvalidTargetField { ref field, ref reason, .. }
            if field == "tools" && reason.contains("Gemini 3")
    ));
}

#[test]
fn pure_openai_chat_to_responses_preserves_explicit_tool_strict() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}],
        "tools": [{
            "type": "function",
            "function": {
                "name": "lookup",
                "parameters": {"type": "object"},
                "strict": true
            }
        }]
    });

    let converted = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["tools"][0]["strict"], true);
}

#[test]
fn pure_openai_responses_to_chat_preserves_explicit_tool_strict() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "tools": [{
            "type": "function",
            "name": "lookup",
            "parameters": {"type": "object"},
            "strict": false
        }]
    });

    let converted = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["tools"][0]["function"]["strict"], false);
}

#[test]
fn responses_namespace_tool_is_audited_consistently_for_pure_and_runtime_chat_conversion() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "validate the finding"}],
        "tools": [{
            "type": "namespace",
            "name": "mcp__vulnerability_report",
            "description": "Reporting tools",
            "tools": [{
                "type": "function",
                "name": "vulnerability_report",
                "description": "Create the confirmed vulnerability report",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "report_path": {"type": "string"},
                        "markdown": {"type": "string"}
                    },
                    "required": ["report_path", "markdown"],
                    "additionalProperties": false
                },
                "strict": true
            }]
        }],
        "tool_choice": "auto"
    });

    let pure = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("representable namespace should pass pure validation")
        .value;
    let runtime = convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .expect("representable namespace should pass runtime validation");

    assert_eq!(pure, runtime);
    assert_eq!(pure["tools"].as_array().map(Vec::len), Some(1));
    assert_eq!(pure["tools"][0]["function"]["name"], "vulnerability_report");
    assert_eq!(
        pure["tools"][0]["function"]["description"],
        "Create the confirmed vulnerability report"
    );
    assert_eq!(
        pure["tools"][0]["function"]["parameters"],
        body["tools"][0]["tools"][0]["parameters"]
    );
    assert_eq!(pure["tools"][0]["function"]["strict"], true);
    assert_eq!(pure["tool_choice"], "auto");
}

#[test]
fn responses_namespace_allowed_tools_and_nullable_schema_match_for_pure_and_runtime() {
    let long_name = format!("write_report_{}", "x".repeat(80));
    let body = json!({
        "model": "gpt-source",
        "input": "validate the finding",
        "tools": [{
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "tools": [{
                "type": "function",
                "name": long_name,
                "description": null,
                "parameters": null,
                "strict": null
            }]
        }],
        "tool_choice": {
            "type": "allowed_tools",
            "mode": "required",
            "tools": [{"type": "function", "name": long_name}]
        }
    });

    let pure = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("allowed namespace tool choice should pass pure validation")
        .value;
    let runtime = convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .expect("allowed namespace tool choice should pass runtime validation");

    assert_eq!(pure, runtime);
    let definition = &pure["tools"][0]["function"];
    let definition_alias = definition["name"].as_str().expect("definition alias");
    let choice_alias = pure["tool_choice"]["allowed_tools"]["tools"][0]["function"]["name"]
        .as_str()
        .expect("choice alias");
    assert_eq!(choice_alias, definition_alias);
    assert!(definition_alias.len() <= 64);
    assert!(definition.get("description").is_none());
    assert!(definition.get("parameters").is_none());
    assert!(definition["strict"].is_null());
}

#[test]
fn responses_namespace_allowed_tools_fails_closed_when_child_name_is_ambiguous() {
    let body = json!({
        "model": "gpt-source",
        "input": "validate the finding",
        "tools": [
            {
                "type": "namespace",
                "name": "first",
                "description": "First tools",
                "tools": [{
                    "type": "function",
                    "name": "write_report",
                    "parameters": {"type": "object"}
                }]
            },
            {
                "type": "namespace",
                "name": "second",
                "description": "Second tools",
                "tools": [{
                    "type": "function",
                    "name": "write_report",
                    "parameters": {"type": "object"}
                }]
            }
        ],
        "tool_choice": {
            "type": "allowed_tools",
            "mode": "required",
            "tools": [{"type": "function", "name": "write_report"}]
        }
    });

    assert!(convert_request_pure("openai:responses", "openai:chat", &body).is_err());
    assert!(convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .is_err());
}

#[test]
fn runtime_chat_response_restores_namespaced_tool_identity_from_request_context() {
    let original_request = json!({
        "model": "qwen",
        "input": "write the report",
        "tools": [
            {
                "type": "function",
                "name": "vulnerability_report",
                "parameters": {"type": "object"}
            },
            {
                "type": "namespace",
                "name": "mcp__vulnerability_report",
                "description": "Reporting tools",
                "tools": [{
                    "type": "function",
                    "name": "vulnerability_report",
                    "parameters": {
                        "type": "object",
                        "properties": {"report_path": {"type": "string"}},
                        "required": ["report_path"]
                    },
                    "strict": true
                }]
            }
        ]
    });
    let chat_request = convert_request(
        "openai:responses",
        "openai:chat",
        &original_request,
        &FormatContext::default(),
    )
    .expect("namespace request should convert to Chat");
    let alias = chat_request["tools"][1]["function"]["name"]
        .as_str()
        .expect("namespace child should have a Chat alias");
    assert_ne!(alias, "vulnerability_report");

    let provider_response = json!({
        "id": "chatcmpl_namespace_sync",
        "object": "chat.completion",
        "model": "qwen",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_report",
                    "type": "function",
                    "function": {
                        "name": alias,
                        "arguments": "{\"report_path\":\"reports/sql-001.md\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    });
    let context = FormatContext::default().with_report_context(json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:responses",
        "original_request_body": original_request
    }));

    let response = convert_response(
        "openai:chat",
        "openai:responses",
        &provider_response,
        &context,
    )
    .expect("Chat response should convert back to Responses");
    let call = &response["output"][0];
    assert_eq!(response["object"], "response");
    assert_eq!(call["type"], "function_call");
    assert_eq!(call["name"], "vulnerability_report");
    assert_eq!(call["namespace"], "mcp__vulnerability_report");
    assert_eq!(call["call_id"], "call_report");
}

#[test]
fn responses_namespace_history_uses_the_same_chat_alias_as_the_definition() {
    let body = json!({
        "model": "gpt-source",
        "input": [
            {
                "type": "function_call",
                "id": "fc_report",
                "call_id": "call_report",
                "namespace": "mcp__reports",
                "name": "write_report",
                "arguments": "{\"report_path\":\"reports/finding.md\"}",
                "status": "completed"
            },
            {
                "type": "function_call_output",
                "call_id": "call_report",
                "output": "created"
            }
        ],
        "tools": [
            {
                "type": "function",
                "name": "write_report",
                "description": "An ordinary function",
                "parameters": {"type": "object"}
            },
            {
                "type": "namespace",
                "name": "mcp__reports",
                "description": "Reporting tools",
                "tools": [{
                    "type": "function",
                    "name": "write_report",
                    "description": "Create a report file",
                    "parameters": {
                        "type": "object",
                        "properties": {"report_path": {"type": "string"}},
                        "required": ["report_path"]
                    }
                }]
            }
        ],
        "tool_choice": "auto"
    });

    let pure = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("namespace history should pass pure validation")
        .value;
    let runtime = convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .expect("namespace history should pass runtime validation");

    assert_eq!(pure, runtime);
    let alias = pure["tools"][1]["function"]["name"]
        .as_str()
        .expect("namespace alias");
    assert_ne!(alias, "write_report");
    assert_eq!(
        pure["messages"][0]["tool_calls"][0]["function"]["name"],
        alias
    );
    assert_eq!(pure["messages"][0]["tool_calls"][0]["id"], "call_report");
    assert!(pure["messages"][0]["tool_calls"][0].get("status").is_none());
    assert_eq!(pure["messages"][1]["tool_call_id"], "call_report");
}

#[test]
fn responses_namespace_history_rejects_non_completed_status_for_chat() {
    let body = json!({
        "model": "gpt-source",
        "input": [{
            "type": "function_call",
            "id": "fc_report",
            "call_id": "call_report",
            "namespace": "mcp__reports",
            "name": "write_report",
            "arguments": "{}",
            "status": "in_progress"
        }],
        "tools": [{
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "tools": [{
                "type": "function",
                "name": "write_report",
                "parameters": {"type": "object"}
            }]
        }]
    });

    assert!(convert_request_pure("openai:responses", "openai:chat", &body).is_err());
    assert!(convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .is_err());
}

#[test]
fn responses_namespace_history_rejects_unknown_sidecars_for_pure_and_runtime() {
    let body = json!({
        "model": "gpt-source",
        "input": [{
            "type": "function_call",
            "id": "fc_report",
            "call_id": "call_report",
            "namespace": "mcp__reports",
            "name": "write_report",
            "arguments": "{}",
            "status": "completed",
            "future_call_semantics": {"owner": "future-semantic-owner"}
        }],
        "tools": [{
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "tools": [{
                "type": "function",
                "name": "write_report",
                "parameters": {"type": "object"}
            }]
        }]
    });

    assert!(convert_request_pure("openai:responses", "openai:chat", &body).is_err());
    assert!(convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .is_err());
}

#[test]
fn responses_namespace_same_format_preserves_the_raw_contract() {
    let body = json!({
        "model": "gpt-source",
        "input": "look up the customer",
        "tools": [{
            "type": "namespace",
            "name": "crm",
            "description": "CRM tools",
            "tools": [{
                "type": "function",
                "name": "get_customer",
                "description": "Fetch a customer",
                "defer_loading": true,
                "parameters": {"type": "object"},
                "output_schema": {"type": "object"}
            }]
        }]
    });

    let converted = convert_request_pure("openai:responses", "openai:responses", &body)
        .expect("same-format namespace request should remain transparent")
        .value;

    assert_eq!(converted["tools"], body["tools"]);
    assert_eq!(converted["tools"].as_array().map(Vec::len), Some(1));
}

#[test]
fn responses_namespace_tool_validation_fails_closed_for_unrepresentable_children() {
    let cases = [
        json!({
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "tools": [{
                "type": "custom",
                "name": "write_report",
                "description": "custom child"
            }]
        }),
        json!({
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "tools": [{
                "type": "function",
                "name": "write_report",
                "parameters": {"type": "object", "properties": {}},
                "future_child_field": {"cannot": "preserve"}
            }]
        }),
        json!({
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "future_namespace_field": true,
            "tools": [{
                "type": "function",
                "name": "write_report",
                "parameters": {"type": "object", "properties": {}}
            }]
        }),
        json!({
            "type": "namespace",
            "name": "mcp__reports",
            "description": "Reporting tools",
            "tools": [{
                "type": "function",
                "name": "write_report",
                "parameters": "not-a-schema-object"
            }]
        }),
    ];

    for namespace_tool in cases {
        let body = json!({
            "model": "gpt-source",
            "input": "hello",
            "tools": [namespace_tool]
        });

        let pure_error = convert_request_pure("openai:responses", "openai:chat", &body)
            .expect_err("pure conversion must reject an unrepresentable namespace");
        let runtime_error = convert_request(
            "openai:responses",
            "openai:chat",
            &body,
            &FormatContext::default(),
        )
        .expect_err("runtime conversion must reject an unrepresentable namespace");

        for error in [pure_error, runtime_error] {
            assert!(matches!(
                error,
                FormatError::LossyConversionBlocked { ref field, .. }
                    if field == "tools[0]"
            ));
        }
    }
}

#[test]
fn pure_openai_chat_to_responses_maps_tool_call_ids_to_call_ids() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{
            "role": "assistant",
            "content": null,
            "tool_calls": [{
                "id": "call_lookup_1",
                "type": "function",
                "function": {
                    "name": "lookup",
                    "arguments": "{\"q\":\"rust\"}"
                }
            }]
        }, {
            "role": "tool",
            "tool_call_id": "call_lookup_1",
            "content": "{\"ok\":true}"
        }]
    });

    let converted = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["input"][0]["type"], "function_call");
    assert!(converted["input"][0].get("id").is_none());
    assert_eq!(converted["input"][0]["call_id"], "call_lookup_1");
    assert_eq!(converted["input"][1]["type"], "function_call_output");
    assert_eq!(converted["input"][1]["call_id"], "call_lookup_1");
}

#[test]
fn pure_openai_responses_to_chat_maps_call_ids_to_tool_call_ids() {
    let body = json!({
        "model": "gpt-source",
        "input": [{
            "type": "function_call",
            "call_id": "call_lookup_1",
            "name": "lookup",
            "arguments": "{\"q\":\"rust\"}"
        }, {
            "type": "function_call_output",
            "call_id": "call_lookup_1",
            "output": "{\"ok\":true}"
        }]
    });

    let converted = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(
        converted["messages"][0]["tool_calls"][0]["id"],
        "call_lookup_1"
    );
    assert_eq!(converted["messages"][1]["tool_call_id"], "call_lookup_1");
}

#[test]
fn pure_gemini_to_openai_chat_maps_function_response_id_to_tool_call_id() {
    let body = json!({
        "contents": [{
            "role": "model",
            "parts": [{
                "functionCall": {
                    "id": "call_lookup_1",
                    "name": "lookup",
                    "args": {"q": "rust"}
                }
            }]
        }, {
            "role": "user",
            "parts": [{
                "functionResponse": {
                    "id": "call_lookup_1",
                    "name": "lookup",
                    "response": {"result": {"ok": true}}
                }
            }]
        }]
    });

    let converted = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(
        converted["messages"][0]["tool_calls"][0]["id"],
        "call_lookup_1"
    );
    assert_eq!(converted["messages"][1]["tool_call_id"], "call_lookup_1");
}

#[test]
fn pure_gemini_idless_parallel_tool_history_stays_paired_for_standard_targets() {
    let body = json!({
        "model": "gemini-source",
        "contents": [{
            "role": "model",
            "parts": [
                {"functionCall": {"name": "lookup", "args": {"q": "first"}}},
                {"functionCall": {"name": "lookup", "args": {"q": "second"}}}
            ]
        }, {
            "role": "user",
            "parts": [
                {"functionResponse": {"name": "lookup", "response": {"result": "one"}}},
                {"functionResponse": {"name": "lookup", "response": {"result": "two"}}}
            ]
        }]
    });

    let chat = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect("Gemini history should convert to Chat")
        .value;
    let chat_call_ids = chat["messages"][0]["tool_calls"]
        .as_array()
        .expect("Chat tool calls")
        .iter()
        .map(|call| call["id"].as_str().expect("Chat call ID"))
        .collect::<Vec<_>>();
    let chat_result_ids = chat["messages"]
        .as_array()
        .expect("Chat messages")
        .iter()
        .filter(|message| message["role"] == "tool")
        .map(|message| {
            message["tool_call_id"]
                .as_str()
                .expect("Chat result call ID")
        })
        .collect::<Vec<_>>();
    assert_eq!(chat_result_ids, chat_call_ids);

    let responses = convert_request_pure("gemini:generate_content", "openai:responses", &body)
        .expect("Gemini history should convert to Responses")
        .value;
    let response_items = responses["input"].as_array().expect("Responses input");
    let response_call_ids = response_items
        .iter()
        .filter(|item| item["type"] == "function_call")
        .map(|item| item["call_id"].as_str().expect("Responses call ID"))
        .collect::<Vec<_>>();
    let response_result_ids = response_items
        .iter()
        .filter(|item| item["type"] == "function_call_output")
        .map(|item| item["call_id"].as_str().expect("Responses result call ID"))
        .collect::<Vec<_>>();
    assert_eq!(response_result_ids, response_call_ids);

    let claude = convert_request_pure("gemini:generate_content", "claude:messages", &body)
        .expect("Gemini history should convert to Claude Messages")
        .value;
    let claude_messages = claude["messages"].as_array().expect("Claude messages");
    let claude_call_ids = claude_messages
        .iter()
        .flat_map(|message| message["content"].as_array().into_iter().flatten())
        .filter(|block| block["type"] == "tool_use")
        .map(|block| block["id"].as_str().expect("Claude tool use ID"))
        .collect::<Vec<_>>();
    let claude_result_ids = claude_messages
        .iter()
        .flat_map(|message| message["content"].as_array().into_iter().flatten())
        .filter(|block| block["type"] == "tool_result")
        .map(|block| {
            block["tool_use_id"]
                .as_str()
                .expect("Claude tool result ID")
        })
        .collect::<Vec<_>>();
    assert_eq!(claude_result_ids, claude_call_ids);
}

#[test]
fn pure_claude_to_openai_chat_maps_disable_parallel_tool_use() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 64,
        "tool_choice": {
            "type": "auto",
            "disable_parallel_tool_use": true
        }
    });

    let converted = convert_request_pure("claude:messages", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["parallel_tool_calls"], false);
}

#[test]
fn pure_claude_to_openai_chat_preserves_max_output_effort() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 64,
        "output_config": {
            "effort": "max"
        }
    });

    let converted = convert_request_pure("claude:messages", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["reasoning_effort"], "max");
}

#[test]
fn pure_openai_chat_to_claude_maps_parallel_tool_calls() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}],
        "parallel_tool_calls": false
    });

    let converted = convert_request_pure("openai:chat", "claude:messages", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["tool_choice"]["type"], "auto");
    assert_eq!(converted["tool_choice"]["disable_parallel_tool_use"], true);
}

#[test]
fn pure_claude_to_openai_responses_blocks_message_thinking_loss() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{
            "role": "assistant",
            "content": [{
                "type": "thinking",
                "thinking": "plan",
                "signature": "sig_123"
            }]
        }],
        "max_tokens": 64
    });

    let error = convert_request_pure("claude:messages", "openai:responses", &body)
        .expect_err("Claude thinking blocks should fail closed for Responses");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "messages[].content[].type"
    ));
}

#[test]
fn pure_claude_to_openai_chat_blocks_structured_tool_result_loss() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_123",
                "content": [{
                    "type": "text",
                    "text": "first"
                }, {
                    "type": "text",
                    "text": "second"
                }]
            }]
        }],
        "max_tokens": 64
    });

    let error = convert_request_pure("claude:messages", "openai:chat", &body)
        .expect_err("Claude tool_result block arrays should fail closed for Chat");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "messages[].content[].tool_result.content"
    ));
}

#[test]
fn pure_claude_to_gemini_blocks_redacted_thinking_loss() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{
            "role": "assistant",
            "content": [{
                "type": "redacted_thinking",
                "data": "enc_123"
            }]
        }],
        "max_tokens": 64
    });

    let error = convert_request_pure("claude:messages", "gemini:generate_content", &body)
        .expect_err("Claude redacted thinking should fail closed for Gemini");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "messages[].content[].type"
    ));
}

#[test]
fn pure_gemini_to_openai_chat_maps_allowed_function_names_to_named_tool_choice() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": ["lookup"]
            }
        }
    });

    let converted = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["tool_choice"]["type"], "function");
    assert_eq!(converted["tool_choice"]["function"]["name"], "lookup");
}

#[test]
fn pure_gemini_to_openai_chat_maps_thinking_level_to_reasoning_effort() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {
            "thinkingConfig": {
                "includeThoughts": true,
                "thinkingLevel": "high"
            }
        }
    });

    let converted = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(converted["reasoning_effort"], "high");
}

#[test]
fn pure_openai_chat_to_gemini_maps_named_tool_choice_to_allowed_function_names() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}],
        "tool_choice": {
            "type": "function",
            "function": {"name": "lookup"}
        }
    });

    let converted = convert_request_pure("openai:chat", "gemini:generate_content", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(
        converted["toolConfig"]["functionCallingConfig"]["allowedFunctionNames"][0],
        "lookup"
    );
}

#[test]
fn pure_openai_chat_to_gemini_preserves_json_tool_output_as_string() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{
            "role": "assistant",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "read_file", "arguments": "{}"}
            }]
        }, {
            "role": "tool",
            "tool_call_id": "call_1",
            "content": "{\"key\":\"value\",\"items\":[1,2,3]}"
        }]
    });

    let converted = convert_request_pure("openai:chat", "gemini:generate_content", &body)
        .expect("pure conversion should succeed")
        .value;

    assert_eq!(
        converted["contents"][1]["parts"][0]["functionResponse"]["response"]["result"],
        "{\"key\":\"value\",\"items\":[1,2,3]}"
    );
}

#[test]
fn pure_openai_responses_to_gemini_aligns_parallel_tool_outputs() {
    let body = json!({
        "model": "gpt-source",
        "input": [{
            "type": "function_call",
            "call_id": "call_1",
            "name": "read_file",
            "arguments": "{\"path\":\"one\"}"
        }, {
            "type": "function_call",
            "call_id": "call_2",
            "name": "read_file",
            "arguments": "{\"path\":\"two\"}"
        }, {
            "type": "function_call_output",
            "call_id": "call_2",
            "output": "two"
        }, {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": "one"
        }]
    });

    let converted = convert_request_pure("openai:responses", "gemini:generate_content", &body)
        .expect("pure conversion should succeed")
        .value;
    let parts = converted["contents"][1]["parts"]
        .as_array()
        .expect("tool response parts");
    let calls = converted["contents"][0]["parts"]
        .as_array()
        .expect("parallel function call parts");

    assert_eq!(
        calls[0]["thoughtSignature"],
        "skip_thought_signature_validator"
    );
    assert!(calls[1].get("thoughtSignature").is_none());
    assert_eq!(parts[0]["functionResponse"]["id"], "call_1");
    assert_eq!(parts[0]["functionResponse"]["response"]["result"], "one");
    assert_eq!(parts[1]["functionResponse"]["id"], "call_2");
    assert_eq!(parts[1]["functionResponse"]["response"]["result"], "two");
}

#[test]
fn pure_claude_to_gemini_aligns_parallel_tool_results_before_text() {
    let body = json!({
        "model": "claude-source",
        "max_tokens": 64,
        "messages": [{
            "role": "assistant",
            "content": [{
                "type": "tool_use",
                "id": "call_1",
                "name": "read_file",
                "input": {"path": "one"}
            }, {
                "type": "tool_use",
                "id": "call_2",
                "name": "read_file",
                "input": {"path": "two"}
            }]
        }, {
            "role": "user",
            "content": [{
                "type": "text",
                "text": "Results follow."
            }, {
                "type": "tool_result",
                "tool_use_id": "call_2",
                "content": "two"
            }, {
                "type": "tool_result",
                "tool_use_id": "call_1",
                "content": "one"
            }, {
                "type": "text",
                "text": "Continue."
            }]
        }]
    });

    let converted = convert_request_pure("claude:messages", "gemini:generate_content", &body)
        .expect("pure conversion should succeed")
        .value;
    let parts = converted["contents"][1]["parts"]
        .as_array()
        .expect("tool response parts");

    assert_eq!(parts[0]["functionResponse"]["id"], "call_1");
    assert_eq!(parts[1]["functionResponse"]["id"], "call_2");
    assert_eq!(parts[2]["text"], "Results follow.");
    assert_eq!(parts[3]["text"], "Continue.");
}

#[test]
fn pure_openai_responses_to_gemini_signs_synthetic_tool_history() {
    let body = json!({
        "model": "gemini-3-flash-preview",
        "input": [{
            "type": "function_call",
            "call_id": "call_weather",
            "name": "get_weather",
            "arguments": "{\"city\":\"Shanghai\"}"
        }, {
            "type": "function_call_output",
            "call_id": "call_weather",
            "output": "sunny"
        }]
    });

    let converted = convert_request_pure("openai:responses", "gemini:generate_content", &body)
        .expect("synthetic Responses tool history should be compatible with Gemini")
        .value;

    assert_eq!(
        converted["contents"][0]["parts"][0]["thoughtSignature"],
        "skip_thought_signature_validator"
    );
}

#[test]
fn gemini_tool_signature_roundtrips_through_openai_responses_history() {
    let signature = "opaque-gemini-tool-signature";
    let gemini_response = json!({
        "responseId": "resp_gemini_tool",
        "modelVersion": "gemini-3-flash-preview",
        "candidates": [{
            "content": {
                "role": "model",
                "parts": [{
                    "functionCall": {
                        "id": "call_weather",
                        "name": "get_weather",
                        "args": {"city": "Shanghai"}
                    },
                    "thoughtSignature": signature
                }]
            },
            "finishReason": "STOP"
        }]
    });

    let responses = convert_response_pure(
        "gemini:generate_content",
        "openai:responses",
        &gemini_response,
    )
    .expect("Gemini tool response should convert to Responses")
    .value;
    let mut input = responses["output"]
        .as_array()
        .expect("Responses output items")
        .clone();
    input.push(json!({
        "type": "function_call_output",
        "call_id": "call_weather",
        "output": "sunny"
    }));

    let converted = convert_request_pure(
        "openai:responses",
        "gemini:generate_content",
        &json!({
            "model": "gemini-3-flash-preview",
            "input": input
        }),
    )
    .expect("Responses tool history should convert back to Gemini")
    .value;

    assert_eq!(
        converted["contents"][0]["parts"][0]["thoughtSignature"],
        signature
    );
}

#[test]
fn post_call_gemini_signature_carrier_replays_to_previous_function_call() {
    let signature = "opaque-late-tool-signature";
    let carrier =
        crate::formats::openai::responses::encode_gemini_tool_signature_carrier_with_direction(
            signature,
            crate::formats::openai::responses::GeminiToolSignatureCarrierDirection::Previous,
        )
        .expect("signature carrier");
    let body = json!({
        "model": "gemini-3-flash-preview",
        "input": [{
            "type": "function_call",
            "call_id": "call_weather",
            "name": "get_weather",
            "arguments": "{\"city\":\"Shanghai\"}"
        }, {
            "type": "reasoning",
            "id": "rs_aether_late_signature",
            "status": "completed",
            "encrypted_content": carrier,
            "summary": []
        }, {
            "type": "function_call_output",
            "call_id": "call_weather",
            "output": "sunny"
        }]
    });

    let converted = convert_request_pure("openai:responses", "gemini:generate_content", &body)
        .expect("post-call signature carrier should replay")
        .value;

    assert_eq!(
        converted["contents"][0]["parts"][0]["thoughtSignature"],
        signature
    );
}

#[test]
fn pure_gemini_to_openai_responses_blocks_thought_part_loss() {
    let body = json!({
        "contents": [{
            "role": "model",
            "parts": [{
                "text": "plan",
                "thought": true,
                "thoughtSignature": "sig_123"
            }]
        }]
    });

    let error = convert_request_pure("gemini:generate_content", "openai:responses", &body)
        .expect_err("Gemini thought parts should fail closed for Responses");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "contents[].parts[].thoughtSignature"
    ));
}

#[test]
fn pure_gemini_to_openai_chat_blocks_safety_settings_loss() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "safetySettings": [{
            "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
            "threshold": "BLOCK_NONE"
        }]
    });

    let error = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect_err("Gemini safetySettings should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "safetySettings"
    ));
}

#[test]
fn pure_gemini_to_openai_chat_blocks_response_modalities_loss() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "generationConfig": {
            "responseModalities": ["TEXT", "IMAGE"]
        }
    });

    let error = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect_err("Gemini responseModalities should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "generationConfig.responseModalities"
    ));
}

#[test]
fn pure_gemini_to_openai_chat_blocks_multiple_allowed_function_names() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": ["lookup", "search"]
            }
        }
    });

    let error = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect_err("multiple Gemini allowed function names should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "toolConfig.functionCallingConfig.allowedFunctionNames"
    ));
}

#[test]
fn pure_gemini_to_openai_chat_rejects_invalid_tool_mode_enum() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "SOMETIME"
            }
        }
    });

    let error = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect_err("invalid Gemini tool mode should fail closed");

    assert!(matches!(
        error,
        super::FormatError::InvalidEnumValue { ref field, ref value, .. }
            if field == "toolConfig.functionCallingConfig.mode" && value == "SOMETIME"
    ));
}

#[test]
fn pure_gemini_to_claude_blocks_code_execution_tool_loss() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "tools": [{
            "codeExecution": {}
        }]
    });

    let error = convert_request_pure("gemini:generate_content", "claude:messages", &body)
        .expect_err("Gemini codeExecution should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "tools[].codeExecution"
    ));
}

#[test]
fn pure_claude_to_openai_chat_blocks_cache_control_loss() {
    let body = json!({
        "model": "claude-sonnet",
        "system": [{
            "type": "text",
            "text": "Cache this.",
            "cache_control": {"type": "ephemeral"}
        }],
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 64
    });

    let error = convert_request_pure("claude:messages", "openai:chat", &body)
        .expect_err("Claude cache_control should fail closed cross-format");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "cache_control"
    ));
}

#[test]
fn pure_claude_to_openai_chat_allows_tool_schema_property_named_cache_control() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 64,
        "tools": [{
            "name": "configure_cache",
            "description": "Configure application cache behavior",
            "input_schema": {
                "type": "object",
                "properties": {
                    "cache_control": {
                        "type": "string",
                        "description": "Application-level cache policy"
                    }
                }
            }
        }]
    });

    let converted = convert_request_pure("claude:messages", "openai:chat", &body)
        .expect("tool schema property names should not be treated as Claude cache_control")
        .value;

    assert_eq!(
        converted["tools"][0]["function"]["parameters"]["properties"]["cache_control"]["type"],
        "string"
    );
}

#[test]
fn pure_openai_chat_to_responses_blocks_lossy_chat_only_fields() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}],
        "n": 2
    });

    let error = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect_err("lossy field should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. } if field == "n"
    ));
}

#[test]
fn pure_openai_responses_to_chat_drops_include() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "include": [
            "reasoning.encrypted_content",
            "file_search_call.results"
        ]
    });

    let converted = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("Responses include should be safely omitted for Chat")
        .value;

    assert_eq!(converted["model"], "gpt-source");
    assert_eq!(converted["messages"][0]["content"], "hello");
    assert!(converted.get("include").is_none());
}

#[test]
fn pure_openai_responses_to_chat_ignores_client_transport_metadata() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "client_metadata": {
            "session_id": "session-123",
            "thread_id": "thread-123"
        }
    });

    let converted = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("client transport metadata should not block cross-format conversion")
        .value;

    assert_eq!(converted["model"], "gpt-source");
    assert_eq!(converted["messages"][0]["content"], "hello");
    assert!(converted.get("client_metadata").is_none());
}

#[test]
fn pure_openai_responses_to_claude_ignores_client_transport_metadata() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "client_metadata": {
            "session_id": "session-123",
            "thread_id": "thread-123"
        }
    });

    let converted = convert_request_pure("openai:responses", "claude:messages", &body)
        .expect("client transport metadata should not block cross-format conversion")
        .value;

    assert_eq!(converted["model"], "gpt-source");
    assert_eq!(converted["messages"][0]["content"], "hello");
    assert!(converted.get("client_metadata").is_none());
}

#[test]
fn pure_cross_format_rejects_unknown_source_root_field() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}],
        "future_field": true
    });

    let error = convert_request_pure("openai:chat", "gemini:generate_content", &body)
        .expect_err("unknown source root field should fail closed");

    assert!(matches!(
        error,
        super::FormatError::UnauditedField {
            ref source_format,
            ref target_format,
            ref field,
            ..
        } if source_format == "openai:chat"
            && target_format == "gemini:generate_content"
            && field == "future_field"
    ));
}

#[test]
fn same_format_canonical_roundtrip_preserves_future_root_fields() {
    let cases = [
        (
            "openai:chat",
            json!({
                "model": "gpt-source",
                "messages": [{"role": "user", "content": "hello"}],
                "future_field": {"enabled": true}
            }),
            "future_field",
        ),
        (
            "openai:responses",
            json!({
                "model": "gpt-source",
                "input": [{"role": "user", "content": "hello"}],
                "future_field": {"enabled": true}
            }),
            "future_field",
        ),
        (
            "claude:messages",
            json!({
                "model": "claude-source",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": "hello"}],
                "future_field": {"enabled": true}
            }),
            "future_field",
        ),
        (
            "gemini:generate_content",
            json!({
                "model": "gemini-source",
                "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
                "futureField": {"enabled": true}
            }),
            "futureField",
        ),
    ];

    for (format, body, field) in cases {
        let converted = convert_request_pure(format, format, &body)
            .unwrap_or_else(|err| panic!("{format} same-format roundtrip failed: {err}"));
        assert_eq!(
            converted.value.get(field),
            body.get(field),
            "{format} must preserve unknown provider root fields in canonical roundtrip"
        );
    }
}

#[test]
fn pure_openai_responses_request_same_format_preserves_raw_tools_and_roles() {
    let body = json!({
        "model": "gpt-source",
        "input": [
            {
                "type": "message",
                "role": "developer",
                "content": [{"type": "input_text", "text": "Use policy"}]
            },
            {"role": "user", "content": "hello"}
        ],
        "tools": [
            {
                "type": "file_search",
                "vector_store_ids": ["vs_123"],
                "max_num_results": 3
            },
            {
                "type": "mcp",
                "server_label": "docs",
                "server_url": "https://example.com/mcp"
            }
        ]
    });

    let converted = convert_request_pure("openai:responses", "openai:responses", &body)
        .expect("same-format Responses request should preserve official raw fields")
        .value;

    assert_eq!(converted["input"][0]["role"], "developer");
    assert_eq!(converted["input"][0]["content"][0]["text"], "Use policy");
    assert_eq!(converted["tools"][0]["type"], "file_search");
    assert_eq!(converted["tools"][0]["vector_store_ids"][0], "vs_123");
    assert_eq!(converted["tools"][1]["type"], "mcp");
    assert_eq!(converted["tools"][1]["server_label"], "docs");
}

#[test]
fn pure_openai_responses_same_format_preserves_ptc_contract() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [
            {
                "type": "program",
                "id": "program_1",
                "code": "result = lookup({\"id\": 7})"
            },
            {
                "type": "program_output",
                "program_id": "program_1",
                "output": "ok"
            },
            {
                "type": "agent_message",
                "agent_id": "agent_1",
                "content": [{"type": "input_text", "text": "delegated result"}]
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "lookup",
                "arguments": "{\"id\":7}",
                "caller": {"type": "program", "id": "program_1"}
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "found",
                "caller": {"type": "program", "id": "program_1"}
            }
        ],
        "tools": [
            {
                "type": "function",
                "name": "lookup",
                "description": "Look up a record",
                "parameters": {
                    "type": "object",
                    "properties": {"id": {"type": "integer"}},
                    "required": ["id"]
                },
                "strict": true,
                "output_schema": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}}
                },
                "allowed_callers": ["programmatic"]
            },
            {"type": "programmatic_tool_calling"}
        ]
    });

    let converted = convert_request_pure("openai:responses", "openai:responses", &body)
        .expect("same-format Responses conversion should preserve the PTC wire contract")
        .value;

    assert_eq!(converted["input"], body["input"]);
    assert_eq!(converted["tools"], body["tools"]);
    assert_eq!(
        converted["tools"][0]["allowed_callers"],
        json!(["programmatic"])
    );
    assert_eq!(converted["tools"][1]["type"], "programmatic_tool_calling");
}

#[test]
fn pure_openai_responses_same_format_preserves_message_items() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [
            {
                "type": "message",
                "id": "msg_developer",
                "status": "completed",
                "role": "developer",
                "content": [{
                    "type": "input_text",
                    "text": "Use policy",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                }]
            },
            {
                "type": "message",
                "id": "msg_user",
                "status": "completed",
                "role": "user",
                "content": [{"type": "input_text", "text": "hello"}]
            },
            {
                "type": "message",
                "id": "msg_assistant",
                "status": "completed",
                "role": "assistant",
                "phase": "commentary",
                "content": [
                    {"type": "output_text", "text": ""},
                    {"type": "refusal", "refusal": "cannot comply"},
                    {"type": "future_content", "payload": {"value": 1}}
                ]
            }
        ]
    });

    let converted = convert_request_pure("openai:responses", "openai:responses", &body)
        .expect("same-format Responses conversion should preserve message input items")
        .value;

    assert_eq!(converted["input"], body["input"]);
}

#[test]
fn pure_openai_responses_refusal_history_fails_closed_across_formats() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "message",
            "role": "assistant",
            "content": [{"type": "refusal", "refusal": "cannot comply"}]
        }]
    });

    for target in ["openai:chat", "claude:messages", "gemini:generate_content"] {
        let error = convert_request_pure("openai:responses", target, &body)
            .expect_err("target request format cannot represent refusal history");
        assert!(matches!(
            error,
            super::FormatError::LossyConversionBlocked { ref field, .. }
                if field == "messages[].content[].type"
        ));
    }
}

#[test]
fn pure_openai_responses_cross_format_rejects_opaque_input_item() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "program",
            "call_id": "program-call-1",
            "fingerprint": "fp-program-1"
        }]
    });

    let error = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect_err("provider-scoped opaque item must fail closed across formats");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "input[0]"
    ));
}

#[test]
fn openai_responses_additional_tools_prefix_maps_to_chat_tools() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [
            {
                "type": "additional_tools",
                "role": "developer",
                "tools": [{
                    "type": "function",
                    "name": "lookup",
                    "description": "Look up a value",
                    "parameters": {
                        "type": "object",
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"]
                    },
                    "strict": true
                }, {
                    "type": "custom",
                    "name": "shell_command",
                    "description": "Run a shell command",
                    "format": {"type": "text"}
                }]
            },
            {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "hello"}]}
        ],
        "tools": [{
            "type": "function",
            "name": "existing",
            "parameters": {"type": "object"}
        }]
    });

    for converted in [
        convert_request_pure("openai:responses", "openai:chat", &body)
            .expect("pure conversion should map additional tools")
            .value,
        convert_request(
            "openai:responses",
            "openai:chat",
            &body,
            &FormatContext::default(),
        )
        .expect("runtime conversion should map additional tools"),
    ] {
        assert_eq!(converted["tools"][0]["type"], "function");
        assert_eq!(converted["tools"][0]["function"]["name"], "lookup");
        assert_eq!(converted["tools"][0]["function"]["strict"], true);
        assert_eq!(converted["tools"][1]["type"], "custom");
        assert_eq!(converted["tools"][1]["custom"]["name"], "shell_command");
        assert_eq!(converted["tools"][2]["function"]["name"], "existing");
        assert_eq!(converted["messages"][0]["role"], "user");
        assert_eq!(converted["messages"].as_array().map(Vec::len), Some(1));
    }
}

#[test]
fn openai_responses_additional_tools_prefix_rejects_unmapped_tool() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "additional_tools",
            "role": "developer",
            "tools": [{"type": "tool_search", "execution": "client"}]
        }]
    });

    let error = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect_err("Chat cannot represent client tool_search");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "tools"
    ));
}

#[test]
fn openai_responses_additional_tools_prefix_rejects_unknown_fields() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "additional_tools",
            "role": "developer",
            "tools": [],
            "future_field": true
        }]
    });

    let error = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect_err("unknown additional_tools fields must not be dropped");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "input[0]"
    ));
}

#[test]
fn openai_responses_additional_tools_is_only_consumed_as_a_leading_prefix() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [
            {"type": "message", "role": "user", "content": "hello"},
            {
                "type": "additional_tools",
                "role": "developer",
                "tools": [{"type": "function", "name": "lookup", "parameters": {}}]
            }
        ]
    });

    let error = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect_err("additional_tools after conversation history must remain unsupported");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "input[1]"
    ));
}

#[test]
fn runtime_openai_responses_cross_format_rejects_unknown_content_block() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                {"type": "input_text", "text": "hello"},
                {"type": "future_content_block", "payload": {"keep": true}}
            ]
        }]
    });

    let error = convert_request(
        "openai:responses",
        "claude:messages",
        &body,
        &FormatContext::default(),
    )
    .expect_err("runtime conversion must fail closed for raw provider content blocks");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "input[0].content[1]"
    ));
}

#[test]
fn openai_responses_compaction_trigger_is_not_converted_to_gemini() {
    let body = json!({
        "model": "gemini-3.7-flash",
        "input": [
            {"type": "message", "role": "user", "content": "hello"},
            {"type": "compaction_trigger"}
        ]
    });

    let error = convert_request_pure("openai:responses", "gemini:generate_content", &body)
        .expect_err("Gemini cannot represent the Responses compaction control item");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "input[1]"
    ));
}

#[test]
fn openai_responses_rejects_official_multi_agent_incompatibilities() {
    let cases = [
        (
            "openai:responses:compact",
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "multi_agent": {"enabled": true}
            }),
            "multi_agent",
        ),
        (
            "openai:responses",
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "multi_agent": {"enabled": true},
                "reasoning": {"effort": "high", "summary": "auto"}
            }),
            "reasoning.summary",
        ),
        (
            "openai:responses",
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "multi_agent": {"enabled": true},
                "max_tool_calls": 8
            }),
            "max_tool_calls",
        ),
    ];

    for (format, body, expected_field) in cases {
        let error = convert_request_pure(format, format, &body)
            .expect_err("invalid multi-agent combination must be rejected");
        assert!(matches!(
            error,
            super::FormatError::InvalidTargetField { ref field, .. }
                if field == expected_field
        ));
    }
}

#[test]
fn openai_responses_multi_agent_disabled_does_not_trigger_incompatibilities() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": "hello",
        "multi_agent": {"enabled": false},
        "reasoning": {"effort": "high", "summary": "auto"},
        "max_tool_calls": 8
    });

    let converted = convert_request_pure("openai:responses", "openai:responses", &body)
        .expect("disabled multi-agent config must not activate incompatibility rules");

    assert_eq!(converted.value["multi_agent"]["enabled"], false);
}

#[test]
fn openai_prompt_cache_contract_converts_responses_to_chat_without_loss() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "store": false,
        "user": "user_123",
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"},
        "input": [{
            "type": "message",
            "role": "user",
            "content": [
                {
                    "type": "input_text",
                    "text": "stable text",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {
                    "type": "input_image",
                    "image_url": "https://example.com/stable.png",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {
                    "type": "input_file",
                    "file_id": "file_123",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                }
            ]
        }]
    });

    let converted = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("official Responses cache contract should map to Chat")
        .value;

    assert_eq!(
        converted["prompt_cache_options"],
        body["prompt_cache_options"]
    );
    assert_eq!(converted["store"], false);
    assert_eq!(converted["user"], "user_123");
    let parts = converted["messages"][0]["content"]
        .as_array()
        .expect("chat content should remain structured");
    assert_eq!(parts.len(), 3);
    assert!(parts
        .iter()
        .all(|part| { part["prompt_cache_breakpoint"] == json!({"mode": "explicit"}) }));
}

#[test]
fn openai_prompt_cache_contract_converts_chat_to_responses_without_loss() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "store": false,
        "user": "user_123",
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"},
        "messages": [{
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": "stable text",
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {
                    "type": "image_url",
                    "image_url": {"url": "https://example.com/stable.png"},
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                },
                {
                    "type": "file",
                    "file": {"file_id": "file_123"},
                    "prompt_cache_breakpoint": {"mode": "explicit"}
                }
            ]
        }]
    });

    let converted = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect("official Chat cache contract should map to Responses")
        .value;

    assert_eq!(
        converted["prompt_cache_options"],
        body["prompt_cache_options"]
    );
    assert_eq!(converted["store"], false);
    assert_eq!(converted["user"], "user_123");
    let parts = converted["input"][0]["content"]
        .as_array()
        .expect("Responses content should remain structured");
    assert_eq!(parts.len(), 3);
    assert!(parts
        .iter()
        .all(|part| { part["prompt_cache_breakpoint"] == json!({"mode": "explicit"}) }));
}

#[test]
fn openai_prompt_cache_contract_is_preserved_for_compact() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "prompt_cache_key": "tenant:compact",
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"},
        "messages": [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": "stable text",
                "prompt_cache_breakpoint": {"mode": "explicit"}
            }]
        }]
    });

    let converted = convert_request_pure("openai:chat", "openai:responses:compact", &body)
        .expect("Compact supports the GPT-5.6 prompt cache contract")
        .value;

    assert_eq!(converted["prompt_cache_key"], "tenant:compact");
    assert_eq!(
        converted["prompt_cache_options"],
        json!({"mode": "explicit", "ttl": "30m"})
    );
    assert_eq!(
        converted["input"][0]["content"][0]["prompt_cache_breakpoint"],
        json!({"mode": "explicit"})
    );
}

#[test]
fn openai_prompt_cache_contract_uses_gpt_5_6_options_and_rejects_invalid_values() {
    let retention = json!({
        "model": "gpt-5.6-sol",
        "messages": [{"role": "user", "content": "hello"}],
        "prompt_cache_retention": "24h"
    });
    let error = convert_request_pure("openai:chat", "openai:responses", &retention)
        .expect_err("GPT-5.6 uses prompt_cache_options.ttl");
    assert!(matches!(
        error,
        super::FormatError::InvalidTargetField { ref field, .. }
            if field == "prompt_cache_retention"
    ));

    let invalid_options = json!({
        "model": "gpt-5.6-sol",
        "input": "hello",
        "prompt_cache_options": {"mode": "explicit", "ttl": "1h"}
    });
    let error = convert_request_pure("openai:responses", "openai:chat", &invalid_options)
        .expect_err("cross-format conversion must validate cache option enums");
    assert!(matches!(
        error,
        super::FormatError::InvalidEnumValue { ref field, .. }
            if field == "prompt_cache_options.ttl"
    ));
}

#[test]
fn openai_prompt_cache_contract_preserves_earlier_model_retention() {
    let body = json!({
        "model": "gpt-5.5",
        "input": "hello",
        "prompt_cache_retention": "24h"
    });
    let converted = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect("GPT-5.5 24h retention should map between OpenAI APIs")
        .value;

    assert_eq!(converted["prompt_cache_retention"], "24h");
}

#[test]
fn runtime_openai_prompt_cache_contract_uses_mapped_provider_model() {
    let options = json!({
        "model": "gpt-5.5",
        "messages": [{"role": "user", "content": "hello"}],
        "prompt_cache_options": {"mode": "explicit", "ttl": "30m"}
    });
    let converted = convert_request(
        "openai:chat",
        "openai:responses",
        &options,
        &FormatContext::default().with_mapped_model("gpt-5.6-sol"),
    )
    .expect("mapped GPT-5.6 provider model should accept prompt_cache_options");
    assert_eq!(
        converted["prompt_cache_options"],
        options["prompt_cache_options"]
    );

    let error = convert_request(
        "openai:chat",
        "openai:responses",
        &options,
        &FormatContext::default().with_mapped_model("gpt-5.5"),
    )
    .expect_err("mapped earlier provider model should reject prompt_cache_options");
    assert!(matches!(
        error,
        super::FormatError::InvalidTargetField { ref field, .. }
            if field == "prompt_cache_options"
    ));

    let retention = json!({
        "model": "gpt-5.6-sol",
        "input": "hello",
        "prompt_cache_retention": "24h"
    });
    let converted = convert_request(
        "openai:responses",
        "openai:chat",
        &retention,
        &FormatContext::default().with_mapped_model("gpt-5.5"),
    )
    .expect("mapped earlier provider model should accept its retention contract");
    assert_eq!(converted["prompt_cache_retention"], "24h");

    let error = convert_request(
        "openai:responses",
        "openai:chat",
        &retention,
        &FormatContext::default().with_mapped_model("gpt-5.6-luna"),
    )
    .expect_err("mapped GPT-5.6 provider model uses prompt_cache_options.ttl");
    assert!(matches!(
        error,
        super::FormatError::InvalidTargetField { ref field, .. }
            if field == "prompt_cache_retention"
    ));
}

#[test]
fn openai_cross_format_store_only_maps_false() {
    for (source, target, body) in [
        (
            "openai:chat",
            "openai:responses",
            json!({
                "model": "gpt-5.6-sol",
                "messages": [{"role": "user", "content": "hello"}],
                "store": true
            }),
        ),
        (
            "openai:responses",
            "openai:chat",
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "store": true
            }),
        ),
    ] {
        let error = convert_request_pure(source, target, &body)
            .expect_err("store=true has different behavior across the OpenAI APIs");
        assert!(matches!(
            error,
            super::FormatError::LossyConversionBlocked { ref field, .. }
                if field == "store"
        ));
    }
}

#[test]
fn openai_chat_store_is_validated_then_omitted_for_compact() {
    for store in [true, false] {
        let converted = convert_request_pure(
            "openai:chat",
            "openai:responses:compact",
            &json!({
                "model": "gpt-5.6-sol",
                "messages": [{"role": "user", "content": "hello"}],
                "store": store
            }),
        )
        .expect("Compact should validate and omit the transport-ineligible store field");
        assert!(converted.value.get("store").is_none());
    }

    let error = convert_request_pure(
        "openai:chat",
        "openai:responses:compact",
        &json!({
            "model": "gpt-5.6-sol",
            "messages": [{"role": "user", "content": "hello"}],
            "store": "true"
        }),
    )
    .expect_err("Compact should reject a non-boolean store field");
    assert!(matches!(
        error,
        super::FormatError::InvalidTargetField { ref field, .. }
            if field == "store"
    ));
}

#[test]
fn openai_chat_audio_cache_breakpoint_fails_closed_for_responses() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "messages": [{
            "role": "user",
            "content": [{
                "type": "input_audio",
                "input_audio": {"data": "ZmFrZQ==", "format": "mp3"},
                "prompt_cache_breakpoint": {"mode": "explicit"}
            }]
        }]
    });

    let error = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect_err("Responses does not support cache breakpoints on audio blocks");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "messages[].content[].prompt_cache_breakpoint"
    ));
}

#[test]
fn openai_chat_assistant_text_cache_breakpoint_fails_closed_for_responses() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "messages": [{
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": "previous output",
                "prompt_cache_breakpoint": {"mode": "explicit"}
            }]
        }]
    });

    let error = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect_err("assistant text maps to output_text, which cannot carry a breakpoint");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "messages[].content[].prompt_cache_breakpoint"
    ));
}

#[test]
fn runtime_responses_cross_format_blocks_advanced_reasoning_and_caller_fields() {
    for (body, expected_field) in [
        (
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "reasoning": {"effort": "high", "mode": "pro"}
            }),
            "thinking.openai_responses.mode",
        ),
        (
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "reasoning": {"effort": "high", "context": "all_turns"}
            }),
            "thinking.openai_responses.context",
        ),
        (
            json!({
                "model": "gpt-5.6-sol",
                "input": "hello",
                "multi_agent": {"enabled": true}
            }),
            "multi_agent",
        ),
        (
            json!({
                "model": "gpt-5.6-sol",
                "input": [{
                    "type": "function_call",
                    "call_id": "call_1",
                    "name": "lookup",
                    "arguments": "{}",
                    "caller": {"type": "multi_agent", "id": "agent_1"}
                }]
            }),
            "input[0].caller",
        ),
        (
            json!({
                "model": "gpt-5.6-sol",
                "input": [{
                    "type": "web_search_call",
                    "id": "search_1",
                    "action": {"query": "latest docs"}
                }]
            }),
            "input[0]",
        ),
    ] {
        let error = convert_request(
            "openai:responses",
            "openai:chat",
            &body,
            &FormatContext::default(),
        )
        .expect_err("runtime conversion must fail closed for provider-only fields");
        assert!(
            matches!(
                error,
                super::FormatError::LossyConversionBlocked { ref field, .. }
                    if field == expected_field
            ),
            "unexpected conversion error for {expected_field}: {error:?}"
        );
    }
}

#[test]
fn pure_openai_chat_to_claude_blocks_target_unsupported_generation_field() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{"role": "user", "content": "hello"}],
        "top_logprobs": 2
    });

    let error = convert_request_pure("openai:chat", "claude:messages", &body)
        .expect_err("target-unsupported generation field should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "top_logprobs"
    ));
}

#[test]
fn pure_openai_chat_to_gemini_blocks_unknown_content_part_loss() {
    let body = json!({
        "model": "gpt-source",
        "messages": [{
            "role": "user",
            "content": [{
                "type": "input_video",
                "input_video": {"url": "https://example.com/movie.mp4"}
            }]
        }]
    });

    let error = convert_request_pure("openai:chat", "gemini:generate_content", &body)
        .expect_err("unknown content part should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "messages[].content[].type"
    ));
}

#[test]
fn pure_openai_responses_to_chat_blocks_unmapped_context_management() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "context_management": []
    });

    let error = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect_err("Responses context_management should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "openai_responses.context_management"
    ));
}

#[test]
fn pure_claude_to_openai_chat_blocks_container_loss() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 64,
        "container": "container_123"
    });

    let error = convert_request_pure("claude:messages", "openai:chat", &body)
        .expect_err("Claude container should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "claude.container"
    ));
}

#[test]
fn pure_gemini_to_openai_chat_blocks_service_tier_loss() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "serviceTier": "priority"
    });

    let error = convert_request_pure("gemini:generate_content", "openai:chat", &body)
        .expect_err("Gemini serviceTier should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "gemini.serviceTier"
    ));
}

#[test]
fn pure_openai_cross_format_preserves_reasoning_effort_contract() {
    for effort in ["none", "low", "medium", "high", "xhigh", "max"] {
        let chat = json!({
            "model": "gpt-5.6-sol",
            "messages": [{"role": "user", "content": "hello"}],
            "reasoning_effort": effort
        });
        let responses = convert_request_pure("openai:chat", "openai:responses", &chat)
            .expect("Chat effort should convert to Responses")
            .value;
        assert_eq!(responses["reasoning"]["effort"], effort);

        let responses = json!({
            "model": "gpt-5.6-sol",
            "input": [{"role": "user", "content": "hello"}],
            "reasoning": {"effort": effort}
        });
        let chat = convert_request_pure("openai:responses", "openai:chat", &responses)
            .expect("Responses effort should convert to Chat")
            .value;
        assert_eq!(chat["reasoning_effort"], effort);
    }
}

#[test]
fn pure_openai_responses_to_chat_drops_reasoning_summary() {
    for summary in ["auto", "concise", "detailed"] {
        let body = json!({
            "model": "gpt-5.6-sol",
            "input": [{"role": "user", "content": "hello"}],
            "reasoning": {
                "effort": "high",
                "summary": summary
            }
        });

        let converted = convert_request_pure("openai:responses", "openai:chat", &body)
            .expect("Responses reasoning summary should be safely omitted for Chat")
            .value;

        assert_eq!(converted["reasoning_effort"], "high");
        assert!(converted.get("reasoning").is_none());
    }

    let summary_only = json!({
        "model": "gpt-5.6-sol",
        "input": "hello",
        "reasoning": {"summary": "auto"}
    });
    let converted = convert_request_pure("openai:responses", "openai:chat", &summary_only)
        .expect("Responses summary alone should be safely omitted for Chat")
        .value;

    assert!(converted.get("reasoning_effort").is_none());
    assert!(converted.get("reasoning").is_none());
}

#[test]
fn runtime_openai_responses_to_chat_drops_reasoning_summary() {
    let body = json!({
        "model": "deployment-alias",
        "input": "hello",
        "reasoning": {
            "effort": "high",
            "summary": "auto"
        }
    });

    let converted = convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default().with_mapped_model("qwen3.6-upstream-35004"),
    )
    .expect("runtime Responses summary should not block Chat provider body construction");

    assert_eq!(converted["model"], "qwen3.6-upstream-35004");
    assert_eq!(converted["reasoning_effort"], "high");
    assert!(converted.get("reasoning").is_none());
}

#[test]
fn pure_openai_responses_to_chat_rejects_reasoning_budget_tokens() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "input": [{"role": "user", "content": "hello"}],
        "reasoning": {
            "effort": "high",
            "budget_tokens": 4096
        }
    });

    let error = convert_request_pure("openai:responses", "openai:chat", &body)
        .expect_err("Responses reasoning budget must remain fail-closed for Chat");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "reasoning.budget_tokens"
    ));
}

#[test]
fn runtime_openai_cross_format_preserves_gpt_5_6_reasoning_efforts() {
    for target in ["openai:responses", "openai:responses:compact"] {
        for effort in ["none", "low", "medium", "high", "xhigh", "max"] {
            let chat = json!({
                "model": "deployment-alias",
                "messages": [{"role": "user", "content": "hello"}],
                "reasoning_effort": effort
            });
            let responses = convert_request(
                "openai:chat",
                target,
                &chat,
                &FormatContext::default().with_mapped_model("gpt-5.6-sol"),
            )
            .expect("runtime Chat effort should convert to Responses");
            assert_eq!(responses["reasoning"]["effort"], effort, "target: {target}");
        }
    }
}

#[test]
fn runtime_reasoning_effort_is_preserved_across_concrete_model_mapping() {
    let alias_to_gpt_5_6 = json!({
        "model": "deployment-alias",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "max"
    });
    let converted = convert_request(
        "openai:chat",
        "openai:responses",
        &alias_to_gpt_5_6,
        &FormatContext::default().with_mapped_model("gpt-5.6-sol"),
    )
    .expect("a concrete GPT-5.6 mapped target should accept max");
    assert_eq!(converted["model"], "gpt-5.6-sol");
    assert_eq!(converted["reasoning"]["effort"], "max");

    let gpt_5_6_to_gpt_5_4 = json!({
        "model": "gpt-5.6-sol",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "max"
    });
    let converted = convert_request(
        "openai:chat",
        "openai:responses",
        &gpt_5_6_to_gpt_5_4,
        &FormatContext::default().with_mapped_model("gpt-5.4"),
    )
    .expect("mapped model capability cards must not reject explicit efforts");
    assert_eq!(converted["model"], "gpt-5.4");
    assert_eq!(converted["reasoning"]["effort"], "max");
}

#[test]
fn pure_conversion_preserves_effort_across_concrete_model_mapping() {
    let alias_to_gpt_5_6 = json!({
        "model": "deployment-alias",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "max"
    });
    let converted = convert_request_pure_with_context(
        "openai:chat",
        "openai:responses",
        &alias_to_gpt_5_6,
        &FormatContext::default().with_mapped_model("gpt-5.6-sol"),
    )
    .expect("a concrete GPT-5.6 target should accept max");
    assert_eq!(converted.value["reasoning"]["effort"], "max");

    let gpt_5_6_to_gpt_5_4 = json!({
        "model": "gpt-5.6-sol",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "max"
    });
    let converted = convert_request_pure_with_context(
        "openai:chat",
        "openai:responses",
        &gpt_5_6_to_gpt_5_4,
        &FormatContext::default().with_mapped_model("gpt-5.4"),
    )
    .expect("mapped model capability cards must not reject explicit efforts");
    assert_eq!(converted.value["reasoning"]["effort"], "max");
}

#[test]
fn runtime_openai_cross_format_preserves_explicit_efforts() {
    let alias_minimal = json!({
        "model": "deployment-alias",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "minimal"
    });
    for target in ["openai:responses", "openai:responses:compact"] {
        let converted = convert_request(
            "openai:chat",
            target,
            &alias_minimal,
            &FormatContext::default().with_mapped_model("gpt-5.6-terra"),
        )
        .expect("mapped model capability cards must not reject explicit efforts");
        assert_eq!(converted["reasoning"]["effort"], "minimal");
    }

    let custom = json!({
        "model": "gpt-5.6-sol",
        "input": [{"role": "user", "content": "hello"}],
        "reasoning": {"effort": "future"}
    });
    for source in ["openai:responses", "openai:responses:compact"] {
        let converted = convert_request(
            source,
            "openai:chat",
            &custom,
            &FormatContext::default().with_mapped_model("gpt-5.6-sol"),
        )
        .expect("model-advertised effort should survive runtime conversion");
        assert_eq!(converted["reasoning_effort"], "future");
    }

    let ultra = json!({
        "model": "gpt-5.6-sol",
        "input": [{"role": "user", "content": "hello"}],
        "reasoning": {"effort": "ultra"}
    });
    for source in ["openai:responses", "openai:responses:compact"] {
        let converted = convert_request(
            source,
            "openai:chat",
            &ultra,
            &FormatContext::default().with_mapped_model("gpt-5.6-sol"),
        )
        .expect("explicit effort support should be validated by the upstream");
        assert_eq!(converted["reasoning_effort"], "ultra");
    }
}

#[test]
fn pure_openai_cross_format_preserves_custom_reasoning_effort() {
    let body = json!({
        "model": "gpt-5.6-sol",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "future"
    });

    let converted = convert_request_pure("openai:chat", "openai:responses", &body)
        .expect("custom effort should remain wire-compatible");
    assert_eq!(converted.value["reasoning"]["effort"], "future");

    let ultra = json!({
        "model": "gpt-5.6-sol",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "ultra"
    });
    let converted = convert_request_pure("openai:chat", "openai:responses", &ultra)
        .expect("explicit effort support should be validated by the upstream");
    assert_eq!(converted.value["reasoning"]["effort"], "ultra");
}

#[test]
fn pure_openai_cross_format_preserves_unpublished_effort() {
    let chat = json!({
        "model": "gpt-5.6-sol",
        "messages": [{"role": "user", "content": "hello"}],
        "reasoning_effort": "minimal"
    });
    let converted = convert_request_pure("openai:chat", "openai:responses", &chat)
        .expect("upstream should validate unpublished reasoning efforts");
    assert_eq!(converted.value["reasoning"]["effort"], "minimal");

    let responses = json!({
        "model": "gpt-5.6-terra",
        "input": [{"role": "user", "content": "hello"}],
        "reasoning": {"effort": "minimal"}
    });
    let converted = convert_request_pure("openai:responses", "openai:chat", &responses)
        .expect("upstream should validate unpublished reasoning efforts");
    assert_eq!(converted.value["reasoning_effort"], "minimal");
}

#[test]
fn pure_response_conversion_rejects_unknown_openai_chat_finish_reason() {
    let body = json!({
        "id": "chatcmpl_unknown_finish",
        "object": "chat.completion",
        "model": "gpt-source",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "hello"
            },
            "finish_reason": "future_reason"
        }]
    });

    let error = convert_response_pure("openai:chat", "claude:messages", &body)
        .expect_err("unknown OpenAI finish reason should fail closed cross-format");

    assert!(matches!(
        error,
        super::FormatError::InvalidEnumValue { ref field, ref value, .. }
            if field == "choices[].finish_reason" && value == "future_reason"
    ));
}

#[test]
fn pure_response_conversion_blocks_known_but_unmappable_gemini_finish_reason() {
    let body = json!({
        "responseId": "resp_other_finish",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [{
            "index": 0,
            "content": {
                "role": "model",
                "parts": [{"text": "hello"}]
            },
            "finishReason": "OTHER"
        }]
    });

    let error = convert_response_pure("gemini:generate_content", "openai:chat", &body)
        .expect_err("unmappable Gemini finish reason should fail closed cross-format");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "candidates[].finishReason"
    ));
}

#[test]
fn pure_response_conversion_preserves_openai_responses_content_filter_finish_reason() {
    let body = json!({
        "id": "resp_content_filter",
        "object": "response",
        "model": "gpt-source",
        "status": "incomplete",
        "incomplete_details": {
            "reason": "content_filter"
        },
        "output": [{
            "type": "message",
            "id": "msg_content_filter",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "blocked"
            }]
        }]
    });

    let converted = convert_response_pure("openai:responses", "openai:chat", &body)
        .expect("content_filter incomplete reason should map to Chat")
        .value;

    assert_eq!(converted["choices"][0]["finish_reason"], "content_filter");
}

#[test]
fn pure_response_conversion_reports_response_field_mappings() {
    let body = json!({
        "id": "resp_report",
        "object": "response",
        "model": "gpt-source",
        "status": "completed",
        "output": [{
            "type": "message",
            "id": "msg_report",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "hello"
            }]
        }]
    });

    let converted = convert_response_pure("openai:responses", "openai:chat", &body)
        .expect("response conversion should succeed");

    assert!(converted.report.fields.iter().any(|field| {
        field.field == "output" && field.status == super::ConversionFieldStatus::Mapped
    }));
    assert!(converted.report.fields.iter().any(|field| {
        field.field == "choices" && field.status == super::ConversionFieldStatus::Mapped
    }));
}

#[test]
fn pure_response_conversion_blocks_non_terminal_openai_responses_status() {
    let body = json!({
        "id": "resp_queued",
        "object": "response",
        "model": "gpt-source",
        "status": "queued",
        "output": [{
            "type": "message",
            "id": "msg_queued",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "not final"
            }]
        }]
    });

    let error = convert_response_pure("openai:responses", "openai:chat", &body)
        .expect_err("non-terminal Responses status should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. } if field == "status"
    ));
}

#[test]
fn pure_openai_chat_response_same_format_preserves_unknown_finish_reason() {
    let body = json!({
        "id": "chatcmpl_unknown_finish",
        "object": "chat.completion",
        "model": "gpt-source",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "hello"
            },
            "finish_reason": "future_reason"
        }]
    });

    let converted = convert_response_pure("openai:chat", "openai:chat", &body)
        .expect("same-format response roundtrip should preserve unknown finish reason")
        .value;

    assert_eq!(converted["choices"][0]["finish_reason"], "future_reason");
}

#[test]
fn pure_openai_responses_response_same_format_preserves_incomplete_status() {
    let body = json!({
        "id": "resp_incomplete",
        "object": "response",
        "model": "gpt-source",
        "status": "incomplete",
        "incomplete_details": {
            "reason": "max_output_tokens"
        },
        "output": [{
            "type": "message",
            "id": "msg_incomplete",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "partial"
            }]
        }]
    });

    let converted = convert_response_pure("openai:responses", "openai:responses", &body)
        .expect("same-format response roundtrip should preserve Responses status")
        .value;

    assert_eq!(converted["status"], "incomplete");
    assert_eq!(
        converted["incomplete_details"]["reason"],
        "max_output_tokens"
    );
}

#[test]
fn pure_openai_responses_response_same_format_preserves_raw_output_items() {
    let body = json!({
        "id": "resp_raw_items",
        "object": "response",
        "model": "gpt-source",
        "status": "completed",
        "output": [
            {
                "type": "file_search_call",
                "id": "fs_123",
                "status": "completed",
                "queries": ["rust"],
                "results": [{"file_id": "file_123", "text": "Rust"}]
            },
            {
                "type": "code_interpreter_call",
                "id": "ci_123",
                "status": "completed",
                "code": "print('hi')",
                "outputs": []
            },
            {
                "type": "message",
                "id": "msg_123",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "done"}]
            }
        ]
    });

    let converted = convert_response_pure("openai:responses", "openai:responses", &body)
        .expect("same-format Responses response should preserve raw output items")
        .value;

    assert_eq!(converted["output"], body["output"]);
}

#[test]
fn pure_openai_responses_response_preserves_ptc_and_blocks_lossy_targets() {
    let body = json!({
        "id": "resp_ptc",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [
            {
                "type": "function_call",
                "id": "fc_1",
                "call_id": "call_1",
                "status": "completed",
                "name": "lookup",
                "arguments": "{\"id\":7}",
                "caller": {"type": "program", "id": "program_1"}
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "found",
                "caller": {"type": "program", "id": "program_1"}
            }
        ]
    });

    let same_format = convert_response_pure("openai:responses", "openai:responses", &body)
        .expect("same-format response should preserve PTC output items")
        .value;
    assert_eq!(same_format["output"], body["output"]);

    for target in ["openai:chat", "claude:messages", "gemini:generate_content"] {
        let error = convert_response_pure("openai:responses", target, &body)
            .expect_err("target response format cannot preserve PTC caller provenance");
        assert!(matches!(
            error,
            super::FormatError::LossyConversionBlocked { ref field, .. }
                if field.ends_with(".caller")
        ));
    }
}

#[test]
fn chat_backed_function_call_metadata_is_only_droppable_for_chat_targets() {
    let body = json!({
        "id": "resp_chat_metadata",
        "object": "response",
        "model": "gpt-5",
        "status": "completed",
        "output": [{
            "type": "function_call",
            "id": "fc_chat_metadata",
            "call_id": "call_chat_metadata",
            "status": "completed",
            "name": "lookup",
            "arguments": "{}",
            "metadata": {"source": "chat"},
            "internal_chat_message_metadata_passthrough": {
                "turn_id": "turn_123"
            }
        }]
    });

    let chat = convert_response_pure("openai:responses", "openai:chat", &body)
        .expect("Chat-backed metadata should be droppable for a Chat client")
        .value;
    assert_eq!(
        chat["choices"][0]["message"]["tool_calls"][0]["id"],
        "call_chat_metadata"
    );

    for target in ["claude:messages", "gemini:generate_content"] {
        let error = convert_response_pure("openai:responses", target, &body)
            .expect_err("non-Chat targets must reject Chat-private metadata");
        assert!(
            matches!(
                error,
                super::FormatError::LossyConversionBlocked { ref field, .. }
                    if field.ends_with(".metadata")
                        || field.contains("internal_chat_message_metadata_passthrough")
            ),
            "{target}: {error:?}"
        );
    }

    let mut unpaired = body;
    unpaired["output"][0]
        .as_object_mut()
        .expect("function call should be an object")
        .remove("internal_chat_message_metadata_passthrough");
    let error = convert_response_pure("openai:responses", "openai:chat", &unpaired)
        .expect_err("generic metadata without the Chat-private marker must be rejected");
    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field.ends_with(".metadata")
    ));
}

#[test]
fn pure_openai_responses_response_cross_format_blocks_raw_output_items() {
    let body = json!({
        "id": "resp_raw_items",
        "object": "response",
        "model": "gpt-source",
        "status": "completed",
        "output": [{
            "type": "mcp_call",
            "id": "mcp_123",
            "status": "completed",
            "name": "lookup"
        }]
    });

    let error = convert_response_pure("openai:responses", "openai:chat", &body)
        .expect_err("cross-format raw Responses output items should fail closed");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "output[].type"
    ));
}

#[test]
fn pure_openai_responses_response_refusal_only_maps_to_openai_chat() {
    let body = json!({
        "id": "resp_refusal",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [{
            "type": "message",
            "id": "msg_refusal",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "refusal", "refusal": "cannot comply"}]
        }]
    });

    let chat = convert_response_pure("openai:responses", "openai:chat", &body)
        .expect("OpenAI Chat has a refusal response field")
        .value;
    assert_eq!(chat["choices"][0]["message"]["refusal"], "cannot comply");

    for target in ["claude:messages", "gemini:generate_content"] {
        let error = convert_response_pure("openai:responses", target, &body)
            .expect_err("target response format cannot preserve refusal content");
        assert!(matches!(
            error,
            super::FormatError::LossyConversionBlocked { ref field, .. }
                if field == "output[].type"
        ));
    }
}

#[test]
fn pure_openai_responses_response_blocks_unknown_message_item_fields_cross_format() {
    let body = json!({
        "id": "resp_message_extension",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [{
            "type": "message",
            "id": "msg_extension",
            "status": "completed",
            "role": "assistant",
            "future_message_option": {"mode": "verified"},
            "content": [{"type": "output_text", "text": "done"}]
        }]
    });

    let same_format = convert_response_pure("openai:responses", "openai:responses", &body)
        .expect("same-format response should preserve message item fields")
        .value;
    assert_eq!(same_format["output"], body["output"]);

    for target in ["openai:chat", "claude:messages", "gemini:generate_content"] {
        let error = convert_response_pure("openai:responses", target, &body)
            .expect_err("target response format cannot preserve message item extensions");
        assert!(matches!(
            error,
            super::FormatError::LossyConversionBlocked { ref field, .. }
                if field == "output[0].future_message_option"
        ));
    }
}

#[test]
fn pure_openai_responses_response_preserves_usage_and_audits_unknown_details() {
    let body = json!({
        "id": "resp_usage_extension",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [],
        "usage": {
            "input_tokens": 120,
            "output_tokens": 30,
            "total_tokens": 150,
            "input_tokens_details": {
                "cached_tokens": 40,
                "future_cache_tokens": 3
            },
            "output_tokens_details": {
                "reasoning_tokens": 20,
                "future_reasoning_tokens": 2
            },
            "future_usage_option": "metered"
        }
    });

    let same_format = convert_response_pure("openai:responses", "openai:responses", &body)
        .expect("same-format response should preserve usage extensions")
        .value;
    assert_eq!(same_format["usage"], body["usage"]);

    for target in ["openai:chat", "claude:messages", "gemini:generate_content"] {
        let error = convert_response_pure("openai:responses", target, &body)
            .expect_err("target response format cannot preserve unknown usage details");
        assert!(matches!(
            error,
            super::FormatError::LossyConversionBlocked { ref field, .. }
                if field == "usage.input_tokens_details.future_cache_tokens"
        ));
    }
}

#[test]
fn pure_claude_response_same_format_preserves_unknown_stop_reason() {
    let body = json!({
        "id": "msg_unknown_stop",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet",
        "content": [{
            "type": "text",
            "text": "hello"
        }],
        "stop_reason": "future_reason",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 1,
            "output_tokens": 1
        }
    });

    let converted = convert_response_pure("claude:messages", "claude:messages", &body)
        .expect("same-format response roundtrip should preserve unknown stop reason")
        .value;

    assert_eq!(converted["stop_reason"], "future_reason");
    assert_eq!(converted["stop_sequence"], json!(null));
}

#[test]
fn pure_gemini_response_same_format_preserves_unknown_finish_reason() {
    let body = json!({
        "responseId": "resp_unknown_finish",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [{
            "index": 0,
            "content": {
                "role": "model",
                "parts": [{"text": "hello"}]
            },
            "finishReason": "FUTURE_REASON"
        }]
    });

    let converted =
        convert_response_pure("gemini:generate_content", "gemini:generate_content", &body)
            .expect("same-format response roundtrip should preserve unknown finish reason")
            .value;

    assert_eq!(converted["candidates"][0]["finishReason"], "FUTURE_REASON");
}

#[test]
fn pure_claude_same_format_roundtrip_preserves_raw_tool_input_schema() {
    let body = json!({
        "model": "claude-sonnet",
        "messages": [{"role": "user", "content": "hello"}],
        "max_tokens": 64,
        "tools": [{
            "name": "lookup",
            "description": "Lookup",
            "input_schema": {"type": "object"}
        }]
    });

    let converted = convert_request_pure("claude:messages", "claude:messages", &body)
        .expect("same-format roundtrip should succeed")
        .value;

    assert_eq!(
        converted["tools"][0]["input_schema"],
        json!({"type": "object"})
    );
}

#[test]
fn pure_gemini_same_format_roundtrip_preserves_raw_function_parameters() {
    let body = json!({
        "contents": [{"role": "user", "parts": [{"text": "hello"}]}],
        "tools": [{
            "functionDeclarations": [{
                "name": "lookup",
                "description": "Lookup",
                "parameters": {"type": "object"}
            }]
        }]
    });

    let converted =
        convert_request_pure("gemini:generate_content", "gemini:generate_content", &body)
            .expect("same-format roundtrip should succeed")
            .value;

    assert_eq!(
        converted["tools"][0]["functionDeclarations"][0]["parameters"],
        json!({"type": "object"})
    );
}

#[test]
fn runtime_openai_responses_to_chat_rejects_missing_previous_response_history() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "include": ["reasoning.encrypted_content"],
        "previous_response_id": "resp_123",
        "stream": true
    });

    let error = convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default(),
    )
    .expect_err("missing previous response history must fail closed");

    assert!(matches!(
        error,
        FormatError::UnsupportedField { ref field, .. }
            if field == "previous_response_id"
    ));
}

#[test]
fn runtime_openai_responses_to_chat_drops_include() {
    let body = json!({
        "model": "gpt-source",
        "input": [{"role": "user", "content": "hello"}],
        "include": ["reasoning.encrypted_content"],
        "stream": true
    });

    let converted = convert_request(
        "openai:responses",
        "openai:chat",
        &body,
        &FormatContext::default().with_upstream_stream(true),
    )
    .expect("runtime conversion should safely omit Responses include");

    assert_eq!(converted["model"], "gpt-source");
    assert_eq!(converted["messages"][0]["content"], "hello");
    assert_eq!(converted["stream"], true);
    assert!(converted.get("include").is_none());
}

#[test]
fn converts_openai_embedding_to_jina_without_chat_fields() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": ["alpha", "beta"],
        "dimensions": 2
    });
    let ctx = FormatContext::default().with_mapped_model("jina-embeddings-v3");

    let converted = convert_request("openai:embedding", "jina:embedding", &body, &ctx)
        .expect("embedding request conversion should succeed");

    assert_eq!(converted["model"], "jina-embeddings-v3");
    assert_eq!(converted["task"], "text-matching");
    assert_eq!(converted["input"], json!(["alpha", "beta"]));
    assert!(converted.get("messages").is_none());
}

#[test]
fn converts_openai_embedding_to_gemini_and_doubao_payload_shapes() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": ["alpha", "beta"],
        "dimensions": 2
    });

    let gemini = convert_request(
        "openai:embedding",
        "gemini:embedding",
        &body,
        &FormatContext::default().with_mapped_model("gemini-embedding-001"),
    )
    .expect("gemini embedding conversion should succeed");
    assert!(gemini.get("model").is_none());
    assert_eq!(
        gemini["requests"][0]["model"],
        "models/gemini-embedding-001"
    );
    assert_eq!(
        gemini["requests"][0]["content"]["parts"][0]["text"],
        "alpha"
    );
    assert_eq!(gemini["requests"][0]["outputDimensionality"], 2);
    assert!(gemini.get("messages").is_none());

    let doubao = convert_request(
        "openai:embedding",
        "doubao:embedding",
        &body,
        &FormatContext::default().with_mapped_model("doubao-embedding-text-240515"),
    )
    .expect("doubao embedding conversion should succeed");
    assert_eq!(doubao["model"], "doubao-embedding-text-240515");
    assert_eq!(doubao["input"], json!(["alpha", "beta"]));
    assert!(doubao.get("messages").is_none());
}

#[test]
fn converts_openai_embedding_to_aliyun_multimodal_payload_shape() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": [
            {"text": "white running shoes"},
            {"image": "https://example.com/shoe.png"},
            {"multi_images": ["https://example.com/a.png", "https://example.com/b.png"]}
        ],
        "dimensions": 1024,
        "parameters": {
            "enable_fusion": true,
            "res_level": 2,
            "max_video_frames": 64
        }
    });

    let converted = convert_request(
        "openai:embedding",
        "aliyun:multimodal_embedding",
        &body,
        &FormatContext::default().with_mapped_model("qwen3-vl-embedding"),
    )
    .expect("aliyun multimodal embedding conversion should succeed");

    assert_eq!(converted["model"], "qwen3-vl-embedding");
    assert_eq!(converted["input"]["contents"], body["input"]);
    assert_eq!(converted["parameters"]["dimension"], 1024);
    assert_eq!(converted["parameters"]["enable_fusion"], true);
    assert_eq!(converted["parameters"]["res_level"], 2);
    assert_eq!(converted["parameters"]["max_video_frames"], 64);
    assert!(converted.get("messages").is_none());
}

#[test]
fn pure_embedding_conversion_parses_gemini_source_to_openai() {
    let body = json!({
        "model": "models/gemini-embedding-001",
        "content": {
            "parts": [{"text": "alpha"}]
        },
        "outputDimensionality": 768
    });

    let converted = convert_request_pure("gemini:embedding", "openai:embedding", &body)
        .expect("Gemini embedding source should parse")
        .value;

    assert_eq!(converted["model"], "models/gemini-embedding-001");
    assert_eq!(converted["input"], "alpha");
    assert_eq!(converted["dimensions"], 768);
}

#[test]
fn pure_embedding_conversion_blocks_gemini_task_to_openai() {
    let body = json!({
        "model": "models/gemini-embedding-001",
        "content": {
            "parts": [{"text": "alpha"}]
        },
        "taskType": "RETRIEVAL_QUERY"
    });

    let error = convert_request_pure("gemini:embedding", "openai:embedding", &body)
        .expect_err("OpenAI embeddings have no task field");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. } if field == "task"
    ));
}

#[test]
fn pure_embedding_conversion_parses_aliyun_text_source_to_openai() {
    let body = json!({
        "model": "qwen3-vl-embedding",
        "input": {
            "contents": [
                {"text": "alpha"},
                {"text": "beta"}
            ]
        },
        "parameters": {
            "dimension": 1024
        }
    });

    let converted = convert_request_pure("aliyun:multimodal_embedding", "openai:embedding", &body)
        .expect("Aliyun text embedding source should parse")
        .value;

    assert_eq!(converted["model"], "qwen3-vl-embedding");
    assert_eq!(converted["input"], json!(["alpha", "beta"]));
    assert_eq!(converted["dimensions"], 1024);
}

#[test]
fn pure_embedding_conversion_blocks_token_input_to_gemini() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": [1, 2, 3]
    });

    let error = convert_request_pure("openai:embedding", "gemini:embedding", &body)
        .expect_err("Gemini cannot receive token-array embedding input");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. } if field == "input"
    ));
}

#[test]
fn pure_embedding_conversion_blocks_openai_only_fields_to_doubao() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": "alpha",
        "encoding_format": "base64"
    });

    let error = convert_request_pure("openai:embedding", "doubao:embedding", &body)
        .expect_err("Doubao cannot carry OpenAI encoding_format");

    assert!(matches!(
        error,
        super::FormatError::LossyConversionBlocked { ref field, .. }
            if field == "encoding_format"
    ));
}

#[test]
fn pure_embedding_conversion_blocks_unknown_provider_fields_cross_format() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": "alpha",
        "unknown_vendor_field": true
    });

    let error = convert_request_pure("openai:embedding", "jina:embedding", &body)
        .expect_err("unknown provider fields cannot be dropped cross-format");

    assert!(matches!(
        error,
        super::FormatError::UnsupportedField { ref field, .. }
            if field == "embedding.extensions"
    ));
}

#[test]
fn pure_rerank_conversion_blocks_unknown_provider_fields_cross_format() {
    let body = json!({
        "model": "rerank-model",
        "query": "rust",
        "documents": ["rust book"],
        "unknown_vendor_field": true
    });

    let error = convert_request_pure("openai:rerank", "jina:rerank", &body)
        .expect_err("unknown rerank provider fields cannot be dropped cross-format");

    assert!(matches!(
        error,
        super::FormatError::UnsupportedField { ref field, .. }
            if field == "rerank.extensions"
    ));
}

#[test]
fn aliyun_embedding_conversion_rejects_token_arrays() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": [1, 2, 3]
    });

    assert!(convert_request(
        "openai:embedding",
        "aliyun:multimodal_embedding",
        &body,
        &FormatContext::default().with_mapped_model("qwen3-vl-embedding"),
    )
    .is_err());
}

#[test]
fn multimodal_embedding_conversion_is_aliyun_only() {
    let body = json!({
        "model": "qwen3-vl-embedding",
        "input": [
            {"text": "white running shoes"},
            {"image": "https://example.com/shoe.png"}
        ]
    });
    let ctx = FormatContext::default().with_mapped_model("qwen3-vl-embedding");

    assert!(convert_request("openai:embedding", "openai:embedding", &body, &ctx).is_err());
    assert!(convert_request("openai:embedding", "jina:embedding", &body, &ctx).is_err());
    assert!(convert_request("openai:embedding", "gemini:embedding", &body, &ctx).is_err());
    assert!(convert_request("openai:embedding", "doubao:embedding", &body, &ctx).is_err());
    assert!(convert_request(
        "openai:embedding",
        "aliyun:multimodal_embedding",
        &body,
        &ctx
    )
    .is_ok());
}

#[test]
fn parses_aliyun_embedding_response_to_openai_shape() {
    let body = json!({
        "output": {
            "embeddings": [
                {
                    "index": 0,
                    "embedding": [0.1, 0.2, 0.3],
                    "type": "fused"
                }
            ]
        },
        "usage": {
            "input_tokens": 432,
            "input_tokens_details": {
                "image_tokens": 402,
                "text_tokens": 30
            },
            "output_tokens": 1,
            "total_tokens": 433
        },
        "request_id": "aliyun-request-1"
    });

    let canonical =
        crate::protocol::canonical::from_embedding_to_canonical_response(&body, "aliyun")
            .expect("aliyun embedding response should parse");
    let emitted = crate::protocol::canonical::canonical_to_embedding_response(&canonical, "openai")
        .expect("openai embedding response should emit");

    assert_eq!(emitted["request_id"], "aliyun-request-1");
    assert_eq!(emitted["data"][0]["embedding"], json!([0.1, 0.2, 0.3]));
    assert_eq!(emitted["data"][0]["type"], "fused");
    assert_eq!(emitted["usage"]["prompt_tokens"], 432);
    assert_eq!(emitted["usage"]["completion_tokens"], 1);
    assert_eq!(emitted["usage"]["total_tokens"], 433);
}

#[test]
fn embedding_registry_parses_gemini_and_doubao_sources() {
    let gemini_body = json!({
        "model": "gemini-embedding-001",
        "content": {"parts": [{"text": "alpha"}]}
    });
    let doubao_body = json!({
        "model": "doubao-embedding",
        "input": ["alpha"]
    });
    let ctx = FormatContext::default();

    let gemini = convert_request("gemini:embedding", "openai:embedding", &gemini_body, &ctx)
        .expect("Gemini embedding source should parse");
    assert_eq!(gemini["model"], "gemini-embedding-001");
    assert_eq!(gemini["input"], "alpha");

    let doubao = convert_request("doubao:embedding", "openai:embedding", &doubao_body, &ctx)
        .expect("Doubao embedding source should parse");
    assert_eq!(doubao["model"], "doubao-embedding");
    assert_eq!(doubao["input"], json!(["alpha"]));
}

#[test]
fn embedding_registry_rejects_chat_payload_for_embedding_format() {
    let body = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "hello"}]
    });
    let ctx = FormatContext::default();

    assert!(convert_request("openai:embedding", "jina:embedding", &body, &ctx).is_err());
}

#[test]
fn converts_openai_rerank_to_jina_without_chat_fields() {
    let body = json!({
        "model": "rerank-source",
        "query": "best document",
        "documents": ["alpha", {"text": "beta"}],
        "top_n": 1,
        "return_documents": true
    });
    let ctx = FormatContext::default().with_mapped_model("jina-reranker-v2-base-multilingual");

    let converted = convert_request("openai:rerank", "jina:rerank", &body, &ctx)
        .expect("rerank request conversion should succeed");

    assert_eq!(converted["model"], "jina-reranker-v2-base-multilingual");
    assert_eq!(converted["query"], "best document");
    assert_eq!(converted["documents"], json!(["alpha", {"text": "beta"}]));
    assert_eq!(converted["top_n"], 1);
    assert_eq!(converted["return_documents"], true);
    assert!(converted.get("messages").is_none());
}

#[test]
fn rerank_registry_rejects_invalid_payloads() {
    let ctx = FormatContext::default();
    for body in [
        json!({"model": "rerank", "documents": ["alpha"]}),
        json!({"model": "rerank", "query": "q", "documents": []}),
        json!({"model": "rerank", "query": "q", "documents": [""]}),
        json!({"model": "rerank", "query": "q", "documents": ["alpha"], "top_n": 0}),
    ] {
        assert!(convert_request("openai:rerank", "jina:rerank", &body, &ctx).is_err());
    }
}

#[test]
fn field_coverage_matrix_covers_all_documented_provider_schema_fields() {
    let definitions = include_str!("../../../../../../docs/api/provider-interface-definitions.md");
    let matrix = include_str!("../../../../../../docs/api/format-field-coverage-matrix.md");
    let documented = parse_documented_schema_fields(definitions);
    let covered = parse_field_coverage_matrix_fields(matrix);

    let missing = documented
        .difference(&covered)
        .take(20)
        .cloned()
        .collect::<Vec<_>>();
    let extra = covered
        .difference(&documented)
        .take(20)
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        missing.is_empty(),
        "field coverage matrix is missing documented schema fields: {missing:?}"
    );
    assert!(
        extra.is_empty(),
        "field coverage matrix contains fields not present in provider definitions: {extra:?}"
    );
    assert!(
        matrix.contains(&format!(
            "Total covered schema fields: {}.",
            documented.len()
        )),
        "field coverage matrix total must match provider-interface-definitions.md"
    );
    assert_field_coverage_statuses_are_explicit(matrix);
}

#[test]
fn request_root_field_whitelists_cover_documented_generation_request_schemas() {
    let definitions = include_str!("../../../../../../docs/api/provider-interface-definitions.md");
    let documented = parse_documented_schema_fields(definitions);

    for (format, provider, schema) in [
        (
            FormatId::OpenAiChat,
            "OpenAI",
            "CreateChatCompletionRequest",
        ),
        (FormatId::OpenAiResponses, "OpenAI", "CreateResponse"),
        (
            FormatId::OpenAiResponsesCompact,
            "OpenAI",
            "CompactResponseMethodPublicBody",
        ),
        (
            FormatId::ClaudeMessages,
            "Claude",
            "MessageCreateParamsBase",
        ),
        (
            FormatId::GeminiGenerateContent,
            "Gemini",
            "GenerateContentRequest",
        ),
    ] {
        let missing = documented
            .iter()
            .filter(|field| field.provider == provider && field.schema == schema)
            .filter(|field| !super::standard_request_root_field_is_audited(format, &field.field))
            .map(|field| field.field.clone())
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "{provider} `{schema}` has root fields missing from runtime audit whitelist: {missing:?}"
        );
    }
}

#[test]
fn format_conversion_audit_has_no_unresolved_field_coverage_markers() {
    let audit = include_str!("../../../../../../docs/api/format-conversion-audit.md");
    for forbidden in [
        "strict audit pending",
        "Nested per-field",
        "field-by-field decision pending",
        "is still pending",
        "coverage exists, but strict audit is pending",
    ] {
        assert!(
            !audit.contains(forbidden),
            "format conversion audit still contains unresolved marker: {forbidden}"
        );
    }
}

#[test]
fn registry_does_not_call_wire_specific_canonical_functions_directly() {
    let implementation = include_str!("../registry.rs")
        .split("#[cfg(test)]")
        .next()
        .expect("registry implementation should be readable");
    for forbidden in [
        "canonical_to_openai",
        "canonical_to_claude",
        "canonical_to_gemini",
        "from_openai_chat_to_canonical",
        "from_openai_responses_to_canonical",
        "from_claude_to_canonical",
        "from_gemini_to_canonical",
    ] {
        assert!(
            !implementation.contains(forbidden),
            "registry should dispatch through formats::<provider>::<surface> adapters, found {forbidden}"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DocumentedSchemaField {
    provider: String,
    schema: String,
    field: String,
}

fn parse_documented_schema_fields(
    source: &str,
) -> std::collections::BTreeSet<DocumentedSchemaField> {
    let mut fields = std::collections::BTreeSet::new();
    let mut provider: Option<&str> = None;
    let mut schema: Option<String> = None;

    for line in source.lines() {
        if line.starts_with("## ") {
            provider = if line.contains("OpenAI Schema") {
                Some("OpenAI")
            } else if line.contains("Claude / Anthropic TypeScript") {
                Some("Claude")
            } else if line.contains("Gemini Schema") {
                Some("Gemini")
            } else {
                None
            };
            schema = None;
            continue;
        }
        let Some(active_provider) = provider else {
            continue;
        };
        if let Some(schema_name) = markdown_code_heading(line) {
            schema = Some(schema_name);
            continue;
        }
        let Some(active_schema) = schema.as_ref() else {
            continue;
        };
        if !line.starts_with("| `") {
            continue;
        }
        let cells = split_markdown_row(line);
        if cells.len() < 4 || !matches!(cells[2].as_str(), "是" | "否") {
            continue;
        }
        fields.insert(DocumentedSchemaField {
            provider: active_provider.to_string(),
            schema: active_schema.clone(),
            field: strip_markdown_code(&cells[1]),
        });
    }

    fields
}

fn parse_field_coverage_matrix_fields(
    matrix: &str,
) -> std::collections::BTreeSet<DocumentedSchemaField> {
    let mut fields = std::collections::BTreeSet::new();
    for line in matrix.lines() {
        if !line.starts_with("| ") {
            continue;
        }
        let cells = split_markdown_row(line);
        if cells.len() < 10 || !matches!(cells[1].as_str(), "OpenAI" | "Claude" | "Gemini") {
            continue;
        }
        fields.insert(DocumentedSchemaField {
            provider: cells[1].to_string(),
            schema: strip_markdown_code(&cells[2]),
            field: strip_markdown_code(&cells[3]),
        });
    }
    fields
}

fn assert_field_coverage_statuses_are_explicit(matrix: &str) {
    const VALID_STATUSES: &[&str] = &[
        "native",
        "mapped",
        "mapped/lossy-blocked",
        "extension-preserved",
        "unaudited",
        "unsupported",
        "invalid-enum",
        "lossy-blocked",
        "not-in-conversion-surface",
    ];

    for line in matrix.lines() {
        if !line.starts_with("| ") {
            continue;
        }
        let cells = split_markdown_row(line);
        if cells.len() < 10 || !matches!(cells[1].as_str(), "OpenAI" | "Claude" | "Gemini") {
            continue;
        }
        for index in [7, 8, 9] {
            assert!(
                VALID_STATUSES.contains(&cells[index].as_str()),
                "field coverage matrix has invalid status `{}` in row `{line}`",
                cells[index]
            );
        }
    }
}

fn markdown_code_heading(line: &str) -> Option<String> {
    line.strip_prefix("### `")
        .and_then(|rest| rest.split_once('`'))
        .map(|(value, _)| value.to_string())
}

fn strip_markdown_code(value: &str) -> String {
    value
        .trim()
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
        .unwrap_or_else(|| value.trim())
        .replace("\\|", "|")
}

fn split_markdown_row(line: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in line.chars() {
        if ch == '|' && !escaped {
            cells.push(current.trim().to_string());
            current.clear();
        } else {
            current.push(ch);
        }
        escaped = ch == '\\' && !escaped;
        if escaped && ch != '\\' {
            escaped = false;
        }
    }
    cells.push(current.trim().to_string());
    cells
}
