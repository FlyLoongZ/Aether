use super::{
    canonical_request_unknown_block_count, canonical_response_unknown_block_count,
    canonical_to_claude_request, canonical_to_claude_response, canonical_to_gemini_request,
    canonical_to_gemini_response, canonical_to_openai_chat_request,
    canonical_to_openai_chat_response, canonical_to_openai_responses_request,
    canonical_to_openai_responses_response, canonical_unknown_block_count,
    from_claude_to_canonical_request, from_claude_to_canonical_response,
    from_gemini_to_canonical_request, from_gemini_to_canonical_response,
    from_openai_chat_to_canonical_request, from_openai_chat_to_canonical_response,
    from_openai_responses_to_canonical_request, from_openai_responses_to_canonical_response,
    CanonicalContentBlock, CanonicalEmbedding, CanonicalEmbeddingContent, CanonicalEmbeddingInput,
    CanonicalEmbeddingRequest, CanonicalRole, CanonicalUsage,
};
use serde_json::{json, Value};

#[test]
fn canonical_embedding_request_accepts_axonhub_input_shapes() {
    for input in [
        json!("hello"),
        json!(["hello", "world"]),
        json!([1, 2, 3]),
        json!([[1, 2], [3, 4]]),
    ] {
        let request = json!({
            "model": "text-embedding-3-small",
            "embedding": {
                "input": input,
                "encoding_format": "float",
                "dimensions": 3
            }
        });
        let canonical = serde_json::from_value::<super::CanonicalRequest>(request)
            .expect("embedding request should deserialize");
        assert!(canonical.embedding.is_some());
        assert!(canonical.messages.is_empty());
        let encoded = serde_json::to_value(&canonical).expect("serialize");
        assert!(encoded.get("messages").is_some());
        assert!(encoded.get("embedding").is_some());
    }
}

#[test]
fn embedding_wire_request_accepts_all_axonhub_input_shapes() {
    let cases = [
        (
            json!("hello"),
            "single string",
            CanonicalEmbeddingInput::String("hello".to_string()),
        ),
        (
            json!(["hello", "world"]),
            "string array",
            CanonicalEmbeddingInput::StringArray(vec!["hello".to_string(), "world".to_string()]),
        ),
        (
            json!([1, 2, 3]),
            "token array",
            CanonicalEmbeddingInput::TokenArray(vec![1, 2, 3]),
        ),
        (
            json!([[1, 2], [3, 4]]),
            "nested token array",
            CanonicalEmbeddingInput::TokenArrayArray(vec![vec![1, 2], vec![3, 4]]),
        ),
        (
            json!([
                {"text": "white running shoes"},
                {"image": "https://example.com/shoe.png"},
                {"video": "https://example.com/demo.mp4"},
                {"multi_images": ["https://example.com/a.png", "https://example.com/b.png"]}
            ]),
            "multimodal array",
            CanonicalEmbeddingInput::Multimodal(vec![
                CanonicalEmbeddingContent {
                    text: Some("white running shoes".to_string()),
                    image: None,
                    video: None,
                    multi_images: None,
                },
                CanonicalEmbeddingContent {
                    text: None,
                    image: Some("https://example.com/shoe.png".to_string()),
                    video: None,
                    multi_images: None,
                },
                CanonicalEmbeddingContent {
                    text: None,
                    image: None,
                    video: Some("https://example.com/demo.mp4".to_string()),
                    multi_images: None,
                },
                CanonicalEmbeddingContent {
                    text: None,
                    image: None,
                    video: None,
                    multi_images: Some(vec![
                        "https://example.com/a.png".to_string(),
                        "https://example.com/b.png".to_string(),
                    ]),
                },
            ]),
        ),
    ];

    for (input, label, expected_input) in cases {
        let body = json!({
            "model": "text-embedding-3-small",
            "input": input
        });
        let canonical = super::from_embedding_to_canonical_request(&body, "openai")
            .unwrap_or_else(|| panic!("{label} should parse"));

        assert_eq!(
            canonical.embedding.expect("embedding request").input,
            expected_input,
            "{label} should preserve its canonical variant"
        );
        assert!(canonical.messages.is_empty());
    }
}

#[test]
fn embedding_wire_request_rejects_empty_invalid_or_chat_payloads() {
    for body in [
        json!({"model": "text-embedding-3-small", "input": "   "}),
        json!({"model": "text-embedding-3-small", "input": []}),
        json!({"model": "text-embedding-3-small", "input": [1, "two"]}),
        json!({"model": "text-embedding-3-small", "input": [[1], []]}),
        json!({"model": "text-embedding-3-small", "input": [{"image": "   "}]}),
        json!({"model": "text-embedding-3-small", "input": [{"multi_images": []}]}),
        json!({"model": "text-embedding-3-small", "input": ["hello", {"image": "https://example.com/a.png"}]}),
        json!({"model": "", "input": "hello"}),
        json!({"input": "hello"}),
        json!({"model": "text-embedding-3-small", "messages": []}),
    ] {
        assert!(
            super::from_embedding_to_canonical_request(&body, "openai").is_none(),
            "invalid embedding payload should be rejected: {body}"
        );
    }
}

#[test]
fn embedding_openai_request_response_roundtrip_stays_non_chat() {
    let body = json!({
        "model": "text-embedding-3-small",
        "input": ["hello", "world"],
        "encoding_format": "float",
        "dimensions": 2,
        "user": "user-1",
        "extra": true
    });
    let canonical =
        super::from_embedding_to_canonical_request(&body, "openai").expect("embedding request");
    assert_eq!(canonical.model, "text-embedding-3-small");
    assert!(canonical.messages.is_empty());
    assert!(matches!(
        canonical.embedding.as_ref().map(|embedding| &embedding.input),
        Some(CanonicalEmbeddingInput::StringArray(values)) if values == &vec!["hello".to_string(), "world".to_string()]
    ));

    let rebuilt = super::canonical_to_embedding_request(&canonical, "upstream-embedding", "openai")
        .expect("openai embedding request");
    assert_eq!(rebuilt["model"], "upstream-embedding");
    assert_eq!(rebuilt["input"], json!(["hello", "world"]));
    assert!(rebuilt.get("messages").is_none());

    let response = json!({
        "object": "list",
        "model": "upstream-embedding",
        "data": [
            {"object": "embedding", "index": 0, "embedding": [0.1, 0.2]},
            {"object": "embedding", "index": 1, "embedding": [0.3, 0.4]}
        ],
        "usage": {"prompt_tokens": 4, "total_tokens": 4}
    });
    let canonical_response = super::from_embedding_to_canonical_response(&response, "openai")
        .expect("embedding response");
    assert_eq!(canonical_response.embeddings.len(), 2);
    let emitted = super::canonical_to_embedding_response(&canonical_response, "openai")
        .expect("embedding response output");
    assert_eq!(emitted["data"][0]["embedding"], json!([0.1, 0.2]));
    assert!(emitted.get("choices").is_none());
    assert!(emitted.get("messages").is_none());
}

#[test]
fn embedding_provider_request_emitters_preserve_provider_contracts() {
    let canonical = super::CanonicalRequest {
        model: "text-embedding-3-small".to_string(),
        embedding: Some(CanonicalEmbeddingRequest {
            input: CanonicalEmbeddingInput::StringArray(vec![
                "alpha".to_string(),
                "beta".to_string(),
            ]),
            encoding_format: Some("float".to_string()),
            dimensions: Some(2),
            task: None,
            user: None,
            parameters: None,
            extensions: Default::default(),
        }),
        ..Default::default()
    };

    let jina = super::canonical_to_embedding_request(&canonical, "jina-embeddings-v3", "jina")
        .expect("jina embedding request");
    assert_eq!(jina["task"], "text-matching");
    assert_eq!(jina["input"], json!(["alpha", "beta"]));

    let gemini =
        super::canonical_to_embedding_request(&canonical, "gemini-embedding-001", "gemini")
            .expect("gemini embedding request");
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

    let doubao =
        super::canonical_to_embedding_request(&canonical, "doubao-embedding-text-240515", "doubao")
            .expect("doubao embedding request");
    assert_eq!(doubao["input"], json!(["alpha", "beta"]));
    assert!(doubao.get("messages").is_none());
}

#[test]
fn embedding_provider_request_emitters_cover_golden_payload_variants() {
    let single = super::CanonicalRequest {
        model: "text-embedding-3-small".to_string(),
        embedding: Some(CanonicalEmbeddingRequest {
            input: CanonicalEmbeddingInput::String("alpha".to_string()),
            encoding_format: Some("float".to_string()),
            dimensions: Some(1536),
            task: Some("retrieval.passage".to_string()),
            user: Some("user-1".to_string()),
            parameters: None,
            extensions: Default::default(),
        }),
        ..Default::default()
    };

    let openai = super::canonical_to_embedding_request(&single, "text-embedding-3-large", "openai")
        .expect("openai embedding request");
    assert_eq!(openai["model"], "text-embedding-3-large");
    assert_eq!(openai["input"], "alpha");
    assert_eq!(openai["encoding_format"], "float");
    assert_eq!(openai["dimensions"], 1536);
    assert_eq!(openai["user"], "user-1");
    assert_eq!(openai["task"], "retrieval.passage");

    let jina = super::canonical_to_embedding_request(&single, "jina-embeddings-v3", "jina")
        .expect("jina embedding request");
    assert_eq!(jina["task"], "retrieval.passage");
    assert_eq!(jina["input"], "alpha");

    let gemini = super::canonical_to_embedding_request(&single, "gemini-embedding-001", "gemini")
        .expect("gemini single embedding request");
    assert_eq!(gemini["model"], "gemini-embedding-001");
    assert_eq!(gemini["content"]["parts"][0]["text"], "alpha");
    assert!(gemini.get("requests").is_none());

    let doubao =
        super::canonical_to_embedding_request(&single, "doubao-embedding-text-240515", "doubao")
            .expect("doubao embedding request");
    assert_eq!(doubao["model"], "doubao-embedding-text-240515");
    assert_eq!(doubao["input"], json!(["alpha"]));
    assert_eq!(doubao["dimensions"], 1536);
}

#[test]
fn gemini_and_doubao_embedding_emitters_reject_token_inputs() {
    for input in [
        CanonicalEmbeddingInput::TokenArray(vec![1, 2, 3]),
        CanonicalEmbeddingInput::TokenArrayArray(vec![vec![1, 2], vec![3, 4]]),
    ] {
        let canonical = super::CanonicalRequest {
            model: "token-model".to_string(),
            embedding: Some(CanonicalEmbeddingRequest {
                input,
                encoding_format: None,
                dimensions: None,
                task: None,
                user: None,
                parameters: None,
                extensions: Default::default(),
            }),
            ..Default::default()
        };

        assert!(super::canonical_to_embedding_request(
            &canonical,
            "gemini-embedding-001",
            "gemini"
        )
        .is_none());
        assert!(
            super::canonical_to_embedding_request(&canonical, "doubao-embedding", "doubao")
                .is_none()
        );
    }
}

#[test]
fn embedding_response_parser_rejects_error_and_malformed_vectors() {
    for body in [
        json!({"error": {"message": "bad"}}),
        json!({"object": "list"}),
        json!({"data": [{"object": "embedding", "embedding": [0.1, "bad"]}]}),
        json!({"data": [{"object": "embedding"}]}),
    ] {
        assert!(
            super::from_embedding_to_canonical_response(&body, "openai").is_none(),
            "malformed embedding response should be rejected: {body}"
        );
    }
}

#[test]
fn embedding_response_parser_uses_openai_fallback_fields() {
    let body = json!({
        "object": "list",
        "data": [
            {"object": "embedding", "embedding": [0.1, 0.2]},
            {"object": "embedding", "index": 7, "embedding": [0.3, 0.4]}
        ]
    });

    let canonical = super::from_embedding_to_canonical_response(&body, "openai")
        .expect("fallback embedding response");
    assert_eq!(canonical.id, "embd-unknown");
    assert_eq!(canonical.model, "unknown");
    assert_eq!(canonical.embeddings[0].index, 0);
    assert_eq!(canonical.embeddings[1].index, 7);
    assert!(super::canonical_to_embedding_response(&canonical, "jina").is_some());
    assert!(super::canonical_to_embedding_response(&canonical, "gemini").is_none());
    assert!(super::canonical_to_embedding_response(&canonical, "doubao").is_none());
}

#[test]
fn embedding_response_contract_serializes_vectors_without_chat_outputs() {
    let response = super::CanonicalEmbeddingResponse {
        id: "embd-1".to_string(),
        model: "text-embedding-3-small".to_string(),
        embeddings: vec![CanonicalEmbedding {
            index: 0,
            embedding: vec![0.1, 0.2, 0.3],
            extensions: Default::default(),
        }],
        usage: Some(CanonicalUsage {
            input_tokens: 3,
            total_tokens: 3,
            ..Default::default()
        }),
        extensions: Default::default(),
    };

    let encoded = serde_json::to_value(&response).expect("serialize");
    assert_eq!(
        encoded["embeddings"][0]["embedding"],
        json!([0.1, 0.2, 0.3])
    );
    assert!(encoded.get("choices").is_none());
    let decoded =
        serde_json::from_value::<super::CanonicalEmbeddingResponse>(encoded).expect("deserialize");
    assert_eq!(decoded, response);
}

#[test]
fn canonical_request_preserves_openai_multimodal_tools_and_extensions() {
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "system", "content": "Be exact.", "cache_control": {"type": "ephemeral"}},
            {"role": "developer", "content": [{"type": "text", "text": "Prefer short answers."}]},
            {"role": "user", "content": [
                {"type": "text", "text": "inspect this"},
                {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgo=", "detail": "high"}},
                {"type": "file", "file": {"file_data": "data:application/pdf;base64,JVBERi0x", "filename": "a.pdf"}},
                {"type": "input_audio", "input_audio": {"data": "AAAA", "format": "mp3"}},
                {"type": "future_part", "value": 1}
            ]},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "lookup", "arguments": "{\"q\":\"rust\"}"}
            }]},
            {"role": "tool", "tool_call_id": "call_1", "content": "{\"ok\":true}"}
        ],
        "tools": [{
            "type": "function",
            "function": {"name": "lookup", "description": "Lookup", "parameters": {"type": "object"}}
        }],
        "tool_choice": {"type": "function", "function": {"name": "lookup"}},
        "max_completion_tokens": 42,
        "temperature": 0.2,
        "reasoning_effort": "high",
        "metadata": {"trace": "abc"},
        "vendor_extra": true
    });

    let canonical = from_openai_chat_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical.model, "gpt-5");
    assert_eq!(canonical.instructions.len(), 2);
    assert_eq!(canonical.instructions[1].role, CanonicalRole::Developer);
    assert_eq!(canonical.generation.max_tokens, Some(42));
    assert_eq!(canonical.tools[0].name, "lookup");
    assert!(canonical.thinking.is_some());
    assert!(canonical.extensions.contains_key("openai"));

    let user_blocks = &canonical.messages[0].content;
    assert!(
        matches!(user_blocks[1], CanonicalContentBlock::Image { ref data, ref media_type, .. } if data.as_deref() == Some("iVBORw0KGgo=") && media_type.as_deref() == Some("image/png"))
    );
    assert!(
        matches!(user_blocks[2], CanonicalContentBlock::File { ref data, ref filename, .. } if data.as_deref() == Some("JVBERi0x") && filename.as_deref() == Some("a.pdf"))
    );
    assert!(
        matches!(user_blocks[3], CanonicalContentBlock::Audio { ref data, ref format, .. } if data.as_deref() == Some("AAAA") && format.as_deref() == Some("mp3"))
    );
    assert_eq!(canonical_unknown_block_count(user_blocks), 1);
    assert_eq!(canonical_request_unknown_block_count(&canonical), 1);

    let rebuilt = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(rebuilt["model"], "gpt-5");
    assert_eq!(rebuilt["messages"][0]["role"], "system");
    assert_eq!(rebuilt["messages"][1]["role"], "system");
    assert_eq!(
        rebuilt["messages"][2]["content"][1]["image_url"]["url"],
        "data:image/png;base64,iVBORw0KGgo="
    );
    assert_eq!(
        rebuilt["messages"][3]["tool_calls"][0]["function"]["arguments"],
        "{\"q\":\"rust\"}"
    );
    assert_eq!(rebuilt["vendor_extra"], true);
}

#[test]
fn openai_chat_request_adapter_preserves_custom_tools_and_choices() {
    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "run a custom tool"}],
        "tools": [{
            "type": "custom",
            "custom": {
                "name": "shell_command",
                "description": "Run a shell command",
                "format": {
                    "type": "text"
                }
            }
        }],
        "tool_choice": {
            "type": "custom",
            "custom": {"name": "shell_command"}
        }
    });

    let canonical = from_openai_chat_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical.tools.len(), 1);
    assert_eq!(canonical.tools[0].name, "shell_command");
    assert!(matches!(
        canonical.tool_choice,
        Some(super::CanonicalToolChoice::Tool { ref name }) if name == "shell_command"
    ));

    let rebuilt = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(rebuilt["tools"][0]["type"], "custom");
    assert_eq!(rebuilt["tools"][0]["custom"]["name"], "shell_command");
    assert_eq!(rebuilt["tools"][0]["custom"]["format"]["type"], "text");
    assert_eq!(rebuilt["tool_choice"]["type"], "custom");
    assert_eq!(rebuilt["tool_choice"]["custom"]["name"], "shell_command");

    let responses = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(responses["tools"][0]["type"], "custom");
    assert_eq!(responses["tools"][0]["name"], "shell_command");
    assert_eq!(responses["tools"][0]["format"]["type"], "text");
    assert_eq!(responses["tool_choice"]["type"], "custom");
    assert_eq!(responses["tool_choice"]["name"], "shell_command");
}

#[test]
fn openai_chat_request_adapter_preserves_allowed_tool_choice_for_responses() {
    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "choose an allowed tool"}],
        "tools": [
            {
                "type": "function",
                "function": {"name": "lookup_weather", "parameters": {"type": "object"}}
            },
            {
                "type": "custom",
                "custom": {"name": "shell_command", "format": {"type": "text"}}
            }
        ],
        "tool_choice": {
            "type": "allowed_tools",
            "allowed_tools": {
                "mode": "required",
                "tools": [
                    {"type": "function", "function": {"name": "lookup_weather"}},
                    {"type": "custom", "custom": {"name": "shell_command"}}
                ]
            }
        }
    });

    let canonical = from_openai_chat_to_canonical_request(&request).expect("canonical request");
    assert!(canonical.tool_choice.is_none());

    let rebuilt = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(rebuilt["tool_choice"], request["tool_choice"]);

    let responses = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(responses["tool_choice"]["type"], "allowed_tools");
    assert_eq!(responses["tool_choice"]["mode"], "required");
    assert_eq!(responses["tool_choice"]["tools"][0]["type"], "function");
    assert_eq!(
        responses["tool_choice"]["tools"][0]["name"],
        "lookup_weather"
    );
    assert_eq!(responses["tool_choice"]["tools"][1]["type"], "custom");
    assert_eq!(
        responses["tool_choice"]["tools"][1]["name"],
        "shell_command"
    );
}

#[test]
fn canonical_response_roundtrips_reasoning_tool_usage_and_unknown_extensions() {
    let response = json!({
        "id": "chatcmpl_1",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "reasoning_content": "thinking",
                "content": [{"type": "text", "text": "done"}],
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": "{\"q\":\"rust\"}"}
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {
            "prompt_tokens": 3,
            "completion_tokens": 5,
            "total_tokens": 8,
            "prompt_tokens_details": {"cached_tokens": 2},
            "completion_tokens_details": {"reasoning_tokens": 1}
        },
        "service_tier": "default"
    });

    let canonical = from_openai_chat_to_canonical_response(&response).expect("canonical response");
    assert_eq!(canonical.id, "chatcmpl_1");
    assert!(
        matches!(canonical.content[0], CanonicalContentBlock::Thinking { ref text, .. } if text == "thinking")
    );
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolUse { name, .. } if name == "lookup")
    ));
    assert_eq!(canonical.usage.as_ref().unwrap().cache_read_tokens, 2);
    assert_eq!(canonical.usage.as_ref().unwrap().reasoning_tokens, 1);
    assert_eq!(canonical_response_unknown_block_count(&canonical), 0);
    assert!(canonical.extensions.contains_key("openai"));

    let encoded = serde_json::to_value(&canonical).expect("serialize");
    let decoded = serde_json::from_value::<super::CanonicalResponse>(encoded).expect("deserialize");
    assert_eq!(decoded, canonical);

    let rebuilt = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt["choices"][0]["message"]["reasoning_content"],
        "thinking"
    );
    assert_eq!(
        rebuilt["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "lookup"
    );
    assert_eq!(
        rebuilt["usage"]["completion_tokens_details"]["reasoning_tokens"],
        1
    );
}

#[test]
fn openai_chat_response_preserves_custom_tool_calls_for_responses() {
    let response = json!({
        "id": "chatcmpl_custom",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": null,
                "tool_calls": [{
                    "id": "call_custom_1",
                    "type": "custom",
                    "custom": {
                        "name": "shell_command",
                        "input": "ls -la"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }]
    });

    let canonical = from_openai_chat_to_canonical_response(&response).expect("canonical response");
    assert!(matches!(
        canonical.content[0],
        CanonicalContentBlock::ToolUse { ref id, ref name, ref input, .. }
            if id == "call_custom_1" && name == "shell_command" && input == "ls -la"
    ));

    let rebuilt_chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["tool_calls"][0]["type"],
        "custom"
    );
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["tool_calls"][0]["custom"]["name"],
        "shell_command"
    );

    let responses = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(responses["output"][0]["type"], "custom_tool_call");
    assert_eq!(responses["output"][0]["name"], "shell_command");
    assert_eq!(responses["output"][0]["input"], "ls -la");
}

#[test]
fn openai_responses_response_preserves_custom_tool_calls_for_chat() {
    let response = json!({
        "id": "resp_custom",
        "object": "response",
        "status": "completed",
        "model": "gpt-5",
        "output": [{
            "type": "custom_tool_call",
            "id": "call_custom_1",
            "call_id": "call_custom_1",
            "status": "completed",
            "name": "shell_command",
            "input": "pwd"
        }]
    });

    let canonical =
        from_openai_responses_to_canonical_response(&response).expect("canonical response");
    assert!(matches!(
        canonical.content[0],
        CanonicalContentBlock::ToolUse { ref id, ref name, ref input, .. }
            if id == "call_custom_1" && name == "shell_command" && input == "pwd"
    ));

    let chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        chat["choices"][0]["message"]["tool_calls"][0]["type"],
        "custom"
    );
    assert_eq!(
        chat["choices"][0]["message"]["tool_calls"][0]["custom"]["name"],
        "shell_command"
    );
    assert_eq!(
        chat["choices"][0]["message"]["tool_calls"][0]["custom"]["input"],
        "pwd"
    );

    let rebuilt = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(rebuilt["output"][0]["type"], "custom_tool_call");
    assert_eq!(rebuilt["output"][0]["input"], "pwd");
}

#[test]
fn openai_response_adapters_preserve_audio_output_shapes() {
    let chat_response = json!({
        "id": "chatcmpl_audio",
        "object": "chat.completion",
        "model": "gpt-5-audio",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "hello",
                "audio": {
                    "id": "audio_1",
                    "data": "QUJDRA==",
                    "expires_at": 123,
                    "transcript": "hello"
                }
            },
            "finish_reason": "stop"
        }]
    });

    let canonical =
        from_openai_chat_to_canonical_response(&chat_response).expect("canonical response");
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::Audio { data, .. } if data.as_deref() == Some("QUJDRA=="))
    ));

    let rebuilt_chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["audio"]["id"],
        "audio_1"
    );
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["audio"]["transcript"],
        "hello"
    );

    let responses = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(responses["output"][0]["content"][1]["type"], "output_audio");
    assert_eq!(responses["output"][0]["content"][1]["data"], "QUJDRA==");
    assert_eq!(responses["output"][0]["content"][1]["transcript"], "hello");

    let responses_response = json!({
        "id": "resp_audio",
        "object": "response",
        "status": "completed",
        "model": "gpt-5-audio",
        "output": [{
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "output_audio",
                "data": "RUZHSA==",
                "transcript": "hi"
            }]
        }]
    });

    let canonical = from_openai_responses_to_canonical_response(&responses_response)
        .expect("canonical response");
    let chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(chat["choices"][0]["message"]["audio"]["data"], "RUZHSA==");
    assert_eq!(chat["choices"][0]["message"]["audio"]["transcript"], "hi");
}

#[test]
fn canonical_request_adapter_keeps_existing_simple_chat_shape() {
    let request = json!({
        "model": "gpt-5",
        "messages": [{"role": "user", "content": "hello"}],
        "stop": ["x", "y"],
        "n": 2
    });
    let canonical = from_openai_chat_to_canonical_request(&request).expect("canonical request");
    let rebuilt = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(rebuilt["model"], request["model"]);
    assert_eq!(rebuilt["messages"], request["messages"]);
    assert_eq!(rebuilt["stop"], Value::Array(vec![json!("x"), json!("y")]));
    assert_eq!(rebuilt["n"], 2);
}

#[test]
fn openai_chat_request_adapter_preserves_reasoning_content_for_responses() {
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "user", "content": "hi"},
            {
                "role": "assistant",
                "reasoning_content": "internal plan",
                "content": "final answer"
            }
        ]
    });

    let canonical = from_openai_chat_to_canonical_request(&request).expect("canonical request");
    assert!(matches!(
        canonical.messages[1].content.first(),
        Some(CanonicalContentBlock::Thinking { text, .. }) if text == "internal plan"
    ));

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    let parts = rebuilt["input"][1]["content"]
        .as_array()
        .expect("content parts");

    assert_eq!(parts[0]["type"], "output_text");
    assert!(parts[0]["text"]
        .as_str()
        .expect("reasoning text")
        .contains("<thinking>internal plan</thinking>"));
    assert_eq!(parts[1]["type"], "output_text");
    assert_eq!(parts[1]["text"], "final answer");
}

#[test]
fn openai_responses_request_adapter_preserves_audio_reasoning_tools_and_text_config() {
    let request = json!({
        "model": "gpt-5",
        "instructions": "Be exact.",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "transcribe"},
                    {
                        "type": "input_audio",
                        "input_audio": {
                            "data": "AAAA",
                            "format": "mp3"
                        }
                    }
                ]
            },
            {
                "type": "function_call",
                "call_id": "call_123",
                "name": "lookup",
                "arguments": "{\"q\":\"audio\"}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "{\"ok\":true}"
            }
        ],
        "reasoning": {"effort": "high"},
        "text": {
            "format": {
                "type": "json_schema",
                "name": "answer",
                "schema": {"type": "object"},
                "strict": true
            },
            "verbosity": "low"
        },
        "tools": [{
            "type": "function",
            "name": "lookup",
            "parameters": {"type": "object"}
        }],
        "tool_choice": {"type": "function", "name": "lookup"}
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical.instructions[0].text, "Be exact.");
    assert!(matches!(
        canonical.messages[0].content[1],
        CanonicalContentBlock::Audio {
            ref data,
            ref format,
            ..
        } if data.as_deref() == Some("AAAA") && format.as_deref() == Some("mp3")
    ));
    assert!(matches!(
        canonical.messages[1].content[0],
        CanonicalContentBlock::ToolUse { ref id, ref name, .. }
            if id == "call_123" && name == "lookup"
    ));
    assert_eq!(
        canonical
            .thinking
            .as_ref()
            .and_then(|thinking| thinking.extensions.get("openai_responses"))
            .and_then(|value| value.get("effort"))
            .and_then(Value::as_str),
        Some("high")
    );
    assert_eq!(
        canonical
            .response_format
            .as_ref()
            .and_then(|format| format.json_schema.as_ref())
            .and_then(|schema| schema.get("name"))
            .and_then(Value::as_str),
        Some("answer")
    );
    assert_eq!(
        canonical
            .response_format
            .as_ref()
            .and_then(|format| format.json_schema.as_ref())
            .and_then(|schema| schema.get("strict"))
            .and_then(Value::as_bool),
        Some(true)
    );

    let rebuilt_chat = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(rebuilt_chat["response_format"]["type"], "json_schema");
    assert_eq!(
        rebuilt_chat["response_format"]["json_schema"]["name"],
        "answer"
    );

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["model"], "gpt-5-upstream");
    assert_eq!(rebuilt["text"]["format"]["name"], "answer");
    assert_eq!(rebuilt["text"]["format"]["strict"], true);
    assert!(rebuilt["text"]["format"].get("json_schema").is_none());
    assert_eq!(rebuilt["text"]["verbosity"], "low");
    assert_eq!(rebuilt["tool_choice"]["name"], "lookup");
}

#[test]
fn openai_responses_request_adapter_accepts_nested_function_and_custom_tools() {
    let request = json!({
        "model": "gpt-5",
        "input": "Use a tool",
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "lookup_weather",
                    "description": "Lookup weather",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "city": {"type": "string"}
                        },
                        "required": ["city"]
                    }
                }
            },
            {
                "type": "custom",
                "custom": {
                    "name": "shell_command",
                    "description": "Run a shell command"
                }
            }
        ],
        "tool_choice": {
            "type": "function",
            "function": {"name": "lookup_weather"}
        }
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical.tools.len(), 2);
    assert_eq!(canonical.tools[0].name, "lookup_weather");
    assert_eq!(
        canonical.tools[0]
            .parameters
            .as_ref()
            .and_then(|value| value.get("required"))
            .and_then(Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    assert_eq!(canonical.tools[1].name, "shell_command");
    assert!(matches!(
        canonical.tool_choice,
        Some(super::CanonicalToolChoice::Tool { ref name }) if name == "lookup_weather"
    ));

    let claude = canonical_to_claude_request(&canonical, "claude-sonnet-4-upstream", false)
        .expect("claude request");
    assert_eq!(claude["tools"][0]["name"], "lookup_weather");
    assert_eq!(claude["tools"][1]["name"], "shell_command");
    assert_eq!(claude["tool_choice"]["name"], "lookup_weather");

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["tools"][0]["name"], "lookup_weather");
    assert_eq!(rebuilt["tools"][1]["type"], "custom");
    assert_eq!(rebuilt["tools"][1]["custom"]["name"], "shell_command");
}

#[test]
fn openai_responses_request_adapter_preserves_custom_tool_choice_for_chat() {
    let request = json!({
        "model": "gpt-5",
        "input": "Use the custom tool",
        "tools": [{
            "type": "custom",
            "name": "shell_command",
            "description": "Run a shell command",
            "format": {"type": "text"}
        }],
        "tool_choice": {
            "type": "custom",
            "name": "shell_command"
        }
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");
    assert!(matches!(
        canonical.tool_choice,
        Some(super::CanonicalToolChoice::Tool { ref name }) if name == "shell_command"
    ));

    let chat = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(chat["tools"][0]["type"], "custom");
    assert_eq!(chat["tools"][0]["custom"]["name"], "shell_command");
    assert_eq!(chat["tools"][0]["custom"]["format"]["type"], "text");
    assert_eq!(chat["tool_choice"]["type"], "custom");
    assert_eq!(chat["tool_choice"]["custom"]["name"], "shell_command");

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["tools"][0]["type"], "custom");
    assert_eq!(rebuilt["tools"][0]["name"], "shell_command");
    assert_eq!(rebuilt["tool_choice"]["type"], "custom");
    assert_eq!(rebuilt["tool_choice"]["name"], "shell_command");
}

#[test]
fn openai_responses_request_adapter_accepts_single_input_item_object() {
    let request = json!({
        "model": "gpt-5",
        "input": {
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical.messages.len(), 1);
    assert_eq!(canonical.messages[0].role, CanonicalRole::User);

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["input"][0]["type"], "message");
    assert_eq!(rebuilt["input"][0]["role"], "user");
    assert_eq!(rebuilt["input"][0]["content"][0]["text"], "hello");
}

#[test]
fn openai_responses_request_adapter_preserves_custom_tool_history() {
    let request = json!({
        "model": "gpt-5",
        "input": [
            {
                "type": "custom_tool_call",
                "id": "ctc_1",
                "call_id": "call_custom_1",
                "name": "shell_command",
                "input": "ls -la",
                "status": "completed"
            },
            {
                "type": "custom_tool_call_output",
                "call_id": "call_custom_1",
                "output": "ok"
            }
        ]
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");

    let chat = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(chat["messages"][0]["tool_calls"][0]["type"], "custom");
    assert_eq!(
        chat["messages"][0]["tool_calls"][0]["custom"]["name"],
        "shell_command"
    );
    assert_eq!(
        chat["messages"][0]["tool_calls"][0]["custom"]["input"],
        "ls -la"
    );

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["input"][0]["type"], "custom_tool_call");
    assert_eq!(rebuilt["input"][0]["id"], "ctc_1");
    assert_eq!(rebuilt["input"][0]["call_id"], "call_custom_1");
    assert_eq!(rebuilt["input"][0]["name"], "shell_command");
    assert_eq!(rebuilt["input"][0]["input"], "ls -la");
    assert_eq!(rebuilt["input"][1]["type"], "custom_tool_call_output");
    assert_eq!(rebuilt["input"][1]["output"], "ok");
}

#[test]
fn openai_responses_request_adapter_preserves_reasoning_encrypted_content_history() {
    let request = json!({
        "model": "gpt-5",
        "input": [
            {
                "type": "reasoning",
                "id": "rs_1",
                "summary": [{"type": "summary_text", "text": "think"}],
                "encrypted_content": "enc_reasoning"
            },
            {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "done"}]
            }
        ]
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");
    assert!(matches!(
        canonical.messages[0].content[0],
        CanonicalContentBlock::Thinking { ref text, ref encrypted_content, .. }
            if text == "think" && encrypted_content.as_deref() == Some("enc_reasoning")
    ));

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["input"][0]["type"], "reasoning");
    assert_eq!(rebuilt["input"][0]["content"][0]["type"], "reasoning_text");
    assert_eq!(rebuilt["input"][0]["content"][0]["text"], "think");
    assert_eq!(rebuilt["input"][0]["encrypted_content"], "enc_reasoning");
    assert_eq!(rebuilt["input"][1]["type"], "message");
    assert_eq!(rebuilt["input"][1]["content"][0]["text"], "done");
}

#[test]
fn openai_responses_request_adapter_preserves_file_url_and_tool_output_parts() {
    let request = json!({
        "model": "gpt-5",
        "messages": [
            {"role": "user", "content": [
                {"type": "file", "file": {"file_url": "https://example.com/input.pdf", "filename": "input.pdf"}}
            ]},
            {"role": "assistant", "content": null, "tool_calls": [{
                "id": "call_render",
                "type": "function",
                "function": {"name": "render", "arguments": "{}"}
            }]},
            {"role": "tool", "tool_call_id": "call_render", "content": [
                {"type": "text", "text": "Rendered result attached."},
                {"type": "image_url", "image_url": {"url": "https://example.com/out.png", "detail": "high"}},
                {"type": "file", "file": {"file_url": "https://example.com/report.pdf", "filename": "report.pdf"}}
            ]}
        ]
    });

    let canonical = from_openai_chat_to_canonical_request(&request).expect("canonical request");
    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");

    assert_eq!(
        rebuilt["input"][0]["content"][0]["file_url"],
        "https://example.com/input.pdf"
    );
    assert!(rebuilt["input"][0]["content"][0].get("file_data").is_none());
    assert_eq!(rebuilt["input"][2]["type"], "function_call_output");
    assert_eq!(rebuilt["input"][2]["output"][0]["type"], "input_text");
    assert_eq!(
        rebuilt["input"][2]["output"][1]["image_url"],
        "https://example.com/out.png"
    );
    assert_eq!(rebuilt["input"][2]["output"][1]["detail"], "high");
    assert_eq!(
        rebuilt["input"][2]["output"][2]["file_url"],
        "https://example.com/report.pdf"
    );
}

#[test]
fn openai_responses_request_adapter_preserves_allowed_tool_choice_for_chat() {
    let request = json!({
        "model": "gpt-5",
        "input": "choose an allowed tool",
        "tools": [
            {"type": "function", "name": "lookup_weather", "parameters": {"type": "object"}},
            {"type": "custom", "name": "shell_command", "format": {"type": "text"}}
        ],
        "tool_choice": {
            "type": "allowed_tools",
            "mode": "auto",
            "tools": [
                {"type": "function", "name": "lookup_weather"},
                {"type": "custom", "name": "shell_command"}
            ]
        }
    });

    let canonical =
        from_openai_responses_to_canonical_request(&request).expect("canonical request");
    assert!(canonical.tool_choice.is_none());

    let chat = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(chat["tool_choice"]["type"], "allowed_tools");
    assert_eq!(chat["tool_choice"]["allowed_tools"]["mode"], "auto");
    assert_eq!(
        chat["tool_choice"]["allowed_tools"]["tools"][0]["function"]["name"],
        "lookup_weather"
    );
    assert_eq!(
        chat["tool_choice"]["allowed_tools"]["tools"][1]["custom"]["name"],
        "shell_command"
    );

    let rebuilt = canonical_to_openai_responses_request(&canonical, "gpt-5-upstream", false)
        .expect("openai responses request");
    assert_eq!(rebuilt["tool_choice"], request["tool_choice"]);
}

#[test]
fn openai_responses_response_adapter_preserves_output_items_reasoning_tools_and_usage() {
    let response = json!({
        "id": "resp_123",
        "object": "response",
        "status": "completed",
        "error": null,
        "model": "gpt-5",
        "output": [
            {
                "type": "reasoning",
                "id": "rs_1",
                "summary": [{
                    "type": "summary_text",
                    "text": "think"
                }]
            },
            {
                "type": "message",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": "done",
                        "annotations": [{
                            "type": "file_citation",
                            "start_index": 1,
                            "end_index": 3
                        }]
                    },
                    {"type": "refusal", "refusal": "partial refusal"},
                    {
                        "type": "output_image",
                        "image_url": "data:image/png;base64,iVBORw0KGgo="
                    },
                    {
                        "type": "file",
                        "file": {
                            "file_data": "data:application/pdf;base64,JVBERi0x",
                            "filename": "report.pdf"
                        }
                    }
                ]
            },
            {
                "type": "function_call",
                "id": "call_1",
                "call_id": "call_1",
                "name": "lookup",
                "arguments": "{\"q\":\"rust\"}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": {"ok": true}
            },
            {
                "type": "local_shell_call",
                "id": "lsc_1",
                "call_id": "call_shell_1",
                "status": "completed",
                "action": {
                    "type": "exec",
                    "command": ["pwd"]
                }
            },
            {
                "type": "local_shell_call_output",
                "call_id": "call_shell_1",
                "output": {
                    "stdout": "/tmp/project\n",
                    "stderr": "",
                    "outcome": "success"
                }
            },
            {
                "type": "future_item",
                "payload": true
            }
        ],
        "usage": {
            "input_tokens": 3,
            "input_tokens_details": {
                "cache_write_tokens": 1,
                "cached_tokens": 2
            },
            "output_tokens": 5,
            "output_tokens_details": {"reasoning_tokens": 1},
            "total_tokens": 8
        },
        "service_tier": "flex"
    });

    let canonical =
        from_openai_responses_to_canonical_response(&response).expect("canonical response");
    assert_eq!(canonical.id, "resp_123");
    assert!(matches!(
        canonical.content[0],
        CanonicalContentBlock::Thinking { ref text, .. } if text == "think"
    ));
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolUse { name, .. } if name == "lookup")
    ));
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolUse { name, input, .. }
            if name == "local_shell" && input["action"]["command"][0] == "pwd")
    ));
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolResult { tool_use_id, .. } if tool_use_id == "call_1")
    ));
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolResult { tool_use_id, name: Some(name), .. }
            if tool_use_id == "call_shell_1" && name == "local_shell")
    ));
    assert_eq!(canonical_response_unknown_block_count(&canonical), 2);
    assert_eq!(canonical.usage.as_ref().unwrap().cache_read_tokens, 2);
    assert_eq!(canonical.usage.as_ref().unwrap().cache_write_tokens, 1);
    assert_eq!(canonical.usage.as_ref().unwrap().reasoning_tokens, 1);

    let rebuilt_chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["annotations"],
        json!([{"type": "file_citation", "start_index": 1, "end_index": 3}])
    );
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["refusal"],
        "partial refusal"
    );
    assert_eq!(rebuilt_chat["service_tier"], "flex");

    let rebuilt = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(rebuilt["id"], "resp_123");
    assert_eq!(rebuilt["output"][0]["type"], "reasoning");
    assert_eq!(rebuilt["output"][1]["content"][0]["text"], "done");
    assert_eq!(rebuilt["output"][1]["content"][1]["type"], "refusal");
    assert_eq!(rebuilt["output"][2]["type"], "function_call");
    assert_eq!(rebuilt["output"][3]["type"], "function_call_output");
    assert_eq!(rebuilt["output"][4]["type"], "local_shell_call");
    assert_eq!(rebuilt["output"][4]["action"]["command"][0], "pwd");
    assert_eq!(rebuilt["output"][5]["type"], "local_shell_call_output");
    assert_eq!(rebuilt["output"][5]["call_id"], "call_shell_1");
    assert_eq!(rebuilt["usage"]["input_tokens_details"]["cached_tokens"], 2);
    assert_eq!(
        rebuilt["usage"]["input_tokens_details"]["cache_write_tokens"],
        1
    );
    assert_eq!(
        rebuilt["usage"]["output_tokens_details"]["reasoning_tokens"],
        1
    );
    assert_eq!(rebuilt["service_tier"], "flex");
}

#[test]
fn openai_responses_to_claude_response_drops_empty_pages_only_for_read_tool() {
    let response = json!({
        "id": "resp_read_pages",
        "object": "response",
        "status": "completed",
        "model": "gpt-5.5",
        "output": [
            {
                "type": "function_call",
                "id": "call_read",
                "call_id": "call_read",
                "name": "Read",
                "arguments": "{\"file_path\":\"/tmp/a.txt\",\"offset\":0,\"limit\":20,\"pages\":\"\"}"
            },
            {
                "type": "function_call",
                "id": "call_search",
                "call_id": "call_search",
                "name": "Search",
                "arguments": "{\"query\":\"\",\"pages\":\"\"}"
            }
        ],
        "usage": {
            "input_tokens": 1,
            "output_tokens": 1,
            "total_tokens": 2
        }
    });

    let canonical =
        from_openai_responses_to_canonical_response(&response).expect("canonical response");
    let claude = canonical_to_claude_response(&canonical);

    assert_eq!(
        claude["content"][0]["input"],
        json!({
            "file_path": "/tmp/a.txt",
            "offset": 0,
            "limit": 20,
        })
    );
    assert_eq!(
        claude["content"][1]["input"],
        json!({
            "query": "",
            "pages": "",
        })
    );

    let rebuilt_responses = canonical_to_openai_responses_response(&canonical, &json!({}));
    let read_arguments = serde_json::from_str::<Value>(
        rebuilt_responses["output"][0]["arguments"]
            .as_str()
            .expect("arguments should be a string"),
    )
    .expect("arguments should be json");
    assert_eq!(
        read_arguments,
        json!({
            "file_path": "/tmp/a.txt",
            "offset": 0,
            "limit": 20,
            "pages": "",
        })
    );
}

#[test]
fn openai_responses_image_generation_call_becomes_canonical_image_block() {
    let response = json!({
        "id": "resp_img",
        "model": "gpt-image-2",
        "status": "completed",
        "output": [{
            "id": "ig_1",
            "type": "image_generation_call",
            "status": "completed",
            "output_format": "png",
            "result": "aW1hZ2U="
        }]
    });

    let canonical =
        from_openai_responses_to_canonical_response(&response).expect("canonical response");
    assert!(matches!(
        canonical.content[0],
        CanonicalContentBlock::Image { ref data, ref media_type, .. }
            if data.as_deref() == Some("aW1hZ2U=")
                && media_type.as_deref() == Some("image/png")
    ));

    let rebuilt_chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["content"][0]["type"],
        json!("image_url")
    );
    assert_eq!(
        rebuilt_chat["choices"][0]["message"]["content"][0]["image_url"]["url"],
        json!("data:image/png;base64,aW1hZ2U=")
    );

    let rebuilt_responses = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(
        rebuilt_responses["output"][0]["type"],
        json!("image_generation_call")
    );
    assert_eq!(rebuilt_responses["output"][0]["result"], json!("aW1hZ2U="));
}

#[test]
fn claude_request_adapter_preserves_cache_thinking_tools_and_builtin_extensions() {
    let request = json!({
        "model": "claude-sonnet-4-5",
        "system": [
            {
                "type": "text",
                "text": "Cache this.",
                "cache_control": {"type": "ephemeral"}
            },
            {"type": "text", "text": "Be exact."}
        ],
        "messages": [
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "thinking",
                        "thinking": "plan",
                        "signature": "sig_123"
                    },
                    {
                        "type": "tool_use",
                        "name": "lookup",
                        "input": {"q": "rust"}
                    }
                ]
            },
            {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_auto_0",
                    "content": {"ok": true}
                }]
            }
        ],
        "tools": [
            {
                "name": "lookup",
                "description": "Lookup",
                "input_schema": {"type": "object"}
            },
            {
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 5
            }
        ],
        "tool_choice": {
            "type": "auto",
            "disable_parallel_tool_use": false
        },
        "thinking": {"type": "enabled", "budget_tokens": 2048},
        "output_config": {"effort": "medium"}
    });

    let canonical = from_claude_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical.instructions.len(), 2);
    assert_eq!(
        canonical.instructions[0]
            .extensions
            .get("claude")
            .and_then(|value| value.get("cache_control"))
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str),
        Some("ephemeral")
    );
    assert!(matches!(
        canonical.messages[0].content[0],
        CanonicalContentBlock::Thinking {
            ref text,
            ref signature,
            ..
        } if text == "plan" && signature.as_deref() == Some("sig_123")
    ));
    assert!(matches!(
        canonical.messages[0].content[1],
        CanonicalContentBlock::ToolUse { ref id, .. } if id == "toolu_auto_0"
    ));

    let openai_chat = canonical_to_openai_chat_request(&canonical).expect("openai chat request");
    assert_eq!(
        openai_chat["messages"][2]["reasoning_parts"][0]["signature"],
        "sig_123"
    );
    assert_eq!(
        openai_chat["web_search_options"]["search_context_size"],
        "medium"
    );

    let rebuilt =
        canonical_to_claude_request(&canonical, "claude-upstream", false).expect("claude");
    assert_eq!(rebuilt["model"], "claude-upstream");
    assert_eq!(rebuilt["system"][0]["cache_control"]["type"], "ephemeral");
    assert_eq!(rebuilt["messages"][1]["content"][0]["signature"], "sig_123");
    assert_eq!(rebuilt["tools"][1]["type"], "web_search_20250305");
    assert_eq!(rebuilt["thinking"]["budget_tokens"], 2048);
    assert_eq!(rebuilt["output_config"]["effort"], "medium");
}

#[test]
fn claude_request_adapter_preserves_official_raw_content_blocks() {
    let request = json!({
        "model": "claude-sonnet-4-5",
        "messages": [{
            "role": "assistant",
            "content": [
                {
                    "type": "server_tool_use",
                    "id": "srvu_1",
                    "name": "web_search",
                    "input": {"query": "rust"}
                },
                {
                    "type": "web_search_tool_result",
                    "tool_use_id": "srvu_1",
                    "content": [{
                        "type": "web_search_result",
                        "title": "Rust",
                        "url": "https://www.rust-lang.org/",
                        "encrypted_content": "enc"
                    }]
                }
            ]
        }]
    });

    let canonical = from_claude_to_canonical_request(&request).expect("canonical request");
    assert_eq!(canonical_request_unknown_block_count(&canonical), 2);

    let rebuilt =
        canonical_to_claude_request(&canonical, "claude-upstream", false).expect("claude request");
    assert_eq!(
        rebuilt["messages"][1]["content"][0]["type"],
        "server_tool_use"
    );
    assert_eq!(
        rebuilt["messages"][1]["content"][1]["type"],
        "web_search_tool_result"
    );
    assert_eq!(
        rebuilt["messages"][1]["content"][1]["content"][0]["encrypted_content"],
        "enc"
    );
}

#[test]
fn canonical_to_openai_chat_request_rejects_unrepresentable_claude_tool_result() {
    let request = json!({
        "model": "claude-sonnet",
        "messages": [{
            "role": "user",
            "content": [{
                "type": "tool_result",
                "tool_use_id": "toolu_read",
                "content": [{
                    "type": "image",
                    "source": {
                        "type": "unsupported",
                        "media_type": "image/png",
                        "data": "AAAA"
                    }
                }]
            }]
        }]
    });

    let canonical = from_claude_to_canonical_request(&request).expect("canonical request");

    assert!(canonical_to_openai_chat_request(&canonical).is_none());
}

#[test]
fn claude_response_adapter_preserves_thinking_signature_tool_and_cache_usage() {
    let response = json!({
        "id": "msg_123",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4-5",
        "content": [
            {
                "type": "thinking",
                "thinking": "plan",
                "signature": "sig_123"
            },
            {"type": "text", "text": "done"},
            {
                "type": "tool_use",
                "id": "toolu_123",
                "name": "lookup",
                "input": {"q": "rust"}
            }
        ],
        "stop_reason": "tool_use",
        "usage": {
            "input_tokens": 11,
            "output_tokens": 7,
            "cache_read_input_tokens": 3,
            "cache_creation_input_tokens": 2,
            "output_tokens_details": {"thinking_tokens": 4}
        }
    });

    let canonical = from_claude_to_canonical_response(&response).expect("canonical response");
    assert!(matches!(
        canonical.content[0],
        CanonicalContentBlock::Thinking {
            ref text,
            ref signature,
            ..
        } if text == "plan" && signature.as_deref() == Some("sig_123")
    ));
    assert!(matches!(
        canonical.content[2],
        CanonicalContentBlock::ToolUse { ref id, ref name, .. }
            if id == "toolu_123" && name == "lookup"
    ));
    assert_eq!(canonical.usage.as_ref().unwrap().cache_read_tokens, 3);
    assert_eq!(canonical.usage.as_ref().unwrap().cache_write_tokens, 2);
    assert_eq!(canonical.usage.as_ref().unwrap().reasoning_tokens, 4);

    let rebuilt = canonical_to_claude_response(&canonical);
    assert_eq!(rebuilt["content"][0]["signature"], "sig_123");
    assert_eq!(rebuilt["content"][2]["name"], "lookup");
    assert_eq!(rebuilt["stop_reason"], "tool_use");
    assert_eq!(rebuilt["usage"]["cache_read_input_tokens"], 3);
    assert_eq!(rebuilt["usage"]["cache_creation_input_tokens"], 2);
    assert_eq!(
        rebuilt["usage"]["output_tokens_details"]["thinking_tokens"],
        4
    );

    let rebuilt_chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt_chat["usage"]["completion_tokens_details"]["reasoning_tokens"],
        4
    );

    let rebuilt_openai = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(
        rebuilt_openai["usage"]["output_tokens_details"]["reasoning_tokens"],
        4
    );
    assert_eq!(rebuilt_openai["usage"]["input_tokens"], 16);
    assert_eq!(
        rebuilt_openai["usage"]["input_tokens_details"]["cached_tokens"],
        3
    );
    assert_eq!(
        rebuilt_openai["usage"]["input_tokens_details"]["cache_write_tokens"],
        2
    );
    assert_eq!(rebuilt_openai["usage"]["total_tokens"], 23);

    let rebuilt_gemini = canonical_to_gemini_response(&canonical, &json!({})).expect("gemini");
    assert_eq!(rebuilt_gemini["usageMetadata"]["promptTokenCount"], 16);
    assert_eq!(
        rebuilt_gemini["usageMetadata"]["cachedContentTokenCount"],
        3
    );
    assert_eq!(rebuilt_gemini["usageMetadata"]["totalTokenCount"], 23);
}

#[test]
fn openai_usage_prefers_cache_write_tokens_and_emits_the_official_field() {
    let response = json!({
        "id": "resp_cache_write",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [],
        "usage": {
            "input_tokens": 20,
            "output_tokens": 4,
            "total_tokens": 24,
            "input_tokens_details": {
                "cached_tokens": 3,
                "cache_write_tokens": 7,
                "cached_creation_tokens": 99
            }
        }
    });

    let canonical = from_openai_responses_to_canonical_response(&response)
        .expect("Responses usage should parse");
    assert_eq!(canonical.usage.as_ref().unwrap().cache_write_tokens, 7);

    let rebuilt = canonical_to_openai_responses_response(&canonical, &json!({}));
    assert_eq!(
        rebuilt["usage"]["input_tokens_details"]["cache_write_tokens"],
        7
    );
    assert!(rebuilt["usage"]["input_tokens_details"]
        .get("cached_creation_tokens")
        .is_none());

    let rebuilt_chat = canonical_to_openai_chat_response(&canonical);
    assert_eq!(
        rebuilt_chat["usage"]["prompt_tokens_details"]["cache_write_tokens"],
        7
    );
    assert!(rebuilt_chat["usage"]["prompt_tokens_details"]
        .get("cached_creation_tokens")
        .is_none());
}

#[test]
fn openai_usage_accepts_cached_creation_tokens_as_legacy_input_alias() {
    let response = json!({
        "id": "resp_cache_write_legacy",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [],
        "usage": {
            "input_tokens": 10,
            "output_tokens": 2,
            "input_tokens_details": {"cached_creation_tokens": 5}
        }
    });

    let canonical = from_openai_responses_to_canonical_response(&response)
        .expect("legacy usage alias should parse");
    assert_eq!(canonical.usage.as_ref().unwrap().cache_write_tokens, 5);
}

#[test]
fn gemini_request_pairs_parallel_idless_function_responses_by_order() {
    let contents = json!([
        {
            "role": "model",
            "parts": [
                {"functionCall": {"name": "lookup", "args": {"q": "first"}}},
                {"functionCall": {"name": "lookup", "args": {"q": "second"}}}
            ]
        },
        {
            "role": "user",
            "parts": [
                {"functionResponse": {"name": "lookup", "response": {"result": "one"}}},
                {"functionResponse": {"name": "lookup", "response": {"result": "two"}}}
            ]
        }
    ]);

    let messages = super::gemini_contents_to_canonical_messages(Some(&contents))
        .expect("Gemini contents should parse");
    let call_ids = messages[0]
        .content
        .iter()
        .map(|block| match block {
            CanonicalContentBlock::ToolUse { id, .. } => id.as_str(),
            _ => panic!("expected tool use"),
        })
        .collect::<Vec<_>>();
    let result_ids = messages[1]
        .content
        .iter()
        .map(|block| match block {
            CanonicalContentBlock::ToolResult { tool_use_id, .. } => tool_use_id.as_str(),
            _ => panic!("expected tool result"),
        })
        .collect::<Vec<_>>();

    assert_ne!(call_ids[0], call_ids[1]);
    assert_eq!(result_ids, call_ids);
}

#[test]
fn gemini_request_pairs_idless_function_responses_by_name() {
    let contents = json!([{
        "role": "model",
        "parts": [
            {"functionCall": {"name": "first", "args": {}}},
            {"functionCall": {"name": "second", "args": {}}}
        ]
    }, {
        "role": "user",
        "parts": [
            {"functionResponse": {"name": "second", "response": {"result": 2}}},
            {"functionResponse": {"name": "first", "response": {"result": 1}}}
        ]
    }]);

    let messages = super::gemini_contents_to_canonical_messages(Some(&contents))
        .expect("Gemini contents should parse");
    let call_ids = messages[0]
        .content
        .iter()
        .map(|block| match block {
            CanonicalContentBlock::ToolUse { id, .. } => id.as_str(),
            _ => panic!("expected tool use"),
        })
        .collect::<Vec<_>>();
    let result_ids = messages[1]
        .content
        .iter()
        .map(|block| match block {
            CanonicalContentBlock::ToolResult { tool_use_id, .. } => tool_use_id.as_str(),
            _ => panic!("expected tool result"),
        })
        .collect::<Vec<_>>();

    assert_eq!(result_ids, vec![call_ids[1], call_ids[0]]);
}

#[test]
fn gemini_request_generated_function_call_ids_avoid_explicit_ids() {
    let contents = json!([{
        "role": "model",
        "parts": [
            {"functionCall": {"name": "first", "args": {}}},
            {"functionCall": {"id": "call_auto_0", "name": "second", "args": {}}}
        ]
    }]);

    let messages = super::gemini_contents_to_canonical_messages(Some(&contents))
        .expect("Gemini contents should parse");
    let call_ids = messages[0]
        .content
        .iter()
        .map(|block| match block {
            CanonicalContentBlock::ToolUse { id, .. } => id.as_str(),
            _ => panic!("expected tool use"),
        })
        .collect::<Vec<_>>();

    assert_eq!(call_ids, vec!["call_auto_1", "call_auto_0"]);
}

#[test]
fn gemini_request_generated_function_call_ids_avoid_explicit_response_ids() {
    let contents = json!([{
        "role": "model",
        "parts": [{"functionCall": {"name": "generated", "args": {}}}]
    }, {
        "role": "user",
        "parts": [{
            "functionResponse": {
                "id": "call_auto_0",
                "name": "external",
                "response": {"result": "done"}
            }
        }]
    }]);

    let messages = super::gemini_contents_to_canonical_messages(Some(&contents))
        .expect("Gemini contents should parse");
    let CanonicalContentBlock::ToolUse { id: call_id, .. } = &messages[0].content[0] else {
        panic!("expected tool use");
    };
    let CanonicalContentBlock::ToolResult { tool_use_id, .. } = &messages[1].content[0] else {
        panic!("expected tool result");
    };

    assert_eq!(call_id, "call_auto_1");
    assert_eq!(tool_use_id, "call_auto_0");
}

#[test]
fn gemini_request_adapter_preserves_thinking_tools_media_and_extensions() {
    let request = json!({
        "systemInstruction": {
            "parts": [
                {"text": "Be exact.", "cacheControl": {"type": "ephemeral"}}
            ]
        },
        "contents": [
            {
                "role": "user",
                "parts": [
                    {"text": "Inspect this"},
                    {"inlineData": {"mimeType": "image/png", "data": "iVBORw0KGgo="}},
                    {"fileData": {"fileUri": "https://example.com/spec.pdf", "mimeType": "application/pdf"}}
                ]
            },
            {
                "role": "model",
                "parts": [
                    {"text": "plan", "thought": true, "thoughtSignature": "sig_123"},
                    {"functionCall": {"id": "call_123", "name": "lookup", "args": {"q": "rust"}}}
                ]
            },
            {
                "role": "user",
                "parts": [{
                    "functionResponse": {
                        "id": "call_123",
                        "name": "lookup",
                        "response": {"result": {"ok": true}}
                    }
                }]
            }
        ],
        "generationConfig": {
            "maxOutputTokens": 64,
            "temperature": 0.2,
            "topP": 0.9,
            "topK": 40,
            "candidateCount": 2,
            "seed": 7,
            "stopSequences": ["END"],
            "thinkingConfig": {"includeThoughts": true, "thinkingBudget": 2048},
            "responseMimeType": "application/json",
            "responseSchema": {"type": "object"},
            "responseModalities": ["TEXT"],
            "routingConfig": {"autoMode": {}}
        },
        "tools": [
            {"googleSearch": {}},
            {"codeExecution": {}},
            {
                "functionDeclarations": [{
                    "name": "lookup",
                    "description": "Lookup data",
                    "parameters": {"type": "object"}
                }]
            }
        ],
        "toolConfig": {
            "functionCallingConfig": {
                "mode": "ANY",
                "allowedFunctionNames": ["lookup"]
            }
        },
        "safetySettings": [{"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"}],
        "cachedContent": "cached/abc"
    });

    let canonical =
        from_gemini_to_canonical_request(&request, "/v1beta/models/gemini-2.5-pro:generateContent")
            .expect("canonical request");

    assert_eq!(canonical.model, "gemini-2.5-pro");
    assert_eq!(canonical.instructions[0].text, "Be exact.");
    assert_eq!(canonical.generation.max_tokens, Some(64));
    assert_eq!(canonical.generation.top_k, Some(40));
    assert_eq!(canonical.generation.n, Some(2));
    assert_eq!(canonical.tools[0].name, "lookup");
    assert!(matches!(
        canonical.messages[0].content[1],
        CanonicalContentBlock::Image {
            ref data,
            ref media_type,
            ..
        } if data.as_deref() == Some("iVBORw0KGgo=")
            && media_type.as_deref() == Some("image/png")
    ));
    assert!(matches!(
        canonical.messages[1].content[0],
        CanonicalContentBlock::Thinking {
            ref text,
            ref signature,
            ..
        } if text == "plan" && signature.as_deref() == Some("sig_123")
    ));
    assert_eq!(
        canonical
            .extensions
            .get("gemini")
            .and_then(|value| value.get("cached_content"))
            .and_then(Value::as_str),
        Some("cached/abc")
    );
    assert_eq!(
        canonical
            .extensions
            .get("openai")
            .and_then(|value| value.get("web_search_options")),
        Some(&json!({}))
    );

    let rebuilt =
        canonical_to_gemini_request(&canonical, "gemini-upstream", false).expect("gemini");
    assert_eq!(rebuilt["model"], "gemini-upstream");
    assert_eq!(
        rebuilt["generationConfig"]["thinkingConfig"]["thinkingBudget"],
        2048
    );
    assert_eq!(
        rebuilt["generationConfig"]["responseModalities"],
        json!(["TEXT"])
    );
    assert_eq!(
        rebuilt["generationConfig"]["routingConfig"]["autoMode"],
        json!({})
    );
    assert_eq!(rebuilt["safetySettings"], request["safetySettings"]);
    assert_eq!(rebuilt["cachedContent"], "cached/abc");
    assert_eq!(rebuilt["tools"], request["tools"]);
    assert_eq!(rebuilt["toolConfig"], request["toolConfig"]);
}

#[test]
fn gemini_request_adapter_reads_json_schema_fields_and_lowercases_openapi_types() {
    let request = json!({
        "contents": [{"role": "user", "parts": [{"text": "hi"}]}],
        "tools": [{"functionDeclarations": [
            {
                "name": "search",
                "parametersJsonSchema": {
                    "type": "object",
                    "properties": {"q": {"type": "string"}},
                    "required": ["q"]
                }
            },
            {
                "name": "legacy",
                "parameters": {
                    "type": "OBJECT",
                    "properties": {
                        "type": {"type": "STRING", "enum": ["OBJECT", "STRING"]},
                        "tags": {"type": "ARRAY", "items": {"type": "STRING"}}
                    }
                }
            }
        ]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseJsonSchema": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"]
            }
        }
    });

    let canonical =
        from_gemini_to_canonical_request(&request, "/v1beta/models/grok-4.7:generateContent")
            .expect("canonical request");

    assert_eq!(
        canonical.tools[0].parameters,
        Some(json!({
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "required": ["q"]
        }))
    );
    assert_eq!(
        canonical.tools[1].parameters,
        Some(json!({
            "type": "object",
            "properties": {
                "type": {"type": "string", "enum": ["OBJECT", "STRING"]},
                "tags": {"type": "array", "items": {"type": "string"}}
            }
        }))
    );
    let response_format = canonical.response_format.as_ref().expect("response format");
    assert_eq!(response_format.format_type, "json_schema");
    assert_eq!(
        response_format.json_schema.as_ref().expect("schema")["schema"]["required"],
        json!(["name"])
    );

    let legacy_schema = super::gemini_response_format_to_canonical(Some(&json!({
        "responseMimeType": "application/json",
        "responseSchema": {"type": "OBJECT", "properties": {"n": {"type": "INTEGER"}}}
    })))
    .expect("legacy response format");
    assert_eq!(
        legacy_schema.json_schema.expect("legacy schema")["schema"],
        json!({"type": "object", "properties": {"n": {"type": "integer"}}})
    );
}

#[test]
fn gemini_request_adapter_normalizes_google_search_grounding_aliases() {
    let cases = [
        (
            "current_camel",
            json!({"googleSearch": {"excludeDomains": ["example.com"]}}),
            "googleSearch",
            false,
            json!({"excludeDomains": ["example.com"]}),
            json!({"excludeDomains": ["example.com"]}),
        ),
        (
            "current_snake",
            json!({"google_search": {"exclude_domains": ["example.com"]}}),
            "google_search",
            false,
            json!({"excludeDomains": ["example.com"]}),
            json!({"excludeDomains": ["example.com"]}),
        ),
        (
            "legacy_snake",
            json!({
                "google_search_retrieval": {
                    "dynamic_retrieval_config": {
                        "mode": "MODE_DYNAMIC",
                        "dynamic_threshold": 0.7
                    }
                }
            }),
            "google_search_retrieval",
            true,
            json!({
                "dynamicRetrievalConfig": {
                    "mode": "MODE_DYNAMIC",
                    "dynamicThreshold": 0.7
                }
            }),
            json!({}),
        ),
        (
            "legacy_camel",
            json!({
                "googleSearchRetrieval": {
                    "dynamicRetrievalConfig": {
                        "mode": "MODE_DYNAMIC",
                        "dynamicThreshold": 0.7
                    }
                }
            }),
            "googleSearchRetrieval",
            true,
            json!({
                "dynamicRetrievalConfig": {
                    "mode": "MODE_DYNAMIC",
                    "dynamicThreshold": 0.7
                }
            }),
            json!({}),
        ),
    ];

    for (name, tool, source_field, legacy, expected_extension_payload, expected_output_payload) in
        cases
    {
        let request = json!({
            "model": "gemini-2.5-pro",
            "contents": [{"role": "user", "parts": [{"text": "search"}]}],
            "tools": [tool]
        });

        let canonical = from_gemini_to_canonical_request(
            &request,
            "/v1beta/models/gemini-2.5-pro:generateContent",
        )
        .unwrap_or_else(|| panic!("{name}: canonical request"));

        assert_eq!(
            canonical
                .extensions
                .get("openai")
                .and_then(|value| value.get("web_search_options")),
            Some(&json!({})),
            "{name}: web search option"
        );
        let google_search = canonical
            .extensions
            .get("gemini")
            .and_then(|value| value.get("grounding"))
            .and_then(|value| value.get("google_search"))
            .unwrap_or_else(|| panic!("{name}: gemini google_search grounding"));
        assert_eq!(
            google_search.get("source_field").and_then(Value::as_str),
            Some(source_field),
            "{name}: source field"
        );
        assert_eq!(
            google_search.get("legacy").and_then(Value::as_bool),
            Some(legacy),
            "{name}: legacy flag"
        );
        assert_eq!(
            google_search.get("payload"),
            Some(&expected_extension_payload),
            "{name}: normalized payload"
        );

        let rebuilt = canonical_to_gemini_request(&canonical, "gemini-upstream", false).unwrap();
        assert_eq!(
            rebuilt["tools"],
            json!([{"googleSearch": expected_output_payload}]),
            "{name}: canonical output"
        );
    }
}

#[test]
fn gemini_request_adapter_keeps_agent_search_retrieval_separate_from_google_search() {
    let request = json!({
        "model": "gemini-2.5-pro",
        "contents": [{"role": "user", "parts": [{"text": "private data"}]}],
        "tools": [{
            "retrieval": {
                "vertexAiSearch": {
                    "datastore": "projects/p/locations/global/collections/default_collection/dataStores/d"
                }
            }
        }]
    });

    let canonical =
        from_gemini_to_canonical_request(&request, "/v1beta/models/gemini-2.5-pro:generateContent")
            .expect("canonical request");

    assert_eq!(
        canonical
            .extensions
            .get("openai")
            .and_then(|value| value.get("web_search_options")),
        None
    );

    let rebuilt = canonical_to_gemini_request(&canonical, "gemini-upstream", false).unwrap();
    assert_eq!(rebuilt["tools"], request["tools"]);
    assert!(rebuilt["tools"]
        .as_array()
        .unwrap()
        .iter()
        .all(|tool| tool.get("googleSearch").is_none()));
}

#[test]
fn gemini_request_adapter_preserves_combined_search_builtin_tool_fields() {
    let cases = [
        (
            "current_snake",
            json!({
                "google_search": {},
                "code_execution": {},
                "url_context": {},
                "retrieval": {
                    "vertexAiSearch": {
                        "datastore": "projects/p/locations/global/collections/default_collection/dataStores/d"
                    }
                }
            }),
        ),
        (
            "legacy_snake",
            json!({
                "google_search_retrieval": {
                    "dynamic_retrieval_config": {
                        "mode": "MODE_DYNAMIC",
                        "dynamic_threshold": 0.7
                    }
                },
                "code_execution": {},
                "url_context": {}
            }),
        ),
    ];

    for (name, tool) in cases {
        let request = json!({
            "model": "gemini-2.5-pro",
            "contents": [{"role": "user", "parts": [{"text": "search with builtins"}]}],
            "tools": [tool]
        });

        let canonical = from_gemini_to_canonical_request(
            &request,
            "/v1beta/models/gemini-2.5-pro:generateContent",
        )
        .unwrap_or_else(|| panic!("{name}: canonical request"));

        let rebuilt = canonical_to_gemini_request(&canonical, "gemini-upstream", false).unwrap();
        let tools = rebuilt["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("{name}: tools array"));
        assert!(
            tools.iter().any(|tool| tool.get("googleSearch").is_some()),
            "{name}: google search should be preserved"
        );
        assert!(
            tools.iter().any(|tool| tool.get("codeExecution").is_some()),
            "{name}: code execution should be preserved"
        );
        assert!(
            tools.iter().any(|tool| tool.get("urlContext").is_some()),
            "{name}: URL context should be preserved"
        );
        if name == "current_snake" {
            assert!(
                tools.iter().any(|tool| tool.get("retrieval").is_some()),
                "{name}: unhandled retrieval should be preserved"
            );
        }
    }
}

#[test]
fn gemini_response_adapter_preserves_thought_signature_tool_and_usage() {
    let response = json!({
        "responseId": "resp_123",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [{
            "index": 0,
            "finishReason": "STOP",
            "content": {
                "parts": [
                    {"text": "plan", "thought": true, "thoughtSignature": "sig_123"},
                    {"text": "done"},
                    {"functionCall": {"id": "call_123", "name": "lookup", "args": {"q": "rust"}}},
                    {
                        "functionResponse": {
                            "id": "call_123",
                            "name": "lookup",
                            "response": {"result": {"ok": true}}
                        }
                    }
                ]
            }
        }],
        "usageMetadata": {
            "promptTokenCount": 10,
            "cachedContentTokenCount": 4,
            "candidatesTokenCount": 5,
            "thoughtsTokenCount": 2,
            "totalTokenCount": 17
        }
    });

    let canonical = from_gemini_to_canonical_response(&response).expect("canonical response");
    assert_eq!(canonical.id, "resp_123");
    assert_eq!(canonical.model, "gemini-2.5-pro");
    assert!(matches!(
        canonical.content[0],
        CanonicalContentBlock::Thinking {
            ref text,
            ref signature,
            ..
        } if text == "plan" && signature.as_deref() == Some("sig_123")
    ));
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolUse { name, .. } if name == "lookup")
    ));
    assert!(canonical.content.iter().any(
        |block| matches!(block, CanonicalContentBlock::ToolResult {
            tool_use_id,
            name: Some(name),
            output: Some(output),
            ..
        } if tool_use_id == "call_123" && name == "lookup" && output == &json!({"ok": true}))
    ));
    assert_eq!(canonical.usage.as_ref().unwrap().input_tokens, 10);
    assert!(canonical.usage.as_ref().unwrap().input_tokens_include_cache);
    assert_eq!(canonical.usage.as_ref().unwrap().cache_read_tokens, 4);
    assert_eq!(canonical.usage.as_ref().unwrap().output_tokens, 7);
    assert_eq!(canonical.usage.as_ref().unwrap().reasoning_tokens, 2);

    let claude = canonical_to_claude_response(&canonical);
    assert_eq!(claude["usage"]["input_tokens"], 6);
    assert_eq!(claude["usage"]["cache_read_input_tokens"], 4);

    let rebuilt = canonical_to_gemini_response(&canonical, &json!({})).expect("gemini");
    assert_eq!(
        rebuilt["candidates"][0]["content"]["parts"][0]["thoughtSignature"],
        "sig_123"
    );
    assert_eq!(
        rebuilt["candidates"][0]["content"]["parts"][2]["functionCall"]["name"],
        "lookup"
    );
    assert_eq!(
        rebuilt["candidates"][0]["content"]["parts"][3]["functionResponse"]["response"],
        json!({"ok": true})
    );
    assert_eq!(rebuilt["usageMetadata"]["promptTokenCount"], 10);
    assert_eq!(rebuilt["usageMetadata"]["cachedContentTokenCount"], 4);
    assert_eq!(rebuilt["usageMetadata"]["totalTokenCount"], 17);
    assert_eq!(rebuilt["usageMetadata"]["thoughtsTokenCount"], 2);
}

#[test]
fn gemini_response_adapter_preserves_grounding_metadata() {
    let grounding_metadata = json!({
        "webSearchQueries": ["query"],
        "searchEntryPoint": {"renderedContent": "<style></style>"},
        "groundingChunks": [{
            "web": {
                "uri": "https://example.com",
                "title": "Example"
            }
        }],
        "groundingSupports": []
    });
    let response = json!({
        "responseId": "resp_grounded",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [{
            "index": 0,
            "finishReason": "STOP",
            "groundingMetadata": grounding_metadata,
            "content": {
                "parts": [{"text": "grounded answer"}]
            }
        }]
    });

    let canonical = from_gemini_to_canonical_response(&response).expect("canonical response");
    assert_eq!(
        canonical.outputs[0]
            .extensions
            .get("gemini")
            .and_then(|value| value.get("groundingMetadata")),
        Some(&grounding_metadata)
    );

    let rebuilt = canonical_to_gemini_response(&canonical, &json!({})).expect("gemini");
    assert_eq!(
        rebuilt["candidates"][0]["groundingMetadata"],
        grounding_metadata
    );
}

#[test]
fn canonical_response_preserves_openai_choices_and_gemini_candidates() {
    let openai_response = json!({
        "id": "chatcmpl_multi",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "first"},
                "finish_reason": "stop"
            },
            {
                "index": 1,
                "message": {"role": "assistant", "content": "second"},
                "finish_reason": "length"
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3}
    });
    let canonical = from_openai_chat_to_canonical_response(&openai_response).expect("canonical");
    assert_eq!(canonical.outputs.len(), 2);
    let rebuilt = canonical_to_openai_chat_response(&canonical);
    assert_eq!(rebuilt["choices"][0]["message"]["content"], "first");
    assert_eq!(rebuilt["choices"][1]["message"]["content"], "second");
    assert_eq!(rebuilt["choices"][1]["finish_reason"], "length");

    let gemini_response = json!({
        "responseId": "gemini_multi",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [
            {
                "index": 0,
                "finishReason": "STOP",
                "content": {"role": "model", "parts": [{"text": "first"}]}
            },
            {
                "index": 1,
                "finishReason": "MAX_TOKENS",
                "content": {"role": "model", "parts": [{"text": "second"}]}
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 1,
            "candidatesTokenCount": 2,
            "totalTokenCount": 3
        }
    });
    let canonical = from_gemini_to_canonical_response(&gemini_response).expect("canonical");
    assert_eq!(canonical.outputs.len(), 2);
    let rebuilt = canonical_to_openai_chat_response(&canonical);
    assert_eq!(rebuilt["choices"][0]["message"]["content"], "first");
    assert_eq!(rebuilt["choices"][1]["message"]["content"], "second");
    assert_eq!(rebuilt["choices"][1]["finish_reason"], "length");
}
