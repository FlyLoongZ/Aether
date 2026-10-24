use super::*;
use crate::formats::claude::messages::stream::ClaudeClientEmitter;
use crate::formats::gemini::generate_content::stream::GeminiClientEmitter;
use crate::formats::openai::responses::encode_gemini_tool_signature_carrier;

fn data_line(value: Value) -> Vec<u8> {
    format!("data: {}\n", value).into_bytes()
}

fn terminal_frames(frames: Vec<CanonicalStreamFrame>) -> Vec<CanonicalStreamFrame> {
    frames
        .into_iter()
        .filter(|frame| {
            matches!(
                frame.event,
                CanonicalStreamEvent::Start
                    | CanonicalStreamEvent::UnknownEvent(_)
                    | CanonicalStreamEvent::Finish { .. }
            )
        })
        .collect()
}

fn assert_no_responses_observation_content(state: &OpenAIResponsesProviderState) {
    assert!(state.text_parts.is_empty());
    assert_eq!(state.reasoning.capacity(), 0);
    assert!(state.reasoning_parts.is_empty());
    assert!(state
        .tool_calls
        .values()
        .all(|tool| tool.arguments.capacity() == 0));
    assert!(state.tool_results.is_empty());
    assert!(state.image_item_keys.is_empty());
    assert!(state
        .opaque_completed_item_keys
        .iter()
        .all(|key| matches!(key, OpenAIResponsesOutputItemKey::Digest(_))));
}

#[test]
fn openai_responses_terminal_observation_does_not_retain_long_stream_content() {
    let context = json!({"provider_api_format": "openai:responses"});
    let mut observed = OpenAIResponsesProviderState::terminal_observation();
    let mut full = OpenAIResponsesProviderState::default();
    let initial = json!({
        "type": "response.output_item.added", "output_index": 0,
        "item": {"type": "function_call", "id": "fc_1", "call_id": "call_1", "name": "read", "arguments": ""}
    });
    assert_eq!(
        observed.push_event(&context, &initial).unwrap(),
        terminal_frames(full.push_event(&context, &initial).unwrap())
    );
    let text = "x".repeat(1024);
    let events = [
        json!({"type": "response.output_text.delta", "delta": text, "output_index": 3}),
        json!({"type": "response.reasoning_summary_text.delta", "delta": text, "summary_index": 0}),
        json!({"type": "response.function_call_arguments.delta", "delta": text, "output_index": 0}),
        json!({"type": "response.custom_tool_call_input.delta", "delta": text, "output_index": 1}),
        json!({"type": "response.function_call_output.delta", "delta": text, "output_index": 2, "call_id": "call_1"}),
    ];
    for iteration in 0..2048 {
        for event in &events {
            let frames = observed.push_event(&context, event).unwrap();
            assert!(frames.is_empty());
            if iteration < 32 {
                assert_eq!(
                    frames,
                    terminal_frames(full.push_event(&context, event).unwrap())
                );
            }
        }
        assert_no_responses_observation_content(&observed);
    }
    assert_eq!(observed.tool_calls.len(), 2);
    assert_eq!(observed.tool_index_by_key, full.tool_index_by_key);
    assert!(full.text_parts.values().any(|text| text.len() == 32 * 1024));
    assert_eq!(full.reasoning.len(), 32 * 1024);
    assert_eq!(full.reasoning_parts[&0].len(), 32 * 1024);
    assert_eq!(full.tool_calls[&0].arguments.len(), 32 * 1024);
    assert!(!full.tool_results.is_empty());
    assert_eq!(
        observed.finish(&context).unwrap(),
        full.finish(&context).unwrap()
    );
}

#[test]
fn openai_responses_terminal_observation_skips_completed_content_and_images() {
    let context = json!({});
    let text = "x".repeat(64 * 1024);
    let items = [
        json!({"type": "message", "content": [{"type": "output_text", "text": text}]}),
        json!({"type": "reasoning", "summary": [{"type": "summary_text", "text": text}]}),
        json!({"type": "custom_tool_call", "input": text, "name": "custom"}),
        json!({"type": "shell_call", "action": {"command": text}}),
        json!({"type": "apply_patch_call", "operation": {"patch": text}}),
        json!({"type": "computer_call", "action": {"keys": [text]}}),
        json!({"type": "function_call_output", "output": text}),
        json!({"type": "custom_tool_call_output", "output": {"text": text}}),
        json!({"type": "image_generation_call", "result": text, "status": "completed"}),
    ];
    let mut observed = OpenAIResponsesProviderState::terminal_observation();
    let mut full = OpenAIResponsesProviderState::default();
    for (index, item) in items.iter().enumerate() {
        let event =
            json!({"type": "response.output_item.done", "output_index": index, "item": item});
        assert_eq!(
            observed.push_event(&context, &event).unwrap(),
            terminal_frames(full.push_event(&context, &event).unwrap())
        );
        assert_no_responses_observation_content(&observed);
    }
    let completed = json!({"type": "response.completed", "response": {
        "id": "resp_complete", "model": "model", "service_tier": "Priority",
        "output": items, "usage": {"input_tokens": 100, "output_tokens": 20, "input_tokens_details": {"cached_tokens": 0}}
    }});
    assert_eq!(
        observed.push_event(&context, &completed).unwrap(),
        terminal_frames(full.push_event(&context, &completed).unwrap())
    );
    assert_eq!(observed.actual_service_tier(), Some("priority"));
    assert_no_responses_observation_content(&observed);
}

#[test]
fn openai_responses_terminal_observation_hashes_opaque_keys_without_changing_deduplication() {
    let context = json!({});
    let text = "x".repeat(64 * 1024);
    let items = [
        json!({"type": "future_item", "id": "id:with:separators", "encrypted_content": text}),
        json!({"type": "compaction", "encrypted_content": text}),
        json!({"type": "future_item", "payload": {"text": text, "escaped": "\n\"\\"}}),
    ];
    let mut observed = OpenAIResponsesProviderState::terminal_observation();
    let mut full = OpenAIResponsesProviderState::default();
    for item in &items {
        let object = item.as_object().unwrap();
        let normal_key = OpenAIResponsesProviderState::output_item_key(object);
        let expected: [u8; 32] = Sha256::digest(normal_key.as_bytes()).into();
        let OpenAIResponsesOutputItemKey::Digest(actual) =
            observed.retained_output_item_key(object)
        else {
            panic!("observation must retain only an opaque item digest");
        };
        assert_eq!(actual, expected);
        let event = json!({"type": "response.output_item.done", "item": item});
        for _ in 0..2 {
            assert_eq!(
                observed.push_event(&context, &event).unwrap(),
                terminal_frames(full.push_event(&context, &event).unwrap())
            );
        }
    }
    assert_eq!(observed.opaque_completed_item_keys.len(), items.len());
    assert_no_responses_observation_content(&observed);
    let mut final_items = items.to_vec();
    final_items.push(json!({"type": "future_item", "id": "new_item"}));
    let final_event = json!({"type": "response.completed", "response": {"output": final_items}});
    let frames = observed.push_event(&context, &final_event).unwrap();
    assert_eq!(
        frames,
        terminal_frames(full.push_event(&context, &final_event).unwrap())
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_)))
            .count(),
        1
    );
}

#[test]
fn openai_responses_terminal_observation_preserves_tool_identity_validation() {
    let context = json!({"original_request_body": {"tools": [{
        "type": "namespace", "name": "reports", "description": "Reporting tools", "tools": [{
            "type": "function", "name": "write_report", "parameters": {"type": "object"}
        }]
    }]}});
    let expected_alias = NamespaceToolAliases::from_report_context(&context)
        .chat_name("reports", "write_report")
        .expect("fixture must contain a valid namespace tool")
        .to_owned();
    let mut observed = OpenAIResponsesProviderState::terminal_observation();
    let mut full = OpenAIResponsesProviderState::default();
    let events = [
        json!({"type": "response.function_call_arguments.delta", "item_id": "fc_1", "output_index": 0, "delta": "{"}),
        json!({"type": "response.output_item.added", "output_index": 0, "item": {
            "type": "function_call", "id": "fc_1", "namespace": "reports", "name": "write_report", "arguments": ""
        }}),
        json!({"type": "response.function_call_arguments.done", "item_id": "fc_1", "call_id": "call_1", "namespace": "reports", "arguments": "{}"}),
        json!({"type": "response.function_call_output.delta", "call_id": "call_1", "output_index": 7, "delta": "result"}),
        json!({"type": "response.function_call_arguments.done", "item_id": "fc_other", "name": "ordinary", "arguments": "{}"}),
        json!({"type": "response.function_call_arguments.done", "item_id": "fc_1", "namespace": "missing", "arguments": "{}"}),
        json!({"type": "response.output_item.done", "item": {
            "type": "function_call", "id": "invalid", "name": "read", "caller": {"type": "future"}, "arguments": "{}"
        }}),
        json!({"type": "response.output_item.done", "item": {"content": "missing type"}}),
        json!({"type": "response.future.delta", "payload": "unsupported"}),
    ];
    let mut unknowns = 0;
    for (step, event) in events.into_iter().enumerate() {
        let frames = observed.push_event(&context, &event).unwrap();
        assert_eq!(
            frames,
            terminal_frames(full.push_event(&context, &event).unwrap())
        );
        unknowns += frames
            .iter()
            .filter(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_)))
            .count();
        assert_eq!(observed.tool_index_by_key, full.tool_index_by_key);
        assert_eq!(observed.last_tool_index, full.last_tool_index);
        assert_eq!(observed.tool_calls.len(), full.tool_calls.len());
        for (index, tool) in &observed.tool_calls {
            assert_eq!(tool.name, full.tool_calls[index].name);
            assert_eq!(tool.call_id, full.tool_calls[index].call_id);
        }
        if step == 1 || step == 2 {
            assert_eq!(unknowns, 0);
            assert_eq!(observed.tool_calls[&0].name, expected_alias);
            assert_eq!(
                observed.tool_calls[&0].call_id,
                if step == 1 { "" } else { "call_1" }
            );
        }
        assert_no_responses_observation_content(&observed);
    }
    assert_eq!(unknowns, 4);
    assert_eq!(
        observed.finish(&context).unwrap(),
        full.finish(&context).unwrap()
    );
}

#[test]
fn openai_chat_terminal_observation_does_not_buffer_arguments_before_identity() {
    let context = json!({});
    let mut observed = OpenAIChatProviderState::terminal_observation();
    let mut full = OpenAIChatProviderState::default();
    let delta = json!({"choices": [{"delta": {
        "content": "x".repeat(1024), "reasoning_content": "r".repeat(1024),
        "tool_calls": [{"index": 0, "function": {"arguments": "a".repeat(1024)}}]
    }}]});
    for iteration in 0..2048 {
        let frames = observed
            .push_line(&context, data_line(delta.clone()))
            .unwrap();
        if iteration < 32 {
            assert_eq!(
                frames,
                terminal_frames(full.push_line(&context, data_line(delta.clone())).unwrap())
            );
        }
        assert!(observed.tool_calls.is_empty());
    }
    assert_eq!(full.tool_calls[&0].pending_arguments.len(), 32 * 1024);
    for event in [
        json!({"choices": [{"delta": {"tool_calls": [{"index": 0, "id": "call_1", "function": {"name": "read", "arguments": "end"}}]}}]}),
        json!({"choices": [{"delta": {"future": "unknown"}}]}),
        json!({"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}),
        json!({"choices": [], "usage": {"prompt_tokens": 100, "completion_tokens": 20, "prompt_tokens_details": {"cached_tokens": 0}}, "service_tier": "Flex"}),
    ] {
        assert_eq!(
            observed
                .push_line(&context, data_line(event.clone()))
                .unwrap(),
            terminal_frames(full.push_line(&context, data_line(event)).unwrap())
        );
    }
    assert_eq!(observed.actual_service_tier(), Some("flex"));
    assert!(observed.tool_calls.is_empty());
    assert_eq!(
        observed.finish(&context).unwrap(),
        full.finish(&context).unwrap()
    );
}

fn response_sequence_numbers(sse: &str) -> Vec<u64> {
    let mut sequence_numbers = Vec::new();
    for payload in sse.lines().filter_map(|line| line.strip_prefix("data: ")) {
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        let Some(ty) = value.get("type").and_then(Value::as_str) else {
            continue;
        };
        if !ty.starts_with("response.") {
            continue;
        }
        if let Some(sequence_number) = value.get("sequence_number").and_then(Value::as_u64) {
            sequence_numbers.push(sequence_number);
        }
    }
    sequence_numbers
}

fn response_reasoning_text_done_parts(sse: &str) -> Vec<(u64, String)> {
    let mut parts = Vec::new();
    for block in sse.split("\n\n") {
        let mut event_name = None;
        let mut data = None;
        for line in block.lines() {
            if let Some(value) = line.strip_prefix("event: ") {
                event_name = Some(value);
            } else if let Some(value) = line.strip_prefix("data: ") {
                data = Some(value);
            }
        }
        if event_name != Some("response.reasoning_text.done") {
            continue;
        }
        let Some(data) = data else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        let Some(part_index) = value
            .get("content_index")
            .or_else(|| value.get("summary_index"))
            .and_then(Value::as_u64)
        else {
            continue;
        };
        let Some(text) = value.get("text").and_then(Value::as_str) else {
            continue;
        };
        parts.push((part_index, text.to_string()));
    }
    parts
}

fn openai_chat_tool_call_indices(sse: &str) -> Vec<u64> {
    let mut indices = Vec::new();
    for payload in sse.lines().filter_map(|line| line.strip_prefix("data: ")) {
        let Ok(value) = serde_json::from_str::<Value>(payload) else {
            continue;
        };
        let Some(tool_calls) = value
            .pointer("/choices/0/delta/tool_calls")
            .and_then(Value::as_array)
        else {
            continue;
        };
        for tool_call in tool_calls {
            if let Some(index) = tool_call.get("index").and_then(Value::as_u64) {
                indices.push(index);
            }
        }
    }
    indices
}

#[test]
fn openai_chat_provider_state_emits_unknown_events_for_unrecognized_deltas() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "chatcmpl_unknown_123",
                "model": "gpt-5.4",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "future_delta_type": {
                            "payload": true
                        }
                    }
                }]
            })),
        )
        .expect("unknown delta should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::UnknownEvent(ref payload)
            if payload.get("delta")
                .and_then(|delta| delta.get("future_delta_type"))
                .is_some()
    )));
}

#[test]
fn openai_chat_provider_state_reads_openrouter_reasoning_once() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "gen-openrouter-123",
                "model": "stealth/ox-alpha",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "content": "",
                        "role": "assistant",
                        "reasoning": " me translate the",
                        "reasoning_details": [{
                            "type": "reasoning.text",
                            "text": " me translate the",
                            "format": "unknown",
                            "index": 0
                        }]
                    },
                    "finish_reason": Value::Null
                }]
            })),
        )
        .expect("openrouter reasoning delta should parse");

    let reasoning = frames
        .iter()
        .filter_map(|frame| match frame.event {
            CanonicalStreamEvent::ReasoningDelta(ref text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    // `reasoning` and `reasoning_details` repeat the same text, so only one
    // of the two may reach the client.
    assert_eq!(reasoning, vec![" me translate the"]);
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_))));
}

#[test]
fn openai_chat_provider_state_still_reads_deepseek_reasoning_content() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "chatcmpl-deepseek",
                "model": "deepseek-reasoner",
                "choices": [{
                    "index": 0,
                    "delta": {"role": "assistant", "reasoning_content": "let me work it out"},
                    "finish_reason": Value::Null
                }]
            })),
        )
        .expect("deepseek reasoning delta should parse");

    let reasoning = frames
        .iter()
        .filter_map(|frame| match frame.event {
            CanonicalStreamEvent::ReasoningDelta(ref text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reasoning, vec!["let me work it out"]);
    // A single reasoning block carries no index, so nothing may close a part.
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::ReasoningSummaryDone)));
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_))));
}

#[test]
fn openai_chat_provider_state_splits_reasoning_details_on_block_index() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let reasoning_chunk = |index: u64, text: &str| {
        data_line(json!({
            "id": "gen-openrouter-123",
            "model": "stealth/ox-alpha",
            "choices": [{
                "index": 0,
                "delta": {
                    "content": "",
                    "role": "assistant",
                    "reasoning_details": [{
                        "type": "reasoning.text",
                        "text": text,
                        "index": index
                    }]
                },
                "finish_reason": Value::Null
            }]
        }))
    };
    let mut frames = state
        .push_line(&report_context, reasoning_chunk(0, "first"))
        .expect("first reasoning block should parse");
    frames.extend(
        state
            .push_line(&report_context, reasoning_chunk(1, "second"))
            .expect("second reasoning block should parse"),
    );

    let reasoning = frames
        .iter()
        .filter_map(|frame| match frame.event {
            CanonicalStreamEvent::ReasoningDelta(ref text) => Some(text.as_str()),
            CanonicalStreamEvent::ReasoningSummaryDone => Some("<part-done>"),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reasoning, vec!["first", "<part-done>", "second"]);
}

#[test]
fn openai_chat_reasoning_only_stream_reaches_responses_clients() {
    // Regression: OpenRouter streams a long reasoning phase as chunks whose
    // `delta.content` is an empty string and whose text sits under
    // `reasoning`.  Dropping those chunks left Responses clients with
    // nothing after `response.in_progress` until they timed the stream out.
    let mut state = OpenAIChatProviderState::default();
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let report_context = json!({});
    let mut bytes = Vec::new();
    for piece in ["Let", " me think."] {
        let frames = state
            .push_line(
                &report_context,
                data_line(json!({
                    "id": "gen-openrouter-123",
                    "model": "stealth/ox-alpha",
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "content": "",
                            "role": "assistant",
                            "reasoning": piece,
                            "reasoning_details": [{
                                "type": "reasoning.text",
                                "text": piece,
                                "format": "unknown",
                                "index": 0
                            }]
                        },
                        "finish_reason": Value::Null
                    }]
                })),
            )
            .expect("reasoning chunk should parse");
        for frame in frames {
            bytes.extend(emitter.emit(frame).expect("frame should encode"));
        }
    }

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.reasoning_text.delta\n"));
    assert!(!sse.contains("event: response.reasoning_summary_text.delta\n"));
    assert!(sse.contains("\"delta\":\"Let\""));
    assert!(sse.contains("\"delta\":\" me think.\""));
}

#[test]
fn openai_chat_provider_state_waits_for_real_tool_call_identity() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let mut frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "chatcmpl_tool_123",
                "model": "deepseek-v4-flash",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "tool_calls": [{
                            "index": 0,
                            "function": {"arguments": "{\"path\":"}
                        }]
                    }
                }]
            })),
        )
        .expect("arguments-first delta should parse");
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "id": "chatcmpl_tool_123",
                    "model": "deepseek-v4-flash",
                    "choices": [{
                        "index": 0,
                        "delta": {
                            "tool_calls": [{
                                "index": 0,
                                "function": {"name": "security_scan", "arguments": "\"src\"}"}
                            }]
                        }
                    }]
                })),
            )
            .expect("name delta should parse"),
    );

    assert!(!frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { .. }
            | CanonicalStreamEvent::ToolCallArgumentsDelta { .. }
    )));

    let identified = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "chatcmpl_tool_123",
                "model": "deepseek-v4-flash",
                "choices": [{
                    "index": 0,
                    "delta": {"tool_calls": [{"index": 0, "id": "call_security_123"}]}
                }]
            })),
        )
        .expect("id delta should parse");

    assert!(matches!(
        identified.first().map(|frame| &frame.event),
        Some(CanonicalStreamEvent::ToolCallStart { call_id, name, .. })
            if call_id == "call_security_123" && name == "security_scan"
    ));
    assert!(matches!(
        identified.get(1).map(|frame| &frame.event),
        Some(CanonicalStreamEvent::ToolCallArgumentsDelta { arguments, .. })
            if arguments == "{\"path\":\"src\"}"
    ));
}

#[test]
fn openai_responses_provider_state_emits_unknown_events_for_unknown_response_types() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.future.delta",
                "response": {
                    "id": "resp_unknown_123",
                    "model": "gpt-5.4"
                },
                "delta": {
                    "payload": true
                }
            })),
        )
        .expect("unknown response event should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::UnknownEvent(ref payload)
            if payload.get("type").and_then(Value::as_str) == Some("response.future.delta")
    )));
}

#[test]
fn openai_responses_provider_state_recognizes_compaction_output_without_index() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let raw_event = json!({
        "type": "response.output_item.done",
        "item": {
            "type": "compaction",
            "encrypted_content": "ENCRYPTED_CONTEXT_COMPACTION_SUMMARY"
        }
    });
    let frames = state
        .push_line(&report_context, data_line(raw_event.clone()))
        .expect("compaction output item should parse");

    assert!(frames.iter().any(|frame| matches!(
        &frame.event,
        CanonicalStreamEvent::OpenAiResponsesOutputItem {
            output_index: None,
            item,
            raw_event: emitted_event,
        } if item["type"] == "compaction" && emitted_event == &raw_event
    )));
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_))));

    let terminal = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.completed",
                "response": {
                    "id": "resp-compact",
                    "usage": {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "total_tokens": 0
                    }
                }
            })),
        )
        .expect("completed response should parse");
    assert!(terminal
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::Finish { .. })));
    assert!(!terminal
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_))));
}

#[test]
fn openai_responses_provider_state_rejects_function_call_provenance() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.added",
                "response_id": "resp_ptc_123",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_123",
                    "call_id": "call_123",
                    "status": "completed",
                    "name": "lookup",
                    "arguments": "{}",
                    "caller": {"type": "program", "id": "program_123"}
                }
            })),
        )
        .expect("function call event should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::UnknownEvent(ref payload)
            if payload.get("caller").is_some()
    )));
    assert!(!frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { .. }
            | CanonicalStreamEvent::ToolCallArgumentsDelta { .. }
    )));
}

#[test]
fn openai_responses_provider_state_accepts_chat_backed_function_call_metadata() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.added",
                "response_id": "resp_chat_metadata_123",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_chat_metadata_123",
                    "call_id": "call_chat_metadata_123",
                    "status": "completed",
                    "name": "lookup",
                    "arguments": "{\"query\":\"aether\"}",
                    "internal_chat_message_metadata_passthrough": {
                        "turn_id": "turn_123"
                    },
                    "metadata": {
                        "source": "chat"
                    }
                }
            })),
        )
        .expect("chat-backed function call event should parse");

    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_))));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart {
            index: 0,
            ref call_id,
            ref name,
        } if call_id == "call_chat_metadata_123" && name == "lookup"
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallArgumentsDelta {
            index: 0,
            ref arguments,
        } if arguments == "{\"query\":\"aether\"}"
    )));
}

#[test]
fn openai_responses_provider_state_rejects_unpaired_function_call_metadata() {
    let mut state = OpenAIResponsesProviderState::default();
    let frames = state
        .push_line(
            &json!({}),
            data_line(json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_metadata_only_123",
                    "call_id": "call_metadata_only_123",
                    "status": "completed",
                    "name": "lookup",
                    "arguments": "{}",
                    "metadata": {"semantic": true}
                }
            })),
        )
        .expect("function call event should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::UnknownEvent(ref payload)
            if payload.get("metadata").is_some()
    )));
    assert!(!frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { .. }
            | CanonicalStreamEvent::ToolCallArgumentsDelta { .. }
    )));
}

#[test]
fn openai_responses_provider_state_rejects_chat_metadata_for_non_openai_clients() {
    let mut state = OpenAIResponsesProviderState::default();
    let frames = state
        .push_line(
            &json!({"client_api_format": "claude:messages"}),
            data_line(json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_chat_metadata_123",
                    "call_id": "call_chat_metadata_123",
                    "status": "completed",
                    "name": "lookup",
                    "arguments": "{}",
                    "metadata": {"source": "chat"},
                    "internal_chat_message_metadata_passthrough": {
                        "turn_id": "turn_123"
                    }
                }
            })),
        )
        .expect("function call event should parse");

    assert!(frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent(_))));
}

#[test]
fn openai_responses_provider_state_treats_failed_event_as_terminal() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.failed",
                "response": {
                    "id": "resp_failed_123",
                    "model": "gpt-5.4",
                    "status": "failed",
                    "error": {
                        "message": "policy failure",
                        "type": "invalid_request_error",
                        "code": "cyber_policy"
                    }
                }
            })),
        )
        .expect("failed response event should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::UnknownEvent(ref payload)
            if payload.get("type").and_then(Value::as_str) == Some("response.failed")
    )));
    assert!(state
        .finish(&report_context)
        .expect("terminal failure should not synthesize completion")
        .is_empty());
}

#[test]
fn openai_usage_derives_missing_input_tokens_from_total() {
    let usage = canonical_usage_from_openai_usage(Some(&json!({
        "output_tokens": 177,
        "output_tokens_details": {
            "reasoning_tokens": 7,
        },
        "total_tokens": 20_612,
        "input_tokens_details": {
            "cached_tokens": 19_840,
        },
    })))
    .expect("usage should parse");

    assert_eq!(usage.input_tokens, 20_435);
    assert!(usage.input_tokens_include_cache);
    assert_eq!(usage.output_tokens, 177);
    assert_eq!(usage.cache_read_tokens, 19_840);
    assert_eq!(usage.reasoning_tokens, 7);
}

#[test]
fn openai_chat_provider_state_accepts_usage_only_terminal_chunk() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let _ = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "chatcmpl_123",
                "object": "chat.completion.chunk",
                "model": "gpt-5.4",
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop",
                }],
            })),
        )
        .expect("finish chunk should parse");
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "usage": {
                    "input_tokens": 26,
                    "input_tokens_details": {
                        "cached_tokens": 0,
                    },
                    "output_tokens": 144,
                    "output_tokens_details": {
                        "reasoning_tokens": 10,
                    },
                    "total_tokens": 170,
                },
            })),
        )
        .expect("usage-only chunk should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::Finish {
            finish_reason: Some(ref reason),
            usage: Some(CanonicalUsage {
                input_tokens: 26,
                output_tokens: 144,
                cache_read_tokens: 0,
                reasoning_tokens: 10,
                ..
            }),
        } if reason == "stop"
    )));
}

#[test]
fn openai_responses_provider_state_extracts_response_completed_usage() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_063494bbd780be940169eb8191c4ec8191916347b2080805ee",
                    "object": "response",
                    "model": "gpt-5.5",
                    "status": "completed",
                    "output": [],
                    "usage": {
                        "input_tokens": 26,
                        "input_tokens_details": {
                            "cached_tokens": 0,
                            "cache_write_tokens": 6,
                        },
                        "output_tokens": 137,
                        "output_tokens_details": {
                            "reasoning_tokens": 0,
                        },
                        "total_tokens": 163,
                    },
                },
                "sequence_number": 139,
            })),
        )
        .expect("completed event should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::Finish {
            usage: Some(CanonicalUsage {
                input_tokens: 26,
                output_tokens: 137,
                cache_creation_tokens: 6,
                cache_read_tokens: 0,
                ..
            }),
            ..
        }
    )));
}

#[test]
fn openai_responses_client_emitter_emits_doc_like_text_events() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let start = CanonicalStreamFrame {
        id: "chatcmpl_stream_123".to_string(),
        model: "gpt-5.4".to_string(),
        event: CanonicalStreamEvent::Start,
    };
    let text = CanonicalStreamFrame {
        id: "chatcmpl_stream_123".to_string(),
        model: "gpt-5.4".to_string(),
        event: CanonicalStreamEvent::TextDelta("Hello".to_string()),
    };
    let finish = CanonicalStreamFrame {
        id: "chatcmpl_stream_123".to_string(),
        model: "gpt-5.4".to_string(),
        event: CanonicalStreamEvent::Finish {
            finish_reason: Some("stop".to_string()),
            usage: Some(CanonicalUsage {
                input_tokens: 1,
                output_tokens: 2,
                total_tokens: 3,
                ..CanonicalUsage::default()
            }),
        },
    };

    let mut bytes = emitter.emit(start).expect("start should encode");
    bytes.extend(emitter.emit(text).expect("text should encode"));
    bytes.extend(emitter.emit(finish).expect("finish should encode"));

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.created\n"));
    assert!(sse.contains("event: response.in_progress\n"));
    assert!(sse.contains("event: response.output_item.added\n"));
    assert!(sse.contains("event: response.content_part.added\n"));
    assert!(sse.contains("event: response.output_text.delta\n"));
    assert!(sse.contains("event: response.output_text.done\n"));
    assert!(sse.contains("event: response.content_part.done\n"));
    assert!(sse.contains("event: response.output_item.done\n"));
    assert!(sse.contains("event: response.completed\n"));
    assert!(sse.contains("\"response_id\":\"resp_stream_123\""));
    assert!(sse.contains("\"item_id\":\"msg_aether_"));
    assert!(sse.contains("\"text\":\"Hello\""));
    assert!(sse.contains("\"output_text\":\"Hello\""));
    assert!(sse.contains("\"created_at\":"));
    assert!(sse.contains("\"completed_at\":"));
    assert_eq!(response_sequence_numbers(&sse), (1..=9).collect::<Vec<_>>());
}

#[test]
fn openai_responses_client_emitter_forwards_failed_unknown_event() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_failed_123".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::UnknownEvent(json!({
                "type": "response.failed",
                "response": {
                    "id": "resp_failed_123",
                    "model": "gpt-5.4",
                    "status": "failed",
                    "error": {
                        "message": "policy failure",
                        "type": "invalid_request_error",
                        "code": "cyber_policy"
                    }
                }
            })),
        })
        .expect("failed response event should encode");
    let mut all = bytes;
    all.extend(
        emitter
            .finish()
            .expect("failed stream should not synthesize completion"),
    );

    let sse = String::from_utf8(all).expect("sse should be utf8");
    assert!(sse.contains("event: response.failed\n"));
    assert!(sse.contains("\"message\":\"policy failure\""));
    assert!(!sse.contains("event: response.completed\n"));
}

#[test]
fn openai_responses_client_emitter_keeps_text_item_id_stable_after_text_started() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "msg_first".to_string(),
            model: "claude-haiku-4-5-20251001".to_string(),
            event: CanonicalStreamEvent::TextDelta("Hel".to_string()),
        })
        .expect("first text should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "msg_second".to_string(),
                model: "claude-haiku-4-5-20251001".to_string(),
                event: CanonicalStreamEvent::TextDelta("lo".to_string()),
            })
            .expect("second text should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("\"item_id\":\"msg_aether_"));
    assert!(!sse.contains("msg_second_msg"));
}

#[test]
fn openai_responses_provider_state_accepts_done_events_without_deltas() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.created",
                    "response": {
                        "id": "resp_123",
                        "model": "gpt-5.4",
                    }
                })),
            )
            .expect("created should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.output_text.done",
                    "response_id": "resp_123",
                    "output_index": 0,
                    "item_id": "resp_123_msg",
                    "content_index": 0,
                    "text": "Hello",
                })),
            )
            .expect("text done should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.function_call_arguments.done",
                    "response_id": "resp_123",
                    "output_index": 1,
                    "item_id": "call_123",
                    "call_id": "call_123",
                    "arguments": "{\"city\":\"SF\"}",
                })),
            )
            .expect("arguments done should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.completed",
                    "response": {
                        "id": "resp_123",
                        "object": "response",
                        "model": "gpt-5.4",
                        "status": "completed",
                        "output": [{
                            "type": "message",
                            "id": "resp_123_msg",
                            "role": "assistant",
                            "status": "completed",
                            "content": [{
                                "type": "output_text",
                                "text": "Hello",
                                "annotations": [],
                            }]
                        }, {
                            "type": "function_call",
                            "id": "call_123",
                            "call_id": "call_123",
                            "name": "get_weather",
                            "arguments": "{\"city\":\"SF\"}",
                        }],
                        "usage": {
                            "input_tokens": 1,
                            "output_tokens": 2,
                            "total_tokens": 3,
                        }
                    }
                })),
            )
            .expect("completed should parse"),
    );

    assert!(matches!(
        frames.first().map(|frame| &frame.event),
        Some(CanonicalStreamEvent::Start)
    ));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::TextDelta(ref text) if text == "Hello"
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { ref call_id, .. } if call_id == "call_123"
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallArgumentsDelta { ref arguments, .. }
            if arguments == "{\"city\":\"SF\"}"
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::Finish {
            finish_reason: Some(ref reason),
            usage: Some(CanonicalUsage {
                input_tokens: 1,
                output_tokens: 2,
                total_tokens: 3,
                ..
            }),
        } if reason == "tool_calls"
    )));
}

#[test]
fn openai_responses_provider_state_accepts_refusal_events() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.refusal.delta",
                    "response_id": "resp_refusal_123",
                    "output_index": 0,
                    "item_id": "msg_refusal_123",
                    "content_index": 0,
                    "delta": "I can't",
                })),
            )
            .expect("refusal delta should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.refusal.done",
                    "response_id": "resp_refusal_123",
                    "output_index": 0,
                    "item_id": "msg_refusal_123",
                    "content_index": 0,
                    "refusal": "I can't help with that.",
                })),
            )
            .expect("refusal done should parse"),
    );

    assert!(frames.iter().any(|frame| matches!(
        &frame.event,
        CanonicalStreamEvent::TextDelta(text) if text == "I can't"
    )));
    assert!(frames.iter().any(|frame| matches!(
        &frame.event,
        CanonicalStreamEvent::TextDelta(text) if text == " help with that."
    )));
}

#[test]
fn openai_responses_provider_state_does_not_duplicate_text_snapshot_deltas() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    for event in [
        json!({
            "type": "response.output_text.delta",
            "response_id": "resp_snapshot_delta",
            "output_index": 0,
            "content_index": 0,
            "delta": {
                "text": "Hello",
            }
        }),
        json!({
            "type": "response.output_text.delta",
            "response_id": "resp_snapshot_delta",
            "output_index": 0,
            "content_index": 0,
            "delta": {
                "text": "Hello world",
            }
        }),
        json!({
            "type": "response.output_text.delta",
            "response_id": "resp_snapshot_delta",
            "output_index": 0,
            "content_index": 0,
            "delta": {
                "text": "Hello",
            }
        }),
        json!({
            "type": "response.output_text.done",
            "response_id": "resp_snapshot_delta",
            "output_index": 0,
            "content_index": 0,
            "text": "Hello world",
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": "resp_snapshot_delta",
                "object": "response",
                "model": "gpt-5.4",
                "status": "completed",
                "output": [{
                    "type": "message",
                    "id": "msg_snapshot_delta",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "Hello world",
                        "annotations": [],
                    }]
                }],
            }
        }),
    ] {
        frames.extend(
            state
                .push_line(&report_context, data_line(event))
                .expect("responses text event should parse"),
        );
    }

    let text = frames
        .iter()
        .filter_map(|frame| match &frame.event {
            CanonicalStreamEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "Hello world");
}

#[test]
fn openai_responses_provider_state_dedupes_text_snapshots_per_output_item() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    for event in [
        json!({
            "type": "response.output_text.delta",
            "response_id": "resp_multi_message",
            "output_index": 0,
            "item_id": "msg_1",
            "content_index": 0,
            "delta": "First message.",
        }),
        json!({
            "type": "response.output_text.done",
            "response_id": "resp_multi_message",
            "output_index": 0,
            "item_id": "msg_1",
            "content_index": 0,
            "text": "First message.",
        }),
        json!({
            "type": "response.output_item.done",
            "response_id": "resp_multi_message",
            "output_index": 0,
            "item": {
                "type": "message",
                "id": "msg_1",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "First message.",
                }],
            },
        }),
        json!({
            "type": "response.output_text.delta",
            "response_id": "resp_multi_message",
            "output_index": 1,
            "item_id": "msg_2",
            "content_index": 0,
            "delta": "Second message.",
        }),
        json!({
            "type": "response.output_text.done",
            "response_id": "resp_multi_message",
            "output_index": 1,
            "item_id": "msg_2",
            "content_index": 0,
            "text": "Second message.",
        }),
        json!({
            "type": "response.content_part.done",
            "response_id": "resp_multi_message",
            "output_index": 1,
            "item_id": "msg_2",
            "content_index": 0,
            "part": {
                "type": "output_text",
                "text": "Second message.",
            },
        }),
        json!({
            "type": "response.output_item.done",
            "response_id": "resp_multi_message",
            "output_index": 1,
            "item": {
                "type": "message",
                "id": "msg_2",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "Second message.",
                }],
            },
        }),
    ] {
        frames.extend(
            state
                .push_line(&report_context, data_line(event))
                .expect("responses text event should parse"),
        );
    }

    let text = frames
        .iter()
        .filter_map(|frame| match &frame.event {
            CanonicalStreamEvent::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<String>();
    assert_eq!(text, "First message.Second message.");
}

#[test]
fn openai_responses_provider_state_replays_gemini_signature_carrier_to_client() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let signature = "skip_thought_signature_validator";
    let carrier =
        encode_gemini_tool_signature_carrier(signature).expect("signature carrier should encode");
    let reasoning_item = json!({
        "type": "reasoning",
        "id": "rs_signature_0",
        "status": "completed",
        "encrypted_content": carrier,
        "summary": []
    });
    let events = [
        json!({
            "type": "response.output_item.added",
            "response_id": "resp_signed_fallback",
            "output_index": 0,
            "item": reasoning_item
        }),
        json!({
            "type": "response.output_item.done",
            "response_id": "resp_signed_fallback",
            "output_index": 0,
            "item": reasoning_item
        }),
        json!({
            "type": "response.output_item.added",
            "response_id": "resp_signed_fallback",
            "output_index": 1,
            "item": {
                "type": "function_call",
                "id": "fc_signed_1",
                "call_id": "call_signed_1",
                "name": "fabric_exec",
                "arguments": "{\"code\":\"return 1\"}",
                "status": "completed"
            }
        }),
    ];
    let mut frames = Vec::new();
    for event in events {
        frames.extend(
            state
                .push_line(&report_context, data_line(event))
                .expect("Responses event should parse"),
        );
    }

    let signature_index = frames
        .iter()
        .position(|frame| {
            matches!(
                frame.event,
                CanonicalStreamEvent::ToolCallSignature {
                    index: 1,
                    ref signature
                } if signature == "skip_thought_signature_validator"
            )
        })
        .expect("signature event should be restored");
    let call_index = frames
        .iter()
        .position(|frame| {
            matches!(
                frame.event,
                CanonicalStreamEvent::ToolCallStart { index: 1, .. }
            )
        })
        .expect("tool call should be emitted");
    assert!(signature_index < call_index);
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.event, CanonicalStreamEvent::ToolCallSignature { .. }))
            .count(),
        1,
        "added and done snapshots must not duplicate the signature"
    );

    let mut emitter = GeminiClientEmitter::default();
    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend(emitter.emit(frame).expect("Gemini frame should encode"));
    }
    let sse = String::from_utf8(bytes).expect("Gemini SSE should be UTF-8");
    assert!(sse.contains("\"name\":\"fabric_exec\""));
    assert!(sse.contains("\"thoughtSignature\":\"skip_thought_signature_validator\""));
}

#[test]
fn openai_responses_provider_state_delays_arguments_until_tool_name_is_known() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let arguments = r#"{"file_path":"D:/projects/UIAutoTest/docs/prd/msr.md","offset":0,"limit":2000,"pages":""}"#;
    let mut frames = Vec::new();

    let delta_frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.function_call_arguments.delta",
                "response_id": "resp_read_123",
                "output_index": 0,
                "item_id": "fc_read_123",
                "delta": arguments,
            })),
        )
        .expect("arguments delta should parse");

    assert!(matches!(
        delta_frames.first().map(|frame| &frame.event),
        Some(CanonicalStreamEvent::Start)
    ));
    assert!(!delta_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { .. }
            | CanonicalStreamEvent::ToolCallArgumentsDelta { .. }
    )));
    frames.extend(delta_frames);

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.function_call_arguments.done",
                    "response_id": "resp_read_123",
                    "output_index": 0,
                    "item_id": "fc_read_123",
                    "item": {
                        "type": "function_call",
                        "id": "fc_read_123",
                        "call_id": "call_read_123",
                        "name": "Read",
                        "arguments": arguments,
                    }
                })),
            )
            .expect("arguments done should parse"),
    );

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart {
            ref call_id,
            ref name,
            ..
        } if call_id == "call_read_123" && name == "Read"
    )));
    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallArgumentsDelta {
            ref arguments,
            ..
        } if arguments.contains(r#""pages":"""#)
    )));

    let mut emitter = ClaudeClientEmitter::default();
    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend(emitter.emit(frame).expect("claude frame should encode"));
    }
    bytes.extend(
        emitter
            .finish()
            .expect("claude stream finish should encode"),
    );
    let sse = String::from_utf8(bytes).expect("claude sse should be utf8");

    assert!(sse.contains("\"name\":\"Read\""));
    assert!(sse.contains("\\\"limit\\\":2000"));
    assert!(!sse.contains("\\\"pages\\\":\\\"\\\""));
}

#[test]
fn openai_responses_provider_state_resolves_done_only_namespace_identity() {
    let report_context = json!({
        "original_request_body": {
            "tools": [
                {
                    "type": "function",
                    "name": "write_report",
                    "parameters": {"type": "object"}
                },
                {
                    "type": "namespace",
                    "name": "reports",
                    "description": "Reporting tools",
                    "tools": [{
                        "type": "function",
                        "name": "write_report",
                        "parameters": {"type": "object"}
                    }]
                }
            ]
        }
    });
    let expected_alias = NamespaceToolAliases::from_report_context(&report_context)
        .chat_name("reports", "write_report")
        .expect("namespace alias")
        .to_string();
    let mut state = OpenAIResponsesProviderState::default();

    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.function_call_arguments.done",
                "response_id": "resp_done_only_namespace",
                "output_index": 0,
                "item_id": "fc_done_only_namespace",
                "call_id": "call_done_only_namespace",
                "namespace": "reports",
                "name": "write_report",
                "arguments": "{}"
            })),
        )
        .expect("done-only namespace call should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart {
            ref call_id,
            ref name,
            ..
        } if call_id == "call_done_only_namespace" && name == &expected_alias
    )));
}

#[test]
fn openai_responses_provider_state_keeps_namespace_alias_until_delayed_call_id() {
    let report_context = json!({
        "original_request_body": {
            "tools": [
                {
                    "type": "function",
                    "name": "write_report",
                    "parameters": {"type": "object"}
                },
                {
                    "type": "namespace",
                    "name": "reports",
                    "description": "Reporting tools",
                    "tools": [{
                        "type": "function",
                        "name": "write_report",
                        "parameters": {"type": "object"}
                    }]
                }
            ]
        }
    });
    let expected_alias = NamespaceToolAliases::from_report_context(&report_context)
        .chat_name("reports", "write_report")
        .expect("namespace alias")
        .to_string();
    let mut state = OpenAIResponsesProviderState::default();

    let added = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.added",
                "response_id": "resp_delayed_namespace",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_delayed_namespace",
                    "namespace": "reports",
                    "name": "write_report",
                    "arguments": ""
                }
            })),
        )
        .expect("namespace item should parse");
    assert!(!added
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::ToolCallStart { .. })));

    let done = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.function_call_arguments.done",
                "response_id": "resp_delayed_namespace",
                "output_index": 0,
                "item_id": "fc_delayed_namespace",
                "call_id": "call_delayed_namespace",
                "name": "write_report",
                "arguments": "{\"path\":\"reports/finding.md\"}"
            })),
        )
        .expect("delayed namespace call identity should parse");

    assert!(done.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart {
            ref call_id,
            ref name,
            ..
        } if call_id == "call_delayed_namespace" && name == &expected_alias
    )));
    assert!(!done.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { ref name, .. }
            if name == "write_report" && name != &expected_alias
    )));
}

#[test]
fn openai_responses_provider_state_waits_for_call_id_distinct_from_item_id() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});

    let item_frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.added",
                "response_id": "resp_delayed_call_id",
                "output_index": 0,
                "item": {
                    "type": "function_call",
                    "id": "fc_delayed_call_id",
                    "name": "Read",
                    "arguments": ""
                }
            })),
        )
        .expect("function-call item should parse");
    assert!(!item_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { .. }
            | CanonicalStreamEvent::ToolCallArgumentsDelta { .. }
    )));

    let delta_frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.function_call_arguments.delta",
                "response_id": "resp_delayed_call_id",
                "output_index": 0,
                "item_id": "fc_delayed_call_id",
                "delta": "{\"path\":"
            })),
        )
        .expect("arguments delta should be buffered");
    assert!(!delta_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { .. }
            | CanonicalStreamEvent::ToolCallArgumentsDelta { .. }
    )));

    let identity_frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.function_call_arguments.done",
                "response_id": "resp_delayed_call_id",
                "output_index": 0,
                "item_id": "fc_delayed_call_id",
                "item": {
                    "type": "function_call",
                    "id": "fc_delayed_call_id",
                    "call_id": "call_delayed_call_id",
                    "name": "Read",
                    "arguments": "{\"path\":\"src/lib.rs\"}"
                },
                "arguments": "{\"path\":\"src/lib.rs\"}"
            })),
        )
        .expect("real call identity should flush buffered arguments");

    assert!(identity_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart {
            ref call_id,
            ref name,
            ..
        } if call_id == "call_delayed_call_id" && name == "Read"
    )));
    assert!(!identity_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallStart { ref call_id, .. }
            if call_id == "fc_delayed_call_id"
    )));
    assert!(identity_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolCallArgumentsDelta { ref arguments, .. }
            if arguments == "{\"path\":\"src/lib.rs\"}"
    )));
}

#[test]
fn openai_responses_provider_state_parses_function_call_output_as_tool_result() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.created",
                    "response": {
                        "id": "resp_123",
                        "model": "gpt-5.4",
                    }
                })),
            )
            .expect("created should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.function_call_output.done",
                    "response_id": "resp_123",
                    "output_index": 2,
                    "call_id": "call_123",
                    "name": "lookup",
                    "output": {"ok": true},
                })),
            )
            .expect("tool result should parse"),
    );

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolResultDelta {
            index: 2,
            ref tool_use_id,
            name: Some(ref name),
            ref content,
        } if tool_use_id == "call_123" && name == "lookup" && content == "{\"ok\":true}"
    )));
}

#[test]
fn openai_responses_provider_state_ignores_hosted_tool_progress_events() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    for event_type in [
        "response.local_shell_call.in_progress",
        "response.local_shell_call.running",
        "response.local_shell_call.completed",
        "response.apply_patch_call.in_progress",
        "response.apply_patch_call.completed",
        "response.computer_call.in_progress",
        "response.computer_call.completed",
    ] {
        frames.extend(
            state
                .push_line(
                    &report_context,
                    data_line(json!({
                        "type": event_type,
                        "response_id": "resp_123",
                        "output_index": 0,
                        "item_id": "call_123",
                    })),
                )
                .expect("hosted tool progress event should parse"),
        );
    }

    assert!(frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::Start)));
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::UnknownEvent { .. })));
}

#[test]
fn openai_responses_provider_state_parses_hosted_tool_output_as_tool_result() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.local_shell_call_output.done",
                "response_id": "resp_123",
                "output_index": 3,
                "call_id": "call_shell_123",
                "output": {
                    "stdout": "ok\n",
                    "stderr": "",
                    "outcome": "success"
                },
            })),
        )
        .expect("hosted tool result should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ToolResultDelta {
            index: 3,
            ref tool_use_id,
            name: Some(ref name),
            ref content,
        } if tool_use_id == "call_shell_123"
            && name == "local_shell"
            && content.contains("\"stdout\":\"ok\\n\"")
    )));
}

#[test]
fn openai_responses_provider_state_preserves_image_generation_calls() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.completed",
                "response": {
                    "id": "resp_img_123",
                    "model": "gpt-image-2",
                    "output": [{
                        "id": "ig_123",
                        "type": "image_generation_call",
                        "status": "completed",
                        "output_format": "png",
                        "result": "aGVsbG8="
                    }],
                    "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
                }
            })),
        )
        .expect("completed event should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ImageGenerationCall {
            index: 0,
            ref item,
        } if item["type"] == json!("image_generation_call")
            && item["result"] == json!("aGVsbG8=")
    )));
}

#[test]
fn openai_responses_provider_state_waits_for_final_image_generation_item() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});

    let added_frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.added",
                "output_index": 0,
                "item": {
                    "id": "ig_123",
                    "type": "image_generation_call",
                    "status": "generating",
                    "output_format": "png",
                    "result": "early"
                }
            })),
        )
        .expect("added event should parse");

    assert!(!added_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ImageGenerationCall { .. }
    )));

    let done_frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "id": "ig_123",
                    "type": "image_generation_call",
                    "status": "completed",
                    "output_format": "png",
                    "result": "final"
                }
            })),
        )
        .expect("done event should parse");

    assert!(done_frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ImageGenerationCall {
            index: 0,
            ref item,
        } if item["status"] == json!("completed") && item["result"] == json!("final")
    )));
}

#[test]
fn openai_responses_client_emitter_emits_image_generation_call_events() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_img_123".to_string(),
            model: "gpt-image-2".to_string(),
            event: CanonicalStreamEvent::ImageGenerationCall {
                index: 0,
                item: json!({
                    "id": "ig_123",
                    "type": "image_generation_call",
                    "status": "completed",
                    "output_format": "png",
                    "result": "aGVsbG8="
                }),
            },
        })
        .expect("image event should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_img_123".to_string(),
                model: "gpt-image-2".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.output_item.done\n"));
    assert!(sse.contains("\"type\":\"image_generation_call\""));
    assert!(sse.contains("\"result\":\"aGVsbG8=\""));
    assert!(sse.contains("\"output\":["));
    assert!(sse.contains("\"id\":\"ig_123\""));
}

#[test]
fn openai_responses_client_emitter_emits_function_call_output_events() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_123".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::ToolResultDelta {
                index: 1,
                tool_use_id: "call_123".to_string(),
                name: Some("lookup".to_string()),
                content: "{\"ok\":true}".to_string(),
            },
        })
        .expect("tool result should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_123".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.function_call_output.delta\n"));
    assert!(sse.contains("\"type\":\"function_call_output\""));
    assert!(sse.contains("\"call_id\":\"call_123\""));
    assert!(sse.contains("\"output\":\"{\\\"ok\\\":true}\""));
}

#[test]
fn openai_responses_client_emitter_keeps_call_id_distinct_and_stable() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_tool_identity_123".to_string(),
            model: "deepseek-v4-flash".to_string(),
            event: CanonicalStreamEvent::ToolCallStart {
                index: 0,
                call_id: "call_security_123".to_string(),
                name: "security_scan".to_string(),
            },
        })
        .expect("tool start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_tool_identity_123".to_string(),
                model: "deepseek-v4-flash".to_string(),
                event: CanonicalStreamEvent::ToolCallArgumentsDelta {
                    index: 0,
                    arguments: "{\"depth\":\"deep\"}".to_string(),
                },
            })
            .expect("tool arguments should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_tool_identity_123".to_string(),
                model: "deepseek-v4-flash".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("tool_calls".to_string()),
                    usage: None,
                },
            })
            .expect("tool finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    let item_id = "fc_tool_identity_123_0";
    assert!(sse.contains(&format!("\"id\":\"{item_id}\"")));
    assert!(sse.contains(&format!("\"item_id\":\"{item_id}\"")));
    assert!(sse.contains("\"call_id\":\"call_security_123\""));
    assert!(!sse.contains("\"id\":\"call_security_123\""));
    assert!(
        sse.find("event: response.output_item.added")
            < sse.find("event: response.function_call_arguments.delta")
    );
    assert!(
        sse.find("event: response.function_call_arguments.delta")
            < sse.find("event: response.function_call_arguments.done")
    );
    assert!(
        sse.find("event: response.function_call_arguments.done")
            < sse.find("event: response.output_item.done")
    );
    assert!(sse.find("event: response.output_item.done") < sse.find("event: response.completed"));
}

#[test]
fn openai_responses_client_emitter_carries_gemini_tool_signature() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_signed_tool_123".to_string(),
            model: "gemini-3-flash-preview".to_string(),
            event: CanonicalStreamEvent::ToolCallSignature {
                index: 0,
                signature: "opaque-tool-signature".to_string(),
            },
        })
        .expect("tool signature should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_signed_tool_123".to_string(),
                model: "gemini-3-flash-preview".to_string(),
                event: CanonicalStreamEvent::ToolCallStart {
                    index: 0,
                    call_id: "call_123".to_string(),
                    name: "lookup".to_string(),
                },
            })
            .expect("tool call should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_signed_tool_123".to_string(),
                model: "gemini-3-flash-preview".to_string(),
                event: CanonicalStreamEvent::ToolCallArgumentsDelta {
                    index: 0,
                    arguments: "{\"query\":\"rust\"}".to_string(),
                },
            })
            .expect("tool arguments should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_signed_tool_123".to_string(),
                model: "gemini-3-flash-preview".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("tool_calls".to_string()),
                    usage: None,
                },
            })
            .expect("tool finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    let carrier =
        encode_gemini_tool_signature_carrier("opaque-tool-signature").expect("signature carrier");
    let carrier_index = sse.find(&carrier).expect("carrier in Responses stream");
    let call_index = sse
        .find("\"call_id\":\"call_123\"")
        .expect("function call in Responses stream");
    assert!(carrier_index < call_index);
    assert!(sse.contains("\"encrypted_content\""));
    assert!(sse.contains("event: response.completed\n"));
}

#[test]
fn openai_responses_client_emitter_carries_late_gemini_tool_signature() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_late_signed_tool_123".to_string(),
            model: "gemini-3-flash-preview".to_string(),
            event: CanonicalStreamEvent::ToolCallStart {
                index: 0,
                call_id: "call_123".to_string(),
                name: "lookup".to_string(),
            },
        })
        .expect("tool call should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_late_signed_tool_123".to_string(),
                model: "gemini-3-flash-preview".to_string(),
                event: CanonicalStreamEvent::ToolCallSignature {
                    index: 0,
                    signature: "opaque-late-tool-signature".to_string(),
                },
            })
            .expect("late tool signature should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_late_signed_tool_123".to_string(),
                model: "gemini-3-flash-preview".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("tool_calls".to_string()),
                    usage: None,
                },
            })
            .expect("tool finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    let carrier = encode_gemini_tool_signature_carrier_with_direction(
        "opaque-late-tool-signature",
        GeminiToolSignatureCarrierDirection::Previous,
    )
    .expect("late signature carrier");
    let call_index = sse
        .find("\"call_id\":\"call_123\"")
        .expect("function call in Responses stream");
    let carrier_index = sse
        .find(&carrier)
        .expect("late carrier in Responses stream");
    assert!(call_index < carrier_index);
    assert!(sse.contains("event: response.completed\n"));
}

#[test]
fn openai_responses_client_emitter_emits_web_search_call_item() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_123".to_string(),
            model: "gpt-5-5-low".to_string(),
            event: CanonicalStreamEvent::ToolCallStart {
                index: 0,
                call_id: "call_ws_1".to_string(),
                name: "web_search".to_string(),
            },
        })
        .expect("tool start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_123".to_string(),
                model: "gpt-5-5-low".to_string(),
                event: CanonicalStreamEvent::ToolCallArgumentsDelta {
                    index: 0,
                    arguments: r#"{"query":"today tech"}"#.to_string(),
                },
            })
            .expect("arguments should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_123".to_string(),
                model: "gpt-5-5-low".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("tool_calls".to_string()),
                    usage: None,
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.output_item.added\n"));
    assert!(sse.contains(r#""type":"web_search_call""#));
    assert!(sse.contains(r#""status":"in_progress""#));
    assert!(sse.contains(r#""query":"""#));
    assert!(sse.contains(r#""type":"search""#));
    assert!(sse.contains("event: response.output_item.done\n"));
    assert!(sse.contains(r#""query":"today tech""#));
    assert!(!sse.contains("response.function_call_arguments.delta"));
}

#[test]
fn openai_responses_client_emitter_keeps_client_declared_web_search_function_as_function_call() {
    let mut emitter = OpenAIResponsesClientEmitter::with_report_context(&json!({
        "original_request_body": {
            "tools": [{
                "type": "function",
                "name": "web_search",
                "parameters": {"type": "object", "properties": {"query": {"type": "string"}}}
            }]
        }
    }));
    let mut bytes = Vec::new();
    for event in [
        CanonicalStreamEvent::ToolCallStart {
            index: 0,
            call_id: "call_ws_1".to_string(),
            name: "web_search".to_string(),
        },
        CanonicalStreamEvent::ToolCallArgumentsDelta {
            index: 0,
            arguments: r#"{"query":"today tech"}"#.to_string(),
        },
        CanonicalStreamEvent::Finish {
            finish_reason: Some("tool_calls".to_string()),
            usage: None,
        },
    ] {
        bytes.extend(
            emitter
                .emit(CanonicalStreamFrame {
                    id: "resp_123".to_string(),
                    model: "gemini-3.8-flash".to_string(),
                    event,
                })
                .expect("event should encode"),
        );
    }

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(!sse.contains("web_search_call"));
    assert!(sse.contains(r#""type":"function_call""#));
    assert!(sse.contains(r#""call_id":"call_ws_1""#));
    assert!(sse.contains(r#""name":"web_search""#));
    assert!(sse.contains("response.function_call_arguments.delta"));
}

#[test]
fn openai_responses_provider_state_accepts_legacy_outtext_delta_alias() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.created",
                    "response": {
                        "id": "resp_legacy_123",
                        "model": "gpt-5.4",
                    }
                })),
            )
            .expect("created should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.outtext.delta",
                    "response_id": "resp_legacy_123",
                    "output_index": 0,
                    "item_id": "resp_legacy_123_msg",
                    "content_index": 0,
                    "delta": "Hello from legacy alias",
                })),
            )
            .expect("legacy text delta should parse"),
    );

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::TextDelta(ref text) if text == "Hello from legacy alias"
    )));
}

#[test]
fn openai_chat_client_emitter_emits_reasoning_content_chunks() {
    let mut emitter = OpenAIChatClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "chatcmpl_123".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "chatcmpl_123".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningDelta("because".to_string()),
            })
            .expect("reasoning should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("\"reasoning_content\":\"because\""));
}

#[test]
fn openai_chat_client_emitter_renders_image_parts_as_placeholder() {
    let mut emitter = OpenAIChatClientEmitter::default();
    let bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "chatcmpl_img_123".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::ContentPart(CanonicalContentPart::ImageUrl(
                "data:image/png;base64,iVBORw0KGgo=".to_string(),
            )),
        })
        .expect("image should encode");

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("[Image]"));
}

#[test]
fn openai_chat_client_emitter_normalizes_sparse_tool_call_indices() {
    let mut emitter = OpenAIChatClientEmitter::default();
    let mut bytes = Vec::new();

    for event in [
        CanonicalStreamEvent::ToolCallStart {
            index: 1,
            call_id: "call_first".to_string(),
            name: "first_tool".to_string(),
        },
        CanonicalStreamEvent::ToolCallArgumentsDelta {
            index: 1,
            arguments: "{\"first\":".to_string(),
        },
        CanonicalStreamEvent::ToolCallStart {
            index: 3,
            call_id: "call_second".to_string(),
            name: "second_tool".to_string(),
        },
        CanonicalStreamEvent::ToolCallArgumentsDelta {
            index: 3,
            arguments: "{\"second\":true}".to_string(),
        },
        CanonicalStreamEvent::ToolCallArgumentsDelta {
            index: 1,
            arguments: "true}".to_string(),
        },
    ] {
        bytes.extend(
            emitter
                .emit(CanonicalStreamFrame {
                    id: "chatcmpl_sparse".to_string(),
                    model: "claude-opus-4-6".to_string(),
                    event,
                })
                .expect("tool event should encode"),
        );
    }

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert_eq!(openai_chat_tool_call_indices(&sse), vec![0, 0, 1, 1, 0]);
}

#[test]
fn openai_chat_client_emitter_emits_usage_only_final_chunk() {
    let mut emitter = OpenAIChatClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "chatcmpl_789".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "chatcmpl_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::TextDelta("Hello".to_string()),
            })
            .expect("text should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "chatcmpl_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: Some(CanonicalUsage {
                        input_tokens: 1,
                        input_tokens_include_cache: true,
                        output_tokens: 2,
                        total_tokens: 3,
                        cache_creation_tokens: 5,
                        cache_read_tokens: 4,
                        reasoning_tokens: 1,
                        ..CanonicalUsage::default()
                    }),
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("\"finish_reason\":\"stop\""));
    assert!(sse.contains("\"choices\":[]"));
    assert!(sse.contains("\"prompt_tokens\":1"));
    assert!(sse.contains("\"completion_tokens\":2"));
    assert!(sse.contains("\"completion_tokens_details\":{\"reasoning_tokens\":1}"));
    assert!(sse.contains("\"cache_write_tokens\":5"));
    assert!(!sse.contains("\"cached_creation_tokens\""));
    assert!(sse.contains("\"cached_tokens\":4"));
    assert!(sse.contains("\"total_tokens\":3"));
    assert!(sse.contains("data: [DONE]\n\n"));
}

#[test]
fn openai_chat_provider_state_accepts_usage_only_final_chunk() {
    let mut state = OpenAIChatProviderState::default();
    let report_context = json!({});
    let mut frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "id": "chatcmpl_456",
                "object": "chat.completion.chunk",
                "model": "gpt-5.4",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "content": "Hello",
                    },
                    "finish_reason": Value::Null,
                }]
            })),
        )
        .expect("first chunk should parse");
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "id": "chatcmpl_456",
                    "object": "chat.completion.chunk",
                    "model": "gpt-5.4",
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "stop",
                    }],
                    "usage": {},
                })),
            )
            .expect("stop chunk should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "id": "chatcmpl_456",
                    "object": "chat.completion.chunk",
                    "model": "gpt-5.4",
                    "choices": [],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 2,
                        "total_tokens": 3,
                    }
                })),
            )
            .expect("usage chunk should parse"),
    );

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::Finish {
            finish_reason: Some(ref reason),
            usage: Some(CanonicalUsage {
                input_tokens: 1,
                output_tokens: 2,
                total_tokens: 3,
                ..
            }),
        } if reason == "stop"
    )));
}

#[test]
fn openai_responses_client_emitter_includes_reasoning_in_completed_response() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_123".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_123".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningDelta("because".to_string()),
            })
            .expect("reasoning should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_123".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: Some(CanonicalUsage {
                        input_tokens: 1,
                        output_tokens: 2,
                        total_tokens: 3,
                        cache_creation_tokens: 5,
                        cache_read_tokens: 4,
                        reasoning_tokens: 1,
                        ..CanonicalUsage::default()
                    }),
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("\"type\":\"reasoning\""));
    assert!(sse.contains("\"text\":\"because\""));
    assert!(sse.contains("\"output_tokens_details\":{\"reasoning_tokens\":1}"));
    assert!(sse.contains("\"input_tokens_details\""));
    assert!(sse.contains("\"cache_write_tokens\":5"));
    assert!(!sse.contains("\"cached_creation_tokens\""));
    assert!(sse.contains("\"cached_tokens\":4"));
}

#[test]
fn openai_responses_client_emitter_emits_doc_like_reasoning_events() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_456".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_456".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningDelta("step".to_string()),
            })
            .expect("reasoning should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_456".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.reasoning_text.delta\n"));
    assert!(sse.contains("event: response.reasoning_text.done\n"));
    assert!(sse.contains("\"type\":\"reasoning_text\""));
    // Raw chain-of-thought must not be duplicated onto the summary channel:
    // Codex renders both, so emitting both makes the thinking panel repeat.
    assert!(!sse.contains("event: response.reasoning_summary_text.delta\n"));
    assert!(!sse.contains("event: response.reasoning_summary_text.done\n"));
    assert!(!sse.contains("event: response.reasoning_summary_part.added\n"));
    assert!(!sse.contains("event: response.reasoning_summary_part.done\n"));
    let reasoning_item_id = openai_responses_synthetic_reasoning_item_id("resp_456", 0);
    assert!(sse.contains(&format!("\"item_id\":\"{reasoning_item_id}\"")));
    assert!(sse.contains("\"type\":\"reasoning\""));
    assert_eq!(response_sequence_numbers(&sse), (1..=7).collect::<Vec<_>>());
}

#[test]
fn openai_responses_client_emitter_closes_distinct_reasoning_parts() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_789".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningDelta("alpha".to_string()),
            })
            .expect("first reasoning should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningSummaryDone,
            })
            .expect("first boundary should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningDelta("beta".to_string()),
            })
            .expect("second reasoning should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningSummaryDone,
            })
            .expect("second boundary should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_789".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert_eq!(
        response_reasoning_text_done_parts(&sse),
        vec![(0, "alpha".to_string()), (1, "beta".to_string())]
    );
}

/// Regression: raw thinking must reach the client exactly once.
///
/// Codex renders both the `content` (`reasoning_text`) and `summary`
/// (`summary_text`) channels, so emitting the same chain-of-thought on both
/// made its thinking panel print every line twice.
#[test]
fn openai_responses_client_emitter_sends_raw_thinking_once() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_once".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    for text in ["Let", " me", " think."] {
        bytes.extend(
            emitter
                .emit(CanonicalStreamFrame {
                    id: "resp_once".to_string(),
                    model: "gpt-5.4".to_string(),
                    event: CanonicalStreamEvent::ReasoningDelta(text.to_string()),
                })
                .expect("reasoning delta should encode"),
        );
    }
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_once".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::ReasoningSummaryDone,
            })
            .expect("reasoning boundary should encode"),
    );
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_once".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::Finish {
                    finish_reason: Some("stop".to_string()),
                    usage: None,
                },
            })
            .expect("finish should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    // Each thinking chunk is streamed on exactly one channel. The same delta
    // used to be mirrored onto `reasoning_summary_text.delta`, so clients that
    // render both channels (Codex) printed every chunk twice.
    assert_eq!(
        sse.matches("event: response.reasoning_text.delta\n")
            .count(),
        3,
        "one delta event per thinking chunk: {sse}"
    );
    assert!(!sse.contains("event: response.reasoning_summary_text.delta\n"));
    assert!(!sse.contains("event: response.reasoning_summary_text.done\n"));
    assert!(!sse.contains("\"type\":\"summary_text\""));
    // The completed item carries the thinking on `content`, not `summary`.
    assert!(
        sse.contains("\"content\":[{\"type\":\"reasoning_text\",\"text\":\"Let me think.\"}]"),
        "{sse}"
    );
    assert!(sse.contains("\"summary\":[]"), "{sse}");
}

#[test]
fn openai_responses_client_emitter_emits_failed_event_with_sequence_number() {
    let mut emitter = OpenAIResponsesClientEmitter::default();
    let mut bytes = emitter
        .emit(CanonicalStreamFrame {
            id: "resp_err_123".to_string(),
            model: "gpt-5.4".to_string(),
            event: CanonicalStreamEvent::Start,
        })
        .expect("start should encode");
    bytes.extend(
        emitter
            .emit(CanonicalStreamFrame {
                id: "resp_err_123".to_string(),
                model: "gpt-5.4".to_string(),
                event: CanonicalStreamEvent::TextDelta("Hi".to_string()),
            })
            .expect("text should encode"),
    );
    bytes.extend(
        emitter
            .emit_error(json!({
                "error": {
                    "message": "boom",
                    "type": "server_error",
                    "code": "internal",
                }
            }))
            .expect("error should encode"),
    );

    let sse = String::from_utf8(bytes).expect("sse should be utf8");
    assert!(sse.contains("event: response.failed\n"));
    assert!(sse.contains("\"message\":\"boom\""));
    assert!(!sse.contains("event: response.completed\n"));
    assert_eq!(response_sequence_numbers(&sse), (1..=6).collect::<Vec<_>>());
}

#[test]
fn openai_responses_provider_state_accepts_reasoning_summary_events() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.created",
                    "response": {
                        "id": "resp_456",
                        "model": "gpt-5.4",
                    }
                })),
            )
            .expect("created should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.reasoning_summary_part.added",
                    "response_id": "resp_456",
                    "item_id": "resp_456_rs_0",
                    "output_index": 0,
                    "summary_index": 0,
                    "part": {
                        "type": "summary_text",
                        "text": "",
                    }
                })),
            )
            .expect("part added should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.reasoning_summary_text.delta",
                    "response_id": "resp_456",
                    "item_id": "resp_456_rs_0",
                    "output_index": 0,
                    "summary_index": 0,
                    "delta": "step",
                })),
            )
            .expect("delta should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.reasoning_summary_text.done",
                    "response_id": "resp_456",
                    "item_id": "resp_456_rs_0",
                    "output_index": 0,
                    "summary_index": 0,
                    "text": "step",
                })),
            )
            .expect("done should parse"),
    );

    let reasoning = frames
        .iter()
        .filter(|frame| matches!(frame.event, CanonicalStreamEvent::ReasoningDelta(_)))
        .collect::<Vec<_>>();
    assert_eq!(reasoning.len(), 1);
    assert!(matches!(
        reasoning[0].event,
        CanonicalStreamEvent::ReasoningDelta(ref text) if text == "step"
    ));
}

#[test]
fn openai_responses_provider_state_accepts_reasoning_done_without_delta() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.created",
                    "response": {
                        "id": "resp_done_only",
                        "model": "gpt-5.4",
                    }
                })),
            )
            .expect("created should parse"),
    );
    frames.extend(
        state
            .push_line(
                &report_context,
                data_line(json!({
                    "type": "response.reasoning_summary_text.done",
                    "response_id": "resp_done_only",
                    "item_id": "resp_done_only_rs_0",
                    "output_index": 0,
                    "summary_index": 0,
                    "text": "fallback reasoning",
                })),
            )
            .expect("reasoning done should parse"),
    );

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ReasoningDelta(ref text) if text == "fallback reasoning"
    )));
    assert!(frames
        .iter()
        .any(|frame| matches!(frame.event, CanonicalStreamEvent::ReasoningSummaryDone)));
}

#[test]
fn openai_responses_provider_state_does_not_duplicate_part_scoped_reasoning_done() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    for event in [
        json!({
            "type": "response.reasoning_summary_text.delta",
            "response_id": "resp_parts",
            "item_id": "resp_parts_rs_0",
            "output_index": 0,
            "summary_index": 0,
            "delta": "alpha",
        }),
        json!({
            "type": "response.reasoning_summary_text.done",
            "response_id": "resp_parts",
            "item_id": "resp_parts_rs_0",
            "output_index": 0,
            "summary_index": 0,
            "text": "alpha",
        }),
        json!({
            "type": "response.reasoning_summary_text.delta",
            "response_id": "resp_parts",
            "item_id": "resp_parts_rs_0",
            "output_index": 0,
            "summary_index": 1,
            "delta": "beta",
        }),
        json!({
            "type": "response.reasoning_summary_text.done",
            "response_id": "resp_parts",
            "item_id": "resp_parts_rs_0",
            "output_index": 0,
            "summary_index": 1,
            "text": "beta",
        }),
    ] {
        frames.extend(
            state
                .push_line(&report_context, data_line(event))
                .expect("reasoning event should parse"),
        );
    }

    let reasoning = frames
        .iter()
        .filter_map(|frame| match &frame.event {
            CanonicalStreamEvent::ReasoningDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reasoning, vec!["alpha", "beta"]);
}

#[test]
fn openai_responses_provider_state_does_not_duplicate_added_reasoning_item_summary() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let mut frames = Vec::new();

    for event in [
        json!({
            "type": "response.output_item.added",
            "response_id": "resp_added_summary",
            "output_index": 0,
            "item": {
                "type": "reasoning",
                "id": "resp_added_summary_rs_0",
                "summary": [{
                    "type": "summary_text",
                    "text": "alpha",
                }]
            }
        }),
        json!({
            "type": "response.reasoning_summary_text.delta",
            "response_id": "resp_added_summary",
            "item_id": "resp_added_summary_rs_0",
            "output_index": 0,
            "summary_index": 0,
            "delta": "alpha",
        }),
    ] {
        frames.extend(
            state
                .push_line(&report_context, data_line(event))
                .expect("reasoning event should parse"),
        );
    }

    let reasoning = frames
        .iter()
        .filter_map(|frame| match &frame.event {
            CanonicalStreamEvent::ReasoningDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(reasoning, vec!["alpha"]);
}

#[test]
fn openai_responses_provider_state_uses_reasoning_item_as_fallback() {
    let mut state = OpenAIResponsesProviderState::default();
    let report_context = json!({});
    let frames = state
        .push_line(
            &report_context,
            data_line(json!({
                "type": "response.output_item.done",
                "response_id": "resp_item_fallback",
                "output_index": 0,
                "item": {
                    "type": "reasoning",
                    "id": "resp_item_fallback_rs_0",
                    "summary": [{
                        "type": "summary_text",
                        "text": "item fallback reasoning",
                    }]
                }
            })),
        )
        .expect("reasoning item should parse");

    assert!(frames.iter().any(|frame| matches!(
        frame.event,
        CanonicalStreamEvent::ReasoningDelta(ref text) if text == "item fallback reasoning"
    )));
}
