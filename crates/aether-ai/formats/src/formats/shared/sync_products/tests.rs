use super::{
    aggregate_claude_stream_sync_response, aggregate_gemini_stream_sync_response,
    aggregate_openai_chat_stream_sync_response, aggregate_openai_responses_stream_sync_response,
    convert_standard_chat_response, convert_standard_cli_response,
    decode_non_stream_sync_capture_body, materialize_openai_responses_reasoning_item,
    maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload,
    maybe_build_openai_responses_cross_format_sync_product_from_normalized_payload,
    maybe_build_openai_responses_same_family_sync_body_from_normalized_payload,
    maybe_build_standard_cross_format_sync_product,
    maybe_build_standard_cross_format_sync_product_from_normalized_payload,
    maybe_build_standard_same_format_sync_body_from_normalized_payload,
    maybe_build_standard_sync_finalize_product_from_normalized_payload,
    openai_responses_synthetic_reasoning_item_id,
    try_aggregate_openai_responses_stream_sync_response, OpenAIResponsesSyncReasoningState,
    StandardSyncFinalizeNormalizedProduct,
};
use aether_ai_formats::formats::conversion::response::{
    convert_claude_chat_response_to_openai_chat, convert_gemini_chat_response_to_openai_chat,
    convert_openai_chat_response_to_claude_chat, convert_openai_chat_response_to_gemini_chat,
    convert_openai_chat_response_to_openai_responses,
    convert_openai_responses_response_to_openai_chat,
};
use aether_ai_formats::{sync_cli_response_conversion_kind, SyncCliResponseConversionKind};
use base64::Engine as _;
use serde_json::json;

#[test]
fn native_memories_response_keeps_output_array_and_future_fields() {
    let body = json!({"output":[{"trace_summary":"synthetic", "memory_summary":"memory"}], "future_response_field":{"enabled":true}});
    let context = json!({"provider_api_format":"openai:responses","client_api_format":"openai:responses","needs_conversion":false,"requested_model":"gpt-6.1-sol-max"});
    assert_eq!(
        maybe_build_standard_same_format_sync_body_from_normalized_payload(
            "openai_memories_sync_finalize",
            200,
            Some(&context),
            Some(&body),
            None
        )
        .expect("native response")
        .expect("body"),
        body
    );
}

#[test]
fn converts_openai_images_sync_body_to_gemini_image_body() {
    let provider_body_json = json!({
        "created": 1776839946,
        "model": "gpt-image-2",
        "data": [{
            "revised_prompt": "revised prompt",
            "b64_json": "aGVsbG8="
        }],
        "usage": {
            "input_tokens": 3,
            "output_tokens": 4,
            "total_tokens": 7
        }
    });
    let report_context = json!({
        "provider_api_format": "openai:image",
        "client_api_format": "gemini:generate_content",
    });

    let product = maybe_build_standard_cross_format_sync_product(
        "gemini_chat_sync_finalize",
        "openai:image",
        "gemini:generate_content",
        &report_context,
        provider_body_json,
    )
    .expect("OpenAI Images sync body should convert");

    assert_eq!(product.client_body_json["modelVersion"], "gpt-image-2");
    assert_eq!(
        product.client_body_json["candidates"][0]["content"]["parts"][0]["text"],
        "revised prompt"
    );
    assert_eq!(
        product.client_body_json["candidates"][0]["content"]["parts"][1]["inlineData"]["data"],
        "aGVsbG8="
    );
    assert_eq!(
        product.client_body_json["usageMetadata"]["totalTokenCount"],
        7
    );
}

#[test]
fn converts_openai_responses_image_body_to_gemini_image_body() {
    let provider_body_json = json!({
        "model": "gpt-image-2",
        "output": [{
            "type": "image_generation_call",
            "status": "completed",
            "result": "aGVsbG8=",
            "output_format": "png"
        }]
    });
    let report_context = json!({
        "provider_api_format": "openai:image",
        "client_api_format": "gemini:generate_content",
    });

    let product = maybe_build_standard_cross_format_sync_product(
        "gemini_chat_sync_finalize",
        "openai:image",
        "gemini:generate_content",
        &report_context,
        provider_body_json,
    )
    .expect("OpenAI Responses image body should convert");

    assert_eq!(product.client_body_json["modelVersion"], "gpt-image-2");
    assert_eq!(
        product.client_body_json["candidates"][0]["content"]["parts"][0]["inlineData"]["mimeType"],
        "image/png"
    );
    assert_eq!(
        product.client_body_json["candidates"][0]["content"]["parts"][0]["inlineData"]["data"],
        "aGVsbG8="
    );
}

#[test]
fn aggregates_openai_chat_stream_reasoning_into_sync_body() {
    // The aggregator used to keep only `content` and `tool_calls`, so a
    // stream downgraded to a sync response lost the reasoning entirely —
    // for OpenRouter's `reasoning`/`reasoning_details` and for the
    // DeepSeek-style `reasoning_content` alike.
    let body = concat!(
        "data: {\"id\":\"gen-openrouter-123\",\"object\":\"chat.completion.chunk\",\"model\":\"stealth/ox-alpha\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"role\":\"assistant\",\"reasoning\":\"Let me\",\"reasoning_details\":[{\"type\":\"reasoning.text\",\"text\":\"Let me\",\"index\":0}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-openrouter-123\",\"object\":\"chat.completion.chunk\",\"model\":\"stealth/ox-alpha\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"role\":\"assistant\",\"reasoning\":\" think.\",\"reasoning_details\":[{\"type\":\"reasoning.text\",\"text\":\" think.\",\"index\":0}]},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-openrouter-123\",\"object\":\"chat.completion.chunk\",\"model\":\"stealth/ox-alpha\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Done.\",\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-openrouter-123\",\"object\":\"chat.completion.chunk\",\"model\":\"stealth/ox-alpha\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"role\":\"assistant\",\"reasoning\":null},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
    );

    let result = aggregate_openai_chat_stream_sync_response(body.as_bytes())
        .expect("openrouter chat stream should aggregate into a sync body");

    let message = &result["choices"][0]["message"];
    assert_eq!(message["content"], "Done.");
    // `reasoning` and `reasoning_details` repeat one another, so the
    // reassembled text must not double up.
    assert_eq!(message["reasoning_content"], "Let me think.");
    assert_eq!(result["choices"][0]["finish_reason"], "stop");
}

#[test]
fn aggregates_deepseek_reasoning_content_into_sync_body() {
    let body = concat!(
        "data: {\"id\":\"chatcmpl-deepseek\",\"object\":\"chat.completion.chunk\",\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"Let me\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-deepseek\",\"object\":\"chat.completion.chunk\",\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"reasoning_content\":\" think.\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl-deepseek\",\"object\":\"chat.completion.chunk\",\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"42\"},\"finish_reason\":\"stop\"}]}\n\n",
    );

    let result = aggregate_openai_chat_stream_sync_response(body.as_bytes())
        .expect("deepseek chat stream should aggregate into a sync body");

    let message = &result["choices"][0]["message"];
    assert_eq!(message["content"], "42");
    assert_eq!(message["reasoning_content"], "Let me think.");
    assert_eq!(result["choices"][0]["finish_reason"], "stop");
}

#[test]
fn aggregated_chat_stream_without_reasoning_adds_no_reasoning_key() {
    let body = "data: {\"id\":\"chatcmpl-openai\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":\"stop\"}]}\n\n";

    let result = aggregate_openai_chat_stream_sync_response(body.as_bytes())
        .expect("plain chat stream should aggregate into a sync body");

    let message = &result["choices"][0]["message"];
    assert_eq!(message["content"], "hi");
    assert!(
        message.get("reasoning_content").is_none(),
        "a stream with no reasoning must not gain a reasoning key: {message}"
    );
}

#[test]
fn aggregated_openai_chat_reasoning_reaches_every_client_format() {
    let body = concat!(
        "data: {\"id\":\"gen-openrouter-123\",\"object\":\"chat.completion.chunk\",\"model\":\"stealth/ox-alpha\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"\",\"role\":\"assistant\",\"reasoning\":\"Thinking.\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"gen-openrouter-123\",\"object\":\"chat.completion.chunk\",\"model\":\"stealth/ox-alpha\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Done.\",\"role\":\"assistant\"},\"finish_reason\":\"stop\"}]}\n\n",
    );
    let aggregated = aggregate_openai_chat_stream_sync_response(body.as_bytes())
        .expect("openrouter chat stream should aggregate into a sync body");
    let report_context = json!({});

    for (client_api_format, marker) in [
        ("openai:responses", "\"type\":\"reasoning\""),
        ("claude:messages", "\"type\":\"thinking\""),
        ("gemini:generate_content", "\"thought\":true"),
    ] {
        let converted = convert_standard_chat_response(
            &aggregated,
            "openai:chat",
            client_api_format,
            &report_context,
        )
        .unwrap_or_else(|| panic!("{client_api_format} should convert"));
        let encoded = serde_json::to_string(&converted).expect("converted body should encode");
        assert!(
            encoded.contains(marker),
            "{client_api_format} dropped the reasoning block: {encoded}"
        );
    }
}

#[test]
fn aggregates_openai_chat_stream_tool_usage_and_finish_into_sync_body() {
    let body = concat!(
        "data: {\"id\":\"chatcmpl_stream_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n",
        "data: {\"id\":\"chatcmpl_stream_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello \"}}]}\n\n",
        "data: {\"id\":\"chatcmpl_stream_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_123\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\"\"}}]}}]}\n\n",
        "data: {\"id\":\"chatcmpl_stream_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"rust\\\"}\"}}]}}]}\n\n",
        "data: {\"id\":\"chatcmpl_stream_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
    );

    let result = aggregate_openai_chat_stream_sync_response(body.as_bytes())
        .expect("openai chat stream should aggregate into a sync body");

    assert_eq!(result["id"], "chatcmpl_stream_123");
    assert_eq!(result["model"], "gpt-5");
    assert_eq!(result["choices"][0]["message"]["role"], "assistant");
    assert_eq!(result["choices"][0]["message"]["content"], "Hello ");
    assert_eq!(
        result["choices"][0]["message"]["tool_calls"][0]["id"],
        "call_123"
    );
    assert_eq!(
        result["choices"][0]["message"]["tool_calls"][0]["function"]["name"],
        "lookup"
    );
    assert_eq!(
        result["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"],
        "{\"q\":\"rust\"}"
    );
    assert_eq!(result["choices"][0]["finish_reason"], "tool_calls");
    assert_eq!(result["usage"]["prompt_tokens"], 1);
    assert_eq!(result["usage"]["completion_tokens"], 2);
    assert_eq!(result["usage"]["total_tokens"], 3);
}

#[test]
fn aggregates_claude_stream_thinking_signatures_into_sync_body() {
    let body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"step by step\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig_123\"}}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"input_tokens\":1,\"output_tokens\":2}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    let aggregated =
        aggregate_claude_stream_sync_response(body.as_bytes()).expect("body should aggregate");

    assert_eq!(aggregated["content"][0]["type"], "thinking");
    assert_eq!(aggregated["content"][0]["thinking"], "step by step");
    assert_eq!(aggregated["content"][0]["signature"], "sig_123");
    assert_eq!(aggregated["usage"]["output_tokens"], 2);
}

#[test]
fn aggregates_native_claude_stream_preserves_empty_pages_from_tool_input() {
    let body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_read\",\"name\":\"Read\",\"input\":{}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"file_path\\\":\\\"/tmp/a.txt\\\",\\\"offset\\\":1,\\\"limit\\\":20,\\\"pages\\\":\\\"\\\"}\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    let aggregated =
        aggregate_claude_stream_sync_response(body.as_bytes()).expect("body should aggregate");

    assert_eq!(aggregated["content"][0]["type"], "tool_use");
    assert_eq!(
        aggregated["content"][0]["input"],
        json!({
            "file_path": "/tmp/a.txt",
            "offset": 1,
            "limit": 20,
            "pages": "",
        })
    );
}

#[test]
fn aggregates_native_claude_stream_preserves_empty_pages_from_start_tool_input() {
    let body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_read\",\"name\":\"Read\",\"input\":{\"file_path\":\"/tmp/a.txt\",\"limit\":20,\"pages\":\"\"}}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    let aggregated =
        aggregate_claude_stream_sync_response(body.as_bytes()).expect("body should aggregate");

    assert_eq!(
        aggregated["content"][0]["input"],
        json!({
            "file_path": "/tmp/a.txt",
            "limit": 20,
            "pages": "",
        })
    );
}

#[test]
fn aggregates_claude_stream_preserves_empty_pages_for_non_read_tool_input() {
    let body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_123\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_search\",\"name\":\"Search\",\"input\":{}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"query\\\":\\\"\\\",\\\"pages\\\":\\\"\\\"}\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    let aggregated =
        aggregate_claude_stream_sync_response(body.as_bytes()).expect("body should aggregate");

    assert_eq!(aggregated["content"][0]["type"], "tool_use");
    assert_eq!(
        aggregated["content"][0]["input"],
        json!({
            "query": "",
            "pages": "",
        })
    );
}

#[test]
fn aggregates_gemini_stream_deltas_media_and_signatures_into_sync_body() {
    let body = concat!(
        "data: {\"responseId\":\"resp_gem_stream_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"rea\",\"thought\":true}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"son\",\"thought\":true}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thought\":true,\"thoughtSignature\":\"sig_123\"}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"iVBORw0KGgo=\"}}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
    );

    let aggregated =
        aggregate_gemini_stream_sync_response(body.as_bytes()).expect("body should aggregate");

    assert_eq!(aggregated["responseId"], "resp_gem_stream_123");
    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][0]["text"],
        "reason"
    );
    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][0]["thoughtSignature"],
        "sig_123"
    );
    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][1]["inlineData"]["mimeType"],
        "image/png"
    );
    assert_eq!(aggregated["candidates"][0]["finishReason"], "STOP");
    assert_eq!(aggregated["usageMetadata"]["totalTokenCount"], 5);
}

#[test]
fn aggregates_antigravity_signature_only_reasoning_exhaustion() {
    let body = concat!(
        "data: {\"response\":{\"responseId\":\"resp_signature_only_123\",\"modelVersion\":\"gemini-3.7-flash-tiered\",",
        "\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thoughtSignature\":\"opaque-thought-signature\"}]},\"finishReason\":\"MAX_TOKENS\"}],",
        "\"usageMetadata\":{\"promptTokenCount\":22,\"thoughtsTokenCount\":29,\"totalTokenCount\":51}},",
        "\"traceId\":\"trace-signature-only\"}\n\n",
    );

    let aggregated = aggregate_gemini_stream_sync_response(body.as_bytes())
        .expect("signature-only reasoning terminal should aggregate");

    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][0]["thought"],
        true
    );
    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][0]["thoughtSignature"],
        "opaque-thought-signature"
    );
    assert_eq!(aggregated["candidates"][0]["finishReason"], "MAX_TOKENS");
    assert_eq!(aggregated["usageMetadata"]["thoughtsTokenCount"], 29);
    assert!(crate::formats::gemini::generate_content::response::from_raw(&aggregated).is_some());

    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:chat",
        "mapped_model": "gemini-3.7-flash-tiered",
    });
    let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("signature-only reasoning terminal should convert")
    .expect("cross-format product should exist");

    assert_eq!(
        product.client_body_json["choices"][0]["finish_reason"],
        "length"
    );
    assert_eq!(
        product.client_body_json["usage"]["completion_tokens_details"]["reasoning_tokens"],
        29
    );
}

#[test]
fn gemini_stream_aggregation_rejects_unknown_parts() {
    let body = "data: {\"responseId\":\"resp_gem_unknown_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"futurePart\":{\"kept\":true}}]}}]}\n\n";

    assert!(
        aggregate_gemini_stream_sync_response(body.as_bytes()).is_none(),
        "unknown Gemini stream parts must not be silently aggregated into a successful sync body"
    );
}

#[test]
fn gemini_stream_finalize_rejects_unknown_parts_even_with_json_fallback() {
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:chat",
    });
    let stream_body = "data: {\"responseId\":\"resp_gem_unknown_456\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"futurePart\":{\"kept\":true}}]}}]}\n\n";
    let provider_body_json = json!({
        "responseId": "resp_gem_unknown_456",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [{
            "index": 0,
            "content": {
                "role": "model",
                "parts": [{
                    "text": "fallback"
                }]
            }
        }]
    });

    let result = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    );

    assert!(result.is_err());
    let error = result.expect_err("unknown Gemini stream parts should fail closed");
    assert!(error
        .to_string()
        .contains("Unsupported provider stream event cannot be converted losslessly"));
    assert!(error
        .to_string()
        .contains("field $.futurePart is unsupported"));
}

#[test]
fn aggregates_gemini_stream_function_response_into_sync_body() {
    let body = concat!(
        "data: {\"responseId\":\"resp_gem_tool_result_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionResponse\":{\"id\":\"call_123\",\"name\":\"lookup\",\"response\":{\"ok\":true}}}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_tool_result_123\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
    );

    let aggregated =
        aggregate_gemini_stream_sync_response(body.as_bytes()).expect("body should aggregate");

    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][0]["functionResponse"]["id"],
        "call_123"
    );
    assert_eq!(
        aggregated["candidates"][0]["content"]["parts"][0]["functionResponse"]["response"],
        json!({"ok": true})
    );
}

#[test]
fn builds_openai_chat_cross_format_sync_product_from_gemini_stream_with_media_and_signature() {
    let body = concat!(
        "data: {\"responseId\":\"resp_gem_stream_456\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"thinking\",\"thought\":true}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_456\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thought\":true,\"thoughtSignature\":\"sig_456\"}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_456\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"iVBORw0KGgo=\"}}]}}]}\n\n",
        "data: {\"responseId\":\"resp_gem_stream_456\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":4,\"candidatesTokenCount\":2,\"totalTokenCount\":6}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:chat",
        "mapped_model": "gemini-2.5-pro",
    });

    let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("product build should succeed")
    .expect("product should exist");

    assert_eq!(
        product.provider_body_json["candidates"][0]["content"]["parts"][0]["thoughtSignature"],
        "sig_456"
    );
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["reasoning_parts"][0]["signature"],
        "sig_456"
    );
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"][0]["type"],
        "image_url"
    );
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"][0]["image_url"]["url"],
        "data:image/png;base64,iVBORw0KGgo="
    );
}

#[test]
fn builds_standard_cross_format_sync_product_from_normalized_stream_payload() {
    let body = concat!(
        "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"created\":1,",
        "\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",",
        "\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n",
        "data: {\"id\":\"chatcmpl_123\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",",
        "\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],",
        "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":2,\"total_tokens\":3}}\n\n",
        "data: [DONE]\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
    });

    let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("product build should succeed")
    .expect("product should exist");

    assert_eq!(
        product.provider_body_json,
        json!({
            "id": "chatcmpl_123",
            "object": "chat.completion",
            "created": 1,
            "model": "gpt-5",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "Hello",
                },
                "finish_reason": "stop",
            }],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
            },
        })
    );
    assert_eq!(
        product.client_body_json.get("type"),
        Some(&json!("message"))
    );
    assert_eq!(
        product.client_body_json.get("id"),
        Some(&json!("chatcmpl_123"))
    );
    assert_eq!(
        product.client_body_json.get("content"),
        Some(&json!([{ "type": "text", "text": "Hello" }]))
    );
    assert_eq!(
        product.client_body_json.get("stop_reason"),
        Some(&json!("end_turn"))
    );
}

#[test]
fn falls_back_to_body_json_when_stream_aggregation_returns_none() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
    });
    let provider_body_json = json!({
        "id": "chatcmpl_123",
        "object": "chat.completion",
        "created": 1,
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Hello",
            },
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 2,
            "total_tokens": 3,
        },
    });

    let product = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        Some(&base64::engine::general_purpose::STANDARD.encode("not an sse stream")),
    )
    .expect("fallback build should succeed");

    assert_eq!(
        product.expect("product should exist").provider_body_json,
        provider_body_json
    );
}

#[test]
fn unframed_stream_events_are_not_mistaken_for_provider_bodies() {
    for event in [
        json!({"type": "response.completed", "response": {"status": "completed"}}),
        json!({"type": "response.output_text.delta", "delta": "hi"}),
        json!({"type": "message_start", "message": {"id": "msg_1"}}),
        json!({"type": "content_block_delta", "index": 0, "delta": {"text": "hi"}}),
        json!({"object": "chat.completion.chunk", "choices": []}),
    ] {
        let body_base64 = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(&event).expect("serialize event"));
        assert!(
            decode_non_stream_sync_capture_body(&body_base64).is_none(),
            "stream events belong to the aggregators: {event}"
        );
    }
}

#[test]
fn complete_provider_bodies_are_recovered_from_cross_format_captures() {
    for body in [
        json!({"id": "resp_1", "object": "response", "status": "completed", "output": []}),
        json!({"id": "chatcmpl_1", "object": "chat.completion", "choices": []}),
        json!({"id": "msg_1", "type": "message", "role": "assistant", "content": []}),
        json!({"candidates": [], "modelVersion": "probe-model"}),
    ] {
        let body_base64 = base64::engine::general_purpose::STANDARD
            .encode(serde_json::to_vec(&body).expect("serialize provider body"));
        assert_eq!(
            decode_non_stream_sync_capture_body(&body_base64),
            Some(body.clone()),
            "a complete provider body is not a stream: {body}"
        );
    }
}

#[test]
fn recovers_cross_format_capture_that_is_a_complete_json_body() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "claude:messages",
        "needs_conversion": true,
        "upstream_is_stream": true,
    });
    let provider_body_json = json!({
        "id": "resp_1",
        "object": "response",
        "status": "completed",
        "error": null,
        "model": "probe-model",
        "output": [{
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hello"}]
        }],
        "usage": {"input_tokens": 5, "output_tokens": 7, "total_tokens": 12}
    });
    let body_base64 = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_vec(&provider_body_json).expect("serialize provider body"));

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&body_base64),
    )
    .expect("a complete provider body must not fail the stream aggregator")
    .expect("product should exist");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("cross-format attempt should produce a cross-format product");
    };
    assert_eq!(product.provider_body_json, provider_body_json);
    assert_eq!(product.client_body_json["type"], "message");
    assert_eq!(product.client_body_json["content"][0]["text"], "hello");
}

#[test]
fn keeps_unframed_stream_event_on_the_aggregation_path() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "claude:messages",
        "needs_conversion": true,
        "upstream_is_stream": true,
    });
    let provider_body_json = json!({
        "id": "resp_1",
        "object": "response",
        "status": "completed",
        "model": "probe-model",
        "output": [{
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hello"}]
        }],
        "usage": {"input_tokens": 5, "output_tokens": 7, "total_tokens": 12}
    });
    let event = json!({
        "type": "response.completed",
        "response": provider_body_json.clone(),
    });
    let body_base64 = base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_vec(&event).expect("serialize stream event"));

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&body_base64),
    )
    .expect("unframed stream event should aggregate")
    .expect("product should exist");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("cross-format attempt should produce a cross-format product");
    };
    assert_eq!(product.provider_body_json["id"], provider_body_json["id"]);
    assert_eq!(product.provider_body_json["object"], "response");
    assert!(product.provider_body_json.get("response").is_none());
    assert_eq!(product.client_body_json["type"], "message");
}

#[test]
fn builds_standard_same_format_body_from_stream_payload() {
    let body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-3-5-sonnet-latest\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":5,\"output_tokens\":7}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "claude:messages",
        "needs_conversion": false,
    });

    let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("same-format builder should succeed")
    .expect("body should exist");

    assert_eq!(body_json.get("type"), Some(&json!("message")));
    assert_eq!(body_json.get("role"), Some(&json!("assistant")));
    assert_eq!(
        body_json.get("content"),
        Some(&json!([{ "type": "text", "text": "hello" }]))
    );
}

#[test]
fn falls_back_to_body_json_for_standard_same_format_sync_payload() {
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "gemini:generate_content",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "candidates": [{
            "index": 0,
            "content": {
                "parts": [{ "text": "hello" }],
                "role": "model"
            },
            "finishReason": "STOP"
        }]
    });

    let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "gemini_cli_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("same-format sync body should succeed")
    .expect("body should exist");

    assert_eq!(body_json, provider_body_json);
}

#[test]
fn same_format_claude_sync_body_sanitizes_only_for_legacy_compatibility() {
    let native_report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "claude:messages",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "id": "msg_read",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4-6",
        "future_message_field": {"preserve": true},
        "content": [
            {
                "type": "tool_use",
                "id": "toolu_read",
                "name": "Read",
                "input": {
                    "file_path": "/tmp/a.txt",
                    "limit": 20,
                    "pages": "",
                    "future_input_field": 42
                },
                "future_block_field": "keep"
            },
            {
                "type": "tool_use",
                "id": "toolu_search",
                "name": "Search",
                "input": {
                    "query": "",
                    "pages": ""
                }
            }
        ]
    });

    let native_body = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&native_report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("native same-format sync body should succeed")
    .expect("native body should exist");
    assert_eq!(native_body, provider_body_json);

    let legacy_report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "claude:messages",
        "needs_conversion": false,
        "anthropic_compatibility_profile": "claude_code_legacy",
    });

    let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&legacy_report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("same-format sync body should succeed")
    .expect("body should exist");

    assert_eq!(
        body_json["content"][0]["input"],
        json!({
            "file_path": "/tmp/a.txt",
            "limit": 20,
            "future_input_field": 42,
        })
    );
    assert_eq!(body_json["content"][0]["future_block_field"], "keep");
    assert_eq!(body_json["future_message_field"]["preserve"], true);
    assert_eq!(
        body_json["content"][1]["input"],
        json!({
            "query": "",
            "pages": "",
        })
    );
}

#[test]
fn same_format_claude_stream_sync_body_sanitizes_only_for_legacy_compatibility() {
    let stream = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_read\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"future_message_field\":{\"keep\":true}}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_read\",\"name\":\"Read\",\"input\":{},\"future_block_field\":\"keep\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"file_path\\\":\\\"/tmp/a.txt\\\",\\\"pages\\\":\\\"\\\"}\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );
    let body_base64 = base64::engine::general_purpose::STANDARD.encode(stream);
    let native_report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "claude:messages",
        "needs_conversion": false,
    });

    let native_body = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&native_report_context),
        None,
        Some(&body_base64),
    )
    .expect("native stream sync body should succeed")
    .expect("native stream sync body should exist");
    assert_eq!(native_body["content"][0]["input"]["pages"], "");
    assert_eq!(native_body["content"][0]["future_block_field"], "keep");
    assert_eq!(native_body["future_message_field"]["keep"], true);

    let legacy_report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "claude:messages",
        "needs_conversion": false,
        "provider_type": "claude_code",
    });
    let legacy_body = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&legacy_report_context),
        None,
        Some(&body_base64),
    )
    .expect("legacy stream sync body should succeed")
    .expect("legacy stream sync body should exist");
    assert!(legacy_body["content"][0]["input"].get("pages").is_none());
    assert_eq!(legacy_body["content"][0]["future_block_field"], "keep");
    assert_eq!(legacy_body["future_message_field"]["keep"], true);
}

#[test]
fn same_format_sync_response_restores_model_directive_display_model() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "model": "gpt-5.5-xhigh",
        "mapped_model": "gpt-5.5",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "id": "resp_123",
        "object": "response",
        "model": "gpt-5.5",
        "status": "completed",
        "output": []
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("same-format sync body should succeed")
    .expect("body should exist");
    let StandardSyncFinalizeNormalizedProduct::SuccessBody(body_json) = product else {
        panic!("same-format response should be a success body");
    };

    assert_eq!(body_json["model"], "gpt-5.5-xhigh");
}

#[test]
fn cross_format_sync_response_restores_model_directive_display_model() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "claude:messages",
        "model": "gpt-5.5-xhigh",
        "mapped_model": "gpt-5.5",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "id": "resp_123",
        "object": "response",
        "model": "gpt-5.5",
        "status": "completed",
        "output": [{
            "type": "message",
            "id": "msg_123",
            "role": "assistant",
            "status": "completed",
            "content": [{ "type": "output_text", "text": "done" }]
        }]
    });

    let product = maybe_build_standard_cross_format_sync_product(
        "claude_cli_sync_finalize",
        "openai:responses",
        "claude:messages",
        &report_context,
        provider_body_json,
    )
    .expect("cross-format product should exist");

    assert_eq!(product.client_body_json["model"], "gpt-5.5-xhigh");
    assert_eq!(product.provider_body_json["model"], "gpt-5.5");
}

#[test]
fn rejects_standard_same_format_when_needs_conversion_is_true() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
    });
    let provider_body_json = json!({ "id": "chatcmpl_123" });

    let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("same-format guard should not error");

    assert!(body_json.is_none());
}

#[test]
fn rejects_standard_same_format_error_body_json() {
    let report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "claude:messages",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "type": "error",
        "error": {
            "type": "rate_limit_error",
            "message": "slow down"
        }
    });

    let body_json = maybe_build_standard_same_format_sync_body_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("same-format error guard should not error");

    assert!(body_json.is_none());
}

#[test]
fn builds_openai_responses_same_family_body_from_stream_payload() {
    let body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:responses:compact",
        "client_api_format": "openai:responses:compact",
        "needs_conversion": false,
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_compact_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("openai-responses family stream should succeed")
    .expect("body should exist");

    assert_eq!(body_json.get("id"), Some(&json!("resp_123")));
    assert_eq!(body_json.get("status"), Some(&json!("completed")));
    assert_eq!(body_json["output"][0]["content"][0]["text"], json!("Hello"));
}

#[test]
fn same_family_completed_metadata_uses_materialized_stream_output() {
    for terminal_response in [
        json!({
            "id": "resp_codex_terminal_metadata",
            "object": "response",
            "model": "gpt-5.6-sol",
            "status": "completed",
            "output": [],
            "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
        }),
        json!({
            "id": "resp_codex_terminal_metadata",
            "object": "response",
            "model": "gpt-5.6-sol",
            "status": "completed",
            "usage": {"input_tokens": 1, "output_tokens": 2, "total_tokens": 3}
        }),
    ] {
        let body = format!(
            "event: response.created\ndata: {}\n\n\
             event: response.output_text.delta\ndata: {}\n\n\
             event: response.completed\ndata: {}\n\n",
            json!({
                "type": "response.created",
                "response": {
                    "id": "resp_codex_terminal_metadata",
                    "object": "response",
                    "model": "gpt-5.6-sol",
                    "status": "in_progress",
                    "output": []
                }
            }),
            json!({
                "type": "response.output_text.delta",
                "output_index": 0,
                "content_index": 0,
                "delta": "Hello from Codex"
            }),
            json!({"type": "response.completed", "response": terminal_response})
        );
        let report_context = json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
            "needs_conversion": false,
        });

        let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
            "openai_responses_sync_finalize",
            200,
            Some(&report_context),
            None,
            Some(&base64::engine::general_purpose::STANDARD.encode(body)),
        )
        .expect("Responses stream should aggregate")
        .expect("sync response should exist");

        assert_eq!(
            body_json["output"][0]["content"][0]["text"],
            "Hello from Codex"
        );
    }
}

#[test]
fn same_family_responses_stream_uses_terminal_response_as_authoritative_snapshot() {
    let terminal_response = json!({
        "id": "resp_gpt56_terminal",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "completed",
        "output": [
            {
                "type": "program",
                "call_id": "program-call-1",
                "fingerprint": "fp-program-1"
            },
            {
                "type": "program_output",
                "call_id": "program-call-1",
                "result": "hello",
                "status": "completed"
            },
            {
                "type": "multi_agent_call",
                "call_id": "agent-call-1",
                "action": "delegate",
                "arguments": {"query": "release notes"},
                "agent": "researcher"
            },
            {
                "type": "agent_message",
                "author": "researcher",
                "recipient": "assistant",
                "encrypted_content": "encrypted-agent-message"
            }
        ],
        "usage": {
            "input_tokens": 12,
            "output_tokens": 4,
            "total_tokens": 16,
            "input_tokens_details": {"cache_write_tokens": 3}
        }
    });
    let body = format!(
        "event: response.completed\ndata: {}\n\n",
        json!({"type": "response.completed", "response": terminal_response})
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": false,
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("same-family terminal snapshot extensions should remain lossless")
    .expect("terminal response should become the sync response");

    assert_eq!(body_json, terminal_response);
}

#[test]
fn cross_format_responses_stream_still_rejects_unknown_gpt_5_6_events() {
    let body = concat!(
        "event: response.program.delta\n",
        "data: {\"type\":\"response.program.delta\",\"delta\":\"print\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_cross_unknown\",\"object\":\"response\",\"model\":\"gpt-5.6-sol\",\"status\":\"completed\",\"output\":[{\"type\":\"program\",\"id\":\"program-1\"}]}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
    });

    let result = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    );

    assert!(result.is_err());
}

#[test]
fn same_family_responses_stream_accepts_authoritative_incomplete_snapshot() {
    let terminal_response = json!({
        "id": "resp_gpt56_incomplete",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "incomplete",
        "output": [{
            "type": "message",
            "id": "msg_gpt56_incomplete",
            "role": "assistant",
            "status": "incomplete",
            "content": [{
                "type": "output_text",
                "text": "partial",
                "annotations": []
            }]
        }],
        "incomplete_details": {"reason": "max_output_tokens"}
    });
    let body = format!(
        "event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial\"}}\n\n\
         event: response.incomplete\ndata: {}\n\n",
        json!({"type": "response.incomplete", "response": terminal_response})
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": false,
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("incomplete terminal snapshot should remain a valid Responses result")
    .expect("terminal response should become the sync response");

    assert_eq!(body_json, terminal_response);
}

#[test]
fn same_family_responses_stream_accepts_authoritative_failed_snapshot() {
    let terminal_response = json!({
        "id": "resp_gpt56_failed",
        "object": "response",
        "model": "gpt-5.6-sol",
        "status": "failed",
        "output": [{
            "type": "program",
            "call_id": "program-call-1",
            "fingerprint": "fp-program-1"
        }],
        "error": {"code": "server_error", "message": "upstream failed"}
    });
    let body = format!(
        "event: response.failed\ndata: {}\n\n",
        json!({
            "type": "response.failed",
            "sequence_number": 3,
            "response": terminal_response
        })
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": false,
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("failed terminal snapshot should remain an authoritative Responses result")
    .expect("failed terminal response should become the sync response");

    assert_eq!(body_json, terminal_response);
}

#[test]
fn builds_openai_responses_same_family_body_from_legacy_outtext_delta_alias() {
    let body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_legacy_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.outtext.delta\n",
        "data: {\"type\":\"response.outtext.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello from legacy alias\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_legacy_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello from legacy alias\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":4,\"total_tokens\":5}}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": false,
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("openai-responses legacy alias aggregation should succeed")
    .expect("body should exist");

    assert_eq!(body_json["id"], "resp_legacy_123");
    assert_eq!(
        body_json["output"][0]["content"][0]["text"],
        json!("Hello from legacy alias")
    );
}

#[test]
fn preserves_openai_responses_completed_output_when_already_present() {
    let body = concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Ignored\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_preserve_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_preserve_123\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"Authoritative\",\"annotations\":[]}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["content"][0]["text"], "Authoritative");
}

#[test]
fn reconstructs_openai_responses_image_generation_call_from_output_item_done() {
    let body = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_image_123\",\"object\":\"response\",\"model\":\"gpt-5.4-mini\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.output_item.done\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"ig_123\",\"type\":\"image_generation_call\",\"status\":\"completed\",\"output_format\":\"png\",\"result\":\"aGVsbG8=\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_image_123\",\"object\":\"response\",\"model\":\"gpt-5.4-mini\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["type"], "image_generation_call");
    assert_eq!(result["output"][0]["result"], "aGVsbG8=");
    assert_eq!(result["output"][0]["output_format"], "png");
}

#[test]
fn reconstructs_openai_responses_compaction_without_output_index_or_terminal_output() {
    let body = concat!(
        "event: response.output_item.done\n",
        "data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"compaction\",\"encrypted_content\":\"ENCRYPTED_CONTEXT_COMPACTION_SUMMARY\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-compact\",\"usage\":{\"input_tokens\":0,\"output_tokens\":0,\"total_tokens\":0}}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("compaction stream should aggregate into a sync response");

    assert_eq!(result["output"].as_array().map(Vec::len), Some(1));
    assert_eq!(result["output"][0]["type"], "compaction");
    assert_eq!(
        result["output"][0]["encrypted_content"],
        "ENCRYPTED_CONTEXT_COMPACTION_SUMMARY"
    );
}

#[test]
fn reconstructs_openai_responses_multi_part_message_content_order() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_multi_123\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
        "event: response.content_part.added\n",
        "data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"Hello\",\"annotations\":[]}}\n\n",
        "event: response.content_part.added\n",
        "data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":1,\"part\":{\"type\":\"output_text\",\"text\":\" world\",\"annotations\":[]}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_multi_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["type"], "message");
    assert_eq!(result["output"][0]["content"][0]["text"], "Hello");
    assert_eq!(result["output"][0]["content"][1]["text"], " world");
}

#[test]
fn aggregates_openai_responses_text_snapshot_deltas_without_duplicates() {
    let body = concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":{\"text\":\"Hello\"}}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":{\"text\":\"Hello world\"}}\n\n",
        "event: response.output_text.done\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"Hello world\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_snapshot_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["content"][0]["text"], "Hello world");
}

#[test]
fn preserves_openai_responses_non_text_content_parts() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_non_text_123\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
        "event: response.content_part.done\n",
        "data: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"refusal\",\"refusal\":\"blocked\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_non_text_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["content"][0]["type"], "refusal");
    assert_eq!(result["output"][0]["content"][0]["refusal"], "blocked");
}

#[test]
fn aggregates_official_refusal_stream_events() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_refusal_123\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n",
        "event: response.content_part.added\n",
        "data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"refusal\",\"refusal\":\"\"}}\n\n",
        "event: response.refusal.delta\n",
        "data: {\"type\":\"response.refusal.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_refusal_123\",\"delta\":\"I can't\"}\n\n",
        "event: response.refusal.done\n",
        "data: {\"type\":\"response.refusal.done\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_refusal_123\",\"refusal\":\"I can't help with that.\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses refusal stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["content"][0]["type"], "refusal");
    assert_eq!(
        result["output"][0]["content"][0]["refusal"],
        "I can't help with that."
    );
    assert_eq!(result["output_text"], "");
}

#[test]
fn aggregates_official_output_text_annotation_added_event() {
    let body = concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello annotated\"}\n\n",
        "event: response.output_text.annotation.added\n",
        "data: {\"type\":\"response.output_text.annotation.added\",\"output_index\":0,\"content_index\":0,\"annotation_index\":0,\"annotation\":{\"type\":\"text_annotation\",\"text\":\"annotated\",\"start\":6,\"end\":15}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_annotation_added_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses annotation stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["content"][0]["text"], "Hello annotated");
    assert_eq!(
        result["output"][0]["content"][0]["annotations"][0]["type"],
        "text_annotation"
    );
    assert_eq!(
        result["output"][0]["content"][0]["annotations"][0]["start"],
        6
    );
    assert_eq!(result["output_text"], "Hello annotated");
}

#[test]
fn authoritative_output_text_done_preserves_annotations() {
    let body = concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"Hello\"}\n\n",
        "event: response.output_text.done\n",
        "data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"Hello\",\"annotations\":[{\"type\":\"url_citation\",\"url\":\"https://example.com\"}]}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_annotations_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["content"][0]["text"], "Hello");
    assert_eq!(
        result["output"][0]["content"][0]["annotations"][0]["type"],
        "url_citation"
    );
    assert_eq!(
        result["output"][0]["content"][0]["annotations"][0]["url"],
        "https://example.com"
    );
}

#[test]
fn function_call_arguments_done_uses_item_metadata() {
    let body = concat!(
        "event: response.function_call_arguments.delta\n",
        "data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"item_id\":\"fc_done_123\",\"delta\":\"{\\\"location\\\":\"}\n\n",
        "event: response.function_call_arguments.done\n",
        "data: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"item_id\":\"fc_done_123\",\"item\":{\"type\":\"function_call\",\"id\":\"fc_done_123\",\"call_id\":\"call_done_weather\",\"name\":\"get_weather\",\"arguments\":\"{\\\"location\\\": \\\"Tokyo\\\"}\"}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_done_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("openai-responses stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["type"], "function_call");
    assert_eq!(result["output"][0]["call_id"], "call_done_weather");
    assert_eq!(result["output"][0]["name"], "get_weather");
    assert_eq!(result["output"][0]["arguments"], r#"{"location": "Tokyo"}"#);
}

#[test]
fn validates_and_preserves_chat_backed_function_call_metadata() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_chat_metadata_123\",\"call_id\":\"call_chat_metadata_123\",\"status\":\"completed\",\"arguments\":\"{\\\"query\\\":\\\"aether\\\"}\",\"name\":\"lookup\",\"metadata\":{\"source\":\"chat\"},\"internal_chat_message_metadata_passthrough\":{\"turn_id\":\"turn_123\"}}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat_metadata_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );

    let result = try_aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("chat-backed function call metadata should pass stream validation")
        .expect("chat-backed function call stream should aggregate");

    assert_eq!(result["output"][0]["type"], "function_call");
    assert_eq!(result["output"][0]["call_id"], "call_chat_metadata_123");
    assert_eq!(result["output"][0]["metadata"]["source"], "chat");
    assert_eq!(
        result["output"][0]["internal_chat_message_metadata_passthrough"]["turn_id"],
        "turn_123"
    );
}

#[test]
fn custom_tool_call_input_events_materialize_custom_tool_call() {
    let body = concat!(
        "event: response.custom_tool_call_input.delta\n",
        "data: {\"type\":\"response.custom_tool_call_input.delta\",\"output_index\":0,\"item_id\":\"ctc_123\",\"name\":\"code_exec\",\"delta\":\"print\"}\n\n",
        "event: response.custom_tool_call_input.done\n",
        "data: {\"type\":\"response.custom_tool_call_input.done\",\"output_index\":0,\"item_id\":\"ctc_123\",\"call_id\":\"call_custom_123\",\"name\":\"code_exec\",\"input\":\"print('hi')\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_custom_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("custom tool stream should aggregate into a sync body");

    assert_eq!(result["output"][0]["type"], "custom_tool_call");
    assert_eq!(result["output"][0]["call_id"], "call_custom_123");
    assert_eq!(result["output"][0]["name"], "code_exec");
    assert_eq!(result["output"][0]["input"], "print('hi')");
    assert!(result["output"][0].get("arguments").is_none());
}

#[test]
fn aggregates_modern_reasoning_text_and_response_done_alias() {
    let body = concat!(
        "event: response.reasoning_text.delta\n",
        "data: {\"type\":\"response.reasoning_text.delta\",\"output_index\":0,\"delta\":\"Need\"}\n\n",
        "event: response.reasoning_text.done\n",
        "data: {\"type\":\"response.reasoning_text.done\",\"output_index\":0,\"text\":\"Need care\"}\n\n",
        "event: response.done\n",
        "data: {\"type\":\"response.done\",\"response\":{\"id\":\"resp_done_alias_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\"}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("modern response.done stream should aggregate");

    assert_eq!(result["output"][0]["type"], "reasoning");
    assert_eq!(result["output"][0]["summary"], json!([]));
    assert_eq!(result["output"][0]["content"][0]["type"], "reasoning_text");
    assert_eq!(result["output"][0]["content"][0]["text"], "Need care");
    assert!(result["output"].as_array().is_some());
    assert_eq!(result["output_text"], "");
    assert!(result["completed_at"].as_i64().is_some());
}

#[test]
fn preserves_idless_provider_opaque_reasoning_item_during_materialization() {
    let original = json!({
        "type": "reasoning",
        "encrypted_content": "opaque-provider-state",
        "content": [{
            "type": "reasoning_text",
            "text": "private chain of thought"
        }],
        "summary": [{
            "type": "provider_summary",
            "text": "provider-owned summary"
        }],
        "future_provider_field": {"version": 2}
    });
    let state = OpenAIResponsesSyncReasoningState {
        item: original
            .as_object()
            .expect("reasoning item should be an object")
            .clone(),
        reasoning_text: "must not replace provider-owned state".to_string(),
    };

    let materialized = materialize_openai_responses_reasoning_item("resp_opaque_123", state);

    assert_eq!(materialized, original);
    assert!(materialized.get("id").is_none());
    assert!(materialized.get("status").is_none());
}

#[test]
fn aggregates_authoritative_provider_opaque_reasoning_item_without_mutation() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"opaque-provider-state\",\"content\":[]}}\n\n",
        "event: response.reasoning_text.done\n",
        "data: {\"type\":\"response.reasoning_text.done\",\"output_index\":0,\"text\":\"provider reasoning\"}\n\n",
        "event: response.output_item.done\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"encrypted_content\":\"opaque-provider-state\",\"content\":[{\"type\":\"reasoning_text\",\"text\":\"provider reasoning\"}],\"future_provider_field\":{\"version\":2}}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_opaque_stream_123\",\"object\":\"response\",\"model\":\"deepseek-reasoner\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );

    let result = aggregate_openai_responses_stream_sync_response(body.as_bytes())
        .expect("provider opaque reasoning stream should aggregate");

    assert_eq!(
        result["output"][0],
        json!({
            "type": "reasoning",
            "encrypted_content": "opaque-provider-state",
            "content": [{
                "type": "reasoning_text",
                "text": "provider reasoning"
            }],
            "future_provider_field": {"version": 2}
        })
    );
}

#[test]
fn synthesizes_wire_compatible_id_for_local_reasoning_text() {
    let state = OpenAIResponsesSyncReasoningState {
        item: json!({"type": "reasoning"})
            .as_object()
            .expect("reasoning item should be an object")
            .clone(),
        reasoning_text: "Need care".to_string(),
    };

    let materialized = materialize_openai_responses_reasoning_item("resp_summary_123", state);

    assert_eq!(
        materialized["id"],
        openai_responses_synthetic_reasoning_item_id("resp_summary_123", 0)
    );
    assert_eq!(materialized["summary"], json!([]));
    assert_eq!(materialized["content"][0]["type"], "reasoning_text");
    assert_eq!(materialized["content"][0]["text"], "Need care");
}

#[test]
fn accepts_openai_responses_same_family_stream_when_needs_conversion_is_true() {
    let body = concat!(
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses:compact",
        "needs_conversion": true,
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_compact_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("openai-responses same-family aggregation should not error")
    .expect("aggregated body should exist");

    assert_eq!(body_json["id"], "resp_123");
}

#[test]
fn falls_back_to_body_json_for_openai_responses_same_family_sync_payload() {
    let report_context = json!({
        "provider_api_format": "openai:responses:compact",
        "client_api_format": "openai:responses:compact",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "id": "resp_123",
        "object": "response",
        "status": "completed",
        "output": []
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_compact_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-responses same-family sync should succeed")
    .expect("body should exist");

    assert_eq!(body_json, provider_body_json);
}

#[test]
fn allows_openai_responses_same_family_cross_format_sync_when_conversion_is_flagged() {
    let report_context = json!({
        "provider_api_format": "openai:responses:compact",
        "client_api_format": "openai:responses",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "id": "resp_family_123",
        "object": "response",
        "status": "completed",
        "output": []
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-responses cross-family sync should succeed")
    .expect("body should exist");

    assert_eq!(body_json, provider_body_json);
}

#[test]
fn rejects_openai_responses_same_family_error_body_json() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses:compact",
        "model": "gpt-5",
        "mapped_model": "gpt-5",
    });
    let provider_body_json = json!({
        "error": {
            "message": "quota reached",
            "type": "rate_limit_error"
        }
    });

    let body_json = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-responses same-family error guard should not error");

    assert!(body_json.is_none());
}

#[test]
fn builds_openai_chat_cross_format_sync_product_from_claude_body_json() {
    let report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "claude-sonnet-4",
    });
    let provider_body_json = json!({
        "id": "msg_claude_direct_123",
        "type": "message",
        "model": "claude-sonnet-4",
        "role": "assistant",
        "content": [{"type": "text", "text": "Hello Claude"}],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 2,
            "output_tokens": 3
        }
    });

    let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-chat cross-format should succeed")
    .expect("product should exist");

    assert_eq!(product.provider_body_json, provider_body_json);
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "Hello Claude"
    );
}

#[test]
fn claude_canonical_response_route_matches_legacy_outputs_for_existing_clients() {
    let report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "claude-sonnet-4",
        "original_request_body": {
            "service_tier": "default"
        }
    });
    let provider_body_json = json!({
        "id": "msg_claude_canonical_123",
        "type": "message",
        "model": "claude-sonnet-4",
        "role": "assistant",
        "content": [
            {"type": "thinking", "thinking": "plan", "signature": "sig_123"},
            {"type": "text", "text": "Hello Claude"},
            {
                "type": "tool_use",
                "id": "toolu_123",
                "name": "lookup",
                "input": {"q": "rust"}
            }
        ],
        "stop_reason": "tool_use",
        "usage": {
            "input_tokens": 2,
            "output_tokens": 3,
            "cache_read_input_tokens": 1,
            "cache_creation_input_tokens": 1
        }
    });

    let legacy_openai =
        convert_claude_chat_response_to_openai_chat(&provider_body_json, &report_context)
            .expect("legacy claude -> openai chat");
    let converted_openai = convert_standard_chat_response(
        &provider_body_json,
        "claude:messages",
        "openai:chat",
        &report_context,
    )
    .expect("canonical claude -> openai chat");
    assert_eq!(converted_openai, legacy_openai);

    let legacy_claude =
        convert_openai_chat_response_to_claude_chat(&legacy_openai, &report_context)
            .expect("legacy claude -> openai chat -> claude");
    let converted_claude = convert_standard_chat_response(
        &provider_body_json,
        "claude:messages",
        "claude:messages",
        &report_context,
    )
    .expect("canonical claude -> claude");
    assert_eq!(converted_claude, legacy_claude);

    let legacy_openai_responses =
        convert_openai_chat_response_to_openai_responses(&legacy_openai, &report_context, false)
            .expect("legacy claude -> openai responses");
    let converted_openai_responses = convert_standard_cli_response(
        &provider_body_json,
        "claude:messages",
        "openai:responses",
        &report_context,
    )
    .expect("canonical claude -> openai responses");
    assert_eq!(converted_openai_responses, legacy_openai_responses);
}

#[test]
fn gemini_canonical_response_route_matches_legacy_outputs_for_single_candidate() {
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "gemini-2.5-pro",
    });
    let provider_body_json = json!({
        "responseId": "resp_gemini_canonical_123",
        "modelVersion": "gemini-2.5-pro",
        "candidates": [{
            "index": 0,
            "finishReason": "STOP",
            "content": {
                "role": "model",
                "parts": [
                    {"text": "plan", "thought": true, "thoughtSignature": "sig_123"},
                    {"text": "Hello Gemini"},
                    {
                        "functionCall": {
                            "id": "call_123",
                            "name": "lookup",
                            "args": {"q": "rust"}
                        }
                    }
                ]
            }
        }],
        "usageMetadata": {
            "promptTokenCount": 2,
            "candidatesTokenCount": 3,
            "thoughtsTokenCount": 1,
            "totalTokenCount": 6
        }
    });

    let legacy_openai =
        convert_gemini_chat_response_to_openai_chat(&provider_body_json, &report_context)
            .expect("legacy gemini -> openai chat");
    let converted_openai = convert_standard_chat_response(
        &provider_body_json,
        "gemini:generate_content",
        "openai:chat",
        &report_context,
    )
    .expect("canonical gemini -> openai chat");
    assert_eq!(converted_openai, legacy_openai);

    let legacy_claude =
        convert_openai_chat_response_to_claude_chat(&legacy_openai, &report_context)
            .expect("legacy gemini -> openai chat -> claude");
    let converted_claude = convert_standard_chat_response(
        &provider_body_json,
        "gemini:generate_content",
        "claude:messages",
        &report_context,
    )
    .expect("canonical gemini -> claude");
    assert_eq!(converted_claude, legacy_claude);

    let legacy_openai_responses =
        convert_openai_chat_response_to_openai_responses(&legacy_openai, &report_context, false)
            .expect("legacy gemini -> openai responses");
    let converted_openai_responses = convert_standard_cli_response(
        &provider_body_json,
        "gemini:generate_content",
        "openai:responses",
        &report_context,
    )
    .expect("canonical gemini -> openai responses");
    assert_eq!(converted_openai_responses, legacy_openai_responses);
}

#[test]
fn openai_chat_canonical_response_route_matches_legacy_outputs_for_single_choice() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
        "model": "gpt-5",
        "mapped_model": "gpt-5",
    });
    let provider_body_json = json!({
        "id": "chatcmpl_canonical_123",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Hello OpenAI",
                "reasoning_parts": [{
                    "type": "thinking",
                    "thinking": "plan",
                    "signature": "sig_123"
                }],
                "tool_calls": [{
                    "id": "call_123",
                    "type": "function",
                    "function": {
                        "name": "lookup",
                        "arguments": "{\"q\":\"rust\"}"
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {
            "prompt_tokens": 2,
            "completion_tokens": 4,
            "total_tokens": 6,
            "prompt_tokens_details": {
                "cached_tokens": 1
            },
            "completion_tokens_details": {
                "reasoning_tokens": 1
            }
        }
    });

    let legacy_claude =
        convert_openai_chat_response_to_claude_chat(&provider_body_json, &report_context)
            .expect("legacy openai chat -> claude");
    let converted_claude = convert_standard_chat_response(
        &provider_body_json,
        "openai:chat",
        "claude:messages",
        &report_context,
    )
    .expect("canonical openai chat -> claude");
    assert_eq!(converted_claude, legacy_claude);
    assert_eq!(converted_claude["usage"]["input_tokens"], 1);
    assert_eq!(converted_claude["usage"]["cache_read_input_tokens"], 1);
    assert_eq!(converted_claude["usage"]["output_tokens"], 4);

    let legacy_gemini =
        convert_openai_chat_response_to_gemini_chat(&provider_body_json, &report_context)
            .expect("legacy openai chat -> gemini");
    let converted_gemini = convert_standard_chat_response(
        &provider_body_json,
        "openai:chat",
        "gemini:generate_content",
        &report_context,
    )
    .expect("canonical openai chat -> gemini");
    assert_eq!(converted_gemini, legacy_gemini);

    let converted_openai_responses = convert_standard_cli_response(
        &provider_body_json,
        "openai:chat",
        "openai:responses",
        &report_context,
    )
    .expect("canonical openai chat -> openai responses");
    assert_eq!(converted_openai_responses["id"], "resp_canonical_123");
    assert!(
        converted_openai_responses["output"]
            .as_array()
            .is_some_and(|output| output.iter().any(|item| item["type"] == "reasoning")),
        "canonical OpenAI Chat -> Responses should preserve reasoning blocks"
    );
}

#[test]
fn openai_responses_canonical_response_route_matches_legacy_chat_outputs() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "gpt-5-upstream",
    });
    let provider_body_json = json!({
        "id": "resp_cli_canonical_123",
        "object": "response",
        "created_at": 1741476542i64,
        "status": "completed",
        "model": "gpt-5-upstream",
        "service_tier": "flex",
        "output": [{
            "type": "reasoning",
            "id": "rs_1",
            "summary": [{
                "type": "summary_text",
                "text": "because"
            }]
        }, {
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "Hello",
                "annotations": [{
                    "type": "file_citation",
                    "start_index": 0,
                    "end_index": 5
                }]
            }, {
                "type": "refusal",
                "refusal": "partial refusal"
            }]
        }, {
            "type": "function_call",
            "id": "call_1",
            "call_id": "call_1",
            "name": "lookup",
            "arguments": "{\"q\":\"rust\"}"
        }, {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": {"ok": true}
        }],
        "usage": {
            "input_tokens": 3,
            "input_tokens_details": {"cached_tokens": 2},
            "output_tokens": 5,
            "output_tokens_details": {"reasoning_tokens": 1},
            "total_tokens": 8
        }
    });

    let legacy_openai_chat =
        convert_openai_responses_response_to_openai_chat(&provider_body_json, &report_context)
            .expect("legacy openai responses -> openai chat");
    let converted_openai_chat = convert_standard_chat_response(
        &provider_body_json,
        "openai:responses",
        "openai:chat",
        &report_context,
    )
    .expect("canonical openai responses -> openai chat");
    assert_eq!(converted_openai_chat, legacy_openai_chat);

    let mut representable_body_json = provider_body_json.clone();
    representable_body_json
        .as_object_mut()
        .expect("response object")
        .remove("service_tier");
    representable_body_json["output"][1]["content"] = json!([{
        "type": "output_text",
        "text": "Hello",
        "annotations": []
    }]);

    let converted_claude = convert_standard_chat_response(
        &representable_body_json,
        "openai:responses",
        "claude:messages",
        &report_context,
    )
    .expect("canonical openai responses -> claude");
    assert_eq!(converted_claude["content"][3]["type"], "tool_result");
    assert_eq!(converted_claude["content"][3]["tool_use_id"], "call_1");
    assert_eq!(
        converted_claude["content"][3]["content"],
        json!({"ok": true})
    );
    assert_eq!(converted_claude["usage"]["input_tokens"], 1);
    assert_eq!(converted_claude["usage"]["cache_read_input_tokens"], 2);
    assert_eq!(converted_claude["usage"]["output_tokens"], 5);

    let converted_gemini = convert_standard_chat_response(
        &representable_body_json,
        "openai:responses",
        "gemini:generate_content",
        &report_context,
    )
    .expect("canonical openai responses -> gemini");
    assert_eq!(
        converted_gemini["candidates"][0]["content"]["parts"][3]["functionResponse"]["id"],
        "call_1"
    );
    assert_eq!(
        converted_gemini["candidates"][0]["content"]["parts"][3]["functionResponse"]["response"],
        json!({"ok": true})
    );

    let converted_openai_responses = convert_standard_cli_response(
        &provider_body_json,
        "openai:responses",
        "openai:responses",
        &report_context,
    )
    .expect("canonical openai responses -> openai responses");
    assert_eq!(
        converted_openai_responses["output"][3]["type"],
        "function_call_output"
    );
    assert_eq!(
        converted_openai_responses["usage"]["output_tokens_details"]["reasoning_tokens"],
        1
    );
}

#[test]
fn builds_openai_chat_cross_format_sync_product_from_gemini_body_json() {
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "gemini-2.5-pro",
    });
    let provider_body_json = json!({
        "responseId": "resp_gemini_direct_123",
        "candidates": [{
            "content": {
                "parts": [{"text": "Hello Gemini"}],
                "role": "model"
            },
            "finishReason": "STOP",
            "index": 0
        }],
        "modelVersion": "gemini-2.5-pro-upstream",
        "usageMetadata": {
            "promptTokenCount": 1,
            "candidatesTokenCount": 2,
            "totalTokenCount": 3
        }
    });

    let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-chat cross-format should succeed")
    .expect("product should exist");

    assert_eq!(product.provider_body_json, provider_body_json);
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "Hello Gemini"
    );
    assert_eq!(product.client_body_json["usage"]["completion_tokens"], 2);
}

#[test]
fn builds_openai_chat_cross_format_sync_product_from_openai_responses_body_json() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.4",
        "mapped_model": "gpt-5.4",
    });
    let provider_body_json = json!({
        "id": "resp_cli_direct_123",
        "object": "response",
        "status": "completed",
        "model": "gpt-5.4",
        "output": [{
            "type": "message",
            "id": "msg_cli_direct_123",
            "role": "assistant",
            "status": "completed",
            "content": [{
                "type": "output_text",
                "text": "Hello CLI",
                "annotations": []
            }]
        }]
    });

    let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-chat cross-format should succeed")
    .expect("product should exist");

    assert_eq!(product.provider_body_json, provider_body_json);
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "Hello CLI"
    );
}

#[test]
fn builds_openai_responses_cross_format_sync_product_from_gemini_stream_payload() {
    let body = concat!(
        "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
        "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:responses",
        "model": "gpt-5",
        "mapped_model": "gemini-2.5-pro-upstream",
    });

    let product = maybe_build_openai_responses_cross_format_sync_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("openai-responses cross-format should succeed")
    .expect("product should exist");

    assert_eq!(
        product.provider_body_json["responseId"],
        "resp_cli_stream_123"
    );
    assert_eq!(product.client_body_json["object"], "response");
    assert_eq!(
        product.client_body_json["output"][0]["content"][0]["text"],
        "Hello Gemini CLI"
    );
}

#[test]
fn rejects_openai_responses_cross_format_error_body_json() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses:compact",
        "model": "gpt-5",
        "mapped_model": "gpt-5",
    });
    let provider_body_json = json!({
        "error": {
            "message": "quota reached",
            "type": "rate_limit_error"
        }
    });

    let product = maybe_build_openai_responses_cross_format_sync_product_from_normalized_payload(
        "openai_responses_compact_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-responses cross-format error guard should not error");

    assert!(product.is_none());
}

#[test]
fn rejects_openai_responses_cross_format_for_openai_family_provider() {
    let report_context = json!({
        "provider_api_format": "openai:responses:compact",
        "client_api_format": "openai:responses",
        "model": "gpt-5",
        "mapped_model": "gpt-5",
    });
    let provider_body_json = json!({
        "id": "resp_123",
        "object": "response",
        "status": "completed",
        "output": []
    });

    let product = maybe_build_openai_responses_cross_format_sync_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("openai-responses cross-format openai-family guard should not error");

    assert!(product.is_none());
}

#[test]
fn rejects_openai_chat_cross_format_for_unsupported_matrix() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
    });
    let provider_body_json = json!({ "id": "chatcmpl_123" });

    let product = maybe_build_openai_chat_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("unsupported matrix should not error");

    assert!(product.is_none());
}

#[test]
fn strict_response_conversion_errors_do_not_use_legacy_fallback() {
    let openai_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
        "model": "claude-sonnet-4-5",
        "mapped_model": "gpt-5",
    });
    let openai_body = json!({
        "id": "chatcmpl_unknown_finish",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "done"
            },
            "finish_reason": "future_reason"
        }]
    });

    assert!(convert_standard_chat_response(
        &openai_body,
        "openai:chat",
        "claude:messages",
        &openai_context,
    )
    .is_none());

    let claude_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "claude-sonnet-4-5",
    });
    let claude_body = json!({
        "id": "msg_unknown_stop",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4-5",
        "content": [],
        "stop_reason": "future_reason",
        "usage": {
            "input_tokens": 1,
            "output_tokens": 2
        }
    });

    assert!(convert_standard_chat_response(
        &claude_body,
        "claude:messages",
        "openai:chat",
        &claude_context,
    )
    .is_none());
}

#[test]
fn stream_finalize_rejects_unknown_openai_chat_events_without_body_fallback() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
        "model": "claude-sonnet-4-5",
        "mapped_model": "gpt-5",
    });
    let stream_body = "data: {\"id\":\"chatcmpl_unknown_stream\",\"object\":\"chat.completion.chunk\",\"model\":\"gpt-5\",\"choices\":[{\"index\":0,\"delta\":{\"future_delta\":\"x\"}}]}\n\n";
    let fallback_body = json!({
        "id": "chatcmpl_fallback",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "fallback"},
            "finish_reason": "stop"
        }]
    });

    let error = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&fallback_body),
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect_err("unknown stream event should fail closed before body fallback");

    assert!(error
        .to_string()
        .contains("Unsupported provider stream event cannot be converted losslessly"));
}

#[test]
fn stream_finalize_rejects_unknown_claude_events_without_body_fallback() {
    let report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "claude-sonnet-4-5",
    });
    let stream_body = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_unknown_stream\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null}}\n\n",
        "event: future_event\n",
        "data: {\"type\":\"future_event\",\"payload\":{\"kept\":true}}\n\n",
    );
    let fallback_body = json!({
        "id": "msg_fallback",
        "type": "message",
        "role": "assistant",
        "model": "claude-sonnet-4-5",
        "content": [{"type": "text", "text": "fallback"}],
        "stop_reason": "end_turn",
        "usage": {
            "input_tokens": 1,
            "output_tokens": 2
        }
    });

    let error = maybe_build_standard_cross_format_sync_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&fallback_body),
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect_err("unknown stream event should fail closed before body fallback");

    assert!(error
        .to_string()
        .contains("Unsupported provider stream event cannot be converted losslessly"));
    assert!(error
        .to_string()
        .contains("field $.type = \"future_event\""));
}

#[test]
fn responses_same_family_stream_finalize_rejects_unknown_events_without_body_fallback() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": false,
        "model": "gpt-5",
        "mapped_model": "gpt-5",
    });
    let stream_body = concat!(
        "event: response.future.delta\n",
        "data: {\"type\":\"response.future.delta\",\"response\":{\"id\":\"resp_unknown_stream\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"in_progress\"},\"payload\":{\"kept\":true}}\n\n",
    );
    let fallback_body = json!({
        "id": "resp_fallback",
        "object": "response",
        "model": "gpt-5",
        "status": "completed",
        "output": []
    });

    let error = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&fallback_body),
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect_err("unknown stream event should fail closed before body fallback");

    assert!(error
        .to_string()
        .contains("Unsupported provider stream event cannot be converted losslessly"));
    assert!(error
        .to_string()
        .contains("field $.type = \"response.future.delta\""));
}

#[test]
fn responses_same_family_stream_rejects_unknown_event_before_valid_terminal_snapshot() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": false,
        "model": "gpt-5",
        "mapped_model": "gpt-5",
    });
    let stream_body = concat!(
        "data: {\"type\":\"response.future.delta\",\"payload\":{\"must_not_drop\":true}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_unknown_before_terminal\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"id\":\"msg_unknown_before_terminal\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"must not pass\",\"annotations\":[]}]}]}}\n\n",
    );

    let error = maybe_build_openai_responses_same_family_sync_body_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect_err("unknown event must fail closed even before a valid terminal snapshot");

    assert!(error
        .to_string()
        .contains("Unsupported provider stream event cannot be converted losslessly"));
}

#[test]
fn responses_stream_rejects_conflicting_or_multiple_authoritative_terminals() {
    let cases = [
        (
            "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_conflicting_terminal\",\"object\":\"response\",\"status\":\"incomplete\",\"output\":[]}}\n\n",
            "conflicts with response.status",
        ),
        (
            concat!(
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_multiple_terminal\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_multiple_terminal\",\"object\":\"response\",\"status\":\"failed\",\"output\":[],\"error\":{\"code\":\"server_error\",\"message\":\"late failure\"}}}\n\n",
            ),
            "multiple authoritative terminal events",
        ),
        (
            concat!(
                "event: response.future.delta\n",
                "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"disguised\"}\n\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_disguised_unknown\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n",
            ),
            "SSE event name conflicts with payload.type",
        ),
        (
            concat!(
                "event: response.failed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_disguised_terminal\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n",
            ),
            "SSE event name conflicts with payload.type",
        ),
    ];

    for (stream_body, expected_message) in cases {
        let error = try_aggregate_openai_responses_stream_sync_response(stream_body.as_bytes())
            .expect_err("invalid terminal lifecycle must fail closed");
        assert!(
            error.to_string().contains(expected_message),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn standard_sync_finalize_product_prefers_same_format_success_body() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "openai:chat",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "id": "chatcmpl_123",
        "object": "chat.completion"
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed");

    assert_eq!(
        product,
        Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            provider_body_json
        ))
    );
}

#[test]
fn standard_sync_finalize_uses_explicit_stream_event_format_for_same_format_body() {
    let body = concat!(
        "event: keepalive\n",
        "data: {\"type\":\"keepalive\",\"sequence_number\":1}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_keepalive_sync_123\",\"output_index\":0,\"content_index\":0,\"delta\":\"pong\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_keepalive_sync_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "needs_conversion": false,
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("responses stream event format should aggregate")
    .expect("dispatch should produce a body");

    let StandardSyncFinalizeNormalizedProduct::SuccessBody(body_json) = product else {
        panic!("same-format response should be a success body");
    };
    assert_eq!(body_json["object"], "chat.completion");
    assert_eq!(body_json["choices"][0]["message"]["content"], "pong");
}

#[test]
fn standard_sync_finalize_converts_chat_backed_function_call_metadata() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_chat_metadata_123\",\"call_id\":\"call_chat_metadata_123\",\"status\":\"completed\",\"arguments\":\"{\\\"query\\\":\\\"aether\\\"}\",\"name\":\"lookup\",\"metadata\":{\"source\":\"chat\"},\"internal_chat_message_metadata_passthrough\":{\"turn_id\":\"turn_123\"}}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat_metadata_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "needs_conversion": true,
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("chat-backed function call metadata should convert")
    .expect("dispatch should produce a body");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("Chat-to-Responses conversion should produce a cross-format body");
    };
    let body_json = product.client_body_json;
    let tool_call = &body_json["choices"][0]["message"]["tool_calls"][0];
    assert_eq!(tool_call["id"], "call_chat_metadata_123");
    assert_eq!(tool_call["function"]["name"], "lookup");
    assert_eq!(tool_call["function"]["arguments"], "{\"query\":\"aether\"}");
    assert!(tool_call.get("metadata").is_none());
    assert!(tool_call
        .get("internal_chat_message_metadata_passthrough")
        .is_none());
}

#[test]
fn standard_sync_finalize_preserves_responses_wire_metadata_for_responses_client() {
    let body = concat!(
        "event: response.output_item.added\n",
        "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_chat_metadata_123\",\"call_id\":\"call_chat_metadata_123\",\"status\":\"completed\",\"arguments\":\"{\\\"query\\\":\\\"aether\\\"}\",\"name\":\"lookup\",\"metadata\":{\"source\":\"chat\"},\"internal_chat_message_metadata_passthrough\":{\"turn_id\":\"turn_123\"}}}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_chat_metadata_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "needs_conversion": true,
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("Responses wire metadata should aggregate")
    .expect("dispatch should produce a body");

    let StandardSyncFinalizeNormalizedProduct::SuccessBody(body_json) = product else {
        panic!("matching Responses wire format should produce a success body");
    };
    assert_eq!(body_json["output"][0]["type"], "function_call");
    assert_eq!(body_json["output"][0]["metadata"]["source"], "chat");
    assert_eq!(
        body_json["output"][0]["internal_chat_message_metadata_passthrough"]["turn_id"],
        "turn_123"
    );
}

#[test]
fn standard_same_format_gemini_finalize_rejects_usage_only_body() {
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "gemini:generate_content",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "responseId": "resp-empty-visible-output",
        "modelVersion": "gemini-3-flash-preview",
        "usageMetadata": {
            "promptTokenCount": 8,
            "totalTokenCount": 8
        }
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "gemini_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed");

    assert_eq!(product, None);
}

#[test]
fn standard_cross_format_finalize_uses_explicit_stream_event_format_for_provider_body() {
    let body = concat!(
        "event: keepalive\n",
        "data: {\"type\":\"keepalive\",\"sequence_number\":1}\n\n",
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_keepalive_cross_123\",\"output_index\":0,\"content_index\":0,\"delta\":\"pong\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_keepalive_cross_123\",\"object\":\"response\",\"model\":\"gpt-5\",\"status\":\"completed\",\"output\":[]}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "claude:messages",
        "model": "claude-sonnet-4-5",
        "mapped_model": "gpt-5",
        "needs_conversion": true,
    });
    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("responses stream event format should aggregate")
    .expect("dispatch should produce a product");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("cross-format response should produce a conversion product");
    };
    assert_eq!(product.provider_body_json["object"], "response");
    assert_eq!(product.client_body_json["type"], "message");
    assert_eq!(product.client_body_json["content"][0]["text"], "pong");
}

#[test]
fn standard_sync_finalize_product_handles_openai_responses_same_family_body() {
    let report_context = json!({
        "provider_api_format": "openai:responses:compact",
        "client_api_format": "openai:responses:compact",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "id": "resp_123",
        "object": "response",
        "status": "completed",
        "output": []
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_responses_compact_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed");

    assert_eq!(
        product,
        Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            provider_body_json
        ))
    );
}

#[test]
fn standard_sync_finalize_aggregates_openai_responses_capture_envelope_for_cross_format() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "chunks": [
            {
                "type": "response.output_text.delta",
                "response_id": "resp_capture_123",
                "output_index": 0,
                "content_index": 0,
                "delta": "Hello "
            },
            {
                "type": "response.output_text.done",
                "response_id": "resp_capture_123",
                "output_index": 0,
                "content_index": 0,
                "text": "Hello capture"
            },
            {
                "type": "response.output_item.done",
                "output_index": 0,
                "item": {
                    "id": "msg_capture_123",
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "phase": "final_answer",
                    "content": [{
                        "type": "output_text",
                        "text": "Hello capture",
                        "annotations": [],
                        "logprobs": []
                    }]
                }
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_capture_123",
                    "object": "response",
                    "status": "completed",
                    "model": "gpt-5.6-luna",
                    "background": false,
                    "max_output_tokens": null,
                    "output": [],
                    "usage": {
                        "input_tokens": 2,
                        "output_tokens": 3,
                        "total_tokens": 5
                    }
                }
            }
        ],
        "metadata": {}
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("capture envelope aggregation should succeed")
    .expect("capture envelope should produce a cross-format product");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("responses capture envelope should be converted for an OpenAI Chat client")
    };
    assert_eq!(product.provider_body_json["id"], "resp_capture_123");
    assert_eq!(
        product.provider_body_json["output"][0]["content"][0]["text"],
        "Hello capture"
    );
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "Hello capture"
    );
    assert_eq!(
        product.client_body_json["usage"],
        json!({
            "prompt_tokens": 2,
            "completion_tokens": 3,
            "total_tokens": 5
        })
    );
}

#[test]
fn standard_sync_finalize_projects_validated_responses_stream_metadata_to_chat() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let stream_body = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_stream_metadata_123\",\"output_index\":0,\"content_index\":0,\"delta\":\"stream projection\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"response_id\":\"resp_stream_metadata_123\",\"output_index\":0,\"content_index\":0,\"text\":\"stream projection\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_stream_metadata_123\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"phase\":\"final_answer\",\"content\":[{\"type\":\"output_text\",\"text\":\"stream projection\",\"annotations\":[],\"logprobs\":[]}]}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_stream_metadata_123\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"gpt-5.6-luna\",\"background\":false,\"max_output_tokens\":null,\"output\":[],\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}}\n\n",
    );

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect("validated Responses stream should aggregate")
    .expect("validated Responses stream should produce a Chat projection");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("Responses stream should retain a cross-format provider product")
    };
    assert_eq!(product.provider_body_json["background"], false);
    assert_eq!(
        product.provider_body_json["output"][0]["phase"],
        "final_answer"
    );
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "stream projection"
    );
}

#[test]
fn standard_sync_finalize_projects_forced_responses_stream_to_gemini_and_claude_clients() {
    // Shape of a forced-stream xAI / Codex upstream: the terminal response
    // echoes request metadata and carries encrypted reasoning, which the
    // strict cross-format check refuses.
    let stream_body = concat!(
        "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_forced_123\",\"object\":\"response\",\"status\":\"in_progress\",\"model\":\"grok-4.7-build\",\"output\":[],\"parallel_tool_calls\":true,\"tool_choice\":\"auto\",\"tools\":[],\"temperature\":0.7}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"rs_forced_123\",\"type\":\"reasoning\",\"status\":\"completed\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"greet briefly\"}],\"encrypted_content\":\"opaque-xai-reasoning\"}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"sequence_number\":2,\"item_id\":\"msg_forced_123\",\"output_index\":1,\"content_index\":0,\"delta\":\"Hello there friend\"}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"sequence_number\":3,\"output_index\":1,\"item\":{\"id\":\"msg_forced_123\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Hello there friend\",\"annotations\":[]}]}}\n\n",
        "data: {\"type\":\"response.output_item.done\",\"sequence_number\":4,\"output_index\":2,\"item\":{\"id\":\"fc_forced_123\",\"type\":\"function_call\",\"status\":\"completed\",\"call_id\":\"call_forced_123\",\"name\":\"search\",\"arguments\":\"{\\\"q\\\":\\\"aether\\\"}\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_forced_123\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"grok-4.7-build\",\"output\":[],\"parallel_tool_calls\":true,\"tool_choice\":\"auto\",\"tools\":[{\"type\":\"function\",\"name\":\"search\",\"parameters\":{\"type\":\"object\"}}],\"text\":{\"format\":{\"type\":\"text\"}},\"reasoning\":{\"effort\":null,\"summary\":null},\"temperature\":0.7,\"top_p\":0.95,\"store\":false,\"usage\":{\"input_tokens\":1249,\"input_tokens_details\":{\"cached_tokens\":1152},\"output_tokens\":40,\"output_tokens_details\":{\"reasoning_tokens\":31},\"total_tokens\":1289}}}\n\n",
    );
    let encoded = base64::engine::general_purpose::STANDARD.encode(stream_body);

    for (report_kind, client_api_format) in [
        ("gemini_chat_sync_finalize", "gemini:generate_content"),
        ("gemini_cli_sync_finalize", "gemini:generate_content"),
        ("claude_chat_sync_finalize", "claude:messages"),
        ("claude_cli_sync_finalize", "claude:messages"),
    ] {
        let report_context = json!({
            "provider_api_format": "openai:responses",
            "provider_stream_event_api_format": "openai:responses",
            "client_api_format": client_api_format,
            "model": "grok-4.7",
            "mapped_model": "grok-4.7",
            "needs_conversion": true,
        });
        let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            report_kind,
            200,
            Some(&report_context),
            None,
            Some(&encoded),
        )
        .expect("forced Responses stream should aggregate")
        .unwrap_or_else(|| panic!("{report_kind} should receive a projection"));
        let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
            panic!("{report_kind}: Responses stream should stay a cross-format product")
        };
        assert_eq!(product.provider_body_json["parallel_tool_calls"], true);
        let client = product.client_body_json.to_string();
        assert!(
            !client.contains("opaque-xai-reasoning") && !client.contains("response.created"),
            "{report_kind}: provider-only data leaked into the client body: {client}"
        );
        if client_api_format == "gemini:generate_content" {
            let parts = product.client_body_json["candidates"][0]["content"]["parts"]
                .as_array()
                .expect("gemini parts");
            assert!(parts
                .iter()
                .any(|part| part["text"] == "Hello there friend" && part.get("thought").is_none()));
            assert!(parts
                .iter()
                .any(|part| part["functionCall"]["name"] == "search"
                    && part["functionCall"]["args"]["q"] == "aether"));
            assert_eq!(
                product.client_body_json["usageMetadata"]["promptTokenCount"],
                1249
            );
        } else {
            let content = product.client_body_json["content"]
                .as_array()
                .expect("claude content");
            assert!(content
                .iter()
                .any(|block| block["type"] == "text" && block["text"] == "Hello there friend"));
            assert!(content.iter().any(|block| block["type"] == "tool_use"
                && block["name"] == "search"
                && block["input"]["q"] == "aether"));
            assert_eq!(product.client_body_json["stop_reason"], "tool_use");
        }
    }
}

#[test]
fn standard_sync_finalize_projects_authoritative_incomplete_responses_stream() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let stream_body = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_incomplete_stream_123\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial output\"}\n\n",
        "data: {\"type\":\"response.incomplete\",\"response\":{\"id\":\"resp_incomplete_stream_123\",\"object\":\"response\",\"status\":\"incomplete\",\"model\":\"gpt-5.6-luna\",\"background\":false,\"output\":[],\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n",
    );

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect("authoritative incomplete stream should aggregate")
    .expect("authoritative incomplete stream should produce a Chat projection");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("incomplete Responses stream should retain a cross-format provider product")
    };
    assert_eq!(product.provider_body_json["status"], "incomplete");
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "partial output"
    );
}

#[test]
fn standard_sync_finalize_rejects_responses_stream_without_authoritative_terminal() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let stream_body = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_truncated_stream_123\",\"output_index\":0,\"content_index\":0,\"delta\":\"truncated\"}\n\n",
        "data: {\"type\":\"response.output_text.done\",\"response_id\":\"resp_truncated_stream_123\",\"output_index\":0,\"content_index\":0,\"text\":\"truncated\"}\n\n",
    );

    assert!(aggregate_openai_responses_stream_sync_response(stream_body.as_bytes()).is_none());
    let error = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect_err("a truncated stream must fail closed");
    assert!(
        error
            .to_string()
            .contains("missing an authoritative terminal event"),
        "unexpected error: {error}"
    );
}

#[test]
fn standard_sync_finalize_rejects_unknown_canonical_output_from_known_stream_event() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let stream_body = concat!(
        "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"future_item_123\",\"type\":\"future_output\",\"payload\":\"must-not-drop\"}}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_unknown_output_123\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"gpt-5.6-luna\",\"background\":false,\"output\":[]}}\n\n",
    );

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect("known event with unknown output should be inspected");
    assert!(
        product.is_none(),
        "an unknown canonical output block must not be dropped by the Chat projection"
    );
}

#[test]
fn standard_sync_finalize_keeps_direct_responses_metadata_conversion_strict() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "id": "resp_direct_metadata_123",
        "object": "response",
        "status": "completed",
        "model": "gpt-5.6-luna",
        "background": false,
        "output": [{
            "id": "msg_direct_metadata_123",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "direct body",
                "annotations": []
            }]
        }]
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("direct response inspection should not fail");

    assert!(
        product.is_none(),
        "direct provider JSON must keep the strict lossless conversion boundary"
    );
}

#[test]
fn standard_sync_finalize_never_projects_failed_responses_stream_as_chat_success() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "provider_stream_event_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "gpt-5.6-luna",
        "mapped_model": "gpt-5.6-luna",
        "needs_conversion": true,
    });
    let stream_body = "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed_stream_123\",\"object\":\"response\",\"status\":\"failed\",\"model\":\"gpt-5.6-luna\",\"error\":null,\"output\":[{\"type\":\"message\",\"id\":\"msg_failed_stream_123\",\"role\":\"assistant\",\"status\":\"incomplete\",\"content\":[{\"type\":\"output_text\",\"text\":\"partial failure\",\"annotations\":[]}]}]}}\n\n";

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect("failed stream should remain available to the error boundary");

    assert!(
        product.is_none(),
        "a failed Responses stream must not become a Chat success body"
    );
}

#[test]
fn standard_sync_finalize_aggregates_openai_responses_capture_envelope_for_same_family() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:responses",
        "model": "client-model",
        "mapped_model": "provider-model",
        "needs_conversion": false,
    });
    let provider_body_json = json!({
        "chunks": [
            {
                "type": "response.output_text.delta",
                "response_id": "resp_same_family_capture_123",
                "output_index": 0,
                "content_index": 0,
                "delta": "same family"
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_same_family_capture_123",
                    "object": "response",
                    "status": "completed",
                    "model": "provider-model",
                    "output": []
                }
            }
        ],
        "metadata": {}
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("capture envelope aggregation should succeed")
    .expect("capture envelope should produce a same-family body");

    let StandardSyncFinalizeNormalizedProduct::SuccessBody(body_json) = product else {
        panic!("same-family responses capture should remain a success body")
    };
    assert_eq!(body_json["id"], "resp_same_family_capture_123");
    assert_eq!(body_json["output"][0]["content"][0]["text"], "same family");
    assert!(body_json.get("chunks").is_none());
}

#[test]
fn standard_sync_finalize_rejects_unknown_capture_envelope_events() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "client-model",
        "mapped_model": "provider-model",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "chunks": [{
            "type": "response.future.delta",
            "response": {
                "id": "resp_unknown_capture_123",
                "object": "response",
                "status": "in_progress",
                "model": "provider-model"
            },
            "payload": {"kept": true}
        }],
        "metadata": {}
    });

    let error = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect_err("unknown capture event must fail closed instead of returning raw chunks");

    assert!(error
        .to_string()
        .contains("Unsupported provider stream event cannot be converted losslessly"));
    assert!(error
        .to_string()
        .contains("field $.type = \"response.future.delta\""));
}

#[test]
fn standard_sync_finalize_leaves_capture_error_envelope_to_error_boundary() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "client-model",
        "mapped_model": "provider-model",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "chunks": [{
            "type": "error",
            "error": {
                "type": "server_error",
                "message": "provider failed"
            }
        }],
        "metadata": {}
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("known error envelope should remain available to the gateway error boundary");

    assert_eq!(product, None);
}

#[test]
fn standard_sync_finalize_rejects_malformed_or_incomplete_capture_envelopes() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "client-model",
        "mapped_model": "provider-model",
        "needs_conversion": true,
    });
    let cases = [
        (json!({"chunks": [], "metadata": {}}), "contains no events"),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "metadata": {
                    "stored_chunks": 1,
                    "total_chunks": 2,
                    "dropped_chunks": 1
                }
            }),
            "is incomplete and cannot be finalized losslessly",
        ),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "metadata": {"total_chunks": 1}
            }),
            "is incomplete and cannot be finalized losslessly",
        ),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "metadata": {"dropped_chunks": "1"}
            }),
            "metadata.dropped_chunks must be an unsigned integer",
        ),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "metadata": false
            }),
            "metadata must be an object",
        ),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "metadata": {"stream": false}
            }),
            "metadata.stream must be true",
        ),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "metadata": {"parse_error": "invalid SSE frame"}
            }),
            "reports a stream parse error",
        ),
        (
            json!({
                "chunks": [{"type": "response.completed", "response": {}}],
                "raw_response": "unparsed",
                "metadata": {}
            }),
            "contains an unparsed raw response",
        ),
        (
            json!({"chunks": ["not-an-event"], "metadata": {}}),
            "contains a non-object event",
        ),
    ];

    for (provider_body_json, expected_message) in cases {
        let error = maybe_build_standard_sync_finalize_product_from_normalized_payload(
            "openai_chat_sync_finalize",
            200,
            Some(&report_context),
            Some(&provider_body_json),
            None,
        )
        .expect_err("malformed capture envelope must fail closed");

        assert!(
            error.to_string().contains(expected_message),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn standard_sync_finalize_rejects_capture_envelope_when_target_format_is_unsupported() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "future:chat",
        "model": "client-model",
        "mapped_model": "provider-model",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "chunks": [{
            "type": "response.completed",
            "response": {
                "id": "resp_unsupported_target_123",
                "object": "response",
                "status": "completed",
                "model": "provider-model",
                "output": [{
                    "type": "message",
                    "id": "msg_unsupported_target_123",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "must not leak as raw chunks",
                        "annotations": []
                    }]
                }]
            }
        }],
        "metadata": {}
    });

    let error = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_cli_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect_err("unsupported target must not fall back to the raw capture envelope");

    assert!(error
        .to_string()
        .contains("capture envelope cannot be finalized losslessly"));
}

#[test]
fn standard_sync_finalize_prefers_explicit_stream_body_over_capture_envelope() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "openai:chat",
        "model": "client-model",
        "mapped_model": "provider-model",
        "needs_conversion": true,
    });
    let capture_envelope = json!({
        "chunks": [{"type": "response.future.delta"}],
        "metadata": {}
    });
    let stream_body = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_explicit_stream_123\",\"output_index\":0,\"content_index\":0,\"delta\":\"explicit stream\"}\n\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_explicit_stream_123\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"provider-model\",\"output\":[]}}\n\n",
    );

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&capture_envelope),
        Some(&base64::engine::general_purpose::STANDARD.encode(stream_body)),
    )
    .expect("explicit stream body should take precedence")
    .expect("explicit stream body should produce a product");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("explicit Responses stream should convert to OpenAI Chat")
    };
    assert_eq!(
        product.client_body_json["choices"][0]["message"]["content"],
        "explicit stream"
    );
}

#[test]
fn standard_sync_finalize_product_handles_openai_responses_same_family_cross_format_body() {
    let report_context = json!({
        "provider_api_format": "openai:responses:compact",
        "client_api_format": "openai:responses",
        "needs_conversion": true,
    });
    let provider_body_json = json!({
        "id": "resp_family_123",
        "object": "response",
        "status": "completed",
        "output": []
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed");

    assert_eq!(
        product,
        Some(StandardSyncFinalizeNormalizedProduct::SuccessBody(
            provider_body_json
        ))
    );
}

#[test]
fn standard_sync_finalize_product_handles_openai_chat_cross_format() {
    let report_context = json!({
        "provider_api_format": "claude:messages",
        "client_api_format": "openai:chat",
        "model": "gpt-5",
        "mapped_model": "claude-sonnet-4",
    });
    let provider_body_json = json!({
        "id": "msg_claude_direct_123",
        "type": "message",
        "model": "claude-sonnet-4",
        "role": "assistant",
        "content": [{"type": "text", "text": "Hello Claude"}],
        "stop_reason": "end_turn"
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed")
    .expect("dispatch should produce a product");

    assert!(matches!(
        product,
        StandardSyncFinalizeNormalizedProduct::CrossFormat(_)
    ));
}

#[test]
fn standard_sync_finalize_product_handles_openai_responses_cross_format() {
    let body = concat!(
        "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}],\"role\":\"model\"},\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\"}\n\n",
        "data: {\"responseId\":\"resp_cli_stream_123\",\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello Gemini CLI\"}],\"role\":\"model\"},\"finishReason\":\"STOP\",\"index\":0}],\"modelVersion\":\"gemini-2.5-pro-upstream\",\"usageMetadata\":{\"promptTokenCount\":2,\"candidatesTokenCount\":3,\"totalTokenCount\":5}}\n\n",
    );
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:responses",
        "model": "gpt-5",
        "mapped_model": "gemini-2.5-pro-upstream",
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "openai_responses_sync_finalize",
        200,
        Some(&report_context),
        None,
        Some(&base64::engine::general_purpose::STANDARD.encode(body)),
    )
    .expect("dispatch should succeed")
    .expect("dispatch should produce a product");

    assert!(matches!(
        product,
        StandardSyncFinalizeNormalizedProduct::CrossFormat(_)
    ));
}

#[test]
fn standard_sync_finalize_product_converts_openai_responses_null_error_to_claude_cli() {
    let report_context = json!({
        "provider_api_format": "openai:responses",
        "client_api_format": "claude:messages",
        "model": "claude-sonnet-4-5",
        "mapped_model": "gpt-5",
    });
    let provider_body_json = json!({
        "id": "resp_completed_cli_123",
        "object": "response",
        "model": "gpt-5",
        "status": "completed",
        "error": null,
        "output": [{
            "type": "message",
            "id": "msg_completed_cli_123",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "Done", "annotations": []}]
        }]
    });
    assert_eq!(
        sync_cli_response_conversion_kind("openai:responses", "claude:messages"),
        Some(SyncCliResponseConversionKind::ToClaudeCli)
    );
    assert!(convert_standard_cli_response(
        &provider_body_json,
        "openai:responses",
        "claude:messages",
        &report_context
    )
    .is_some());

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "claude_cli_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed")
    .expect("dispatch should produce a product");

    let StandardSyncFinalizeNormalizedProduct::CrossFormat(product) = product else {
        panic!("openai responses provider body should be converted for claude client")
    };
    assert_eq!(product.client_body_json["type"], "message");
    assert_eq!(product.client_body_json["content"][0]["text"], "Done");
    assert_eq!(product.client_body_json["stop_reason"], "end_turn");
}

#[test]
fn standard_sync_finalize_product_falls_back_to_generic_standard_cross_format() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
    });
    let provider_body_json = json!({
        "id": "chatcmpl_123",
        "object": "chat.completion",
        "created": 1,
        "model": "gpt-5",
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": "Hello",
            },
            "finish_reason": "stop",
        }],
    });

    let product = maybe_build_standard_sync_finalize_product_from_normalized_payload(
        "claude_chat_sync_finalize",
        200,
        Some(&report_context),
        Some(&provider_body_json),
        None,
    )
    .expect("dispatch should succeed")
    .expect("dispatch should produce a product");

    assert!(matches!(
        product,
        StandardSyncFinalizeNormalizedProduct::CrossFormat(_)
    ));
}

#[test]
fn multi_choice_openai_chat_response_keeps_legacy_fallback_route() {
    let report_context = json!({
        "provider_api_format": "openai:chat",
        "client_api_format": "claude:messages",
        "model": "gpt-5",
        "mapped_model": "gpt-5-upstream",
    });
    let provider_body_json = json!({
        "id": "chatcmpl_multi_123",
        "object": "chat.completion",
        "model": "gpt-5-upstream",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "first"},
                "finish_reason": "stop"
            },
            {
                "index": 1,
                "message": {"role": "assistant", "content": "second"},
                "finish_reason": "stop"
            }
        ],
        "usage": {
            "prompt_tokens": 1,
            "completion_tokens": 2,
            "total_tokens": 3
        }
    });

    let legacy = convert_openai_chat_response_to_claude_chat(&provider_body_json, &report_context)
        .expect("legacy multi choice route");
    let converted = convert_standard_chat_response(
        &provider_body_json,
        "openai:chat",
        "claude:messages",
        &report_context,
    )
    .expect("fallback route");
    assert_eq!(converted, legacy);
}

#[test]
fn multi_candidate_gemini_response_keeps_legacy_fallback_route() {
    let report_context = json!({
        "provider_api_format": "gemini:generate_content",
        "client_api_format": "openai:chat",
        "model": "gemini-2.5-pro",
        "mapped_model": "gemini-upstream",
    });
    let provider_body_json = json!({
        "responseId": "resp_multi_123",
        "modelVersion": "gemini-upstream",
        "candidates": [
            {
                "index": 0,
                "finishReason": "STOP",
                "content": {"role": "model", "parts": [{"text": "first"}]}
            },
            {
                "index": 1,
                "finishReason": "STOP",
                "content": {"role": "model", "parts": [{"text": "second"}]}
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 1,
            "candidatesTokenCount": 2,
            "totalTokenCount": 3
        }
    });

    let legacy = convert_gemini_chat_response_to_openai_chat(&provider_body_json, &report_context)
        .expect("legacy multi candidate route");
    let converted = convert_standard_chat_response(
        &provider_body_json,
        "gemini:generate_content",
        "openai:chat",
        &report_context,
    )
    .expect("fallback route");
    assert_eq!(converted, legacy);
}
