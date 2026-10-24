use std::collections::BTreeMap;

use aether_contracts::{
    ExecutionErrorKind, ExecutionPhase, ExecutionPlan, RequestBody, StreamFrame, StreamFramePayload,
};
use axum::body::{Body, Bytes};
use axum::extract::Request;
use axum::routing::any;
use axum::Router;
use base64::Engine as _;
use futures_util::{stream, StreamExt};
use http::{Method, StatusCode};

use super::{
    chat_text_with_images, encode_grok_error_frame, encode_grok_first_byte_timeout_frame,
    extract_grok_attachment_inputs, grok_aspect_ratio_from_provider_body, grok_asset_url,
    grok_attachment_ip_is_public, grok_attachment_payload_from_data_uri,
    grok_attachment_payload_from_data_uri_with_limit, grok_auxiliary_http_error,
    grok_client_json_body, grok_client_stream_body, grok_data_image_parts_with_limit,
    grok_execution_result, grok_handle_imagine_ws_message,
    grok_handle_imagine_ws_message_with_slot_limit, grok_image_count_from_provider_body,
    grok_image_mime_for_payload, grok_image_prompt_from_provider_body,
    grok_imagine_blob_lengths_can_be_retained, grok_imagine_request_message,
    grok_imagine_reset_message, grok_media_post_url, grok_plan_uses_structured_image_generation,
    grok_should_collect_image_stream, grok_should_use_imagine_websocket, grok_success_frame_stream,
    grok_upload_url, grok_upstream_model_name, grok_usage_estimate,
    grok_user_id_from_cookie_header, materialize_grok_image_assets,
    maximum_base64_len_for_decoded_limit, openai_chat_body, openai_image_body, openai_image_sse,
    openai_responses_body, public_socket_addrs_for_url, set_grok_image_edit_config,
    validate_grok_attachment_url, GrokAttachmentInput, GrokCollected, GrokImagineImage,
    GrokStreamAdapter,
};

fn sample_plan(body: serde_json::Value, client_api_format: &str) -> ExecutionPlan {
    ExecutionPlan {
        request_id: "req-1".to_string(),
        candidate_id: Some("cand-1".to_string()),
        provider_name: Some("Grok".to_string()),
        provider_id: "provider-1".to_string(),
        endpoint_id: "endpoint-1".to_string(),
        key_id: "key-1".to_string(),
        method: "POST".to_string(),
        url: "https://grok.com/rest/app-chat/conversations/new".to_string(),
        headers: BTreeMap::new(),
        content_type: Some("application/json".to_string()),
        content_encoding: None,
        body: RequestBody::from_json(body),
        stream: true,
        client_api_format: client_api_format.to_string(),
        provider_api_format: "openai:chat".to_string(),
        model_name: Some("grok-test".to_string()),
        proxy: None,
        transport_profile: None,
        timeouts: None,
    }
}

fn report_context_with_mapped_model(mapped_model: &str) -> serde_json::Value {
    serde_json::json!({
        "mapped_model": mapped_model,
        "provider_type": "grok",
    })
}

fn grok_token_chunk(token: &str) -> Bytes {
    Bytes::from(format!(
        "data: {}\n\n",
        serde_json::json!({
            "result": {
                "response": {
                    "token": token,
                    "messageTag": "final"
                }
            }
        })
    ))
}

async fn collect_decoded_data_frames(
    mut frame_stream: futures_util::stream::BoxStream<'static, Result<Bytes, std::io::Error>>,
) -> Vec<String> {
    let mut out = Vec::new();
    while let Some(item) = frame_stream.next().await {
        let bytes = item.expect("frame should encode");
        let line = String::from_utf8(bytes.to_vec()).expect("frame should be utf8");
        let frame: StreamFrame =
            serde_json::from_str(line.trim()).expect("frame should deserialize");
        if let StreamFramePayload::Data { chunk_b64, text } = frame.payload {
            let chunk = if let Some(chunk_b64) = chunk_b64 {
                base64::engine::general_purpose::STANDARD
                    .decode(chunk_b64)
                    .expect("chunk should decode")
            } else {
                text.unwrap_or_default().into_bytes()
            };
            out.push(String::from_utf8(chunk).expect("chunk should be utf8"));
        }
    }
    out
}

fn decode_encoded_frame(encoded: Bytes) -> StreamFrame {
    let line = String::from_utf8(encoded.to_vec()).expect("frame should be utf8");
    serde_json::from_str(line.trim()).expect("frame should deserialize")
}

#[test]
fn grok_stream_read_error_is_retryable_transport_without_upstream_status() {
    let frame = decode_encoded_frame(
        encode_grok_error_frame("connection reset while reading response body".to_string())
            .expect("error frame should encode"),
    );
    let StreamFramePayload::Error { error } = frame.payload else {
        panic!("encoded frame should contain an execution error");
    };

    assert_eq!(error.kind, ExecutionErrorKind::ProtocolError);
    assert_eq!(error.phase, ExecutionPhase::StreamRead);
    assert_eq!(error.upstream_status, None);
    assert!(error.retryable);
    assert!(error.failover_recommended);
}

#[test]
fn grok_first_byte_timeout_is_retryable_transport_without_upstream_status() {
    let frame = decode_encoded_frame(
        encode_grok_first_byte_timeout_frame(std::time::Duration::from_millis(250))
            .expect("timeout frame should encode"),
    );
    let StreamFramePayload::Error { error } = frame.payload else {
        panic!("encoded frame should contain an execution error");
    };

    assert_eq!(error.kind, ExecutionErrorKind::FirstByteTimeout);
    assert_eq!(error.phase, ExecutionPhase::FirstByte);
    assert_eq!(error.upstream_status, None);
    assert!(error.retryable);
    assert!(error.failover_recommended);
}

#[tokio::test]
async fn grok_success_stream_forwards_token_chunks_incrementally() {
    let plan = sample_plan(
        serde_json::json!({
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true,
        }),
        "openai:chat",
    );
    let upstream = stream::iter(vec![
        Ok(grok_token_chunk("hel")),
        Ok(grok_token_chunk("lo")),
    ])
    .boxed();

    let chunks = collect_decoded_data_frames(grok_success_frame_stream(
        plan,
        200,
        std::time::Instant::now(),
        upstream,
    ))
    .await;

    assert!(
        chunks
            .iter()
            .any(|chunk| chunk.contains("\"content\":\"hel\"")),
        "first upstream token should be emitted as its own client chunk: {chunks:?}"
    );
    assert!(
        chunks
            .iter()
            .any(|chunk| chunk.contains("\"content\":\"lo\"")),
        "second upstream token should be emitted as its own client chunk: {chunks:?}"
    );
    let joined = chunks.join("");
    let first = joined.find("\"content\":\"hel\"").expect("hel chunk");
    let second = joined.find("\"content\":\"lo\"").expect("lo chunk");
    let done = joined.find("data: [DONE]").expect("done chunk");
    assert!(first < second && second < done);
}

#[test]
fn grok_upstream_model_name_prefers_report_context_mapping() {
    let mut plan = sample_plan(
        serde_json::json!({"messages": [{"role": "user", "content": "hello"}]}),
        "openai:chat",
    );
    plan.model_name = Some("grok-4.20-0309-reasoning".to_string());

    let mapped =
        grok_upstream_model_name(Some(&report_context_with_mapped_model("grok-4.20-fast")))
            .expect("mapped model should resolve");

    assert_eq!(mapped, "grok-4.20-fast");
}

#[test]
fn grok_upstream_model_name_requires_report_context_mapping() {
    let err = grok_upstream_model_name(None).expect_err("missing mapped model should fail");

    assert!(err
        .to_string()
        .contains("Grok runtime requires mapped_model"));
}

#[test]
fn adapter_extracts_text_and_image_url() {
    let image_json = serde_json::json!({
        "image_chunk": {
            "progress": 100,
            "imageUrl": "generated/example.png"
        }
    })
    .to_string();
    let line = format!(
        "data: {}\n",
        serde_json::json!({
            "result": {
                "response": {
                    "token": "hello",
                    "messageTag": "final",
                    "cardAttachment": {"jsonData": image_json}
                }
            }
        })
    );
    let mut adapter = GrokStreamAdapter::default();
    adapter.push_chunk(line.as_bytes());

    assert_eq!(adapter.text, "hello");
    assert_eq!(
        adapter.images,
        vec!["https://assets.grok.com/generated/example.png"]
    );
}

#[test]
fn grok_response_chunk_limit_rejects_wire_overflow_without_growing_buffers() {
    let mut upstream_bytes = 4;
    let mut raw_body = b"1234".to_vec();
    let mut adapter = GrokStreamAdapter::default();

    let error = super::collect_grok_response_chunk_with_limit(
        502,
        &mut upstream_bytes,
        &mut raw_body,
        &mut adapter,
        b"56",
        5,
    )
    .expect_err("wire body above the plan limit must fail");

    assert_eq!(upstream_bytes, 4);
    assert_eq!(raw_body, b"1234");
    assert!(matches!(
        error,
        super::ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: super::UpstreamResponseBodyPhase::Wire,
            limit_bytes: 5,
        }
    ));
}

#[test]
fn grok_success_response_does_not_duplicate_raw_body_storage() {
    let mut upstream_bytes = 0;
    let mut raw_body = Vec::new();
    let mut adapter = GrokStreamAdapter::default();
    let line = b"data: {\"result\":{\"response\":{\"token\":\"ok\",\"messageTag\":\"final\"}}}\n";

    super::collect_grok_response_chunk_with_limit(
        200,
        &mut upstream_bytes,
        &mut raw_body,
        &mut adapter,
        line,
        line.len(),
    )
    .expect("body exactly at the limit should pass");

    assert_eq!(upstream_bytes, line.len() as u64);
    assert!(raw_body.is_empty());
    assert_eq!(adapter.text, "ok");
}

#[test]
fn adapter_extracts_grok_image_edit_streaming_response() {
    let line = format!(
        "data: {}\n",
        serde_json::json!({
            "result": {
                "response": {
                    "streamingImageGenerationResponse": {
                        "progress": 100,
                        "imageUrl": "generated/edit.png"
                    }
                }
            }
        })
    );
    let mut adapter = GrokStreamAdapter::default();
    adapter.push_chunk(line.as_bytes());

    assert_eq!(
        adapter.images,
        vec!["https://assets.grok.com/generated/edit.png"]
    );
}

#[test]
fn adapter_extracts_grok_image_edit_model_response_fallbacks() {
    let line = format!(
        "data: {}\n",
        serde_json::json!({
            "result": {
                "response": {
                    "modelResponse": {
                        "generatedImageUrls": ["/generated/a.png"],
                        "fileAttachments": ["asset-123"]
                    }
                }
            }
        })
    );
    let mut adapter = GrokStreamAdapter::default();
    adapter.push_chunk(line.as_bytes());

    assert_eq!(
        adapter.images,
        vec![
            "https://assets.grok.com/generated/a.png",
            "https://assets.grok.com/asset-123/content"
        ]
    );
}

#[test]
fn adapter_bounds_collected_image_urls() {
    let mut adapter = GrokStreamAdapter::default();
    for index in 0..(super::GROK_MAX_IMAGE_COUNT + 3) {
        adapter.push_image_url(format!("https://assets.grok.com/image-{index}.png"));
    }

    assert_eq!(adapter.images.len(), super::GROK_MAX_IMAGE_COUNT);
}

#[test]
fn grok_image_edit_config_sets_references_and_parent_post() {
    let mut body = serde_json::json!({
        "responseMetadata": {
            "modelConfigOverride": {
                "modelMap": {
                    "imageEditModelConfig": {
                        "imageReferences": [],
                        "parentPostId": ""
                    }
                }
            }
        }
    });

    set_grok_image_edit_config(
        &mut body,
        vec![serde_json::json!("https://assets.grok.com/ref.png")],
        "post-1".to_string(),
    )
    .expect("config should update");

    assert_eq!(
        body["responseMetadata"]["modelConfigOverride"]["modelMap"]["imageEditModelConfig"]
            ["imageReferences"][0],
        serde_json::json!("https://assets.grok.com/ref.png")
    );
    assert_eq!(
        body["responseMetadata"]["modelConfigOverride"]["modelMap"]["imageEditModelConfig"]
            ["parentPostId"],
        serde_json::json!("post-1")
    );
}

#[test]
fn grok_image_edit_helpers_resolve_urls_and_cookie_user_id() {
    assert_eq!(
        grok_upload_url("https://grok.com/rest/app-chat/conversations/new"),
        "https://grok.com/rest/app-chat/upload-file"
    );
    assert_eq!(
        grok_media_post_url("https://grok.com/rest/app-chat/conversations/new"),
        "https://grok.com/rest/media/post/create"
    );
    assert_eq!(
        grok_asset_url("/users/u/file/content"),
        "https://assets.grok.com/users/u/file/content"
    );

    let headers = BTreeMap::from([(
        "cookie".to_string(),
        "sso=abc; x-userid=user-1; cf_clearance=ok".to_string(),
    )]);
    assert_eq!(
        grok_user_id_from_cookie_header(&headers),
        Some("user-1".to_string())
    );
}

#[test]
fn grok_imagine_request_message_matches_websocket_protocol() {
    let reset = grok_imagine_reset_message();
    assert_eq!(reset["type"], serde_json::json!("conversation.item.create"));
    assert_eq!(
        reset["item"]["content"][0]["type"],
        serde_json::json!("reset")
    );

    let request = grok_imagine_request_message("a red chair", "16:9", true);
    assert_eq!(
        request["item"]["content"][0]["requestId"]
            .as_str()
            .is_some(),
        true
    );
    assert_eq!(
        request["item"]["content"][0]["text"],
        serde_json::json!("a red chair")
    );
    assert_eq!(
        request["item"]["content"][0]["properties"]["aspect_ratio"],
        serde_json::json!("16:9")
    );
    assert_eq!(
        request["item"]["content"][0]["properties"]["enable_pro"],
        serde_json::json!(true)
    );
}

#[test]
fn grok_auxiliary_errors_omit_response_and_websocket_error_bodies() {
    let upstream_body = "Bearer secret-grok-error-body";
    let upload_message = grok_auxiliary_http_error("attachment upload", 502).to_string();

    assert!(upload_message.contains("Grok attachment upload returned HTTP 502"));
    assert!(!upload_message.contains(upstream_body));

    let mut slots = BTreeMap::new();
    let error = grok_handle_imagine_ws_message(
        &serde_json::json!({
            "type": "error",
            "err_msg": upstream_body,
        }),
        &mut slots,
    )
    .expect_err("websocket error frame should fail");
    let message = error.to_string();

    assert!(message.contains("Grok Imagine websocket returned an error"));
    assert!(!message.contains(upstream_body));
}

#[test]
fn grok_http_errors_do_not_copy_upstream_response_bodies() {
    let secret = "authorization=Bearer secret-grok-error-body";
    let plan = sample_plan(serde_json::json!({"message": "test"}), "openai:chat");
    let collected = GrokCollected {
        status_code: 502,
        text: secret.to_string(),
        ..GrokCollected::default()
    };

    let stream_body = grok_client_stream_body(&plan, &collected, None);
    let result = grok_execution_result(&plan, collected, None);
    let sync_body = result
        .body
        .and_then(|body| body.json_body)
        .expect("sync error body should exist")
        .to_string();

    assert!(stream_body.contains("Grok upstream request returned HTTP 502"));
    assert!(sync_body.contains("Grok upstream request returned HTTP 502"));
    assert!(!stream_body.contains("secret-grok-error-body"));
    assert!(!sync_body.contains("secret-grok-error-body"));
}

#[test]
fn grok_imagine_ws_parser_collects_completed_image() {
    let mut slots = BTreeMap::<String, GrokImagineImage>::new();
    grok_handle_imagine_ws_message(
        &serde_json::json!({
            "type": "json",
            "current_status": "start_stage",
            "image_id": "abc",
            "order": 1
        }),
        &mut slots,
    )
    .expect("start stage should parse");
    grok_handle_imagine_ws_message(
        &serde_json::json!({
            "type": "image",
            "url": "/images/abc.png",
            "blob": "aW1hZ2U="
        }),
        &mut slots,
    )
    .expect("image frame should parse");
    grok_handle_imagine_ws_message(
        &serde_json::json!({
            "type": "json",
            "current_status": "completed",
            "image_id": "abc",
            "order": 1,
            "moderated": false
        }),
        &mut slots,
    )
    .expect("completed frame should parse");

    let image = slots.get("abc").expect("slot should exist");
    assert_eq!(
        image.url.as_deref(),
        Some("https://assets.grok.com/images/abc.png")
    );
    assert_eq!(image.blob_b64.as_deref(), Some("aW1hZ2U="));
    assert!(image.done);
    assert!(!image.moderated);
}

#[test]
fn grok_imagine_helpers_extract_prompt_count_and_route() {
    let body = serde_json::json!({
        "input": [{
            "role": "user",
            "content": [{"type": "input_text", "text": "a chair"}]
        }],
        "tools": [{"size": "1280x720"}],
        "n": 3
    });
    assert_eq!(
        grok_image_prompt_from_provider_body(&body),
        Some("a chair".to_string())
    );
    assert_eq!(grok_image_count_from_provider_body(&body), 3);
    assert_eq!(
        grok_image_count_from_provider_body(&serde_json::json!({"n": 0})),
        1
    );
    assert_eq!(
        grok_image_count_from_provider_body(&serde_json::json!({"n": u64::MAX})),
        4
    );
    assert_eq!(grok_aspect_ratio_from_provider_body(&body), "16:9");

    let plan = sample_plan(body, "openai:image");
    assert!(grok_should_use_imagine_websocket(
        &plan,
        Some(&report_context_with_mapped_model("grok-imagine-image-pro"))
    )
    .expect("route should resolve"));
    assert!(!grok_should_use_imagine_websocket(
        &plan,
        Some(&report_context_with_mapped_model("grok-imagine-image-lite"))
    )
    .expect("route should resolve"));
}

#[test]
fn grok_imagine_ws_parser_bounds_transient_image_slots() {
    let mut slots = BTreeMap::<String, GrokImagineImage>::new();
    for index in 0..8 {
        grok_handle_imagine_ws_message_with_slot_limit(
            &serde_json::json!({
                "type": "json",
                "current_status": "start_stage",
                "image_id": format!("image-{index}"),
                "order": index,
            }),
            &mut slots,
            4,
        )
        .expect("progress frame should parse");
    }

    assert_eq!(slots.len(), 4);
}

#[test]
fn grok_imagine_ws_parser_bounds_aggregate_blob_storage() {
    let total_limit =
        maximum_base64_len_for_decoded_limit(super::GROK_MAX_IMAGINE_BLOB_TOTAL_DECODED_BYTES);
    let existing_len = total_limit.saturating_sub(8);
    assert!(!grok_imagine_blob_lengths_can_be_retained(
        existing_len,
        0,
        16,
    ));
    assert!(grok_imagine_blob_lengths_can_be_retained(
        existing_len,
        existing_len,
        maximum_base64_len_for_decoded_limit(super::GROK_MAX_ATTACHMENT_BYTES),
    ));
    assert!(!grok_imagine_blob_lengths_can_be_retained(
        0,
        0,
        maximum_base64_len_for_decoded_limit(super::GROK_MAX_ATTACHMENT_BYTES).saturating_add(1),
    ));
}

#[test]
fn grok_attachment_public_ip_guard_rejects_private_ranges() {
    for ip in [
        "127.0.0.1",
        "10.0.0.1",
        "172.16.0.1",
        "192.168.1.1",
        "169.254.169.254",
        "::1",
        "fc00::1",
        "fe80::1",
        "100.64.0.1",
        "198.18.0.1",
        "224.0.0.1",
        "64:ff9b::10.0.0.1",
        "2002:0a00:0001::1",
        "2001:0000:4136:e378:8000:63bf:3fff:fdd2",
    ] {
        assert!(
            !grok_attachment_ip_is_public(ip.parse().expect("ip should parse")),
            "{ip} should be rejected"
        );
    }
    assert!(grok_attachment_ip_is_public(
        "8.8.8.8".parse().expect("ip should parse")
    ));
    assert!(grok_attachment_ip_is_public(
        "2606:4700:4700::1111".parse().expect("ip should parse")
    ));
}

#[tokio::test]
async fn grok_attachment_public_target_rejects_bracketed_private_ipv6_literals() {
    for raw_url in [
        "http://[::1]/attachment",
        "http://[fc00::1]/attachment",
        "http://[fe80::1]/attachment",
    ] {
        let url = reqwest::Url::parse(raw_url).expect("URL should parse");
        assert!(
            public_socket_addrs_for_url(&url).await.is_err(),
            "private IPv6 literal should be rejected: {raw_url}"
        );
    }

    let url =
        reqwest::Url::parse("https://[2606:4700:4700::1111]/attachment").expect("URL should parse");
    assert_eq!(
        public_socket_addrs_for_url(&url)
            .await
            .expect("public IPv6 literal should pass"),
        vec!["[2606:4700:4700::1111]:443".parse().unwrap()]
    );
}

#[test]
fn grok_attachment_dns_keeps_all_safe_addresses_for_connection_fallback() {
    let addresses = vec![
        "[2606:4700:4700::1111]:443".parse().unwrap(),
        "8.8.8.8:443".parse().unwrap(),
    ];
    assert_eq!(
        super::validate_grok_attachment_addresses(addresses.clone()).unwrap(),
        addresses
    );
    assert!(super::validate_grok_attachment_addresses(Vec::new()).is_err());
    for blocked in ["198.18.0.1:443", "127.0.0.1:443", "[fd00::1]:443"] {
        let mut mixed = addresses.clone();
        mixed.push(blocked.parse().unwrap());
        assert!(super::validate_grok_attachment_addresses(mixed).is_err());
    }
}

#[test]
fn grok_attachment_url_rejects_credentials_and_fragments_on_every_hop() {
    for raw_url in [
        "https://user@example.com/attachment.png",
        "https://:password@example.com/attachment.png",
        "https://user:password@example.com/attachment.png",
        "https://example.com/attachment.png#private-fragment",
    ] {
        let url = reqwest::Url::parse(raw_url).expect("URL should parse");
        assert!(
            validate_grok_attachment_url(&url).is_err(),
            "unsafe attachment URL should be rejected: {raw_url}"
        );
    }

    let initial = reqwest::Url::parse("https://example.com/attachment.png?signature=abc")
        .expect("URL should parse");
    validate_grok_attachment_url(&initial).expect("signed query URL should remain supported");
    let redirected = initial
        .join("https://redirect-user:redirect-pass@example.net/next.png")
        .expect("redirect URL should parse");
    assert!(validate_grok_attachment_url(&redirected).is_err());
}

#[test]
fn adapter_cleans_inline_citation_render_tags() {
    let card_json = serde_json::json!({
        "id": "803514",
        "url": "https://example.com/source",
        "title": "Example Source"
    })
    .to_string();
    let line = format!(
        "data: {}\n",
        serde_json::json!({
            "result": {
                "response": {
                    "messageTag": "final",
                    "cardAttachment": {"jsonData": card_json},
                    "token": "answer<grok:render card_id=\"803514\" card_type=\"citation_card\" type=\"render_inline_citation\"><argument name=\"citation_id\">5</argument></grok:render>"
                }
            }
        })
    );
    let mut adapter = GrokStreamAdapter::default();
    adapter.push_chunk(line.as_bytes());

    assert!(!adapter.text.contains("<grok:render"));
    assert!(adapter.text.contains("[[1]](https://example.com/source"));
}

#[test]
fn grok_usage_reports_preserve_estimated_provenance_after_wire_roundtrip() {
    use aether_usage_runtime::{
        build_stream_terminal_usage_event, build_sync_terminal_usage_event,
        GatewayStreamReportRequest, GatewaySyncReportRequest, UsageEventType,
    };

    for (format, report_prefix) in [
        ("openai:chat", "openai_chat"),
        ("openai:responses", "openai_responses"),
    ] {
        let mut plan = sample_plan(
            serde_json::json!({
                "messages": [{"role": "user", "content": "hello"}]
            }),
            format,
        );
        plan.stream = false;
        plan.provider_api_format = format.to_string();
        // The trusted planner binds this hint to the Grok runtime adapter.
        // Exercise its transport through the same serialized report as usage.
        let context = serde_json::json!({
            "provider_type": "grok",
            "provider_api_format": format,
            "client_api_format": format,
            "usage_token_source": "estimated"
        });
        let collected = GrokCollected {
            status_code: 200,
            text: "hello back".to_string(),
            thinking: "short reasoning".to_string(),
            ..GrokCollected::default()
        };
        let expected = grok_usage_estimate(&plan, &collected);
        let result = grok_execution_result(&plan, collected, Some(&context));
        let sync_report = GatewaySyncReportRequest {
            trace_id: plan.request_id.clone(),
            report_kind: format!("{report_prefix}_sync_success"),
            report_context: Some(context.clone()),
            status_code: result.status_code,
            headers: result.headers,
            body_json: result.body.and_then(|body| body.json_body),
            client_body_json: None,
            body_base64: None,
            telemetry: result.telemetry,
        };
        let sync_report: GatewaySyncReportRequest =
            serde_json::from_slice(&serde_json::to_vec(&sync_report).unwrap()).unwrap();
        let sync_event = build_sync_terminal_usage_event(
            &plan,
            sync_report.report_context.as_ref(),
            &sync_report,
        )
        .unwrap();

        plan.stream = true;
        let stream_report = GatewayStreamReportRequest {
            trace_id: plan.request_id.clone(),
            report_kind: format!("{report_prefix}_stream_success"),
            report_context: Some(context),
            status_code: 200,
            headers: BTreeMap::new(),
            provider_body_base64: None,
            provider_body_state: None,
            client_body_base64: None,
            client_body_state: None,
            terminal_summary: Some(super::grok_stream_terminal_summary(&plan, expected)),
            telemetry: None,
        };
        let stream_report: GatewayStreamReportRequest =
            serde_json::from_slice(&serde_json::to_vec(&stream_report).unwrap()).unwrap();
        let stream_event = build_stream_terminal_usage_event(
            &plan,
            stream_report.report_context.as_ref(),
            &stream_report,
        )
        .unwrap();

        // Sync honors the response's explicit total. The existing stream
        // summary has no explicit total, so its fallback also adds reasoning.
        let sync_total = expected.input_tokens + expected.output_tokens;
        let stream_total = sync_total + expected.reasoning_tokens;
        for (event, expected_total) in [(sync_event, sync_total), (stream_event, stream_total)] {
            assert_eq!(event.event_type, UsageEventType::Completed, "{format}");
            assert_eq!(event.data.input_tokens, Some(expected.input_tokens));
            assert_eq!(event.data.output_tokens, Some(expected.output_tokens));
            assert_eq!(event.data.total_tokens, Some(expected_total));
            let metadata = event.data.request_metadata.unwrap();
            assert_eq!(metadata["analytics_measurement"]["source"], "estimated");
            assert!(metadata.get("usage_token_source").is_none());
        }
    }
}

#[test]
fn openai_chat_body_includes_estimated_usage() {
    let plan = sample_plan(
        serde_json::json!({
            "messages": [{"role": "user", "content": "hello"}]
        }),
        "openai:chat",
    );
    let collected = GrokCollected {
        text: "hello back".to_string(),
        thinking: "thinking".to_string(),
        ..GrokCollected::default()
    };
    let usage = grok_usage_estimate(&plan, &collected);
    let body = openai_chat_body("grok-test", &collected, usage);

    assert!(body["usage"]["prompt_tokens"].as_u64().unwrap_or_default() > 0);
    assert!(
        body["usage"]["completion_tokens"]
            .as_u64()
            .unwrap_or_default()
            > 0
    );
    assert_eq!(
        body["usage"]["completion_tokens_details"]["reasoning_tokens"],
        serde_json::json!(usage.reasoning_tokens)
    );
}

#[test]
fn openai_responses_body_includes_estimated_usage() {
    let plan = sample_plan(serde_json::json!({"input": "hello"}), "openai:responses");
    let collected = GrokCollected {
        text: "hello back".to_string(),
        thinking: "short reasoning".to_string(),
        ..GrokCollected::default()
    };
    let usage = grok_usage_estimate(&plan, &collected);
    let body = openai_responses_body("grok-test", &collected, usage, false);

    assert_eq!(
        body["usage"]["input_tokens"],
        serde_json::json!(usage.input_tokens)
    );
    assert_eq!(
        body["usage"]["output_tokens"],
        serde_json::json!(usage.output_tokens)
    );
    assert_eq!(
        body["usage"]["output_tokens_details"]["reasoning_tokens"],
        serde_json::json!(usage.reasoning_tokens)
    );
    assert_eq!(body["output"][0]["type"], serde_json::json!("reasoning"));
    assert_eq!(
        body["output"][0]["content"][0]["type"],
        serde_json::json!("reasoning_text")
    );
    assert_eq!(
        body["output"][0]["content"][0]["text"],
        serde_json::json!("short reasoning")
    );
    assert_eq!(body["output"][0]["summary"], serde_json::json!([]));
    assert_eq!(body["output"][1]["type"], serde_json::json!("message"));
    assert!(body["output"][1]["id"]
        .as_str()
        .is_some_and(|id| id.starts_with("msg_")));
}

#[test]
fn openai_responses_body_emits_structured_image_generation_calls() {
    let plan = sample_plan(serde_json::json!({"input": "draw"}), "openai:responses");
    let collected = GrokCollected {
        text: "done".to_string(),
        images: vec!["data:image/png;base64,iVBORw0KGgo=".to_string()],
        ..GrokCollected::default()
    };
    let usage = grok_usage_estimate(&plan, &collected);
    let body = openai_responses_body("grok-imagine-image-lite", &collected, usage, true);

    assert_eq!(body["output"][0]["type"], serde_json::json!("message"));
    assert_eq!(
        body["output"][1]["type"],
        serde_json::json!("image_generation_call")
    );
    assert_eq!(
        body["output"][1]["result"],
        serde_json::json!("iVBORw0KGgo=")
    );
    assert_eq!(body["output"][1]["output_format"], serde_json::json!("png"));
}

#[test]
fn openai_responses_body_preserves_url_images_without_result_field() {
    let plan = sample_plan(serde_json::json!({"input": "draw"}), "openai:responses");
    let collected = GrokCollected {
        images: vec!["https://assets.grok.com/generated/example.png".to_string()],
        ..GrokCollected::default()
    };
    let usage = grok_usage_estimate(&plan, &collected);
    let body = openai_responses_body("grok-imagine-image-lite", &collected, usage, true);

    assert_eq!(
        body["output"][0]["type"],
        serde_json::json!("image_generation_call")
    );
    assert_eq!(
        body["output"][0]["url"],
        serde_json::json!("https://assets.grok.com/generated/example.png")
    );
    assert!(body["output"][0].get("result").is_none());
}

#[test]
fn openai_responses_body_preserves_text_whitespace() {
    let plan = sample_plan(serde_json::json!({"input": "hello"}), "openai:responses");
    let collected = GrokCollected {
        text: "\n  hello back  \n".to_string(),
        ..GrokCollected::default()
    };
    let usage = grok_usage_estimate(&plan, &collected);
    let body = openai_responses_body("grok-test", &collected, usage, false);

    assert_eq!(
        body["output"][0]["content"][0]["text"],
        serde_json::json!("\n  hello back  \n")
    );
}

#[test]
fn openai_responses_body_keeps_non_image_intent_images_as_text() {
    let plan = sample_plan(
        serde_json::json!({"input": "show source"}),
        "openai:responses",
    );
    let collected = GrokCollected {
        text: "source".to_string(),
        images: vec!["https://assets.grok.com/generated/example.png".to_string()],
        ..GrokCollected::default()
    };
    let usage = grok_usage_estimate(&plan, &collected);
    let body = openai_responses_body("grok-test", &collected, usage, false);

    assert_eq!(body["output"][0]["type"], serde_json::json!("message"));
    assert_eq!(
        body["output"][0]["content"][0]["text"],
        serde_json::json!("source\n\nhttps://assets.grok.com/generated/example.png")
    );
    assert_eq!(body["output"].as_array().map(Vec::len), Some(1));
}

#[test]
fn openai_chat_json_body_uses_standard_responses_conversion() {
    let plan = sample_plan(
        serde_json::json!({
            "messages": [{"role": "user", "content": "hello"}]
        }),
        "openai:chat",
    );
    let collected = GrokCollected {
        status_code: 200,
        text: "hello back".to_string(),
        thinking: "Thinking about your request".to_string(),
        ..GrokCollected::default()
    };
    let body = grok_client_json_body(&plan, &collected, None);

    assert_eq!(body["object"], serde_json::json!("chat.completion"));
    assert_eq!(
        body["choices"][0]["message"]["content"],
        serde_json::json!("hello back")
    );
    assert_eq!(
        body["usage"]["completion_tokens_details"]["reasoning_tokens"],
        serde_json::json!(grok_usage_estimate(&plan, &collected).reasoning_tokens)
    );
}

#[test]
fn openai_chat_json_body_converts_grok_images_through_standard_matrix() {
    let plan = sample_plan(
        serde_json::json!({
            "messages": [{"role": "user", "content": "draw"}]
        }),
        "openai:chat",
    );
    let mut plan = plan;
    plan.model_name = Some("grok-imagine-image-lite".to_string());
    let collected = GrokCollected {
        status_code: 200,
        images: vec!["data:image/png;base64,iVBORw0KGgo=".to_string()],
        ..GrokCollected::default()
    };
    let body = grok_client_json_body(&plan, &collected, None);

    assert_eq!(body["object"], serde_json::json!("chat.completion"));
    assert_eq!(
        body["choices"][0]["message"]["content"][0]["type"],
        serde_json::json!("image_url")
    );
    assert_eq!(
        body["choices"][0]["message"]["content"][0]["image_url"]["url"],
        serde_json::json!("data:image/png;base64,iVBORw0KGgo=")
    );
}

#[test]
fn openai_chat_json_body_uses_mapped_image_model_for_alias() {
    let mut plan = sample_plan(
        serde_json::json!({
            "messages": [{"role": "user", "content": "draw"}]
        }),
        "openai:chat",
    );
    plan.model_name = Some("custom-image-alias".to_string());
    let report_context = report_context_with_mapped_model("grok-imagine-image-lite");
    let collected = GrokCollected {
        status_code: 200,
        images: vec!["data:image/png;base64,iVBORw0KGgo=".to_string()],
        ..GrokCollected::default()
    };
    let body = grok_client_json_body(&plan, &collected, Some(&report_context));

    assert_eq!(body["object"], serde_json::json!("chat.completion"));
    assert_eq!(
        body["choices"][0]["message"]["content"][0]["type"],
        serde_json::json!("image_url")
    );
}

#[test]
fn openai_responses_stream_uses_aether_standard_emitter() {
    let plan = sample_plan(
        serde_json::json!({"input": "hello", "stream": true}),
        "openai:responses",
    );
    let collected = GrokCollected {
        status_code: 200,
        text: "hello back".to_string(),
        thinking: "Thinking about your request".to_string(),
        ..GrokCollected::default()
    };
    let body = grok_client_stream_body(&plan, &collected, None);

    assert!(body.contains("event: response.created"));
    assert!(body.contains("event: response.in_progress"));
    // Thinking must stay off the summary channel or clients that render
    // both (Codex) print the raw chain-of-thought twice.
    assert!(!body.contains("event: response.reasoning_summary_part.added"));
    assert!(!body.contains("event: response.reasoning_summary_text.delta"));
    assert!(!body.contains("event: response.reasoning_summary_text.done"));
    assert!(body.contains("\"type\":\"reasoning_text\""));
    assert!(body.contains("event: response.content_part.added"));
    assert!(body.contains("event: response.output_text.done"));
    assert!(body.contains("event: response.completed"));
    assert!(body.contains("\"sequence_number\""));
    assert!(!body.contains("chat.completion.chunk"));
}

#[test]
fn openai_responses_stream_preserves_image_generation_calls() {
    let plan = sample_plan(
        serde_json::json!({
            "input": "draw",
            "stream": true,
            "tools": [{"type": "image_generation"}]
        }),
        "openai:responses",
    );
    let collected = GrokCollected {
        status_code: 200,
        images: vec!["data:image/png;base64,iVBORw0KGgo=".to_string()],
        ..GrokCollected::default()
    };
    let body = grok_client_stream_body(&plan, &collected, None);

    assert!(body.contains("event: response.output_item.done"));
    assert!(body.contains("\"type\":\"image_generation_call\""));
    assert!(body.contains("\"result\":\"iVBORw0KGgo=\""));
    assert!(body.contains("event: response.completed"));
}

#[test]
fn openai_responses_stream_uses_mapped_image_model_for_alias() {
    let mut plan = sample_plan(
        serde_json::json!({
            "input": "draw",
            "stream": true
        }),
        "openai:responses",
    );
    plan.model_name = Some("custom-image-alias".to_string());
    let report_context = report_context_with_mapped_model("grok-imagine-image-lite");
    let collected = GrokCollected {
        status_code: 200,
        images: vec!["data:image/png;base64,iVBORw0KGgo=".to_string()],
        ..GrokCollected::default()
    };
    let body = grok_client_stream_body(&plan, &collected, Some(&report_context));

    assert!(grok_plan_uses_structured_image_generation(
        &plan,
        Some(&report_context)
    ));
    assert!(body.contains("event: response.output_item.done"));
    assert!(body.contains("\"type\":\"image_generation_call\""));
    assert!(body.contains("\"result\":\"iVBORw0KGgo=\""));
}

#[test]
fn grok_lite_image_responses_stream_collects_before_client_bridge() {
    let mut plan = sample_plan(
        serde_json::json!({
            "input": "draw",
            "stream": true,
            "tools": [{"type": "image_generation"}]
        }),
        "openai:responses",
    );
    plan.model_name = Some("grok-imagine-image-lite".to_string());

    assert!(grok_plan_uses_structured_image_generation(&plan, None));
    assert!(grok_should_collect_image_stream(
        &plan,
        Some(&report_context_with_mapped_model("grok-imagine-image-lite"))
    )
    .expect("collect decision should succeed"));
}

#[test]
fn grok_lite_alias_responses_stream_collects_from_mapped_model() {
    let mut plan = sample_plan(
        serde_json::json!({
            "input": "draw",
            "stream": true
        }),
        "openai:responses",
    );
    plan.model_name = Some("custom-image-alias".to_string());

    assert!(!grok_plan_uses_structured_image_generation(&plan, None));
    assert!(grok_should_collect_image_stream(
        &plan,
        Some(&report_context_with_mapped_model("grok-imagine-image-lite"))
    )
    .expect("collect decision should succeed"));
}

#[test]
fn claude_messages_stream_uses_claude_event_shape() {
    let plan = sample_plan(
        serde_json::json!({"messages": [{"role": "user", "content": "hello"}]}),
        "claude:messages",
    );
    let collected = GrokCollected {
        status_code: 200,
        text: "hello back".to_string(),
        thinking: "Thinking about your request".to_string(),
        ..GrokCollected::default()
    };
    let body = grok_client_stream_body(&plan, &collected, None);

    assert!(body.contains("event: message_start"));
    assert!(body.contains("\"type\":\"message_start\""));
    assert!(body.contains("\"type\":\"thinking_delta\""));
    assert!(body.contains("\"type\":\"text_delta\""));
    assert!(body.contains("event: message_delta"));
    assert!(body.contains("event: message_stop"));
    assert!(!body.contains("chat.completion.chunk"));
}

#[test]
fn extracts_openai_chat_image_and_file_attachment_inputs() {
    let inputs = extract_grok_attachment_inputs(
        "openai:chat",
        &serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "describe these"},
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,aGVsbG8="}},
                    {"type": "file", "file": {"filename": "notes.txt", "file_data": "data:text/plain;base64,bm90ZXM="}}
                ]
            }]
        }),
    )
    .expect("attachment inputs should be within limits");

    assert_eq!(inputs.len(), 2);
    assert_eq!(inputs[0].source.as_str(), "data:image/png;base64,aGVsbG8=");
    assert_eq!(inputs[1].filename.as_deref(), Some("notes.txt"));
    assert_eq!(inputs[1].source.as_str(), "data:text/plain;base64,bm90ZXM=");
}

#[test]
fn grok_attachment_inputs_reject_excess_count_before_uploads() {
    let content = (0..=super::GROK_MAX_ATTACHMENT_COUNT)
        .map(|index| {
            serde_json::json!({
                "type": "image_url",
                "image_url": format!("https://example.com/image-{index}.png")
            })
        })
        .collect::<Vec<_>>();
    let error = extract_grok_attachment_inputs(
        "openai:chat",
        &serde_json::json!({
            "messages": [{"role": "user", "content": content}]
        }),
    )
    .expect_err("more than the bounded attachment count must be rejected");

    assert!(error.to_string().contains("at most 16 attachments"));
}

#[test]
fn grok_attachment_inputs_bound_source_and_metadata_fields() {
    let long_url = format!(
        "https://example.com/{}",
        "u".repeat(super::GROK_MAX_ATTACHMENT_URL_BYTES)
    );
    let source_error = extract_grok_attachment_inputs(
        "openai:chat",
        &serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [{"type": "image_url", "image_url": long_url}]
            }]
        }),
    )
    .expect_err("an oversized URL field must be rejected before DNS/fetch");
    assert!(source_error.to_string().contains("source exceeds"));

    let long_filename = "f".repeat(super::GROK_MAX_ATTACHMENT_FILENAME_BYTES + 1);
    let filename_error = extract_grok_attachment_inputs(
        "openai:chat",
        &serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "file",
                    "file": {
                        "filename": long_filename,
                        "file_data": "data:text/plain;base64,Zm9v"
                    }
                }]
            }]
        }),
    )
    .expect_err("an oversized filename field must be rejected");
    assert!(filename_error.to_string().contains("filename exceeds"));

    let long_mime = "a".repeat(super::GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES + 1);
    let mime_error = extract_grok_attachment_inputs(
        "claude:messages",
        &serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": long_mime,
                        "data": "Zm9v"
                    }
                }]
            }]
        }),
    )
    .expect_err("an oversized MIME field must be rejected");
    assert!(mime_error.to_string().contains("MIME type exceeds"));
}

#[test]
fn grok_data_uri_attachment_accepts_content_within_hard_size_cap() {
    const PAYLOAD_BYTES: usize = 25 * 1024 * 1024 + 1;
    let base64_blocks = PAYLOAD_BYTES / 3 + 1;
    let mut source = String::from("data:application/octet-stream;base64,");
    source.extend(std::iter::repeat_n('A', base64_blocks * 4));
    let input = GrokAttachmentInput {
        source,
        filename: Some("large.bin".to_string()),
        mime_type: None,
    };

    let payload = grok_attachment_payload_from_data_uri(&input, 0)
        .expect("attachment within the hard cap should be accepted");

    assert_eq!(payload.filename, "large.bin");
    assert_eq!(payload.mime_type, "application/octet-stream");
    assert_eq!(payload.content_b64.len(), base64_blocks * 4);
}

#[test]
fn grok_data_uri_attachment_rejects_content_above_hard_size_cap() {
    let source = "data:application/octet-stream;base64,MTIzNDU2Nzg5".to_string();
    let input = GrokAttachmentInput {
        source,
        filename: Some("oversized.bin".to_string()),
        mime_type: None,
    };

    let error = grok_attachment_payload_from_data_uri_with_limit(&input, 0, 8)
        .expect_err("attachment above the hard cap must be rejected");

    assert!(error.to_string().contains("exceeds"));
}

#[test]
fn grok_public_data_image_requires_bounded_base64_and_matching_raster_magic() {
    let png = [
        0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 13, b'I', b'H', b'D', b'R',
    ];
    let encoded_png = base64::engine::general_purpose::STANDARD.encode(png);
    let valid = format!("data:image/png;base64,{encoded_png}");
    let (mime, normalized) = grok_data_image_parts_with_limit(&valid, png.len())
        .expect("valid PNG data URL should pass");
    assert_eq!(mime, "image/png");
    assert_eq!(normalized, encoded_png);

    let html = base64::engine::general_purpose::STANDARD.encode(b"<html>not an image</html>");
    assert!(
        grok_data_image_parts_with_limit(format!("data:image/png;base64,{html}").as_str(), 64,)
            .is_none()
    );
    assert!(grok_data_image_parts_with_limit(
        format!("data:image/svg+xml;base64,{html}").as_str(),
        64,
    )
    .is_none());
    assert!(grok_data_image_parts_with_limit(
        format!("data:image/jpeg;base64,{encoded_png}").as_str(),
        png.len(),
    )
    .is_none());
    assert!(grok_data_image_parts_with_limit("data:image/png;base64,not-valid-***", 64,).is_none());
    assert!(grok_data_image_parts_with_limit(&valid, png.len() - 1).is_none());
}

#[test]
fn grok_public_image_outputs_drop_invalid_data_urls_but_keep_http_urls() {
    let svg = base64::engine::general_purpose::STANDARD.encode(b"<svg><script>x</script></svg>");
    let invalid = format!("data:image/svg+xml;base64,{svg}");
    let ordinary_url = "https://assets.grok.com/generated/example.png";
    let collected = GrokCollected {
        status_code: 200,
        text: "done".to_string(),
        images: vec![invalid.clone(), ordinary_url.to_string()],
        ..GrokCollected::default()
    };

    let body = openai_image_body(&collected);
    assert_eq!(body["data"].as_array().map(Vec::len), Some(1));
    assert_eq!(body["data"][0]["url"], serde_json::json!(ordinary_url));

    let text = chat_text_with_images(&collected);
    assert!(text.contains(ordinary_url));
    assert!(!text.contains(&invalid));

    let sse = openai_image_sse(&collected);
    assert!(sse.contains(ordinary_url));
    assert!(!sse.contains(&invalid));
}

#[test]
fn extracts_responses_and_claude_attachment_inputs() {
    let responses = extract_grok_attachment_inputs(
        "openai:responses",
        &serde_json::json!({
            "input": [{
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "read it"},
                    {"type": "input_image", "image_url": "https://example.com/a.png"},
                    {"type": "input_file", "filename": "doc.pdf", "file_data": "data:application/pdf;base64,JVBERi0="}
                ]
            }]
        }),
    )
    .expect("responses attachments should be within limits");
    let claude = extract_grok_attachment_inputs(
        "claude:messages",
        &serde_json::json!({
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "read"},
                    {"type": "image", "source": {"type": "url", "url": "https://example.com/b.png"}},
                    {"type": "document", "filename": "memo.pdf", "source": {"type": "base64", "media_type": "application/pdf", "data": "JVBERi0="}}
                ]
            }]
        }),
    )
    .expect("Claude attachments should be within limits");

    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0].source.as_str(), "https://example.com/a.png");
    assert_eq!(responses[1].filename.as_deref(), Some("doc.pdf"));
    assert_eq!(claude.len(), 2);
    assert_eq!(claude[0].source.as_str(), "https://example.com/b.png");
    assert_eq!(claude[1].filename.as_deref(), Some("memo.pdf"));
    assert_eq!(
        claude[1].source.as_str(),
        "data:application/pdf;base64,JVBERi0="
    );
}

fn response(
    status: StatusCode,
    content_type: &'static str,
    body: impl Into<Body>,
) -> http::Response<Body> {
    http::Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(body.into())
        .expect("response should build")
}

#[tokio::test]
async fn grok_openai_image_sync_materializes_public_asset_urls_for_preview() {
    let listener = crate::test_support::bind_loopback_listener()
        .await
        .expect("listener should bind");
    let addr = listener.local_addr().expect("local addr should resolve");
    let app = Router::new().fallback(any(|request: Request| async move {
        let path = request.uri().path().to_string();
        let method = request.method().clone();
        match (method, path.as_str()) {
            (Method::GET, "/generated.png") => response(
                StatusCode::OK,
                "image/png",
                Body::from(vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a]),
            ),
            _ => response(StatusCode::NOT_FOUND, "text/plain", "not found"),
        }
    }));
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("mock server should run");
    });

    let plan = sample_plan(
        serde_json::json!({"model": "grok-imagine-image-lite"}),
        "openai:image",
    );
    let mut collected = GrokCollected {
        status_code: 200,
        images: vec![format!("http://{addr}/generated.png")],
        ..GrokCollected::default()
    };

    materialize_grok_image_assets(&plan, &mut collected).await;

    server.abort();

    assert!(collected.images[0].starts_with("data:image/png;base64,"));
    let body = openai_image_body(&collected);
    assert_eq!(body["data"][0]["b64_json"].as_str().is_some(), true);
    assert!(body["data"][0].get("url").is_none());
}

#[test]
fn grok_downloaded_image_requires_matching_magic_and_mime() {
    let png = [0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a];
    assert_eq!(
        grok_image_mime_for_payload(&png, Some("image/png; charset=binary")),
        Some("image/png")
    );
    assert_eq!(grok_image_mime_for_payload(&png, Some("text/html")), None);
    assert_eq!(
        grok_image_mime_for_payload(b"<html>not an image</html>", Some("image/png")),
        None
    );
    assert_eq!(grok_image_mime_for_payload(&png, None), Some("image/png"));
}

#[test]
fn grok_openai_image_body_preserves_plain_urls_when_asset_is_not_materialized() {
    let body = openai_image_body(&GrokCollected {
        status_code: 200,
        images: vec!["https://assets.grok.com/generated/example.png".to_string()],
        ..GrokCollected::default()
    });

    assert_eq!(
        body["data"][0]["url"].as_str(),
        Some("https://assets.grok.com/generated/example.png")
    );
    assert!(body["data"][0].get("b64_json").is_none());
}
