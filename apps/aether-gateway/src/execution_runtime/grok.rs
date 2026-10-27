use std::collections::BTreeMap;
use std::future::Future;
use std::io::Error as IoError;
use std::net::IpAddr;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use aether_contracts::{
    ExecutionPlan, ExecutionResult, ExecutionStreamTerminalSummary, ExecutionTelemetry,
    RequestBody, ResponseBody, StandardizedUsage, StreamFrame, StreamFramePayload, StreamFrameType,
    EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER,
};
use axum::body::Bytes;
use base64::Engine as _;
use futures_util::stream::{self, BoxStream};
use futures_util::StreamExt;
use http_body_util::BodyExt;
use regex::{Captures, Regex};
use serde_json::{json, Map, Value};
use uuid::Uuid;
use wreq::ws::message::Message as WreqWsMessage;

use crate::ai_serving::api::{
    convert_standard_chat_response, maybe_bridge_standard_sync_json_to_stream,
    CanonicalContentPart, CanonicalStreamEvent, CanonicalStreamFrame, ClaudeClientEmitter,
    OpenAIChatClientEmitter, OpenAIResponsesClientEmitter, StreamingCanonicalUsage,
};
use crate::ai_serving::{
    openai_responses_message_item_id, openai_responses_synthetic_reasoning_item_id,
};
use crate::clock::current_unix_secs;
use crate::execution_runtime::ndjson::encode_stream_frame_ndjson;
use crate::execution_runtime::transport::{
    build_browser_wreq_client, build_request_body, build_request_headers,
    execution_plan_response_body_limit_bytes, format_hyper_error_chain,
    format_upstream_request_error, format_wreq_upstream_request_error,
    resolve_stream_first_byte_timeout, send_request, stream_first_byte_timeout_message,
    with_non_stream_total_timeout, DirectHttpResponse, ExecutionRuntimeTransportError,
    ExecutionTransportControls, UpstreamResponseBodyPhase,
};

const GROK_INTERNAL_HEADER: &str = "x-aether-grok-runtime";
const GROK_ASSET_BASE: &str = "https://assets.grok.com/";
const GROK_UPLOAD_PATH: &str = "/rest/app-chat/upload-file";
const GROK_MEDIA_POST_PATH: &str = "/rest/media/post/create";
const GROK_IMAGINE_WS_URL: &str = "wss://grok.com/ws/imagine/listen";
const GROK_STANDARD_PROVIDER_API_FORMAT: &str = "openai:responses";
const GROK_PROMPT_OVERHEAD_TOKENS: u64 = 4;
const GROK_MAX_ATTACHMENT_BYTES: usize = 64 * 1024 * 1024;
// Attachment uploads each require a fetch/decode plus a provider upload.  Cap
// the number independently of the request-body byte limit so a compact JSON
// array cannot turn into an unbounded sequence of outbound requests.
const GROK_MAX_ATTACHMENT_COUNT: usize = 16;
const GROK_MAX_ATTACHMENT_URL_BYTES: usize = 64 * 1024;
const GROK_MAX_ATTACHMENT_FILENAME_BYTES: usize = 1024;
const GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES: usize = 256;
const GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES: usize = 4 * 1024;
// A websocket image response may contain several slots.  Bound the aggregate
// encoded blob storage before retaining provider text in the slot map; the
// per-message 64 MiB websocket limit alone would otherwise permit roughly
// 1 GiB across the 16 transient slots.
const GROK_MAX_IMAGINE_BLOB_TOTAL_DECODED_BYTES: usize = 128 * 1024 * 1024;
// Collected image responses are rendered into one client-facing SSE payload
// before being wrapped in a StreamFrame.  Keep this compatibility envelope
// bounded independently of the normal streaming path; regular token streams
// remain incremental and are not subject to this aggregate cap.
const GROK_SYNTHETIC_STREAM_ENVELOPE_MAX_BYTES: usize = 256 * 1024 * 1024;
const GROK_SYNTHETIC_STREAM_ENVELOPE_OVERHEAD_BYTES: usize = 64 * 1024;
const GROK_SYNTHETIC_STREAM_BODY_MAX_BYTES: usize =
    (GROK_SYNTHETIC_STREAM_ENVELOPE_MAX_BYTES - GROK_SYNTHETIC_STREAM_ENVELOPE_OVERHEAD_BYTES) / 4
        * 3;
const GROK_MAX_IMAGE_COUNT: usize = 4;
// A provider can emit progress frames for more IDs than the client requested.
// Keep transient IDs bounded so a malformed stream cannot grow the map forever.
const GROK_MAX_IMAGINE_SLOTS: usize = 16;
const GROK_MAX_ATTACHMENT_REDIRECTS: usize = 5;
const GROK_IMAGINE_STREAM_TIMEOUT_MS: u64 = 10_000;
const GROK_IMAGINE_ROUND_TIMEOUT_MS: u64 = 120_000;

static GROK_RENDER_RE: OnceLock<Regex> = OnceLock::new();

fn grok_render_regex() -> &'static Regex {
    GROK_RENDER_RE.get_or_init(|| {
        Regex::new(
        r#"(?s)<grok:render\s+card_id="([^"]+)"\s+card_type="([^"]+)"\s+type="([^"]+)"[^>]*>.*?</grok:render>"#,
    )
        .expect("Grok render regex should compile")
    })
}

pub(crate) struct GrokRuntimeStream {
    pub(crate) frame_stream: BoxStream<'static, Result<Bytes, IoError>>,
    pub(crate) report_context: Option<Value>,
}

#[derive(Debug)]
struct GrokCollected {
    status_code: u16,
    text: String,
    thinking: String,
    images: Vec<String>,
    telemetry: ExecutionTelemetry,
}

#[derive(Debug, Clone)]
struct GrokImagineImage {
    order: usize,
    url: Option<String>,
    blob_b64: Option<String>,
    done: bool,
    moderated: bool,
}

impl Default for GrokCollected {
    fn default() -> Self {
        Self {
            status_code: 0,
            text: String::new(),
            thinking: String::new(),
            images: Vec::new(),
            telemetry: ExecutionTelemetry {
                ttfb_ms: None,
                elapsed_ms: None,
                upstream_bytes: None,
            },
        }
    }
}

#[derive(Debug, Default)]
struct GrokStreamAdapter {
    buffered: String,
    text: String,
    thinking: String,
    images: Vec<String>,
    cards: BTreeMap<String, GrokCard>,
    citation_order: Vec<String>,
    last_citation_index: Option<usize>,
}

#[derive(Debug, Clone)]
struct GrokCard {
    url: Option<String>,
    title: Option<String>,
}

#[derive(Debug, Clone, Copy)]
struct GrokUsageEstimate {
    input_tokens: u64,
    output_tokens: u64,
    reasoning_tokens: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GrokAttachmentInput {
    source: String,
    filename: Option<String>,
    mime_type: Option<String>,
}

#[derive(Debug)]
struct GrokAttachmentPayload {
    filename: String,
    mime_type: String,
    content_b64: String,
}

#[derive(Debug, Clone)]
struct GrokUploadedAttachment {
    file_id: String,
    file_uri: Option<String>,
}

pub(crate) async fn maybe_execute_grok_sync(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<Option<ExecutionResult>, ExecutionRuntimeTransportError> {
    if !is_grok_plan(plan, report_context) {
        return Ok(None);
    }
    with_non_stream_total_timeout(plan, async move {
        let mut collected = execute_grok_app_chat(plan, report_context).await?;
        materialize_grok_image_assets(plan, &mut collected).await;
        Ok(Some(grok_execution_result(plan, collected, report_context)))
    })
    .await
}

pub(crate) async fn maybe_execute_grok_stream(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<Option<GrokRuntimeStream>, ExecutionRuntimeTransportError> {
    if !is_grok_plan(plan, report_context) {
        return Ok(None);
    }
    if grok_should_collect_image_stream(plan, report_context)? {
        let collected = execute_grok_app_chat(plan, report_context).await?;
        return Ok(Some(GrokRuntimeStream {
            frame_stream: grok_collected_frame_stream(plan.clone(), collected, report_context),
            report_context: report_context.cloned(),
        }));
    }
    Ok(Some(
        execute_grok_app_chat_stream(plan, report_context).await?,
    ))
}

fn is_grok_plan(plan: &ExecutionPlan, report_context: Option<&Value>) -> bool {
    let header_marker = plan
        .headers
        .iter()
        .any(|(name, value)| name.eq_ignore_ascii_case(GROK_INTERNAL_HEADER) && value == "1");
    let context_marker = report_context
        .and_then(|value| value.get("provider_type"))
        .and_then(Value::as_str)
        .map(|value| value.eq_ignore_ascii_case("grok"))
        .unwrap_or(false);
    header_marker || context_marker
}

async fn execute_grok_app_chat(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<GrokCollected, ExecutionRuntimeTransportError> {
    if grok_should_use_imagine_websocket(plan, report_context)? {
        return execute_grok_imagine_websocket(plan, report_context).await;
    }
    let upstream_plan = grok_upstream_plan(plan, report_context).await?;
    let request_body = build_request_body(&upstream_plan)?;
    let started_at = Instant::now();
    let response = send_request(&upstream_plan, request_body).await?;
    let ttfb_ms = started_at.elapsed().as_millis() as u64;
    let status_code = response.status_code();
    let mut upstream_bytes = 0u64;
    let mut raw_body = Vec::new();
    let mut adapter = GrokStreamAdapter::default();
    let response_body_limit_bytes = execution_plan_response_body_limit_bytes(plan);
    collect_grok_response_stream(
        response,
        status_code,
        &mut upstream_bytes,
        &mut raw_body,
        &mut adapter,
        response_body_limit_bytes,
    )
    .await?;
    if (200..300).contains(&status_code) {
        adapter.finish();
    }

    let elapsed_ms = started_at.elapsed().as_millis() as u64;
    if !(200..300).contains(&status_code) {
        return Ok(GrokCollected {
            status_code,
            text: grok_upstream_http_error_message(status_code),
            telemetry: ExecutionTelemetry {
                ttfb_ms: Some(ttfb_ms),
                elapsed_ms: Some(elapsed_ms),
                upstream_bytes: Some(upstream_bytes),
            },
            ..GrokCollected::default()
        });
    }

    Ok(GrokCollected {
        status_code,
        text: adapter.text,
        thinking: adapter.thinking,
        images: adapter.images,
        telemetry: ExecutionTelemetry {
            ttfb_ms: Some(ttfb_ms),
            elapsed_ms: Some(elapsed_ms),
            upstream_bytes: Some(upstream_bytes),
        },
    })
}

async fn execute_grok_app_chat_stream(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<GrokRuntimeStream, ExecutionRuntimeTransportError> {
    let upstream_plan = grok_upstream_plan(plan, report_context).await?;
    let request_body = build_request_body(&upstream_plan)?;
    let started_at = Instant::now();
    let response = send_request(&upstream_plan, request_body).await?;
    let status_code = response.status_code();
    if !(200..300).contains(&status_code) {
        let mut upstream_bytes = 0u64;
        let mut raw_body = Vec::new();
        let mut adapter = GrokStreamAdapter::default();
        let response_body_limit_bytes = execution_plan_response_body_limit_bytes(plan);
        collect_grok_response_stream(
            response,
            status_code,
            &mut upstream_bytes,
            &mut raw_body,
            &mut adapter,
            response_body_limit_bytes,
        )
        .await?;
        let elapsed_ms = started_at.elapsed().as_millis() as u64;
        let collected = GrokCollected {
            status_code,
            text: grok_upstream_http_error_message(status_code),
            telemetry: ExecutionTelemetry {
                ttfb_ms: Some(elapsed_ms),
                elapsed_ms: Some(elapsed_ms),
                upstream_bytes: Some(upstream_bytes),
            },
            ..GrokCollected::default()
        };
        return Ok(GrokRuntimeStream {
            frame_stream: grok_collected_frame_stream(plan.clone(), collected, report_context),
            report_context: report_context.cloned(),
        });
    }

    Ok(GrokRuntimeStream {
        frame_stream: grok_success_frame_stream(
            plan.clone(),
            status_code,
            started_at,
            grok_response_body_stream(response),
        ),
        report_context: report_context.cloned(),
    })
}

fn grok_should_use_imagine_websocket(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<bool, ExecutionRuntimeTransportError> {
    let client_format = normalized_client_api_format(plan);
    if !matches!(
        client_format.as_str(),
        "openai:image" | "openai:responses" | "openai:responses:compact" | "openai:chat"
    ) {
        return Ok(false);
    }
    let mapped_model = grok_upstream_model_name(report_context)?;
    let model = mapped_model.to_ascii_lowercase();
    Ok(model.contains("grok-imagine-image") && !model.contains("lite") && !model.contains("edit"))
}

fn grok_should_collect_image_stream(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<bool, ExecutionRuntimeTransportError> {
    if normalized_client_api_format(plan) == "openai:image" {
        return Ok(true);
    }
    if grok_plan_uses_structured_image_generation(plan, report_context) {
        return Ok(true);
    }
    grok_should_use_imagine_websocket(plan, report_context)
}

async fn execute_grok_imagine_websocket(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<GrokCollected, ExecutionRuntimeTransportError> {
    let body = plan.body.json_body.as_ref().ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok Imagine requires JSON request body".to_string(),
        )
    })?;
    let prompt = grok_image_prompt_from_provider_body(body).ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok Imagine requires a non-empty prompt".to_string(),
        )
    })?;
    let requested = grok_image_count_from_provider_body(body);
    let enable_pro = grok_upstream_model_name(report_context)?
        .to_ascii_lowercase()
        .contains("pro");
    let aspect_ratio = grok_aspect_ratio_from_provider_body(body);
    let started_at = Instant::now();
    let mut images =
        grok_imagine_websocket_images(plan, &prompt, requested, &aspect_ratio, enable_pro).await?;
    images.sort_by_key(|image| image.order);
    Ok(GrokCollected {
        status_code: 200,
        images: images
            .into_iter()
            .filter_map(|image| {
                image
                    .url
                    .or_else(|| image.blob_b64.and_then(grok_data_image_url))
            })
            .collect(),
        telemetry: ExecutionTelemetry {
            ttfb_ms: None,
            elapsed_ms: Some(started_at.elapsed().as_millis() as u64),
            upstream_bytes: None,
        },
        ..GrokCollected::default()
    })
}

async fn grok_imagine_websocket_images(
    plan: &ExecutionPlan,
    prompt: &str,
    requested: usize,
    aspect_ratio: &str,
    enable_pro: bool,
) -> Result<Vec<GrokImagineImage>, ExecutionRuntimeTransportError> {
    let headers = build_request_headers(&plan.headers, None, false)?;
    let profile = plan.transport_profile.as_ref().ok_or_else(|| {
        ExecutionRuntimeTransportError::UnsupportedTransportProfile("browser_wreq".to_string())
    })?;
    let client = build_browser_wreq_client(
        plan.timeouts.as_ref(),
        plan.proxy.as_ref(),
        profile,
        ExecutionTransportControls::default(),
        true,
    )?;
    let response = client
        .websocket(GROK_IMAGINE_WS_URL)
        .headers(headers)
        .max_frame_size(64 << 20)
        .max_message_size(64 << 20)
        .send()
        .await
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_wreq_upstream_request_error(
                &err,
            ))
        })?;
    let status = response.status();
    if !status.is_success() && status.as_u16() != 101 {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(format!(
            "Grok Imagine websocket returned {}",
            status.as_u16()
        )));
    }
    let mut websocket = response.into_websocket().await.map_err(|err| {
        ExecutionRuntimeTransportError::UpstreamRequest(format_wreq_upstream_request_error(&err))
    })?;
    let reset = grok_imagine_reset_message();
    websocket
        .send(WreqWsMessage::text(reset.to_string()))
        .await
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_wreq_upstream_request_error(
                &err,
            ))
        })?;
    let request = grok_imagine_request_message(prompt, aspect_ratio, enable_pro);
    websocket
        .send(WreqWsMessage::text(request.to_string()))
        .await
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_wreq_upstream_request_error(
                &err,
            ))
        })?;

    let deadline = Instant::now() + Duration::from_millis(GROK_IMAGINE_ROUND_TIMEOUT_MS);
    let mut slots: BTreeMap<String, GrokImagineImage> = BTreeMap::new();
    while Instant::now() < deadline {
        if grok_imagine_completed_count(&slots) >= requested {
            break;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let timeout = remaining.min(Duration::from_millis(GROK_IMAGINE_STREAM_TIMEOUT_MS));
        let Some(message) = tokio::time::timeout(timeout, websocket.recv())
            .await
            .map_err(|_| {
                ExecutionRuntimeTransportError::UpstreamRequest(
                    "Grok Imagine websocket timed out waiting for image frames".to_string(),
                )
            })?
        else {
            break;
        };
        let message = message.map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_wreq_upstream_request_error(
                &err,
            ))
        })?;
        let WreqWsMessage::Text(text) = message else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(text.as_str()) else {
            continue;
        };
        grok_handle_imagine_ws_message(&value, &mut slots)?;
    }

    let mut images = slots
        .into_values()
        .filter(|image| {
            image.done && !image.moderated && (image.url.is_some() || image.blob_b64.is_some())
        })
        .collect::<Vec<_>>();
    images.sort_by_key(|image| image.order);
    images.truncate(requested);
    if images.is_empty() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok Imagine returned no images".to_string(),
        ));
    }
    Ok(images)
}

async fn collect_grok_response_stream(
    response: DirectHttpResponse,
    status_code: u16,
    upstream_bytes: &mut u64,
    raw_body: &mut Vec<u8>,
    adapter: &mut GrokStreamAdapter,
    response_body_limit_bytes: usize,
) -> Result<(), ExecutionRuntimeTransportError> {
    match response {
        DirectHttpResponse::Reqwest(response) => {
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|err| {
                    ExecutionRuntimeTransportError::UpstreamRequest(format_upstream_request_error(
                        &err,
                    ))
                })?;
                collect_grok_response_chunk_with_limit(
                    status_code,
                    upstream_bytes,
                    raw_body,
                    adapter,
                    &chunk,
                    response_body_limit_bytes,
                )?;
            }
        }
        DirectHttpResponse::HyperH2c(response) => {
            let mut stream = response.into_body().into_data_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|err| {
                    ExecutionRuntimeTransportError::UpstreamRequest(format_hyper_error_chain(&err))
                })?;
                collect_grok_response_chunk_with_limit(
                    status_code,
                    upstream_bytes,
                    raw_body,
                    adapter,
                    &chunk,
                    response_body_limit_bytes,
                )?;
            }
        }
        DirectHttpResponse::BrowserWreq(response) => {
            let mut stream = response.bytes_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|err| {
                    ExecutionRuntimeTransportError::UpstreamRequest(
                        format_wreq_upstream_request_error(&err),
                    )
                })?;
                collect_grok_response_chunk_with_limit(
                    status_code,
                    upstream_bytes,
                    raw_body,
                    adapter,
                    &chunk,
                    response_body_limit_bytes,
                )?;
            }
        }
    }
    Ok(())
}

fn collect_grok_response_chunk_with_limit(
    status_code: u16,
    upstream_bytes: &mut u64,
    raw_body: &mut Vec<u8>,
    adapter: &mut GrokStreamAdapter,
    chunk: &[u8],
    response_body_limit_bytes: usize,
) -> Result<(), ExecutionRuntimeTransportError> {
    if chunk.len()
        > response_body_limit_bytes
            .saturating_sub(usize::try_from(*upstream_bytes).unwrap_or(usize::MAX))
    {
        return Err(ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Wire,
            limit_bytes: response_body_limit_bytes,
        });
    }
    *upstream_bytes += chunk.len() as u64;
    if (200..300).contains(&status_code) {
        adapter.push_chunk(chunk);
    } else {
        raw_body.extend_from_slice(chunk);
    }
    Ok(())
}

type GrokUpstreamBodyStream = BoxStream<'static, Result<Bytes, String>>;

fn grok_response_body_stream(response: DirectHttpResponse) -> GrokUpstreamBodyStream {
    match response {
        DirectHttpResponse::Reqwest(response) => response
            .bytes_stream()
            .map(|chunk| {
                chunk.map_err(|err| {
                    ExecutionRuntimeTransportError::UpstreamRequest(format_upstream_request_error(
                        &err,
                    ))
                    .to_string()
                })
            })
            .boxed(),
        DirectHttpResponse::HyperH2c(response) => response
            .into_body()
            .into_data_stream()
            .map(|chunk| {
                chunk.map_err(|err| {
                    ExecutionRuntimeTransportError::UpstreamRequest(format_hyper_error_chain(&err))
                        .to_string()
                })
            })
            .boxed(),
        DirectHttpResponse::BrowserWreq(response) => response
            .bytes_stream()
            .map(|chunk| {
                chunk.map_err(|err| {
                    ExecutionRuntimeTransportError::UpstreamRequest(
                        format_wreq_upstream_request_error(&err),
                    )
                    .to_string()
                })
            })
            .boxed(),
    }
}

fn grok_success_frame_stream(
    plan: ExecutionPlan,
    status_code: u16,
    started_at: Instant,
    mut body_stream: GrokUpstreamBodyStream,
) -> BoxStream<'static, Result<Bytes, IoError>> {
    let stream_first_byte_timeout = resolve_stream_first_byte_timeout(&plan);
    let response_body_limit_bytes = execution_plan_response_body_limit_bytes(&plan);
    async_stream::stream! {
        match encode_grok_headers_frame(
            status_code,
            BTreeMap::from([("content-type".to_string(), "text/event-stream".to_string())]),
        ) {
            Ok(frame) => yield Ok(frame),
            Err(err) => {
                yield Err(err);
                return;
            }
        }

        let mut adapter = GrokStreamAdapter::default();
        let mut client_emitter = GrokClientStreamEmitter::new(&plan);
        let mut upstream_bytes = 0u64;
        let mut ttfb_ms = None;
        let mut first_chunk_telemetry_emitted = false;
        let mut text_len = 0usize;
        let mut thinking_len = 0usize;
        let mut image_len = 0usize;
        let mut terminal_error_emitted = false;

        loop {
            let item = if ttfb_ms.is_none() {
                match await_grok_stream_first_byte(
                    body_stream.next(),
                    started_at,
                    stream_first_byte_timeout,
                )
                .await
                {
                    Ok(item) => item,
                    Err(timeout) => {
                        match encode_grok_first_byte_timeout_frame(timeout) {
                            Ok(frame) => yield Ok(frame),
                            Err(err) => {
                                yield Err(err);
                                return;
                            }
                        }
                        terminal_error_emitted = true;
                        break;
                    }
                }
            } else {
                body_stream.next().await
            };
            let Some(item) = item else {
                break;
            };
            let chunk = match item {
                Ok(chunk) => chunk,
                Err(message) => {
                    match encode_grok_error_frame(message) {
                        Ok(frame) => yield Ok(frame),
                        Err(err) => {
                            yield Err(err);
                            return;
                        }
                    }
                    terminal_error_emitted = true;
                    break;
                }
            };
            if chunk.len()
                > response_body_limit_bytes.saturating_sub(
                    usize::try_from(upstream_bytes).unwrap_or(usize::MAX),
                )
            {
                match encode_grok_error_frame(
                    ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
                        phase: UpstreamResponseBodyPhase::Wire,
                        limit_bytes: response_body_limit_bytes,
                    }
                    .to_string(),
                ) {
                    Ok(frame) => yield Ok(frame),
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                }
                terminal_error_emitted = true;
                break;
            }
            if ttfb_ms.is_none() {
                ttfb_ms = Some(started_at.elapsed().as_millis() as u64);
            }
            if !first_chunk_telemetry_emitted {
                match encode_grok_telemetry_frame(ttfb_ms, ttfb_ms, upstream_bytes) {
                    Ok(frame) => yield Ok(frame),
                    Err(err) => {
                        yield Err(err);
                        return;
                    }
                }
                first_chunk_telemetry_emitted = true;
            }
            upstream_bytes += chunk.len() as u64;
            adapter.push_chunk(&chunk);
            match emit_grok_adapter_deltas(
                &mut client_emitter,
                &adapter,
                &mut text_len,
                &mut thinking_len,
                &mut image_len,
            ) {
                Ok(frames) => {
                    for frame in frames {
                        yield Ok(frame);
                    }
                }
                Err(err) => {
                    yield Err(err);
                    return;
                }
            }
        }

        if terminal_error_emitted {
            match encode_grok_telemetry_frame(
                ttfb_ms,
                Some(started_at.elapsed().as_millis() as u64),
                upstream_bytes,
            ) {
                Ok(frame) => yield Ok(frame),
                Err(err) => {
                    yield Err(err);
                    return;
                }
            }
            match encode_stream_frame_ndjson(&StreamFrame::eof_with_summary(None)) {
                Ok(frame) => yield Ok(frame),
                Err(err) => yield Err(err),
            }
            return;
        }

        adapter.finish();
        match emit_grok_adapter_deltas(
            &mut client_emitter,
            &adapter,
            &mut text_len,
            &mut thinking_len,
            &mut image_len,
        ) {
            Ok(frames) => {
                for frame in frames {
                    yield Ok(frame);
                }
            }
            Err(err) => {
                yield Err(err);
                return;
            }
        }

        let elapsed_ms = Some(started_at.elapsed().as_millis() as u64);
        let collected = GrokCollected {
            status_code,
            text: adapter.text,
            thinking: adapter.thinking,
            images: adapter.images,
            telemetry: ExecutionTelemetry {
                ttfb_ms,
                elapsed_ms,
                upstream_bytes: Some(upstream_bytes),
            },
        };
        let usage = grok_usage_estimate(&plan, &collected);
        match emit_grok_client_bytes(client_emitter.finish(usage)) {
            Ok(frames) => {
                for frame in frames {
                    yield Ok(frame);
                }
            }
            Err(err) => {
                yield Err(err);
                return;
            }
        }
        match encode_grok_telemetry_frame(ttfb_ms, elapsed_ms, upstream_bytes) {
            Ok(frame) => yield Ok(frame),
            Err(err) => {
                yield Err(err);
                return;
            }
        }
        match encode_stream_frame_ndjson(&StreamFrame::eof_with_summary(Some(
            grok_stream_terminal_summary(&plan, usage),
        ))) {
            Ok(frame) => yield Ok(frame),
            Err(err) => yield Err(err),
        }
    }
    .boxed()
}

async fn await_grok_stream_first_byte<T, F>(
    future: F,
    started_at: Instant,
    timeout: Option<Duration>,
) -> Result<T, Duration>
where
    F: Future<Output = T>,
{
    let Some(timeout) = timeout else {
        return Ok(future.await);
    };
    let Some(remaining) = timeout.checked_sub(started_at.elapsed()) else {
        return Err(timeout);
    };
    if remaining.is_zero() {
        return Err(timeout);
    }
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| timeout)
}

fn emit_grok_adapter_deltas(
    client_emitter: &mut GrokClientStreamEmitter,
    adapter: &GrokStreamAdapter,
    text_len: &mut usize,
    thinking_len: &mut usize,
    image_len: &mut usize,
) -> Result<Vec<Bytes>, IoError> {
    let mut out = Vec::new();
    if let Some(delta) = adapter.thinking.get(*thinking_len..) {
        if !delta.is_empty() {
            out.extend(emit_grok_client_bytes(
                client_emitter.emit_reasoning_delta(delta.to_string()),
            )?);
        }
    }
    *thinking_len = adapter.thinking.len();
    if let Some(delta) = adapter.text.get(*text_len..) {
        if !delta.is_empty() {
            out.extend(emit_grok_client_bytes(
                client_emitter.emit_text_delta(delta.to_string()),
            )?);
        }
    }
    *text_len = adapter.text.len();
    for image in adapter.images.iter().skip(*image_len) {
        out.extend(emit_grok_client_bytes(
            client_emitter.emit_image_url(image.clone()),
        )?);
    }
    *image_len = adapter.images.len();
    Ok(out)
}

fn emit_grok_client_bytes(
    body: Result<Vec<u8>, ExecutionRuntimeTransportError>,
) -> Result<Vec<Bytes>, IoError> {
    let body = body.map_err(|err| IoError::other(err.to_string()))?;
    if body.is_empty() {
        return Ok(Vec::new());
    }
    encode_grok_data_frame(&Bytes::from(body)).map(|frame| vec![frame])
}

fn encode_grok_headers_frame(
    status_code: u16,
    headers: BTreeMap<String, String>,
) -> Result<Bytes, IoError> {
    encode_stream_frame_ndjson(&StreamFrame {
        frame_type: StreamFrameType::Headers,
        payload: StreamFramePayload::Headers {
            status_code,
            headers,
            response_observation: None,
        },
    })
}

fn encode_grok_data_frame(chunk: &Bytes) -> Result<Bytes, IoError> {
    encode_stream_frame_ndjson(&StreamFrame {
        frame_type: StreamFrameType::Data,
        payload: StreamFramePayload::Data {
            chunk_b64: Some(base64::engine::general_purpose::STANDARD.encode(chunk)),
            text: None,
        },
    })
}

fn encode_grok_telemetry_frame(
    ttfb_ms: Option<u64>,
    elapsed_ms: Option<u64>,
    upstream_bytes: u64,
) -> Result<Bytes, IoError> {
    encode_stream_frame_ndjson(&StreamFrame {
        frame_type: StreamFrameType::Telemetry,
        payload: StreamFramePayload::Telemetry {
            telemetry: ExecutionTelemetry {
                ttfb_ms,
                elapsed_ms,
                upstream_bytes: Some(upstream_bytes),
            },
        },
    })
}

fn encode_grok_error_frame(message: String) -> Result<Bytes, IoError> {
    encode_stream_frame_ndjson(&StreamFrame {
        frame_type: StreamFrameType::Error,
        payload: StreamFramePayload::Error {
            error: aether_contracts::ExecutionError {
                kind: aether_contracts::ExecutionErrorKind::ProtocolError,
                phase: aether_contracts::ExecutionPhase::StreamRead,
                message,
                upstream_status: None,
                retryable: true,
                failover_recommended: true,
            },
        },
    })
}

fn encode_grok_first_byte_timeout_frame(timeout: Duration) -> Result<Bytes, IoError> {
    encode_stream_frame_ndjson(&StreamFrame {
        frame_type: StreamFrameType::Error,
        payload: StreamFramePayload::Error {
            error: aether_contracts::ExecutionError {
                kind: aether_contracts::ExecutionErrorKind::FirstByteTimeout,
                phase: aether_contracts::ExecutionPhase::FirstByte,
                message: stream_first_byte_timeout_message(timeout),
                upstream_status: None,
                retryable: true,
                failover_recommended: true,
            },
        },
    })
}

enum GrokClientStreamEmitter {
    OpenAiChat {
        id: String,
        model: String,
        emitter: OpenAIChatClientEmitter,
    },
    OpenAiResponses {
        id: String,
        model: String,
        emitter: Box<OpenAIResponsesClientEmitter>,
    },
    ClaudeMessages {
        id: String,
        model: String,
        emitter: ClaudeClientEmitter,
    },
}

impl GrokClientStreamEmitter {
    fn new(plan: &ExecutionPlan) -> Self {
        let model = plan
            .model_name
            .clone()
            .unwrap_or_else(|| "grok".to_string());
        match normalized_client_api_format(plan).as_str() {
            "openai:responses" | "openai:responses:compact" => Self::OpenAiResponses {
                id: format!("resp_{}", Uuid::new_v4()),
                model,
                emitter: Box::new(OpenAIResponsesClientEmitter::default()),
            },
            "claude:messages" => Self::ClaudeMessages {
                id: format!("msg_{}", Uuid::new_v4()),
                model,
                emitter: ClaudeClientEmitter::default(),
            },
            _ => Self::OpenAiChat {
                id: format!("chatcmpl-{}", Uuid::new_v4()),
                model,
                emitter: OpenAIChatClientEmitter::default(),
            },
        }
    }

    fn emit_text_delta(&mut self, text: String) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
        self.emit(CanonicalStreamEvent::TextDelta(text))
    }

    fn emit_reasoning_delta(
        &mut self,
        text: String,
    ) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
        self.emit(CanonicalStreamEvent::ReasoningDelta(text))
    }

    fn emit_image_url(&mut self, url: String) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
        self.emit(CanonicalStreamEvent::ContentPart(
            CanonicalContentPart::ImageUrl(url),
        ))
    }

    fn finish(
        &mut self,
        usage: GrokUsageEstimate,
    ) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
        let mut out = self.emit(CanonicalStreamEvent::Finish {
            finish_reason: Some("stop".to_string()),
            usage: Some(grok_canonical_usage(usage)),
        })?;
        out.extend(self.finish_emitter()?);
        Ok(out)
    }

    fn emit(
        &mut self,
        event: CanonicalStreamEvent,
    ) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
        let frame = self.frame(event);
        match self {
            Self::OpenAiChat { emitter, .. } => emitter.emit(frame),
            Self::OpenAiResponses { emitter, .. } => emitter.emit(frame),
            Self::ClaudeMessages { emitter, .. } => emitter.emit(frame),
        }
        .map_err(|err| ExecutionRuntimeTransportError::UpstreamRequest(err.to_string()))
    }

    fn finish_emitter(&mut self) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
        match self {
            Self::OpenAiChat { emitter, .. } => emitter.finish(),
            Self::OpenAiResponses { emitter, .. } => emitter.finish(),
            Self::ClaudeMessages { emitter, .. } => emitter.finish(),
        }
        .map_err(|err| ExecutionRuntimeTransportError::UpstreamRequest(err.to_string()))
    }

    fn frame(&self, event: CanonicalStreamEvent) -> CanonicalStreamFrame {
        match self {
            Self::OpenAiChat { id, model, .. }
            | Self::OpenAiResponses { id, model, .. }
            | Self::ClaudeMessages { id, model, .. } => CanonicalStreamFrame {
                id: id.clone(),
                model: model.clone(),
                event,
            },
        }
    }
}

fn grok_canonical_usage(usage: GrokUsageEstimate) -> StreamingCanonicalUsage {
    StreamingCanonicalUsage {
        input_tokens: usage.input_tokens,
        input_tokens_include_cache: false,
        output_tokens: usage.output_tokens,
        total_tokens: usage.input_tokens.saturating_add(usage.output_tokens),
        cache_creation_tokens: 0,
        cache_creation_ephemeral_5m_tokens: 0,
        cache_creation_ephemeral_1h_tokens: 0,
        cache_read_tokens: 0,
        reasoning_tokens: usage.reasoning_tokens,
    }
}

fn grok_standardized_usage(usage: GrokUsageEstimate) -> StandardizedUsage {
    let mut standardized = StandardizedUsage::new();
    standardized.token_source = Some(aether_contracts::UsageTokenSource::Estimated);
    standardized.input_tokens = i64::try_from(usage.input_tokens).unwrap_or(i64::MAX);
    standardized.output_tokens = i64::try_from(usage.output_tokens).unwrap_or(i64::MAX);
    standardized.reasoning_tokens = i64::try_from(usage.reasoning_tokens).unwrap_or(i64::MAX);
    standardized
}

fn grok_stream_terminal_summary(
    plan: &ExecutionPlan,
    usage: GrokUsageEstimate,
) -> ExecutionStreamTerminalSummary {
    ExecutionStreamTerminalSummary {
        standardized_usage: Some(grok_standardized_usage(usage)),
        finish_reason: Some("stop".to_string()),
        response_id: None,
        model: plan.model_name.clone(),
        provider_actual_service_tier: None,
        observed_finish: true,
        unknown_event_count: 0,
        parser_error: None,
    }
}

async fn grok_upstream_plan(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> Result<ExecutionPlan, ExecutionRuntimeTransportError> {
    let body = plan.body.json_body.as_ref().ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok runtime requires JSON request body".to_string(),
        )
    })?;
    let mapped_model = grok_upstream_model_name(report_context)?;
    let mut upstream_body = crate::ai_serving::transport::build_grok_app_chat_body(
        plan.client_api_format.as_str(),
        Some(mapped_model.as_str()),
        body,
    );
    if grok_is_image_edit_plan(plan, &upstream_body) {
        attach_grok_image_edit_references(plan, body, &mut upstream_body).await?;
    } else {
        attach_grok_uploaded_files(plan, body, &mut upstream_body).await?;
    }
    let mut upstream_plan = plan.clone();
    upstream_plan.body = RequestBody::from_json(upstream_body);
    upstream_plan.stream = true;
    upstream_plan.model_name = Some(mapped_model);
    Ok(upstream_plan)
}

fn grok_is_image_edit_plan(plan: &ExecutionPlan, upstream_body: &Value) -> bool {
    normalized_client_api_format(plan) == "openai:image"
        && upstream_body
            .get("modelName")
            .and_then(Value::as_str)
            .is_some_and(|value| value == "imagine-image-edit")
}

fn grok_upstream_model_name(
    report_context: Option<&Value>,
) -> Result<String, ExecutionRuntimeTransportError> {
    let mapped_model = report_context
        .and_then(|value| value.get("mapped_model"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ExecutionRuntimeTransportError::UpstreamRequest(
                "Grok runtime requires mapped_model in report context".to_string(),
            )
        })?;
    Ok(mapped_model.to_string())
}

async fn attach_grok_uploaded_files(
    plan: &ExecutionPlan,
    original_body: &Value,
    upstream_body: &mut Value,
) -> Result<(), ExecutionRuntimeTransportError> {
    let inputs = extract_grok_attachment_inputs(plan.client_api_format.as_str(), original_body)?;
    if inputs.is_empty() {
        return Ok(());
    }

    let mut attachment_ids = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.into_iter().enumerate() {
        let payload = resolve_grok_attachment_payload(&input, index).await?;
        let uploaded = upload_grok_attachment(plan, payload).await?;
        if !uploaded.file_id.trim().is_empty() {
            attachment_ids.push(Value::String(uploaded.file_id));
        }
    }
    if attachment_ids.is_empty() {
        return Ok(());
    }
    let Some(object) = upstream_body.as_object_mut() else {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok runtime generated non-object app-chat body".to_string(),
        ));
    };
    object.insert("fileAttachments".to_string(), Value::Array(attachment_ids));
    Ok(())
}

async fn attach_grok_image_edit_references(
    plan: &ExecutionPlan,
    original_body: &Value,
    upstream_body: &mut Value,
) -> Result<(), ExecutionRuntimeTransportError> {
    let inputs = extract_grok_attachment_inputs(plan.client_api_format.as_str(), original_body)?;
    if inputs.is_empty() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok image edit requires at least one reference image".to_string(),
        ));
    }

    let mut image_references = Vec::with_capacity(inputs.len());
    for (index, input) in inputs.into_iter().enumerate() {
        let payload = resolve_grok_attachment_payload(&input, index).await?;
        let uploaded = upload_grok_attachment(plan, payload).await?;
        let reference = resolve_grok_uploaded_asset_reference(plan, &uploaded)?;
        image_references.push(Value::String(reference));
    }
    let parent_post_id =
        create_grok_media_post(plan, grok_image_edit_prompt(upstream_body)).await?;
    set_grok_image_edit_config(upstream_body, image_references, parent_post_id)
}

fn extract_grok_attachment_inputs(
    client_api_format: &str,
    body: &Value,
) -> Result<Vec<GrokAttachmentInput>, ExecutionRuntimeTransportError> {
    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:responses" | "openai:responses:compact" => {
            extract_responses_attachment_inputs(body)
        }
        "claude:messages" => extract_claude_attachment_inputs(body),
        _ => extract_openai_chat_attachment_inputs(body),
    }
}

fn extract_openai_chat_attachment_inputs(
    body: &Value,
) -> Result<Vec<GrokAttachmentInput>, ExecutionRuntimeTransportError> {
    let mut out = Vec::new();
    if let Some(messages) = body.get("messages").and_then(Value::as_array) {
        for message in messages {
            collect_content_attachment_inputs(message.get("content"), &mut out)?;
        }
    }
    Ok(out)
}

fn extract_responses_attachment_inputs(
    body: &Value,
) -> Result<Vec<GrokAttachmentInput>, ExecutionRuntimeTransportError> {
    let mut out = Vec::new();
    collect_responses_input_attachment_inputs(body.get("input"), &mut out)?;
    Ok(out)
}

fn collect_responses_input_attachment_inputs(
    value: Option<&Value>,
    out: &mut Vec<GrokAttachmentInput>,
) -> Result<(), ExecutionRuntimeTransportError> {
    let Some(value) = value else {
        return Ok(());
    };
    match value {
        Value::Array(items) => {
            for item in items {
                if item.get("type").and_then(Value::as_str) == Some("message") {
                    collect_content_attachment_inputs(item.get("content"), out)?;
                } else {
                    collect_attachment_input_from_object(item, out)?;
                }
            }
        }
        Value::Object(_) => collect_attachment_input_from_object(value, out)?,
        _ => {}
    }
    Ok(())
}

fn extract_claude_attachment_inputs(
    body: &Value,
) -> Result<Vec<GrokAttachmentInput>, ExecutionRuntimeTransportError> {
    let mut out = Vec::new();
    if let Some(messages) = body.get("messages").and_then(Value::as_array) {
        for message in messages {
            collect_content_attachment_inputs(message.get("content"), &mut out)?;
        }
    }
    Ok(out)
}

fn collect_content_attachment_inputs(
    value: Option<&Value>,
    out: &mut Vec<GrokAttachmentInput>,
) -> Result<(), ExecutionRuntimeTransportError> {
    let Some(value) = value else {
        return Ok(());
    };
    match value {
        Value::Array(items) => {
            for item in items {
                collect_attachment_input_from_object(item, out)?;
            }
        }
        Value::Object(_) => collect_attachment_input_from_object(value, out)?,
        _ => {}
    }
    Ok(())
}

fn collect_attachment_input_from_object(
    value: &Value,
    out: &mut Vec<GrokAttachmentInput>,
) -> Result<(), ExecutionRuntimeTransportError> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    // Do this cheap shape check before parsing/copying any source field.  In
    // particular, a seventeenth Claude base64 block must not be materialized
    // into another multi-megabyte data URI merely to reject it below.
    if out.len() >= GROK_MAX_ATTACHMENT_COUNT && object_may_contain_grok_attachment(object) {
        return Err(grok_attachment_count_error());
    }
    if let Some(input) = claude_source_attachment(object)? {
        return push_grok_attachment_input(out, input);
    }
    if let Some(source) = image_url_source(object)? {
        return push_grok_attachment_input(
            out,
            GrokAttachmentInput {
                source,
                filename: None,
                mime_type: None,
            },
        );
    }
    if let Some(input) = file_source(object)? {
        push_grok_attachment_input(out, input)?;
    }
    Ok(())
}

fn push_grok_attachment_input(
    out: &mut Vec<GrokAttachmentInput>,
    input: GrokAttachmentInput,
) -> Result<(), ExecutionRuntimeTransportError> {
    if out.len() >= GROK_MAX_ATTACHMENT_COUNT {
        return Err(grok_attachment_count_error());
    }
    out.push(input);
    Ok(())
}

fn grok_attachment_count_error() -> ExecutionRuntimeTransportError {
    ExecutionRuntimeTransportError::UpstreamRequest(format!(
        "Grok requests support at most {GROK_MAX_ATTACHMENT_COUNT} attachments"
    ))
}

fn object_may_contain_grok_attachment(object: &Map<String, Value>) -> bool {
    if object.contains_key("image_url")
        || object.contains_key("file_data")
        || object.contains_key("file_url")
        || object.contains_key("file")
    {
        return true;
    }
    object
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .is_some_and(|value| {
            [
                "image_url",
                "input_image",
                "input_file",
                "file",
                "image",
                "document",
            ]
            .iter()
            .any(|candidate| value.eq_ignore_ascii_case(candidate))
        })
}

fn image_url_source(
    object: &Map<String, Value>,
) -> Result<Option<String>, ExecutionRuntimeTransportError> {
    if let Some(source) = object
        .get("image_url")
        .map(string_or_url_value)
        .transpose()?
        .flatten()
    {
        return Ok(Some(source));
    }
    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("image_url"))
    {
        return bounded_grok_attachment_source(object.get("url").and_then(Value::as_str))
            .map(|value| value.map(ToOwned::to_owned));
    }
    if object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| value.eq_ignore_ascii_case("input_image"))
    {
        return object
            .get("image_url")
            .or_else(|| object.get("source"))
            .map(string_or_url_value)
            .transpose()
            .map(Option::flatten);
    }
    Ok(None)
}

fn file_source(
    object: &Map<String, Value>,
) -> Result<Option<GrokAttachmentInput>, ExecutionRuntimeTransportError> {
    let file_object = object.get("file").and_then(Value::as_object);
    let raw_source = file_object
        .and_then(|file| {
            file.get("file_data")
                .or_else(|| file.get("data"))
                .or_else(|| file.get("url"))
                .or_else(|| file.get("file_url"))
                .and_then(Value::as_str)
        })
        .or_else(|| object.get("file_data").and_then(Value::as_str))
        .or_else(|| object.get("file_url").and_then(Value::as_str))
        .or_else(|| object.get("data").and_then(Value::as_str));
    let Some(source) = bounded_grok_attachment_source(raw_source)? else {
        return Ok(None);
    };
    let raw_filename = file_object
        .and_then(|file| file.get("filename").or_else(|| file.get("name")))
        .or_else(|| object.get("filename"))
        .or_else(|| object.get("name"))
        .and_then(Value::as_str);
    let raw_mime_type = file_object
        .and_then(|file| file.get("mime_type").or_else(|| file.get("mimeType")))
        .or_else(|| object.get("mime_type"))
        .or_else(|| object.get("mimeType"))
        .and_then(Value::as_str);
    let filename = bounded_grok_attachment_field(
        raw_filename,
        "filename",
        GROK_MAX_ATTACHMENT_FILENAME_BYTES,
    )?
    .map(ToOwned::to_owned);
    let mime_type = bounded_grok_attachment_field(
        raw_mime_type,
        "MIME type",
        GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES,
    )?
    .map(ToOwned::to_owned);
    Ok(Some(GrokAttachmentInput {
        source: source.to_owned(),
        filename,
        mime_type,
    }))
}

fn claude_source_attachment(
    object: &Map<String, Value>,
) -> Result<Option<GrokAttachmentInput>, ExecutionRuntimeTransportError> {
    let block_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if !matches!(block_type, "image" | "document") {
        return Ok(None);
    }
    let Some(source) = object.get("source").and_then(Value::as_object) else {
        return Ok(None);
    };
    let source_type = source
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let raw_mime_type = source
        .get("media_type")
        .or_else(|| source.get("mediaType"))
        .and_then(Value::as_str);
    let raw_filename = object
        .get("filename")
        .or_else(|| object.get("name"))
        .and_then(Value::as_str);
    let mime_type = bounded_grok_attachment_field(
        raw_mime_type,
        "MIME type",
        GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES,
    )?
    .map(ToOwned::to_owned);
    let filename = bounded_grok_attachment_field(
        raw_filename,
        "filename",
        GROK_MAX_ATTACHMENT_FILENAME_BYTES,
    )?
    .map(ToOwned::to_owned);

    match source_type {
        "base64" => {
            let Some(data) = bounded_grok_attachment_field(
                source.get("data").and_then(Value::as_str),
                "base64 data",
                maximum_base64_len_for_decoded_limit(GROK_MAX_ATTACHMENT_BYTES),
            )?
            else {
                return Ok(None);
            };
            let mime = mime_type.as_deref().unwrap_or("application/octet-stream");
            let mut data_uri = String::with_capacity(
                mime.len()
                    .saturating_add(data.len())
                    .saturating_add("data:;base64,".len()),
            );
            data_uri.push_str("data:");
            data_uri.push_str(mime);
            data_uri.push_str(";base64,");
            data_uri.push_str(data);
            Ok(Some(GrokAttachmentInput {
                source: data_uri,
                filename,
                mime_type,
            }))
        }
        "url" => {
            let Some(url) =
                bounded_grok_attachment_source(source.get("url").and_then(Value::as_str))?
            else {
                return Ok(None);
            };
            Ok(Some(GrokAttachmentInput {
                source: url.to_owned(),
                filename,
                mime_type,
            }))
        }
        _ => Ok(None),
    }
}

fn string_or_url_value(value: &Value) -> Result<Option<String>, ExecutionRuntimeTransportError> {
    let raw = value
        .as_str()
        .or_else(|| value.get("url").and_then(Value::as_str));
    bounded_grok_attachment_source(raw).map(|value| value.map(ToOwned::to_owned))
}

fn bounded_grok_attachment_source(
    raw: Option<&str>,
) -> Result<Option<&str>, ExecutionRuntimeTransportError> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let max_bytes = if grok_is_data_uri(value) {
        let metadata_bytes = value.find(',').unwrap_or(value.len());
        if metadata_bytes > GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES {
            return Err(grok_attachment_field_too_large(
                "data URI metadata",
                GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES,
            ));
        }
        maximum_base64_len_for_decoded_limit(GROK_MAX_ATTACHMENT_BYTES)
            .saturating_add(GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES)
    } else {
        GROK_MAX_ATTACHMENT_URL_BYTES
    };
    bounded_grok_attachment_field(Some(value), "source", max_bytes)
}

fn bounded_grok_attachment_field<'a>(
    raw: Option<&'a str>,
    field: &str,
    max_bytes: usize,
) -> Result<Option<&'a str>, ExecutionRuntimeTransportError> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.len() > max_bytes {
        return Err(grok_attachment_field_too_large(field, max_bytes));
    }
    Ok(Some(value))
}

fn grok_attachment_field_too_large(
    field: &str,
    max_bytes: usize,
) -> ExecutionRuntimeTransportError {
    ExecutionRuntimeTransportError::UpstreamRequest(format!(
        "Grok attachment {field} exceeds {max_bytes} byte limit"
    ))
}

fn trimmed_string(value: &str) -> String {
    value.trim().to_string()
}

async fn resolve_grok_attachment_payload(
    input: &GrokAttachmentInput,
    index: usize,
) -> Result<GrokAttachmentPayload, ExecutionRuntimeTransportError> {
    validate_grok_attachment_input_fields(input)?;
    if grok_is_data_uri(input.source.as_str()) {
        return grok_attachment_payload_from_data_uri(input, index);
    }
    grok_attachment_payload_from_url(input, index).await
}

fn validate_grok_attachment_input_fields(
    input: &GrokAttachmentInput,
) -> Result<(), ExecutionRuntimeTransportError> {
    let source = input.source.trim();
    if source.is_empty() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment source is empty".to_string(),
        ));
    }
    let max_source_bytes = if grok_is_data_uri(source) {
        let metadata_bytes = source.find(',').unwrap_or(source.len());
        if metadata_bytes > GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES {
            return Err(grok_attachment_field_too_large(
                "data URI metadata",
                GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES,
            ));
        }
        maximum_base64_len_for_decoded_limit(GROK_MAX_ATTACHMENT_BYTES)
            .saturating_add(GROK_MAX_ATTACHMENT_DATA_URI_METADATA_BYTES)
    } else {
        GROK_MAX_ATTACHMENT_URL_BYTES
    };
    if source.len() > max_source_bytes {
        return Err(grok_attachment_field_too_large("source", max_source_bytes));
    }
    if input
        .filename
        .as_deref()
        .is_some_and(|filename| filename.trim().len() > GROK_MAX_ATTACHMENT_FILENAME_BYTES)
    {
        return Err(grok_attachment_field_too_large(
            "filename",
            GROK_MAX_ATTACHMENT_FILENAME_BYTES,
        ));
    }
    if input
        .mime_type
        .as_deref()
        .is_some_and(|mime_type| mime_type.trim().len() > GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES)
    {
        return Err(grok_attachment_field_too_large(
            "MIME type",
            GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES,
        ));
    }
    Ok(())
}

fn grok_attachment_payload_from_data_uri(
    input: &GrokAttachmentInput,
    index: usize,
) -> Result<GrokAttachmentPayload, ExecutionRuntimeTransportError> {
    grok_attachment_payload_from_data_uri_with_limit(input, index, GROK_MAX_ATTACHMENT_BYTES)
}

fn grok_attachment_payload_from_data_uri_with_limit(
    input: &GrokAttachmentInput,
    index: usize,
    limit_bytes: usize,
) -> Result<GrokAttachmentPayload, ExecutionRuntimeTransportError> {
    let source = input.source.trim();
    let (header, content_b64) = source.split_once(',').ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment data URI is missing comma separator".to_string(),
        )
    })?;
    if !header
        .split(';')
        .skip(1)
        .any(|parameter| parameter.trim().eq_ignore_ascii_case("base64"))
    {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment data URI must be base64 encoded".to_string(),
        ));
    }
    let header_mime = header
        .get(..5)
        .filter(|prefix| prefix.eq_ignore_ascii_case("data:"))
        .and_then(|_| header.get(5..))
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if header_mime.is_some_and(|value| value.len() > GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES) {
        return Err(grok_attachment_field_too_large(
            "MIME type",
            GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES,
        ));
    }
    let mime_type = input
        .mime_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| header_mime.map(ToOwned::to_owned))
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let max_base64_len = maximum_base64_len_for_decoded_limit(limit_bytes);
    let normalized_b64 = normalize_base64_with_limit(content_b64, max_base64_len)
        .map_err(|_| grok_attachment_too_large(limit_bytes))?;
    let decoded_len = base64::engine::general_purpose::STANDARD
        .decode(&normalized_b64)
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format!(
                "Grok attachment data URI base64 is invalid: {err}"
            ))
        })?
        .len();
    if decoded_len > limit_bytes {
        return Err(grok_attachment_too_large(limit_bytes));
    }
    Ok(GrokAttachmentPayload {
        filename: input
            .filename
            .clone()
            .unwrap_or_else(|| default_attachment_filename(index, &mime_type)),
        mime_type,
        content_b64: normalized_b64,
    })
}

fn normalize_base64_with_limit(input: &str, limit_bytes: usize) -> Result<String, ()> {
    // Do not reserve based on the untrusted URI length.  Whitespace is ignored,
    // so an attacker could otherwise force a large allocation before validation.
    let mut normalized = String::with_capacity(limit_bytes.min(4096));
    for character in input.chars() {
        if character.is_whitespace() {
            continue;
        }
        if normalized.len().saturating_add(character.len_utf8()) > limit_bytes {
            return Err(());
        }
        normalized.push(character);
    }
    Ok(normalized)
}

fn maximum_base64_len_for_decoded_limit(limit_bytes: usize) -> usize {
    limit_bytes
        .saturating_add(2)
        .checked_div(3)
        .unwrap_or(usize::MAX)
        .saturating_mul(4)
}

async fn grok_attachment_payload_from_url(
    input: &GrokAttachmentInput,
    index: usize,
) -> Result<GrokAttachmentPayload, ExecutionRuntimeTransportError> {
    let url = reqwest::Url::parse(input.source.as_str()).map_err(|err| {
        ExecutionRuntimeTransportError::UpstreamRequest(format!(
            "Grok attachment URL is invalid: {err}"
        ))
    })?;
    validate_grok_attachment_url(&url)?;
    let response = fetch_grok_attachment_url(url.clone(), 0).await?;
    let final_url = response.url().clone();
    let response_mime_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next());
    let response_mime_type = bounded_grok_attachment_field(
        response_mime_type,
        "MIME type",
        GROK_MAX_ATTACHMENT_MIME_TYPE_BYTES,
    )?
    .map(ToOwned::to_owned);
    let mime_type = input
        .mime_type
        .clone()
        .or(response_mime_type)
        .unwrap_or_else(|| "application/octet-stream".to_string());
    let bytes = collect_grok_attachment_url_bytes(response).await?;
    Ok(GrokAttachmentPayload {
        filename: input
            .filename
            .clone()
            .or_else(|| filename_from_url_path(final_url.path()))
            .unwrap_or_else(|| default_attachment_filename(index, &mime_type)),
        mime_type,
        content_b64: base64::engine::general_purpose::STANDARD.encode(&bytes),
    })
}

async fn fetch_grok_attachment_url(
    mut url: reqwest::Url,
    mut redirects: usize,
) -> Result<reqwest::Response, ExecutionRuntimeTransportError> {
    loop {
        // Validate every hop. A relative Location may inherit credentials or
        // a fragment from the previous URL, while an absolute Location can
        // introduce either explicitly.
        validate_grok_attachment_url(&url)?;
        let public_addrs = public_socket_addrs_for_url(&url).await?;
        let response = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(url.host_str().unwrap_or_default(), &public_addrs)
            .build()
            .map_err(ExecutionRuntimeTransportError::ClientBuild)?
            .get(url.clone())
            .send()
            .await
            .map_err(|err| {
                ExecutionRuntimeTransportError::UpstreamRequest(format_upstream_request_error(&err))
            })?;
        if response.status().is_redirection() {
            redirects += 1;
            if redirects > GROK_MAX_ATTACHMENT_REDIRECTS {
                return Err(ExecutionRuntimeTransportError::UpstreamRequest(
                    "Grok attachment URL fetch exceeded redirect limit".to_string(),
                ));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    ExecutionRuntimeTransportError::UpstreamRequest(
                        "Grok attachment redirect is missing Location header".to_string(),
                    )
                })?;
            url = url.join(location).map_err(|err| {
                ExecutionRuntimeTransportError::UpstreamRequest(format!(
                    "Grok attachment redirect URL is invalid: {err}"
                ))
            })?;
            continue;
        }
        if !response.status().is_success() {
            return Err(ExecutionRuntimeTransportError::UpstreamRequest(format!(
                "Grok attachment URL fetch returned {}",
                response.status().as_u16()
            )));
        }
        return Ok(response);
    }
}

fn validate_grok_attachment_url(url: &reqwest::Url) -> Result<(), ExecutionRuntimeTransportError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL must use http or https".to_string(),
        ));
    }
    if url.host_str().is_none() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL is missing a host".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL must not contain credentials".to_string(),
        ));
    }
    if url.fragment().is_some() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL must not contain a fragment".to_string(),
        ));
    }
    Ok(())
}

async fn public_socket_addrs_for_url(
    url: &reqwest::Url,
) -> Result<Vec<std::net::SocketAddr>, ExecutionRuntimeTransportError> {
    let host = url.host_str().ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL is missing a host".to_string(),
        )
    })?;
    let port = url.port_or_known_default().ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL is missing a port".to_string(),
        )
    })?;
    let addresses =
        aether_http::lookup_host_with_limits(host, port, aether_http::DEFAULT_DNS_LOOKUP_TIMEOUT)
            .await
            .map_err(|err| {
                ExecutionRuntimeTransportError::UpstreamRequest(format!(
                    "Grok attachment URL DNS resolution failed: {err}"
                ))
            })?;
    validate_grok_attachment_addresses(addresses)
}

fn validate_grok_attachment_addresses(
    addresses: Vec<std::net::SocketAddr>,
) -> Result<Vec<std::net::SocketAddr>, ExecutionRuntimeTransportError> {
    if addresses.is_empty() {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL DNS resolution returned no addresses".to_string(),
        ));
    }
    if addresses
        .iter()
        .any(|address| !grok_attachment_ip_is_public(address.ip()))
    {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok attachment URL resolves to a non-public address".to_string(),
        ));
    }
    Ok(addresses)
}

fn grok_attachment_ip_is_public(ip: IpAddr) -> bool {
    !aether_http::is_private_or_reserved_ip(ip)
}

async fn collect_grok_attachment_url_bytes(
    response: reqwest::Response,
) -> Result<Vec<u8>, ExecutionRuntimeTransportError> {
    if response
        .content_length()
        .is_some_and(|length| length > GROK_MAX_ATTACHMENT_BYTES as u64)
    {
        return Err(grok_attachment_too_large(GROK_MAX_ATTACHMENT_BYTES));
    }
    let mut bytes = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format_upstream_request_error(&err))
        })?;
        if chunk.len() > GROK_MAX_ATTACHMENT_BYTES.saturating_sub(bytes.len()) {
            return Err(grok_attachment_too_large(GROK_MAX_ATTACHMENT_BYTES));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn grok_attachment_too_large(limit_bytes: usize) -> ExecutionRuntimeTransportError {
    ExecutionRuntimeTransportError::UpstreamRequest(format!(
        "Grok attachment exceeds {limit_bytes} byte limit"
    ))
}

async fn upload_grok_attachment(
    plan: &ExecutionPlan,
    payload: GrokAttachmentPayload,
) -> Result<GrokUploadedAttachment, ExecutionRuntimeTransportError> {
    let body = json!({
        "fileName": payload.filename,
        "fileMimeType": payload.mime_type,
        "content": payload.content_b64,
    });
    let mut upload_plan = plan.clone();
    upload_plan.url = grok_upload_url(plan.url.as_str());
    upload_plan.method = "POST".to_string();
    upload_plan.stream = false;
    upload_plan.content_type = Some("application/json".to_string());
    upload_plan.body = RequestBody::from_json(body);
    upload_plan.headers = grok_upload_headers(&plan.headers)?;
    let request_body = build_request_body(&upload_plan)?;
    let response = send_request(&upload_plan, request_body).await?;
    let status_code = response.status_code();
    let bytes = response
        .bytes_with_limit(execution_plan_response_body_limit_bytes(&upload_plan))
        .await?;
    if !(200..300).contains(&status_code) {
        return Err(grok_auxiliary_http_error("attachment upload", status_code));
    }
    let value = serde_json::from_slice::<Value>(&bytes)
        .map_err(ExecutionRuntimeTransportError::InvalidJson)?;
    let file_id = value
        .get("fileMetadataId")
        .or_else(|| value.get("fileId"))
        .and_then(Value::as_str)
        .map(trimmed_string)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ExecutionRuntimeTransportError::UpstreamRequest(
                "Grok attachment upload response is missing fileMetadataId".to_string(),
            )
        })?;
    let file_uri = value
        .get("fileUri")
        .and_then(Value::as_str)
        .map(trimmed_string)
        .filter(|value| !value.is_empty());
    Ok(GrokUploadedAttachment { file_id, file_uri })
}

async fn create_grok_media_post(
    plan: &ExecutionPlan,
    prompt: String,
) -> Result<String, ExecutionRuntimeTransportError> {
    let body = json!({
        "mediaType": "MEDIA_POST_TYPE_IMAGE",
        "prompt": prompt,
    });
    let mut media_plan = plan.clone();
    media_plan.url = grok_media_post_url(plan.url.as_str());
    media_plan.method = "POST".to_string();
    media_plan.stream = false;
    media_plan.content_type = Some("application/json".to_string());
    media_plan.body = RequestBody::from_json(body);
    media_plan.headers = grok_upload_headers(&plan.headers)?;
    media_plan.headers.insert(
        "referer".to_string(),
        grok_imagine_referer(plan.url.as_str()),
    );
    let request_body = build_request_body(&media_plan)?;
    let response = send_request(&media_plan, request_body).await?;
    let status_code = response.status_code();
    let bytes = response
        .bytes_with_limit(execution_plan_response_body_limit_bytes(&media_plan))
        .await?;
    if !(200..300).contains(&status_code) {
        return Err(grok_auxiliary_http_error("media post create", status_code));
    }
    let value = serde_json::from_slice::<Value>(&bytes)
        .map_err(ExecutionRuntimeTransportError::InvalidJson)?;
    value
        .get("post")
        .and_then(|post| post.get("id"))
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .map(trimmed_string)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            ExecutionRuntimeTransportError::UpstreamRequest(
                "Grok media post create response is missing post id".to_string(),
            )
        })
}

fn grok_upload_headers(
    headers: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, String>, ExecutionRuntimeTransportError> {
    let mut out = headers.clone();
    out.insert("accept".to_string(), "application/json".to_string());
    out.insert("content-type".to_string(), "application/json".to_string());
    out.insert("sec-fetch-dest".to_string(), "empty".to_string());
    out.insert("sec-fetch-mode".to_string(), "cors".to_string());
    out.insert("sec-fetch-site".to_string(), "same-origin".to_string());
    out.insert("x-xai-request-id".to_string(), Uuid::new_v4().to_string());
    Ok(out)
}

fn grok_upload_url(chat_url: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(chat_url) else {
        return format!("https://grok.com{GROK_UPLOAD_PATH}");
    };
    url.set_path(GROK_UPLOAD_PATH);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn grok_media_post_url(chat_url: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(chat_url) else {
        return format!("https://grok.com{GROK_MEDIA_POST_PATH}");
    };
    url.set_path(GROK_MEDIA_POST_PATH);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn grok_imagine_referer(chat_url: &str) -> String {
    let Ok(mut url) = reqwest::Url::parse(chat_url) else {
        return "https://grok.com/imagine".to_string();
    };
    url.set_path("/imagine");
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

fn resolve_grok_uploaded_asset_reference(
    plan: &ExecutionPlan,
    uploaded: &GrokUploadedAttachment,
) -> Result<String, ExecutionRuntimeTransportError> {
    if let Some(file_uri) = uploaded.file_uri.as_deref() {
        if !file_uri.trim().is_empty() {
            return Ok(grok_asset_url(file_uri));
        }
    }
    let user_id = grok_user_id_from_cookie_header(&plan.headers).ok_or_else(|| {
        ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok image edit upload response is missing fileUri and cookie x-userid is unavailable"
                .to_string(),
        )
    })?;
    Ok(format!(
        "{GROK_ASSET_BASE}users/{}/{}/content",
        user_id, uploaded.file_id
    ))
}

fn grok_asset_url(value: &str) -> String {
    let value = value.trim();
    if value.starts_with("http://") || value.starts_with("https://") {
        value.to_string()
    } else {
        format!("{GROK_ASSET_BASE}{}", value.trim_start_matches('/'))
    }
}

fn grok_user_id_from_cookie_header(headers: &BTreeMap<String, String>) -> Option<String> {
    let cookie = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("cookie"))
        .map(|(_, value)| value.as_str())?;
    cookie.split(';').find_map(|part| {
        let (name, value) = part.trim().split_once('=')?;
        (name.trim() == "x-userid")
            .then(|| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn grok_image_edit_prompt(upstream_body: &Value) -> String {
    upstream_body
        .get("message")
        .and_then(Value::as_str)
        .map(trimmed_string)
        .unwrap_or_default()
}

fn grok_image_prompt_from_provider_body(body: &Value) -> Option<String> {
    body.get("prompt")
        .and_then(Value::as_str)
        .map(trimmed_string)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            body.get("input")
                .and_then(grok_value_text)
                .map(|value| trimmed_string(&value))
        })
        .or_else(|| grok_last_user_message_text(body))
        .filter(|value| !value.is_empty())
}

fn grok_image_count_from_provider_body(body: &Value) -> usize {
    body.get("n")
        .and_then(Value::as_u64)
        .or_else(|| body.get("imageGenerationCount").and_then(Value::as_u64))
        .or_else(|| {
            body.get("image_config")
                .and_then(|config| {
                    config
                        .get("n")
                        .or_else(|| config.get("imageGenerationCount"))
                })
                .and_then(Value::as_u64)
        })
        .or_else(|| {
            body.get("tools")
                .and_then(Value::as_array)
                .and_then(|tools| tools.first())
                .and_then(|tool| tool.get("n").or_else(|| tool.get("imageGenerationCount")))
                .and_then(Value::as_u64)
        })
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(1)
        .clamp(1, GROK_MAX_IMAGE_COUNT)
}

fn grok_aspect_ratio_from_provider_body(body: &Value) -> String {
    let size_or_ratio = grok_image_option_from_provider_body(body, "aspect_ratio")
        .or_else(|| grok_image_option_from_provider_body(body, "aspectRatio"))
        .or_else(|| grok_image_option_from_provider_body(body, "ratio"))
        .or_else(|| grok_image_option_from_provider_body(body, "size"))
        .unwrap_or("1024x1024");
    match size_or_ratio {
        "1280x720" | "16:9" => "16:9",
        "720x1280" | "9:16" => "9:16",
        "1792x1024" | "3:2" => "3:2",
        "1024x1792" | "2:3" => "2:3",
        "1024x1024" | "1:1" => "1:1",
        _ => "2:3",
    }
    .to_string()
}

fn grok_image_option_from_provider_body<'a>(body: &'a Value, key: &str) -> Option<&'a str> {
    body.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            body.get("image_config")
                .and_then(|config| config.get(key))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            body.get("tools")
                .and_then(Value::as_array)
                .and_then(|tools| tools.first())
                .and_then(|tool| tool.get(key))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
}

fn grok_last_user_message_text(body: &Value) -> Option<String> {
    body.get("messages")
        .and_then(Value::as_array)?
        .iter()
        .rev()
        .find_map(|message| {
            let role = message.get("role").and_then(Value::as_str)?;
            if !role.eq_ignore_ascii_case("user") {
                return None;
            }
            message
                .get("content")
                .and_then(grok_value_text)
                .map(|value| trimmed_string(&value))
                .filter(|value| !value.is_empty())
        })
}

fn grok_value_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(items) => {
            let parts = items
                .iter()
                .filter_map(grok_value_text)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
                .collect::<Vec<_>>();
            (!parts.is_empty()).then(|| parts.join("\n"))
        }
        Value::Object(object) => object
            .get("text")
            .or_else(|| object.get("input_text"))
            .or_else(|| object.get("content"))
            .and_then(grok_value_text),
        _ => None,
    }
}

fn grok_imagine_reset_message() -> Value {
    json!({
        "type": "conversation.item.create",
        "timestamp": current_unix_secs().saturating_mul(1000),
        "item": {
            "type": "message",
            "content": [{"type": "reset"}],
        },
    })
}

fn grok_imagine_request_message(prompt: &str, aspect_ratio: &str, enable_pro: bool) -> Value {
    json!({
        "type": "conversation.item.create",
        "timestamp": current_unix_secs().saturating_mul(1000),
        "item": {
            "type": "message",
            "content": [{
                "requestId": Uuid::new_v4().to_string(),
                "text": prompt,
                "type": "input_text",
                "properties": {
                    "section_count": 0,
                    "is_kids_mode": false,
                    "enable_nsfw": true,
                    "skip_upsampler": false,
                    "enable_side_by_side": true,
                    "is_initial": false,
                    "aspect_ratio": aspect_ratio,
                    "enable_pro": enable_pro,
                },
            }],
        },
    })
}

fn grok_handle_imagine_ws_message(
    value: &Value,
    slots: &mut BTreeMap<String, GrokImagineImage>,
) -> Result<(), ExecutionRuntimeTransportError> {
    grok_handle_imagine_ws_message_with_slot_limit(value, slots, GROK_MAX_IMAGINE_SLOTS)
}

fn grok_handle_imagine_ws_message_with_slot_limit(
    value: &Value,
    slots: &mut BTreeMap<String, GrokImagineImage>,
    max_slots: usize,
) -> Result<(), ExecutionRuntimeTransportError> {
    match value.get("type").and_then(Value::as_str) {
        Some("json") => grok_handle_imagine_json_frame(value, slots, max_slots),
        Some("image") => {
            grok_handle_imagine_image_frame(value, slots, max_slots);
            Ok(())
        }
        Some("error") => Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok Imagine websocket returned an error".to_string(),
        )),
        _ => Ok(()),
    }
}

fn grok_auxiliary_http_error(stage: &str, status_code: u16) -> ExecutionRuntimeTransportError {
    ExecutionRuntimeTransportError::UpstreamRequest(format!(
        "Grok {stage} returned HTTP {status_code}"
    ))
}

fn grok_upstream_http_error_message(status_code: u16) -> String {
    format!("Grok upstream request returned HTTP {status_code}")
}

fn grok_handle_imagine_json_frame(
    value: &Value,
    slots: &mut BTreeMap<String, GrokImagineImage>,
    max_slots: usize,
) -> Result<(), ExecutionRuntimeTransportError> {
    let status = value.get("current_status").and_then(Value::as_str);
    let Some(image_id) = value
        .get("image_id")
        .or_else(|| value.get("job_id"))
        .and_then(Value::as_str)
        .map(trimmed_string)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    let order = value
        .get("order")
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    if !slots.contains_key(&image_id) && slots.len() >= max_slots {
        return Ok(());
    }
    match status {
        Some("start_stage") => {
            slots.entry(image_id.clone()).or_insert(GrokImagineImage {
                order,
                url: None,
                blob_b64: None,
                done: false,
                moderated: false,
            });
        }
        Some("completed") => {
            let slot = slots.entry(image_id.clone()).or_insert(GrokImagineImage {
                order,
                url: None,
                blob_b64: None,
                done: false,
                moderated: false,
            });
            slot.order = order;
            slot.done = true;
            slot.moderated = value
                .get("moderated")
                .and_then(Value::as_bool)
                .unwrap_or(false);
        }
        _ => {}
    }
    Ok(())
}

fn grok_handle_imagine_image_frame(
    value: &Value,
    slots: &mut BTreeMap<String, GrokImagineImage>,
    max_slots: usize,
) {
    let Some(raw_url) = value
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    if raw_url.len() > GROK_MAX_ATTACHMENT_URL_BYTES {
        return;
    }
    let url = grok_asset_url(raw_url);
    if url.len() > GROK_MAX_ATTACHMENT_URL_BYTES {
        return;
    }
    let image_id =
        grok_imagine_image_id_from_url(&url).unwrap_or_else(|| Uuid::new_v4().to_string());
    if !slots.contains_key(&image_id) && slots.len() >= max_slots {
        return;
    }
    let blob = value
        .get("blob")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let Some(blob) = blob else {
        let fallback_order = slots.len();
        let slot = slots.entry(image_id.clone()).or_insert(GrokImagineImage {
            order: fallback_order,
            url: None,
            blob_b64: None,
            done: false,
            moderated: false,
        });
        slot.url = Some(url);
        return;
    };
    let existing_blob_len = slots
        .get(&image_id)
        .and_then(|slot| slot.blob_b64.as_ref())
        .map(String::len)
        .unwrap_or(0);
    if !grok_imagine_blob_can_be_retained(slots, existing_blob_len, blob.len()) {
        return;
    }
    let fallback_order = slots.len();
    let slot = slots.entry(image_id.clone()).or_insert(GrokImagineImage {
        order: fallback_order,
        url: None,
        blob_b64: None,
        done: false,
        moderated: false,
    });
    slot.url = Some(url);
    slot.blob_b64 = Some(blob.to_owned());
}

fn grok_imagine_blob_can_be_retained(
    slots: &BTreeMap<String, GrokImagineImage>,
    existing_blob_len: usize,
    incoming_blob_len: usize,
) -> bool {
    let retained_blob_bytes = slots.values().fold(0usize, |total, slot| {
        total.saturating_add(slot.blob_b64.as_ref().map(String::len).unwrap_or(0))
    });
    grok_imagine_blob_lengths_can_be_retained(
        retained_blob_bytes,
        existing_blob_len,
        incoming_blob_len,
    )
}

fn grok_imagine_blob_lengths_can_be_retained(
    retained_blob_bytes: usize,
    existing_blob_len: usize,
    incoming_blob_len: usize,
) -> bool {
    let per_blob_limit = maximum_base64_len_for_decoded_limit(GROK_MAX_ATTACHMENT_BYTES);
    if incoming_blob_len > per_blob_limit {
        return false;
    }
    let total_blob_limit =
        maximum_base64_len_for_decoded_limit(GROK_MAX_IMAGINE_BLOB_TOTAL_DECODED_BYTES);
    retained_blob_bytes
        .saturating_sub(existing_blob_len)
        .saturating_add(incoming_blob_len)
        <= total_blob_limit
}

fn grok_imagine_image_id_from_url(url: &str) -> Option<String> {
    let path = reqwest::Url::parse(url)
        .ok()
        .map(|url| url.path().to_string())
        .unwrap_or_else(|| url.to_string());
    let file_name = path.rsplit('/').next()?;
    let (stem, _) = file_name.rsplit_once('.')?;
    (!stem.trim().is_empty()).then(|| stem.to_string())
}

fn grok_imagine_completed_count(slots: &BTreeMap<String, GrokImagineImage>) -> usize {
    slots
        .values()
        .filter(|image| {
            image.done && !image.moderated && (image.url.is_some() || image.blob_b64.is_some())
        })
        .count()
}

fn grok_data_image_url(blob_b64: String) -> Option<String> {
    // A websocket `blob` is untrusted provider data.  Do not pass it through
    // as an arbitrary data URL: malformed base64 and active formats such as
    // SVG/HTML could otherwise cross the public image boundary.  Validate the
    // encoded and decoded sizes plus the raster magic before retaining it.
    // Bound the interpolation itself before constructing the candidate URL;
    // otherwise formatting would allocate from an attacker-controlled blob
    // before the parser has a chance to reject it.
    if blob_b64.len() > maximum_base64_len_for_decoded_limit(GROK_MAX_ATTACHMENT_BYTES) {
        return None;
    }
    let mut candidate = String::with_capacity(22usize.saturating_add(blob_b64.len()));
    candidate.push_str("data:image/png;base64,");
    candidate.push_str(&blob_b64);
    grok_data_image_parts(&candidate)?;
    Some(candidate)
}

fn set_grok_image_edit_config(
    upstream_body: &mut Value,
    image_references: Vec<Value>,
    parent_post_id: String,
) -> Result<(), ExecutionRuntimeTransportError> {
    let Some(config) = upstream_body
        .get_mut("responseMetadata")
        .and_then(|value| value.get_mut("modelConfigOverride"))
        .and_then(|value| value.get_mut("modelMap"))
        .and_then(|value| value.get_mut("imageEditModelConfig"))
        .and_then(Value::as_object_mut)
    else {
        return Err(ExecutionRuntimeTransportError::UpstreamRequest(
            "Grok image edit payload is missing imageEditModelConfig".to_string(),
        ));
    };
    config.insert(
        "imageReferences".to_string(),
        Value::Array(image_references),
    );
    config.insert("parentPostId".to_string(), Value::String(parent_post_id));
    Ok(())
}

fn filename_from_url_path(path: &str) -> Option<String> {
    let filename = path.rsplit('/').next()?.trim();
    if filename.is_empty() || filename.len() > GROK_MAX_ATTACHMENT_FILENAME_BYTES {
        return None;
    }
    Some(filename.to_owned())
}

fn default_attachment_filename(index: usize, mime_type: &str) -> String {
    let ext = mime_type
        .rsplit('/')
        .next()
        .map(|value| value.split('+').next().unwrap_or(value))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|value| value.len() <= GROK_MAX_ATTACHMENT_FILENAME_BYTES)
        .unwrap_or("bin");
    format!("file-{}.{}", index + 1, ext)
}

fn grok_execution_result(
    plan: &ExecutionPlan,
    collected: GrokCollected,
    report_context: Option<&Value>,
) -> ExecutionResult {
    let status_code = collected.status_code;
    let body_json = if (200..300).contains(&status_code) {
        grok_client_json_body(plan, &collected, report_context)
    } else {
        json!({
            "error": {
                "message": grok_upstream_http_error_message(status_code),
                "type": "grok_upstream_error",
                "code": status_code,
            }
        })
    };
    ExecutionResult {
        request_id: plan.request_id.clone(),
        candidate_id: plan.candidate_id.clone(),
        status_code,
        headers: BTreeMap::from([("content-type".to_string(), "application/json".to_string())]),
        response_observation: None,
        body: Some(ResponseBody {
            json_body: Some(body_json),
            body_bytes_b64: None,
        }),
        telemetry: Some(collected.telemetry),
        error: None,
    }
}

fn grok_client_json_body(
    plan: &ExecutionPlan,
    collected: &GrokCollected,
    report_context: Option<&Value>,
) -> Value {
    let model = plan.model_name.as_deref().unwrap_or("grok");
    let usage = grok_usage_estimate(plan, collected);
    let client_format = normalized_client_api_format(plan);
    if client_format == "openai:image" {
        return openai_image_body(collected);
    }

    let provider_body = openai_responses_body(
        model,
        collected,
        usage,
        grok_plan_uses_structured_image_generation(plan, report_context),
    );
    if client_format == "openai:responses" {
        return provider_body;
    }

    convert_standard_chat_response(
        &provider_body,
        GROK_STANDARD_PROVIDER_API_FORMAT,
        client_format.as_str(),
        &grok_conversion_report_context(plan, model),
    )
    .unwrap_or_else(|| {
        grok_legacy_client_json_body(client_format.as_str(), model, collected, usage)
    })
}

fn grok_collected_frame_stream(
    plan: ExecutionPlan,
    collected: GrokCollected,
    report_context: Option<&Value>,
) -> BoxStream<'static, Result<Bytes, IoError>> {
    let telemetry = collected.telemetry.clone();
    let status_code = collected.status_code;
    let headers_frame = || StreamFrame {
        frame_type: StreamFrameType::Headers,
        payload: StreamFramePayload::Headers {
            status_code,
            headers: BTreeMap::from([(
                "content-type".to_string(),
                if (200..300).contains(&status_code) {
                    "text/event-stream".to_string()
                } else {
                    "application/json".to_string()
                },
            )]),
            response_observation: None,
        },
    };
    let initial_telemetry_frame = || StreamFrame {
        frame_type: StreamFrameType::Telemetry,
        payload: StreamFramePayload::Telemetry {
            telemetry: ExecutionTelemetry {
                ttfb_ms: telemetry.ttfb_ms,
                elapsed_ms: telemetry.ttfb_ms,
                upstream_bytes: Some(0),
            },
        },
    };
    let frames = match bounded_grok_client_stream_body(&plan, &collected, report_context) {
        Ok(body) => vec![
            headers_frame(),
            initial_telemetry_frame(),
            StreamFrame {
                frame_type: StreamFrameType::Data,
                payload: StreamFramePayload::Data {
                    chunk_b64: Some(
                        base64::engine::general_purpose::STANDARD.encode(body.as_bytes()),
                    ),
                    text: None,
                },
            },
            StreamFrame {
                frame_type: StreamFrameType::Telemetry,
                payload: StreamFramePayload::Telemetry { telemetry },
            },
            StreamFrame::eof(),
        ],
        Err(error) => vec![
            headers_frame(),
            StreamFrame {
                frame_type: StreamFrameType::Error,
                payload: StreamFramePayload::Error {
                    error: aether_contracts::ExecutionError {
                        kind: aether_contracts::ExecutionErrorKind::ProtocolError,
                        phase: aether_contracts::ExecutionPhase::Finalize,
                        message: error.to_string(),
                        upstream_status: None,
                        retryable: true,
                        failover_recommended: true,
                    },
                },
            },
            StreamFrame {
                frame_type: StreamFrameType::Telemetry,
                payload: StreamFramePayload::Telemetry { telemetry },
            },
            StreamFrame::eof_with_summary(None),
        ],
    };
    stream::iter(
        frames
            .into_iter()
            .map(|frame| encode_stream_frame_ndjson(&frame)),
    )
    .boxed()
}

fn bounded_grok_client_stream_body(
    plan: &ExecutionPlan,
    collected: &GrokCollected,
    report_context: Option<&Value>,
) -> Result<String, ExecutionRuntimeTransportError> {
    let body = grok_client_stream_body(plan, collected, report_context);
    if body.len() > GROK_SYNTHETIC_STREAM_BODY_MAX_BYTES {
        return Err(ExecutionRuntimeTransportError::UpstreamResponseTooLarge {
            phase: UpstreamResponseBodyPhase::Decoded,
            limit_bytes: GROK_SYNTHETIC_STREAM_ENVELOPE_MAX_BYTES,
        });
    }
    Ok(body)
}

fn grok_client_stream_body(
    plan: &ExecutionPlan,
    collected: &GrokCollected,
    report_context: Option<&Value>,
) -> String {
    if !(200..300).contains(&collected.status_code) {
        return serde_json::to_string(&json!({
            "error": {
                "message": grok_upstream_http_error_message(collected.status_code),
                "type": "grok_upstream_error",
                "code": collected.status_code,
            }
        }))
        .unwrap_or_else(|_| "{}".to_string());
    }
    let model = plan.model_name.as_deref().unwrap_or("grok");
    let usage = grok_usage_estimate(plan, collected);
    let client_format = normalized_client_api_format(plan);
    if client_format == "openai:image" {
        return openai_image_sse(collected);
    }

    let provider_body = openai_responses_body(
        model,
        collected,
        usage,
        grok_plan_uses_structured_image_generation(plan, report_context),
    );
    let report_context = grok_conversion_report_context(plan, model);
    match maybe_bridge_standard_sync_json_to_stream(
        &provider_body,
        GROK_STANDARD_PROVIDER_API_FORMAT,
        client_format.as_str(),
        Some(&report_context),
    ) {
        Ok(Some(outcome)) => String::from_utf8(outcome.sse_body)
            .unwrap_or_else(|err| String::from_utf8_lossy(&err.into_bytes()).into_owned()),
        Ok(None) | Err(_) => {
            grok_legacy_client_stream_body(client_format.as_str(), model, collected, usage)
        }
    }
}

fn normalized_client_api_format(plan: &ExecutionPlan) -> String {
    let value = plan.client_api_format.trim();
    if value.is_empty() {
        "openai:chat".to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

fn grok_plan_uses_structured_image_generation(
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
) -> bool {
    let client_format = normalized_client_api_format(plan);
    if client_format == "openai:image" {
        return true;
    }
    if !matches!(
        client_format.as_str(),
        "openai:chat" | "openai:responses" | "openai:responses:compact"
    ) {
        return false;
    }
    let model_is_image_generation = plan
        .model_name
        .as_deref()
        .is_some_and(grok_model_name_is_image_generation)
        || grok_report_context_mapped_model(report_context)
            .is_some_and(grok_model_name_is_image_generation);
    let body_has_image_generation_tool = plan
        .body
        .json_body
        .as_ref()
        .is_some_and(grok_body_has_image_generation_tool);
    model_is_image_generation || body_has_image_generation_tool
}

fn grok_report_context_mapped_model(report_context: Option<&Value>) -> Option<&str> {
    report_context
        .and_then(|value| value.get("mapped_model"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn grok_model_name_is_image_generation(model: &str) -> bool {
    let model = model.trim().to_ascii_lowercase();
    model.contains("grok-imagine-image") && !model.contains("edit")
}

fn grok_body_has_image_generation_tool(body: &Value) -> bool {
    body.get("tools")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .any(|tool| {
            tool.get("type")
                .and_then(Value::as_str)
                .is_some_and(|value| value.eq_ignore_ascii_case("image_generation"))
                && !tool
                    .get("action")
                    .and_then(Value::as_str)
                    .is_some_and(|value| value.eq_ignore_ascii_case("edit"))
        })
}

fn grok_conversion_report_context(plan: &ExecutionPlan, model: &str) -> Value {
    json!({
        "provider_type": "grok",
        "provider_api_format": GROK_STANDARD_PROVIDER_API_FORMAT,
        "client_api_format": normalized_client_api_format(plan),
        "mapped_model": model,
        "model": model,
    })
}

fn grok_legacy_client_json_body(
    client_format: &str,
    model: &str,
    collected: &GrokCollected,
    usage: GrokUsageEstimate,
) -> Value {
    match client_format {
        "openai:responses" | "openai:responses:compact" => {
            openai_responses_body(model, collected, usage, false)
        }
        "claude:messages" => claude_messages_body(model, collected, usage),
        _ => openai_chat_body(model, collected, usage),
    }
}

fn grok_legacy_client_stream_body(
    client_format: &str,
    model: &str,
    collected: &GrokCollected,
    usage: GrokUsageEstimate,
) -> String {
    match client_format {
        "openai:responses" | "openai:responses:compact" => {
            openai_responses_sse(model, collected, usage)
        }
        "claude:messages" => claude_messages_sse(model, collected, usage),
        _ => openai_chat_sse(model, collected, usage),
    }
}

impl GrokStreamAdapter {
    fn push_chunk(&mut self, chunk: &[u8]) {
        self.buffered.push_str(&String::from_utf8_lossy(chunk));
        while let Some(index) = self.buffered.find('\n') {
            let line = self.buffered.drain(..=index).collect::<String>();
            self.handle_line(line.trim());
        }
    }

    fn finish(&mut self) {
        if !self.buffered.trim().is_empty() {
            let line = std::mem::take(&mut self.buffered);
            self.handle_line(line.trim());
        }
    }

    fn handle_line(&mut self, line: &str) {
        let line = line.trim();
        if line.is_empty() || line.starts_with("event:") {
            return;
        }
        let data = line.strip_prefix("data:").map(str::trim).unwrap_or(line);
        if data.is_empty() || data == "[DONE]" || !data.starts_with('{') {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            return;
        };
        self.handle_event(&value);
    }

    fn handle_event(&mut self, value: &Value) {
        let Some(response) = value
            .get("result")
            .and_then(|result| result.get("response"))
        else {
            return;
        };
        self.handle_streaming_image_generation_response(response);
        self.handle_model_response_images(response);
        if let Some(card) = response.get("cardAttachment") {
            self.handle_card(card);
        }
        if let Some(token) = response.get("token").and_then(Value::as_str) {
            if response
                .get("isThinking")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                self.thinking.push_str(token);
            } else if response.get("messageTag").and_then(Value::as_str) == Some("final") {
                let cleaned = self.clean_token(token);
                self.text.push_str(&cleaned);
            }
        }
    }

    fn handle_streaming_image_generation_response(&mut self, response: &Value) {
        let Some(stream) = response.get("streamingImageGenerationResponse") else {
            return;
        };
        if stream
            .get("moderated")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return;
        }
        if stream
            .get("progress")
            .and_then(Value::as_u64)
            .unwrap_or_default()
            < 100
        {
            return;
        }
        let url = stream
            .get("assetId")
            .and_then(Value::as_str)
            .map(|asset_id| format!("{GROK_ASSET_BASE}{asset_id}/content"))
            .or_else(|| {
                stream
                    .get("imageUrl")
                    .and_then(Value::as_str)
                    .map(grok_asset_url)
            });
        if let Some(url) = url {
            self.push_image_url(url);
        }
    }

    fn handle_model_response_images(&mut self, response: &Value) {
        let Some(model_response) = response.get("modelResponse") else {
            return;
        };
        if let Some(urls) = model_response
            .get("generatedImageUrls")
            .and_then(Value::as_array)
        {
            for url in urls.iter().filter_map(Value::as_str) {
                self.push_image_url(grok_asset_url(url));
            }
        }
        if let Some(attachments) = model_response
            .get("fileAttachments")
            .and_then(Value::as_array)
        {
            for asset_id in attachments.iter().filter_map(Value::as_str) {
                self.push_image_url(format!("{GROK_ASSET_BASE}{asset_id}/content"));
            }
        }
    }

    fn handle_card(&mut self, card: &Value) {
        let Some(json_data) = card.get("jsonData").and_then(Value::as_str) else {
            return;
        };
        let Ok(value) = serde_json::from_str::<Value>(json_data) else {
            return;
        };
        if let Some(card_id) = value.get("id").and_then(Value::as_str) {
            self.cards.insert(
                card_id.to_string(),
                GrokCard {
                    url: value
                        .get("url")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    title: value
                        .get("title")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                },
            );
        }
        let Some(chunk) = value.get("image_chunk") else {
            return;
        };
        if chunk.get("progress").and_then(Value::as_u64) != Some(100)
            || chunk
                .get("moderated")
                .and_then(Value::as_bool)
                .unwrap_or(false)
        {
            return;
        }
        let Some(path) = chunk.get("imageUrl").and_then(Value::as_str) else {
            return;
        };
        let url = if path.starts_with("http://") || path.starts_with("https://") {
            path.to_string()
        } else {
            format!("{GROK_ASSET_BASE}{path}")
        };
        self.push_image_url(url);
    }

    fn push_image_url(&mut self, url: String) {
        let url = url.trim();
        if url.is_empty()
            || url.len() > GROK_MAX_ATTACHMENT_URL_BYTES
            || self.images.len() >= GROK_MAX_IMAGE_COUNT
        {
            return;
        }
        if !self.images.iter().any(|item| item == url) {
            self.images.push(url.to_owned());
        }
    }

    fn clean_token(&mut self, token: &str) -> String {
        if !token.contains("<grok:render") {
            return token.to_string();
        }
        let replaced = grok_render_regex()
            .replace_all(token, |captures: &Captures<'_>| {
                self.render_replacement(
                    captures
                        .get(1)
                        .map(|value| value.as_str())
                        .unwrap_or_default(),
                    captures
                        .get(3)
                        .map(|value| value.as_str())
                        .unwrap_or_default(),
                )
            })
            .to_string();
        if replaced.starts_with('\n') && replaced.contains("[[") {
            replaced.trim_start_matches('\n').to_string()
        } else {
            replaced
        }
    }

    fn render_replacement(&mut self, card_id: &str, render_type: &str) -> String {
        let Some(card) = self.cards.get(card_id) else {
            return String::new();
        };
        match render_type {
            "render_inline_citation" => {
                let Some(url) = card.url.clone().filter(|value| !value.trim().is_empty()) else {
                    return String::new();
                };
                let index = self
                    .citation_order
                    .iter()
                    .position(|existing| existing == &url)
                    .map(|position| position + 1)
                    .unwrap_or_else(|| {
                        self.citation_order.push(url.clone());
                        self.citation_order.len()
                    });
                if self.last_citation_index == Some(index) {
                    return String::new();
                }
                self.last_citation_index = Some(index);
                let title = card.title.clone().unwrap_or_else(|| url.clone());
                format!(" [[{index}]]({url} \"{}\")", title.replace('"', "'"))
            }
            "render_searched_image" => {
                let Some(url) = card.url.clone().filter(|value| !value.trim().is_empty()) else {
                    return String::new();
                };
                let title = card.title.clone().unwrap_or_else(|| "image".to_string());
                format!("![{}]({url})", title.replace(['[', ']'], ""))
            }
            "render_generated_image" => String::new(),
            _ => String::new(),
        }
    }
}

fn openai_chat_body(model: &str, collected: &GrokCollected, usage: GrokUsageEstimate) -> Value {
    json!({
        "id": format!("chatcmpl-{}", Uuid::new_v4()),
        "object": "chat.completion",
        "created": current_unix_secs(),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {
                "role": "assistant",
                "content": chat_text_with_images(collected),
            },
            "finish_reason": "stop",
        }],
        "usage": openai_chat_usage(usage),
    })
}

fn openai_chat_sse(model: &str, collected: &GrokCollected, usage: GrokUsageEstimate) -> String {
    let id = format!("chatcmpl-{}", Uuid::new_v4());
    let mut body = String::new();
    if !collected.thinking.is_empty() {
        push_sse_data(
            &mut body,
            &json!({
                "id": id,
                "object": "chat.completion.chunk",
                "created": current_unix_secs(),
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": {"role": "assistant", "reasoning_content": collected.thinking},
                }],
            }),
        );
    }
    push_sse_data(
        &mut body,
        &json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": current_unix_secs(),
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {"role": "assistant", "content": chat_text_with_images(collected)},
            }],
        }),
    );
    push_sse_data(
        &mut body,
        &json!({
            "id": id,
            "object": "chat.completion.chunk",
            "created": current_unix_secs(),
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": "stop",
            }],
            "usage": openai_chat_usage(usage),
        }),
    );
    body.push_str("data: [DONE]\n\n");
    body
}

fn openai_responses_body(
    model: &str,
    collected: &GrokCollected,
    usage: GrokUsageEstimate,
    images_as_generation_calls: bool,
) -> Value {
    let response_id = format!("resp_{}", Uuid::new_v4());
    let mut output = Vec::new();
    if !collected.thinking.trim().is_empty() {
        let thinking = collected.thinking.trim();
        output.push(json!({
            "id": openai_responses_synthetic_reasoning_item_id(&response_id, 0),
            "type": "reasoning",
            "status": "completed",
            "summary": [],
            "content": [{
                "type": "reasoning_text",
                "text": thinking,
            }],
        }));
    }
    let message_text = if images_as_generation_calls {
        collected.text.clone()
    } else {
        chat_text_with_images(collected)
    };
    if !message_text.trim().is_empty() {
        output.push(json!({
            "id": openai_responses_message_item_id(response_id.as_str(), output.len()),
            "type": "message",
            "role": "assistant",
            "content": [{"type": "output_text", "text": message_text, "annotations": []}],
            "status": "completed",
        }));
    }
    if images_as_generation_calls {
        for (index, image) in collected.images.iter().enumerate() {
            if let Some(item) = grok_openai_responses_image_generation_item(
                response_id.as_str(),
                index,
                image.as_str(),
            ) {
                output.push(item);
            }
        }
    }
    json!({
        "id": response_id,
        "object": "response",
        "created_at": current_unix_secs(),
        "status": "completed",
        "model": model,
        "output": output,
        "usage": openai_responses_usage(usage),
    })
}

fn grok_openai_responses_image_generation_item(
    response_id: &str,
    index: usize,
    image: &str,
) -> Option<Value> {
    let image = image.trim();
    if image.is_empty() {
        return None;
    }
    let mut item = Map::new();
    item.insert(
        "id".to_string(),
        Value::String(format!("{response_id}_ig_{index}")),
    );
    item.insert(
        "type".to_string(),
        Value::String("image_generation_call".to_string()),
    );
    item.insert("status".to_string(), Value::String("completed".to_string()));
    item.insert("action".to_string(), Value::String("generate".to_string()));
    if grok_is_data_uri(image) {
        let (mime_type, b64_json) = grok_data_image_parts(image)?;
        item.insert("result".to_string(), Value::String(b64_json));
        item.insert(
            "output_format".to_string(),
            Value::String(grok_output_format_from_mime_type(mime_type.as_str())),
        );
        item.insert("mime_type".to_string(), Value::String(mime_type));
    } else {
        item.insert("url".to_string(), Value::String(image.to_string()));
        item.insert(
            "output_format".to_string(),
            Value::String("png".to_string()),
        );
    }
    Some(Value::Object(item))
}

fn grok_output_format_from_mime_type(mime_type: &str) -> String {
    match mime_type.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" => "jpeg",
        "image/webp" => "webp",
        "image/gif" => "gif",
        _ => "png",
    }
    .to_string()
}

fn openai_responses_sse(
    model: &str,
    collected: &GrokCollected,
    usage: GrokUsageEstimate,
) -> String {
    let response_id = format!("resp_{}", Uuid::new_v4());
    let message_id = format!("msg_{}", Uuid::new_v4());
    let text = chat_text_with_images(collected);
    let response = openai_responses_body(model, collected, usage, false);
    let mut body = String::new();
    push_sse_event(
        &mut body,
        "response.created",
        &json!({"type": "response.created", "response": {
            "id": response_id,
            "object": "response",
            "created_at": current_unix_secs(),
            "status": "in_progress",
            "model": model,
            "output": [],
        }}),
    );
    push_sse_event(
        &mut body,
        "response.output_item.added",
        &json!({"type":"response.output_item.added","output_index":0,"item":{
            "id": message_id, "type":"message", "role":"assistant", "content":[], "status":"in_progress"
        }}),
    );
    push_sse_event(
        &mut body,
        "response.output_text.delta",
        &json!({"type":"response.output_text.delta","item_id":message_id,"output_index":0,"content_index":0,"delta":text}),
    );
    push_sse_event(
        &mut body,
        "response.completed",
        &json!({"type":"response.completed","response": response}),
    );
    body.push_str("data: [DONE]\n\n");
    body
}

fn claude_messages_body(model: &str, collected: &GrokCollected, usage: GrokUsageEstimate) -> Value {
    json!({
        "id": format!("msg_{}", Uuid::new_v4()),
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [{"type": "text", "text": chat_text_with_images(collected)}],
        "stop_reason": "end_turn",
        "stop_sequence": Value::Null,
        "usage": {
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
        },
    })
}

fn claude_messages_sse(model: &str, collected: &GrokCollected, usage: GrokUsageEstimate) -> String {
    let message_id = format!("msg_{}", Uuid::new_v4());
    let mut body = String::new();
    let mut next_block_index = 0usize;

    push_sse_event(
        &mut body,
        "message_start",
        &json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": Value::Null,
                "stop_sequence": Value::Null,
                "usage": {
                    "input_tokens": 0,
                    "output_tokens": 0,
                },
            },
        }),
    );

    if !collected.thinking.is_empty() {
        let block_index = next_block_index;
        next_block_index += 1;
        push_sse_event(
            &mut body,
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": block_index,
                "content_block": {
                    "type": "thinking",
                    "thinking": "",
                },
            }),
        );
        push_sse_event(
            &mut body,
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": block_index,
                "delta": {
                    "type": "thinking_delta",
                    "thinking": collected.thinking,
                },
            }),
        );
        push_sse_event(
            &mut body,
            "content_block_stop",
            &json!({
                "type": "content_block_stop",
                "index": block_index,
            }),
        );
    }

    let text = chat_text_with_images(collected);
    if !text.is_empty() {
        let block_index = next_block_index;
        push_sse_event(
            &mut body,
            "content_block_start",
            &json!({
                "type": "content_block_start",
                "index": block_index,
                "content_block": {
                    "type": "text",
                    "text": "",
                },
            }),
        );
        push_sse_event(
            &mut body,
            "content_block_delta",
            &json!({
                "type": "content_block_delta",
                "index": block_index,
                "delta": {
                    "type": "text_delta",
                    "text": text,
                },
            }),
        );
        push_sse_event(
            &mut body,
            "content_block_stop",
            &json!({
                "type": "content_block_stop",
                "index": block_index,
            }),
        );
    }

    push_sse_event(
        &mut body,
        "message_delta",
        &json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": "end_turn",
                "stop_sequence": Value::Null,
            },
            "usage": {
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
            },
        }),
    );
    push_sse_event(
        &mut body,
        "message_stop",
        &json!({
            "type": "message_stop",
        }),
    );
    body
}

fn openai_image_body(collected: &GrokCollected) -> Value {
    json!({
        "created": current_unix_secs(),
        "data": collected
            .images
            .iter()
            .filter_map(|url| grok_openai_image_item(url.as_str()))
            .collect::<Vec<_>>(),
    })
}

fn grok_openai_image_item(url: &str) -> Option<Value> {
    let url = url.trim();
    if url.is_empty() {
        return None;
    }
    if grok_is_data_uri(url) {
        let (mime_type, b64_json) = grok_data_image_parts(url)?;
        return Some(json!({
            "b64_json": b64_json,
            "mime_type": mime_type,
        }));
    }
    Some(json!({ "url": url }))
}

async fn materialize_grok_image_assets(plan: &ExecutionPlan, collected: &mut GrokCollected) {
    if normalized_client_api_format(plan) != "openai:image" || collected.images.is_empty() {
        return;
    }

    let mut resolved_images = Vec::with_capacity(collected.images.len());
    for image_url in &collected.images {
        match grok_download_image_asset(plan, image_url.as_str()).await {
            Ok(Some(data_url)) => resolved_images.push(data_url),
            Ok(None) | Err(_) => resolved_images.push(image_url.clone()),
        }
    }
    collected.images = resolved_images;
}

async fn grok_download_image_asset(
    plan: &ExecutionPlan,
    raw_url: &str,
) -> Result<Option<String>, ExecutionRuntimeTransportError> {
    if !grok_image_asset_url_is_supported(raw_url) {
        return Ok(None);
    }
    let url = reqwest::Url::parse(raw_url).map_err(|err| {
        ExecutionRuntimeTransportError::UpstreamRequest(format!(
            "Grok image asset URL is invalid: {err}"
        ))
    })?;
    let mut download_plan = plan.clone();
    download_plan.method = "GET".to_string();
    download_plan.url = url.to_string();
    download_plan.headers.remove("content-type");
    download_plan
        .headers
        .insert("accept".to_string(), "image/*,*/*;q=0.8".to_string());
    download_plan.headers.insert(
        EXECUTION_REQUEST_FOLLOW_REDIRECTS_HEADER.to_string(),
        "true".to_string(),
    );
    download_plan.body = RequestBody {
        json_body: None,
        body_bytes_b64: None,
        body_ref: None,
    };
    download_plan.stream = false;

    let response = send_request(&download_plan, Vec::new()).await?;
    if !(200..300).contains(&response.status_code()) {
        return Ok(None);
    }
    let headers = response.headers();
    let declared_content_type = headers
        .get("content-type")
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let bytes = response
        .bytes_with_limit(GROK_MAX_ATTACHMENT_BYTES)
        .await
        .map_err(|err| {
            ExecutionRuntimeTransportError::UpstreamRequest(format!(
                "Grok image asset download failed: {err}"
            ))
        })?;
    if bytes.is_empty() {
        return Ok(None);
    }
    let content_type =
        grok_image_mime_for_payload(&bytes, declared_content_type).ok_or_else(|| {
            ExecutionRuntimeTransportError::UpstreamRequest(
                "Grok image asset response is not a supported image".to_string(),
            )
        })?;
    Ok(Some(format!(
        "data:{content_type};base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )))
}

fn grok_image_mime_for_payload(
    bytes: &[u8],
    declared_content_type: Option<&str>,
) -> Option<&'static str> {
    let detected = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        "image/png"
    } else if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        "image/jpeg"
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        "image/webp"
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        "image/gif"
    } else if bytes.len() >= 12
        && &bytes[4..8] == b"ftyp"
        && matches!(&bytes[8..12], b"avif" | b"avis")
    {
        "image/avif"
    } else {
        return None;
    };

    let declared = declared_content_type
        .and_then(|value| value.split(';').next())
        .map(|value| value.trim().to_ascii_lowercase());
    let declared = declared.as_deref().map(|value| {
        if value == "image/jpg" {
            "image/jpeg"
        } else {
            value
        }
    });
    if declared.is_some_and(|value| value != detected) {
        return None;
    }
    Some(detected)
}

fn grok_image_asset_url_is_supported(raw_url: &str) -> bool {
    if raw_url.starts_with("data:image/") {
        return true;
    }
    let Ok(url) = reqwest::Url::parse(raw_url) else {
        return false;
    };
    if !matches!(url.scheme(), "http" | "https") {
        return false;
    }
    let Some(host) = url.host_str() else {
        return false;
    };
    if matches!(host, "assets.grok.com" | "assets.grokusercontent.com") {
        return true;
    }
    cfg!(test) && matches!(host, "localhost" | "127.0.0.1" | "::1")
}

fn grok_data_image_parts(raw_url: &str) -> Option<(String, String)> {
    grok_data_image_parts_with_limit(raw_url, GROK_MAX_ATTACHMENT_BYTES)
}

fn grok_data_image_parts_with_limit(
    raw_url: &str,
    decoded_limit: usize,
) -> Option<(String, String)> {
    let (header, data) = raw_url.trim().split_once(',')?;
    let metadata = header
        .get(..5)
        .filter(|prefix| prefix.eq_ignore_ascii_case("data:"))
        .and_then(|_| header.get(5..))?;
    let mut parameters = metadata.split(';');
    let declared_mime = parameters.next()?.trim();
    let declared_mime = grok_inline_image_mime(declared_mime)?;
    if !parameters.any(|parameter| parameter.trim().eq_ignore_ascii_case("base64")) {
        return None;
    }

    // Check the encoded length before decoding.  The base64 engine allocates
    // based on that length, so a post-decode check alone would still permit an
    // allocation denial of service.
    let max_base64_len = maximum_base64_len_for_decoded_limit(decoded_limit);
    let normalized = normalize_base64_with_limit(data, max_base64_len).ok()?;
    if normalized.is_empty() {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&normalized)
        .ok()?;
    if decoded.len() > decoded_limit {
        return None;
    }
    let detected = grok_image_mime_for_payload(&decoded, Some(declared_mime))?;
    Some((detected.to_string(), normalized))
}

fn grok_inline_image_mime(value: &str) -> Option<&'static str> {
    // Keep the declaration bounded and restricted to passive raster formats.
    // In particular, never permit image/svg+xml or a generic image/* token to
    // cross a client-facing data URL boundary.
    if value.is_empty()
        || value.len() > 64
        || value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control() || byte == b',')
    {
        return None;
    }
    if value.eq_ignore_ascii_case("image/png") {
        Some("image/png")
    } else if value.eq_ignore_ascii_case("image/jpeg") || value.eq_ignore_ascii_case("image/jpg") {
        Some("image/jpeg")
    } else if value.eq_ignore_ascii_case("image/webp") {
        Some("image/webp")
    } else if value.eq_ignore_ascii_case("image/gif") {
        Some("image/gif")
    } else if value.eq_ignore_ascii_case("image/avif") {
        Some("image/avif")
    } else {
        None
    }
}

fn grok_is_data_uri(value: &str) -> bool {
    value
        .trim()
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
}

fn grok_public_image_reference(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if grok_is_data_uri(value) {
        // Validate before exposing any data URL.  Keep the original borrowed
        // text to avoid an additional large allocation for stream responses.
        grok_data_image_parts(value)?;
    }
    Some(value)
}

fn openai_image_sse(collected: &GrokCollected) -> String {
    let mut body = String::new();
    for (index, url) in collected.images.iter().enumerate() {
        let Some(url) = grok_public_image_reference(url) else {
            continue;
        };
        push_sse_event(
            &mut body,
            "image_generation.completed",
            &json!({
                "type": "image_generation.completed",
                "url": url,
                "partial_image_index": index,
            }),
        );
    }
    body.push_str("data: [DONE]\n\n");
    body
}

fn chat_text_with_images(collected: &GrokCollected) -> String {
    let mut text = collected.text.clone();
    for image in &collected.images {
        let Some(image) = grok_public_image_reference(image) else {
            continue;
        };
        if !text.is_empty() {
            text.push_str("\n\n");
        }
        text.push_str(image);
    }
    text
}

fn grok_usage_estimate(plan: &ExecutionPlan, collected: &GrokCollected) -> GrokUsageEstimate {
    let input_tokens = plan
        .body
        .json_body
        .as_ref()
        .map(estimated_prompt_tokens)
        .unwrap_or_default();
    let reasoning_tokens = estimated_text_tokens(&collected.thinking);
    let output_tokens =
        estimated_text_tokens(&chat_text_with_images(collected)).saturating_add(reasoning_tokens);
    GrokUsageEstimate {
        input_tokens,
        output_tokens,
        reasoning_tokens,
    }
}

fn estimated_text_tokens(text: &str) -> u64 {
    if text.trim().is_empty() {
        return 0;
    }
    let chars = text.chars().count() as u64;
    chars.div_ceil(4).max(1)
}

fn estimated_prompt_tokens(value: &Value) -> u64 {
    let Ok(text) = serde_json::to_string(value) else {
        return 0;
    };
    let tokens = estimated_text_tokens(&text);
    if tokens == 0 {
        0
    } else {
        tokens.saturating_add(GROK_PROMPT_OVERHEAD_TOKENS)
    }
}

fn openai_chat_usage(usage: GrokUsageEstimate) -> Value {
    json!({
        "prompt_tokens": usage.input_tokens,
        "completion_tokens": usage.output_tokens,
        "total_tokens": usage.input_tokens.saturating_add(usage.output_tokens),
        "prompt_tokens_details": {
            "cached_tokens": 0,
            "text_tokens": usage.input_tokens,
            "audio_tokens": 0,
            "image_tokens": 0,
        },
        "completion_tokens_details": {
            "text_tokens": usage.output_tokens.saturating_sub(usage.reasoning_tokens),
            "audio_tokens": 0,
            "reasoning_tokens": usage.reasoning_tokens,
        },
    })
}

fn openai_responses_usage(usage: GrokUsageEstimate) -> Value {
    json!({
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.input_tokens.saturating_add(usage.output_tokens),
        "output_tokens_details": {
            "reasoning_tokens": usage.reasoning_tokens,
        },
    })
}

fn push_sse_data(body: &mut String, data: &Value) {
    body.push_str("data: ");
    body.push_str(&serde_json::to_string(data).unwrap_or_else(|_| "{}".to_string()));
    body.push_str("\n\n");
}

fn push_sse_event(body: &mut String, event: &str, data: &Value) {
    body.push_str("event: ");
    body.push_str(event);
    body.push('\n');
    push_sse_data(body, data);
}

#[cfg(test)]
#[path = "grok/tests.rs"]
mod tests;
