#[cfg(test)]
pub(crate) use aether_ai_formats::api::parse_model_directive;
#[cfg(test)]
pub(crate) use aether_ai_formats::api::{LocalStandardSourceFamily, LocalStandardSourceMode};

#[cfg(test)]
pub(crate) use aether_ai_formats::api::{
    build_standard_request_body, convert_claude_chat_response_to_openai_chat,
    convert_claude_response_to_openai_responses, convert_gemini_chat_response_to_openai_chat,
    convert_gemini_response_to_openai_responses, convert_openai_chat_response_to_openai_responses,
    convert_openai_responses_response_to_openai_chat,
};

pub(crate) use aether_ai_formats::api::{
    aggregate_claude_stream_sync_response, aggregate_gemini_stream_sync_response,
    aggregate_openai_chat_stream_sync_response, aggregate_openai_responses_stream_sync_response,
    api_format_alias_matches, apply_codex_openai_compact_terminal_headers,
    apply_codex_openai_responses_chat_body_edits, apply_codex_openai_responses_identity_headers,
    apply_codex_openai_responses_lite_header_for_request_body_with_capabilities,
    apply_codex_openai_responses_special_body_edits,
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities,
    apply_codex_openai_responses_websocket_continuation_body_edits_with_source_model_and_capabilities,
    apply_codex_openai_special_headers, apply_model_directive_mapping_patch,
    apply_openai_responses_compact_special_body_edits, apply_xai_upstream_payload_edits,
    build_chatgpt_web_image_request_body, build_codex_openai_image_api_provider_request_body,
    build_core_error_body_for_client_format, build_cross_format_openai_chat_request_body,
    build_cross_format_openai_chat_request_body_with_provider_context,
    build_cross_format_openai_responses_request_body_with_provider_context,
    build_gemini_image_request_body_from_openai_image_request,
    build_local_openai_chat_request_body,
    build_local_openai_chat_request_body_with_model_directives,
    build_local_openai_responses_request_body,
    build_local_openai_responses_request_body_with_model_directives,
    build_local_success_background_report, build_local_success_conversion_background_report,
    build_openai_image_api_provider_request_body, build_openai_image_provider_request_body,
    build_openai_image_request_body_from_gemini_image_request,
    build_standard_request_body_with_model_directives_and_request_headers_and_reasoning_replay_policy,
    default_model_directive_mapping_patch, default_model_directive_suffixes,
    default_model_for_openai_image_operation, endpoint_config_forces_upstream_stream_policy,
    enforce_request_body_stream_field, extract_openai_text_content,
    finalize_openai_provider_request_with_codex_model_capabilities_and_reasoning_replay_policy,
    finalize_openai_provider_request_with_codex_model_capabilities_and_reasoning_replay_policy_for_websocket_continuation,
    force_upstream_streaming_for_provider, gemini_request_is_image_generation,
    hydrate_response_history, is_matching_stream_http_request, is_openai_responses_compact_format,
    is_openai_responses_family_format, is_openai_responses_format,
    maybe_bridge_standard_sync_json_to_stream, maybe_build_ai_surface_stream_rewriter,
    maybe_build_openai_image_sync_finalize_product, maybe_build_provider_private_stream_normalizer,
    maybe_build_standard_sync_finalize_product_from_normalized_payload, model_directive_base_model,
    model_directive_builtin_suffix_supported_for_source_model,
    model_directive_suffix_has_builtin_mapping, normalize_api_format_alias,
    normalize_openai_image_quality, normalize_openai_image_request,
    normalize_openai_image_request_with_options, normalize_provider_private_report_context,
    normalize_provider_private_response_value, parse_codex_auth_identity,
    parse_model_directive_with_suffixes, prepare_local_success_response_parts,
    project_codex_catalog_model_card, project_codex_openai_image_api_request_body,
    project_openai_image_api_request_body, provider_adaptation_requires_eventstream_accept,
    provider_private_response_allows_sync_finalize, record_converted_response_history,
    request_candidate_api_formats, request_conversion_kind, request_path_implies_stream_request,
    resolve_claude_stream_spec, resolve_claude_sync_spec,
    resolve_codex_responses_model_capabilities,
    resolve_execution_runtime_stream_plan_kind_with_client_surface,
    resolve_execution_runtime_sync_plan_kind_with_client_surface, resolve_gemini_files_stream_spec,
    resolve_gemini_files_sync_spec, resolve_gemini_stream_spec, resolve_gemini_sync_spec,
    resolve_local_image_stream_spec, resolve_local_image_sync_spec,
    resolve_local_same_format_stream_spec, resolve_local_same_format_sync_spec,
    resolve_local_video_sync_spec, resolve_openai_embedding_sync_spec,
    resolve_openai_responses_stream_spec, resolve_openai_responses_sync_spec,
    resolve_requested_openai_image_model_for_request,
    resolve_upstream_is_stream_for_provider as resolve_format_upstream_is_stream_for_provider,
    resolve_upstream_is_stream_from_endpoint_config, response_history_is_loaded,
    response_history_storage_key, sanitize_request_path, sanitize_request_path_and_query,
    sanitize_request_query_string, stream_body_contains_error_event,
    validate_openai_provider_request_contract, AiSurfaceFinalizeError, AiSurfaceStreamRewriter,
    CodexResponsesModelCapabilities, ExecutionRuntimeAuthContext, LocalCoreSyncErrorKind,
    LocalGeminiFilesSpec, LocalOpenAiImageSpec, LocalOpenAiResponsesSpec,
    LocalSameFormatProviderFamily, LocalSameFormatProviderSpec, LocalStandardSpec,
    LocalSyncReportParts, LocalVideoCreateFamily, LocalVideoCreateSpec,
    OpenAiImageNormalizeOptions, OpenAiImageOperation, OpenAiProviderRequestFinalization,
    OpenAiResponsesReasoningReplayPolicy, ProviderPrivateStreamNormalizer, ReasoningEffort,
    RequestConversionKind, ResponseHistoryRecord, ServiceTier,
    StandardSyncFinalizeNormalizedProduct, SyncToStreamBridgeOutcome,
    ANTIGRAVITY_V1INTERNAL_ENVELOPE_NAME, CLAUDE_CHAT_STREAM_PLAN_KIND, CLAUDE_CHAT_SYNC_PLAN_KIND,
    CLAUDE_CLI_STREAM_PLAN_KIND, CLAUDE_CLI_SYNC_PLAN_KIND, CLAUDE_COUNT_TOKENS_SYNC_PLAN_KIND,
    CODEX_LIVE_STREAM_PLAN_KIND, EXECUTION_RUNTIME_STREAM_DECISION_ACTION,
    EXECUTION_RUNTIME_SYNC_DECISION_ACTION, GEMINI_CHAT_STREAM_PLAN_KIND,
    GEMINI_CHAT_SYNC_FINALIZE_REPORT_KIND, GEMINI_CHAT_SYNC_PLAN_KIND, GEMINI_CLI_STREAM_PLAN_KIND,
    GEMINI_CLI_SYNC_PLAN_KIND, GEMINI_EMBEDDING_SYNC_PLAN_KIND, GEMINI_FILES_DELETE_PLAN_KIND,
    GEMINI_FILES_DOWNLOAD_PLAN_KIND, GEMINI_FILES_GET_PLAN_KIND, GEMINI_FILES_LIST_PLAN_KIND,
    GEMINI_FILES_UPLOAD_PLAN_KIND, GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND,
    GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND, KIRO_ENVELOPE_NAME, MODEL_DIRECTIVE_API_FORMATS,
    OPENAI_CHAT_STREAM_PLAN_KIND, OPENAI_CHAT_SYNC_FINALIZE_REPORT_KIND,
    OPENAI_CHAT_SYNC_PLAN_KIND, OPENAI_EMBEDDING_SYNC_PLAN_KIND, OPENAI_IMAGE_STREAM_PLAN_KIND,
    OPENAI_IMAGE_SYNC_FINALIZE_REPORT_KIND, OPENAI_IMAGE_SYNC_PLAN_KIND,
    OPENAI_MEMORIES_SYNC_PLAN_KIND, OPENAI_RERANK_SYNC_PLAN_KIND,
    OPENAI_RESPONSES_COMPACT_STREAM_PLAN_KIND, OPENAI_RESPONSES_COMPACT_SYNC_PLAN_KIND,
    OPENAI_RESPONSES_STREAM_PLAN_KIND, OPENAI_RESPONSES_SYNC_PLAN_KIND,
    OPENAI_SEARCH_SYNC_PLAN_KIND, OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND,
    OPENAI_VIDEO_CONTENT_PLAN_KIND, OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND,
    OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND, OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND,
};
#[cfg(test)]
pub(crate) use aether_ai_formats::api::{
    build_codex_model_catalog_metadata, OPENAI_REALTIME_STREAM_PLAN_KIND,
};
#[cfg(test)]
pub(crate) use aether_ai_formats::strip_incompatible_openai_responses_reasoning_items_with_policy;
pub(crate) use aether_ai_formats::{
    api_format_defaults_to_client_error_failover, api_format_defaults_to_non_stream,
    api_format_permission_covers, codex_responses_lite_tool_is_client_executed,
    intersect_api_format_allowed_lists, is_embedding_api_format, is_rerank_api_format,
    openai_responses_message_item_id, openai_responses_request_operation,
    openai_responses_synthetic_reasoning_item_id, ApiOperation, ClientSurface,
    OPENAI_RESPONSES_OPERATION_COMPACT,
};

pub(crate) fn plan_kind_matches_api_operation(
    plan_kind: &str,
    require_streaming: bool,
    expected_operation: Option<ApiOperation>,
) -> bool {
    let Some(expected_operation) = expected_operation else {
        return true;
    };
    if expected_operation == ApiOperation::OpenAiResponsesCompact {
        return if require_streaming {
            plan_kind == OPENAI_RESPONSES_COMPACT_STREAM_PLAN_KIND
        } else {
            plan_kind == OPENAI_RESPONSES_COMPACT_SYNC_PLAN_KIND
        };
    }
    let resolved_operation = if require_streaming {
        resolve_local_same_format_stream_spec(plan_kind).and_then(|spec| spec.operation)
    } else {
        resolve_local_same_format_sync_spec(plan_kind).and_then(|spec| spec.operation)
    };
    resolved_operation == Some(expected_operation)
}
