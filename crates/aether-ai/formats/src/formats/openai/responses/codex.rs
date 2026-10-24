use std::collections::BTreeMap;
use std::sync::OnceLock;

use crate::codex_profile::codex_client_profile;
use aether_ai_formats::provider_compat::proxy::rules::body_rules_handle_path;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const CODEX_DEFAULT_REASONING_EFFORT: &str = "medium";
const CODEX_REASONING_ENCRYPTED_CONTENT_INCLUDE: &str = "reasoning.encrypted_content";
const CODEX_PROMPT_CACHE_IDENTITY_NAMESPACE: &str =
    "https://github.com/fawney19/Aether/codex/prompt-cache-identity/v1/";
pub const CODEX_RESPONSES_LITE_HEADER: &str = "x-openai-internal-codex-responses-lite";
pub const CODEX_MODEL_CATALOG_METADATA_FIELD: &str = "codex_models";
const CODEX_OPENAI_RESPONSES_UNSUPPORTED_BODY_FIELDS: &[&str] = &[
    "max_output_tokens",
    "max_completion_tokens",
    "temperature",
    "top_p",
    "frequency_penalty",
    "presence_penalty",
    "user",
    "metadata",
    "prompt_cache_options",
    "prompt_cache_retention",
    "safety_identifier",
    "previous_response_id",
];
const CODEX_OPENAI_RESPONSES_COMPACT_BODY_FIELDS: &[&str] = &[
    "model",
    "input",
    "instructions",
    "tools",
    "parallel_tool_calls",
    "reasoning",
    "service_tier",
    "prompt_cache_key",
    "text",
];
pub const CODEX_OPENAI_IMAGE_INTERNAL_MODEL: &str = "gpt-5.4-mini";
pub const CODEX_OPENAI_IMAGE_DEFAULT_MODEL: &str = "gpt-image-2";
pub const CODEX_OPENAI_IMAGE_DEFAULT_VARIATION_MODEL: &str = "dall-e-2";
pub const CODEX_OPENAI_IMAGE_DEFAULT_OUTPUT_FORMAT: &str = "png";
pub const CODEX_OPENAI_IMAGE_DEFAULT_VARIATION_PROMPT: &str =
    "Create a faithful variation of the provided image.";
const CODEX_IMAGE_TOOL_DEFAULT_SIZE: &str = "1024x1024";
const CODEX_IMAGE_TOOL_DEFAULT_QUALITY: &str = "high";
const CODEX_IMAGE_TOOL_DEFAULT_BACKGROUND: &str = "auto";
fn is_codex_openai_responses_request(provider_type: &str, provider_api_format: &str) -> bool {
    provider_type.trim().eq_ignore_ascii_case("codex")
        && aether_ai_formats::is_openai_responses_family_format(provider_api_format)
}

fn is_openai_responses_compact_request(provider_api_format: &str) -> bool {
    aether_ai_formats::is_openai_responses_compact_format(provider_api_format)
}

fn is_openai_image_request(provider_api_format: &str) -> bool {
    provider_api_format
        .trim()
        .eq_ignore_ascii_case("openai:image")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CodexOpenAiEndpointKind {
    Responses,
    Compact,
    Search,
    Images,
    Live,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodexResponsesModelCapabilities {
    pub use_responses_lite: bool,
    pub supports_reasoning_summary_parameter: bool,
    pub default_reasoning_effort: Option<String>,
    pub default_reasoning_summary: Option<String>,
    pub supported_reasoning_efforts: Vec<String>,
    pub supports_parallel_tool_calls: bool,
    pub support_verbosity: bool,
    pub default_verbosity: Option<String>,
    pub supported_service_tiers: Vec<String>,
}

impl CodexResponsesModelCapabilities {
    pub fn supports_reasoning_effort(&self, effort: &str) -> bool {
        self.supported_reasoning_efforts
            .iter()
            .any(|candidate| candidate == effort.trim())
    }
}

fn codex_namespaced_model_suffix(model: &str) -> Option<&str> {
    let (namespace, suffix) = model.split_once('/')?;
    if suffix.contains('/')
        || namespace.is_empty()
        || !namespace
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return None;
    }
    Some(suffix)
}

fn conservative_codex_responses_model_capabilities() -> CodexResponsesModelCapabilities {
    CodexResponsesModelCapabilities {
        use_responses_lite: false,
        supports_reasoning_summary_parameter: true,
        default_reasoning_effort: None,
        default_reasoning_summary: None,
        supported_reasoning_efforts: Vec::new(),
        supports_parallel_tool_calls: false,
        support_verbosity: false,
        default_verbosity: None,
        supported_service_tiers: Vec::new(),
    }
}

pub fn codex_responses_model_capabilities_from_card(
    card: &Value,
) -> Option<(String, CodexResponsesModelCapabilities)> {
    let card = card.as_object()?;
    let model_id = card
        .get("slug")
        .or_else(|| card.get("id"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    let mut capabilities = conservative_codex_responses_model_capabilities();
    if let Some(value) = card.get("use_responses_lite").and_then(Value::as_bool) {
        capabilities.use_responses_lite = value;
    }
    capabilities.supports_reasoning_summary_parameter = card
        .get("supports_reasoning_summary_parameter")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if let Some(value) = card
        .get("default_reasoning_level")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        capabilities.default_reasoning_effort = Some(value.to_string());
    }
    if let Some(value) = card
        .get("default_reasoning_summary")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        capabilities.default_reasoning_summary =
            (!value.eq_ignore_ascii_case("none")).then(|| value.to_ascii_lowercase());
    }
    if let Some(levels) = card
        .get("supported_reasoning_levels")
        .and_then(Value::as_array)
    {
        capabilities.supported_reasoning_efforts = levels
            .iter()
            .filter_map(|level| {
                level
                    .get("effort")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
            .collect();
    }
    if model_id == "gpt-5.4-mini" {
        capabilities.default_reasoning_effort = Some("none".to_string());
        if !capabilities.supported_reasoning_efforts.is_empty()
            && !capabilities.supports_reasoning_effort("none")
        {
            capabilities
                .supported_reasoning_efforts
                .insert(0, "none".to_string());
        }
    }
    if let Some(value) = card
        .get("supports_parallel_tool_calls")
        .and_then(Value::as_bool)
    {
        capabilities.supports_parallel_tool_calls = value;
    }
    if let Some(value) = card.get("support_verbosity").and_then(Value::as_bool) {
        capabilities.support_verbosity = value;
    }
    if let Some(value) = card
        .get("default_verbosity")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        capabilities.default_verbosity = Some(value.to_ascii_lowercase());
    }
    if let Some(service_tiers) = card.get("service_tiers").and_then(Value::as_array) {
        capabilities.supported_service_tiers = service_tiers
            .iter()
            .filter_map(|tier| {
                tier.get("id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
            })
            .collect();
    }
    Some((model_id.to_string(), capabilities))
}

pub fn build_codex_model_catalog_metadata(cards: &[Value]) -> Value {
    let cards = cards
        .iter()
        .filter_map(|card| {
            let model_id = card
                .get("slug")
                .or_else(|| card.get("id"))
                .and_then(Value::as_str)?;
            Some((model_id.to_string(), codex_execution_model_card(card)))
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        CODEX_MODEL_CATALOG_METADATA_FIELD: {
            "cards": cards,
        },
    })
}

/// Projects an upstream Codex catalog card onto an authorized Aether model name.
///
/// Catalog cards are versioned by Codex and may gain fields that Aether does not know about. Keep
/// the matching card opaque: only the Aether-internal identity fields are removed and `slug` is
/// replaced with the downstream model name. A non-empty, exact `id` or `slug` match is the only
/// schema requirement.
pub fn project_codex_catalog_model_card(
    catalog_models: &[Value],
    source_model: &str,
    global_model: &str,
) -> Option<Value> {
    if source_model.trim().is_empty() || global_model.trim().is_empty() {
        return None;
    }

    let mut card = catalog_models
        .iter()
        .filter_map(Value::as_object)
        .find(|card| {
            ["id", "slug"].iter().any(|field| {
                card.get(*field)
                    .and_then(Value::as_str)
                    .filter(|identity| !identity.trim().is_empty())
                    .is_some_and(|identity| identity == source_model)
            })
        })?
        .clone();

    card.remove("id");
    card.remove("api_formats");
    card.insert("slug".to_string(), Value::String(global_model.to_string()));
    Some(Value::Object(card))
}

fn codex_execution_model_card(card: &Value) -> Value {
    const EXCLUDED_FIELDS: [&str; 3] =
        ["base_instructions", "model_messages", "available_in_plans"];
    let Some(card) = card.as_object() else {
        return card.clone();
    };
    Value::Object(
        card.iter()
            .filter(|(key, _)| !EXCLUDED_FIELDS.contains(&key.as_str()))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

pub fn effective_codex_model_cards(remote_cards: &[Value]) -> Vec<Value> {
    if remote_cards.iter().any(|card| {
        card.get("visibility")
            .and_then(Value::as_str)
            .is_some_and(|visibility| visibility == "list")
    }) {
        return remote_cards.to_vec();
    }

    let mut cards = bundled_codex_model_cards().to_vec();
    for remote_card in remote_cards {
        let Some(remote_id) = remote_card
            .get("slug")
            .or_else(|| remote_card.get("id"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        if let Some(index) = cards.iter().position(|card| {
            card.get("slug")
                .or_else(|| card.get("id"))
                .and_then(Value::as_str)
                .is_some_and(|model_id| model_id == remote_id)
        }) {
            let Some(base) = cards[index].as_object() else {
                cards[index] = remote_card.clone();
                continue;
            };
            let Some(overlay) = remote_card.as_object() else {
                cards[index] = remote_card.clone();
                continue;
            };
            let mut merged = base.clone();
            merged.extend(overlay.clone());
            cards[index] = Value::Object(merged);
        } else {
            cards.push(remote_card.clone());
        }
    }
    cards
}

fn reasoning_level_cards(efforts: &[&str]) -> Vec<Value> {
    efforts
        .iter()
        .map(|effort| {
            let description = match *effort {
                "low" => "Fast responses with lighter reasoning",
                "medium" => "Balances speed and reasoning depth for everyday tasks",
                "high" => "Greater reasoning depth for complex problems",
                "xhigh" => "Extra high reasoning depth for complex problems",
                "max" => "Maximum reasoning depth for the hardest problems",
                "ultra" => "Maximum reasoning with automatic task delegation",
                _ => "",
            };
            json!({ "effort": effort, "description": description })
        })
        .collect()
}

struct BundledCodexModelCardSpec<'a> {
    model_id: &'a str,
    display_name: &'a str,
    description: &'a str,
    default_reasoning_level: &'a str,
    default_reasoning_summary: &'a str,
    use_responses_lite: bool,
    efforts: &'a [&'a str],
    default_verbosity: &'a str,
    supports_priority_tier: bool,
}

fn bundled_codex_model_card(spec: BundledCodexModelCardSpec<'_>) -> Value {
    let service_tiers = spec
        .supports_priority_tier
        .then(|| {
            json!({
                "id": "priority",
                "name": "Fast",
                "description": "1.5x speed, increased usage",
            })
        })
        .into_iter()
        .collect::<Vec<_>>();
    json!({
        "id": spec.model_id,
        "slug": spec.model_id,
        "object": "model",
        "owned_by": "openai",
        "display_name": spec.display_name,
        "description": spec.description,
        "api_formats": ["openai:responses"],
        "default_reasoning_level": spec.default_reasoning_level,
        "supported_reasoning_levels": reasoning_level_cards(spec.efforts),
        "default_reasoning_summary": spec.default_reasoning_summary,
        "support_verbosity": true,
        "default_verbosity": spec.default_verbosity,
        "supports_parallel_tool_calls": true,
        "service_tiers": service_tiers,
        "use_responses_lite": spec.use_responses_lite,
    })
}

pub fn bundled_codex_model_cards() -> &'static [Value] {
    static CARDS: OnceLock<Vec<Value>> = OnceLock::new();
    CARDS.get_or_init(|| {
        // 公开能力快照来自 openai/codex rust-v0.159.3 的
        // codex-rs/models-manager/models.json；账户远端目录继续优先。
        let mut cards: Vec<Value> = serde_json::from_str(include_str!("codex_models_0_159_3.json"))
            .expect("bundled Codex model capability snapshot must be valid JSON");
        for card in &mut cards {
            let object = card.as_object_mut().expect("model card must be an object");
            object.insert("id".to_string(), object["slug"].clone());
            object.insert("object".to_string(), json!("model"));
            object.insert("owned_by".to_string(), json!("openai"));
            object.insert("api_formats".to_string(), json!(["openai:responses"]));
        }
        // 保留现有配置路由使用的旧模型标识。
        cards.extend(vec![
            bundled_codex_model_card(BundledCodexModelCardSpec {
                model_id: "gpt-5.4",
                display_name: "GPT-5.4",
                description: "Strong model for everyday coding.",
                default_reasoning_level: "medium",
                default_reasoning_summary: "none",
                use_responses_lite: false,
                efforts: &["low", "medium", "high", "xhigh"],
                default_verbosity: "low",
                supports_priority_tier: true,
            }),
            bundled_codex_model_card(BundledCodexModelCardSpec {
                model_id: "gpt-5.4-mini",
                display_name: "GPT-5.4 Mini",
                description: "Small, fast, and cost-efficient model for simpler coding tasks.",
                default_reasoning_level: "none",
                default_reasoning_summary: "none",
                use_responses_lite: false,
                efforts: &["none", "low", "medium", "high", "xhigh"],
                default_verbosity: "medium",
                supports_priority_tier: false,
            }),
            bundled_codex_model_card(BundledCodexModelCardSpec {
                model_id: "gpt-5.2",
                display_name: "GPT-5.2",
                description: "Optimized for professional work and long-running agents.",
                default_reasoning_level: "medium",
                default_reasoning_summary: "auto",
                use_responses_lite: false,
                efforts: &["low", "medium", "high", "xhigh"],
                default_verbosity: "low",
                supports_priority_tier: false,
            }),
        ]);
        cards
    })
}

fn bundled_codex_model_card_for_identity(model_id: &str) -> Option<&'static Value> {
    let find = |model: &str| {
        bundled_codex_model_cards()
            .iter()
            .filter_map(|card| {
                let slug = card.get("slug").and_then(Value::as_str)?;
                model.starts_with(slug).then_some((slug.len(), card))
            })
            .max_by_key(|(slug_len, _)| *slug_len)
            .map(|(_, card)| card)
    };
    find(model_id).or_else(|| codex_namespaced_model_suffix(model_id).and_then(find))
}

fn catalog_codex_model_card_for_identity<'a>(
    cards: &'a serde_json::Map<String, Value>,
    model_id: &str,
) -> Option<&'a Value> {
    let find = |model: &str| {
        cards
            .iter()
            .filter_map(|(catalog_id, card)| {
                model
                    .starts_with(catalog_id)
                    .then_some((catalog_id.len(), card))
            })
            .max_by_key(|(slug_len, _)| *slug_len)
            .map(|(_, card)| card)
    };
    find(model_id).or_else(|| codex_namespaced_model_suffix(model_id).and_then(find))
}

pub fn resolve_codex_responses_model_capabilities(
    provider_model: &str,
    source_model: &str,
    upstream_metadata: Option<&Value>,
) -> CodexResponsesModelCapabilities {
    let provider_model = provider_model.trim();
    let source_model = source_model.trim();
    let source_fallback = (provider_model.is_empty()
        || crate::formats::shared::model_directives::openai_model_capability_is_opaque(
            provider_model,
            provider_model,
        ))
    .then_some(source_model)
    .filter(|model| !model.is_empty() && *model != provider_model);
    if let Some(cards) = upstream_metadata
        .and_then(|metadata| metadata.get(CODEX_MODEL_CATALOG_METADATA_FIELD))
        .and_then(|catalog| catalog.get("cards"))
        .and_then(Value::as_object)
    {
        if !cards.is_empty() {
            return catalog_codex_model_card_for_identity(cards, provider_model)
                .or_else(|| {
                    source_fallback
                        .and_then(|model| catalog_codex_model_card_for_identity(cards, model))
                })
                .and_then(codex_responses_model_capabilities_from_card)
                .map(|(_, capabilities)| capabilities)
                .unwrap_or_else(conservative_codex_responses_model_capabilities);
        }
    }
    bundled_codex_model_card_for_identity(provider_model)
        .or_else(|| source_fallback.and_then(bundled_codex_model_card_for_identity))
        .and_then(codex_responses_model_capabilities_from_card)
        .map(|(_, capabilities)| capabilities)
        .unwrap_or_else(conservative_codex_responses_model_capabilities)
}

fn codex_openai_endpoint_kind(
    provider_type: &str,
    provider_api_format: &str,
) -> Option<CodexOpenAiEndpointKind> {
    if !provider_type.trim().eq_ignore_ascii_case("codex") {
        return None;
    }
    if is_openai_responses_compact_request(provider_api_format) {
        Some(CodexOpenAiEndpointKind::Compact)
    } else if aether_ai_formats::is_openai_responses_format(provider_api_format) {
        Some(CodexOpenAiEndpointKind::Responses)
    } else if aether_ai_formats::api_format_alias_matches(provider_api_format, "openai:search") {
        Some(CodexOpenAiEndpointKind::Search)
    } else if is_openai_image_request(provider_api_format) {
        Some(CodexOpenAiEndpointKind::Images)
    } else if aether_ai_formats::api_format_alias_matches(provider_api_format, "codex:live") {
        Some(CodexOpenAiEndpointKind::Live)
    } else {
        None
    }
}

/// Matches an explicit image-generation selection in `tool_choice`.
/// The `tools` array describes availability and does not select a tool.
fn codex_openai_responses_tool_choice_references_image_generation(
    body_object: &serde_json::Map<String, Value>,
) -> bool {
    match body_object.get("tool_choice") {
        Some(Value::String(name)) => name.trim().eq_ignore_ascii_case("image_generation"),
        Some(Value::Object(choice)) => choice
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.trim().eq_ignore_ascii_case("image_generation")),
        _ => false,
    }
}

fn apply_codex_openai_image_tool_overrides(body_object: &mut serde_json::Map<String, Value>) {
    let mut tool = body_object
        .get("tools")
        .and_then(Value::as_array)
        .and_then(|tools| tools.first())
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    tool.insert("type".to_string(), json!("image_generation"));
    tool.entry("output_format".to_string())
        .or_insert_with(|| json!(CODEX_OPENAI_IMAGE_DEFAULT_OUTPUT_FORMAT));
    let action = tool
        .get("action")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("generate")
        .to_string();
    if !tool.contains_key("action") {
        tool.insert("action".to_string(), json!("generate"));
    }
    if action == "generate" {
        tool.entry("size".to_string())
            .or_insert_with(|| json!(CODEX_IMAGE_TOOL_DEFAULT_SIZE));
        tool.entry("quality".to_string())
            .or_insert_with(|| json!(CODEX_IMAGE_TOOL_DEFAULT_QUALITY));
        tool.entry("background".to_string())
            .or_insert_with(|| json!(CODEX_IMAGE_TOOL_DEFAULT_BACKGROUND));
    }

    body_object.insert("tools".to_string(), json!([tool]));
    body_object.insert(
        "tool_choice".to_string(),
        json!({
            "type": "image_generation"
        }),
    );
}

fn codex_openai_image_has_prompt(body_object: &serde_json::Map<String, Value>) -> bool {
    body_object
        .get("input")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(|item| item.get("content"))
        .any(|content| match content {
            Value::String(text) => !text.trim().is_empty(),
            Value::Array(items) => items.iter().any(|item| {
                item.as_object()
                    .filter(|item| item.get("type").and_then(Value::as_str) == Some("input_text"))
                    .and_then(|item| item.get("text").and_then(Value::as_str))
                    .map(str::trim)
                    .is_some_and(|text| !text.is_empty())
            }),
            _ => false,
        })
}

fn inject_codex_default_variation_prompt(body_object: &mut serde_json::Map<String, Value>) {
    let Some(action) = body_object
        .get("tools")
        .and_then(Value::as_array)
        .and_then(|tools| tools.first())
        .and_then(Value::as_object)
        .and_then(|tool| tool.get("action"))
        .and_then(Value::as_str)
    else {
        return;
    };
    if action != "edit" || codex_openai_image_has_prompt(body_object) {
        return;
    }

    let Some(input) = body_object.get_mut("input").and_then(Value::as_array_mut) else {
        return;
    };
    let Some(first_message) = input.first_mut().and_then(Value::as_object_mut) else {
        return;
    };
    let Some(content) = first_message
        .get_mut("content")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    content.insert(
        0,
        json!({
            "type": "input_text",
            "text": CODEX_OPENAI_IMAGE_DEFAULT_VARIATION_PROMPT,
        }),
    );
}

fn strip_codex_content_cache_control_fields(content: &mut Value) {
    match content {
        Value::Array(parts) => {
            for part in parts {
                if let Some(part) = part.as_object_mut() {
                    part.remove("cache_control");
                }
            }
        }
        Value::Object(part) => {
            part.remove("cache_control");
        }
        _ => {}
    }
}

fn strip_codex_tool_cache_control_fields(tools: &mut Value) {
    let Some(tools) = tools.as_array_mut() else {
        return;
    };
    for tool in tools {
        let Some(tool) = tool.as_object_mut() else {
            continue;
        };
        tool.remove("cache_control");
        if let Some(function) = tool.get_mut("function").and_then(Value::as_object_mut) {
            function.remove("cache_control");
        }
    }
}

fn strip_codex_cache_control_fields(value: &mut Value) {
    let Some(body) = value.as_object_mut() else {
        return;
    };
    body.remove("cache_control");
    if let Some(tools) = body.get_mut("tools") {
        strip_codex_tool_cache_control_fields(tools);
    }
    if let Some(input) = body.get_mut("input").and_then(Value::as_array_mut) {
        for item in input {
            let Some(item) = item.as_object_mut() else {
                continue;
            };
            item.remove("cache_control");
            if let Some(content) = item.get_mut("content") {
                strip_codex_content_cache_control_fields(content);
            }
            if item
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|item_type| item_type == "additional_tools")
            {
                if let Some(tools) = item.get_mut("tools") {
                    strip_codex_tool_cache_control_fields(tools);
                }
            }
        }
    }
}

fn remove_btree_header(headers: &mut BTreeMap<String, String>, header_name: &str) {
    headers.retain(|name, _| !name.trim().eq_ignore_ascii_case(header_name));
}

fn header_value_contains_media_type(value: &str, media_type: &str) -> bool {
    value.split(',').any(|media_range| {
        media_range
            .split(';')
            .next()
            .map(str::trim)
            .is_some_and(|value| value.eq_ignore_ascii_case(media_type))
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodexAuthIdentity {
    pub account_id: Option<String>,
    pub account_user_id: Option<String>,
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub codex_identity_fingerprint: Option<String>,
    pub is_fedramp: bool,
    pub uses_codex_backend: bool,
}

fn first_non_empty_codex_identity_string<'a>(
    values: impl IntoIterator<Item = Option<&'a Value>>,
) -> Option<String> {
    values.into_iter().find_map(|value| {
        value
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

pub fn parse_codex_auth_identity(decrypted_auth_config_raw: Option<&str>) -> CodexAuthIdentity {
    let Some(raw) = decrypted_auth_config_raw
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return CodexAuthIdentity::default();
    };
    let Ok(value) = serde_json::from_str::<Value>(raw) else {
        return CodexAuthIdentity::default();
    };
    let namespaced_auth = value
        .get("https://api.openai.com/auth")
        .and_then(Value::as_object);
    let namespaced_profile = value
        .get("https://api.openai.com/profile")
        .and_then(Value::as_object);
    let agent_identity = value
        .get("agent_identity")
        .or_else(|| value.get("agentIdentity"))
        .and_then(Value::as_object);
    let account_id = first_non_empty_codex_identity_string([
        value.get("account_id"),
        value.get("accountId"),
        value.get("chatgpt_account_id"),
        value.get("chatgptAccountId"),
        namespaced_auth.and_then(|auth| auth.get("chatgpt_account_id")),
        agent_identity.and_then(|identity| identity.get("account_id")),
        agent_identity.and_then(|identity| identity.get("accountId")),
        agent_identity.and_then(|identity| identity.get("chatgpt_account_id")),
        agent_identity.and_then(|identity| identity.get("chatgptAccountId")),
    ]);
    let account_user_id = first_non_empty_codex_identity_string([
        value.get("account_user_id"),
        value.get("accountUserId"),
        value.get("chatgpt_account_user_id"),
        value.get("chatgptAccountUserId"),
        namespaced_auth.and_then(|auth| auth.get("chatgpt_account_user_id")),
        agent_identity.and_then(|identity| identity.get("account_user_id")),
        agent_identity.and_then(|identity| identity.get("accountUserId")),
        agent_identity.and_then(|identity| identity.get("chatgpt_account_user_id")),
        agent_identity.and_then(|identity| identity.get("chatgptAccountUserId")),
    ]);
    let user_id = first_non_empty_codex_identity_string([
        value.get("user_id"),
        value.get("userId"),
        value.get("chatgpt_user_id"),
        value.get("chatgptUserId"),
        namespaced_auth.and_then(|auth| auth.get("chatgpt_user_id")),
        agent_identity.and_then(|identity| identity.get("user_id")),
        agent_identity.and_then(|identity| identity.get("userId")),
        agent_identity.and_then(|identity| identity.get("chatgpt_user_id")),
        agent_identity.and_then(|identity| identity.get("chatgptUserId")),
        value.get("sub"),
    ]);
    let email = first_non_empty_codex_identity_string([
        value.get("email"),
        value.get("email_address"),
        value.get("emailAddress"),
        value.get("outlook_email"),
        namespaced_auth.and_then(|auth| auth.get("email")),
        namespaced_auth.and_then(|auth| auth.get("email_address")),
        namespaced_auth.and_then(|auth| auth.get("emailAddress")),
        namespaced_auth.and_then(|auth| auth.get("outlook_email")),
        namespaced_profile.and_then(|profile| profile.get("email")),
        namespaced_profile.and_then(|profile| profile.get("email_address")),
        namespaced_profile.and_then(|profile| profile.get("emailAddress")),
        agent_identity.and_then(|identity| identity.get("email")),
        agent_identity.and_then(|identity| identity.get("email_address")),
        agent_identity.and_then(|identity| identity.get("emailAddress")),
        agent_identity.and_then(|identity| identity.get("outlook_email")),
    ]);
    let codex_identity_fingerprint = first_non_empty_codex_identity_string([
        value.get("codex_identity_fingerprint"),
        value.get("codex-identity-fingerprint"),
        value.get("codexIdentityFingerprint"),
        agent_identity.and_then(|identity| identity.get("codex_identity_fingerprint")),
        agent_identity.and_then(|identity| identity.get("codex-identity-fingerprint")),
        agent_identity.and_then(|identity| identity.get("codexIdentityFingerprint")),
    ]);
    let is_fedramp = value
        .get("is_fedramp")
        .or_else(|| value.get("chatgpt_account_is_fedramp"))
        .or_else(|| value.get("chatgptAccountIsFedramp"))
        .or_else(|| namespaced_auth.and_then(|auth| auth.get("chatgpt_account_is_fedramp")))
        .or_else(|| {
            agent_identity.and_then(|identity| {
                identity
                    .get("is_fedramp")
                    .or_else(|| identity.get("chatgpt_account_is_fedramp"))
                    .or_else(|| identity.get("chatgptAccountIsFedramp"))
            })
        })
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let uses_codex_backend = account_id.is_some()
        || account_user_id.is_some()
        || user_id.is_some()
        || codex_identity_fingerprint.is_some()
        || value
            .get("provider_type")
            .and_then(Value::as_str)
            .is_some_and(|provider_type| provider_type.trim().eq_ignore_ascii_case("codex"))
        || value
            .get("auth_mode")
            .or_else(|| value.get("authMode"))
            .and_then(Value::as_str)
            .is_some_and(|auth_mode| auth_mode.trim().eq_ignore_ascii_case("agentIdentity"));

    CodexAuthIdentity {
        account_id,
        account_user_id,
        user_id,
        email,
        codex_identity_fingerprint,
        is_fedramp,
        uses_codex_backend,
    }
}

fn set_codex_client_header(
    provider_request_headers: &mut BTreeMap<String, String>,
    header_name: &str,
    header_value: &str,
) {
    remove_btree_header(provider_request_headers, header_name);
    provider_request_headers.insert(header_name.to_string(), header_value.to_string());
}

pub fn apply_openai_responses_compact_special_body_edits(
    provider_request_body: &mut Value,
    provider_api_format: &str,
) {
    if !is_openai_responses_compact_request(provider_api_format) {
        return;
    }

    let Some(body_object) = provider_request_body.as_object_mut() else {
        return;
    };
    super::request::apply_compact_request_projection(body_object);
}

pub fn apply_codex_openai_responses_compact_body_edits(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
) {
    if !is_codex_openai_responses_request(provider_type, provider_api_format)
        || !is_openai_responses_compact_request(provider_api_format)
    {
        return;
    }
    strip_codex_cache_control_fields(provider_request_body);
    let Some(body_object) = provider_request_body.as_object_mut() else {
        return;
    };
    body_object
        .retain(|field, _| CODEX_OPENAI_RESPONSES_COMPACT_BODY_FIELDS.contains(&field.as_str()));
    body_object
        .entry("parallel_tool_calls".to_string())
        .or_insert_with(|| json!(true));
    for field in [
        "tools",
        "reasoning",
        "service_tier",
        "prompt_cache_key",
        "text",
    ] {
        if body_object.get(field).is_some_and(Value::is_null) {
            body_object.remove(field);
        }
    }
    if body_object
        .get("instructions")
        .is_some_and(|value| value.is_null() || value.as_str().is_some_and(str::is_empty))
    {
        body_object.remove("instructions");
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexOpenAiCompactRequestContractViolation {
    pub field: &'static str,
    pub reason: &'static str,
}

pub fn validate_codex_openai_responses_compact_request_contract(
    provider_request_body: &Value,
    provider_type: &str,
    provider_api_format: &str,
) -> Result<(), CodexOpenAiCompactRequestContractViolation> {
    if codex_openai_endpoint_kind(provider_type, provider_api_format)
        != Some(CodexOpenAiEndpointKind::Compact)
    {
        return Ok(());
    }
    let Some(body) = provider_request_body.as_object() else {
        return Err(CodexOpenAiCompactRequestContractViolation {
            field: "$",
            reason: "Codex Compact request body must be an object",
        });
    };
    if body
        .get("model")
        .and_then(Value::as_str)
        .is_none_or(|model| model.trim().is_empty())
    {
        return Err(CodexOpenAiCompactRequestContractViolation {
            field: "model",
            reason: "Codex Compact model must be a non-empty string",
        });
    }
    let Some(input) = body.get("input").and_then(Value::as_array) else {
        return Err(CodexOpenAiCompactRequestContractViolation {
            field: "input",
            reason: "Codex Compact input must be an array",
        });
    };
    if input.iter().any(|item| {
        !item.as_object().is_some_and(|item| {
            item.get("type")
                .and_then(Value::as_str)
                .is_some_and(|item_type| !item_type.trim().is_empty())
        })
    }) {
        return Err(CodexOpenAiCompactRequestContractViolation {
            field: "input",
            reason: "Codex Compact input items must be typed objects",
        });
    }
    for (field, valid) in [
        (
            "instructions",
            body.get("instructions").is_none_or(Value::is_string),
        ),
        ("tools", body.get("tools").is_none_or(Value::is_array)),
        (
            "reasoning",
            body.get("reasoning").is_none_or(Value::is_object),
        ),
        (
            "service_tier",
            body.get("service_tier").is_none_or(Value::is_string),
        ),
        (
            "prompt_cache_key",
            body.get("prompt_cache_key").is_none_or(Value::is_string),
        ),
        ("text", body.get("text").is_none_or(Value::is_object)),
    ] {
        if !valid {
            return Err(CodexOpenAiCompactRequestContractViolation {
                field,
                reason: "Codex Compact optional field has an invalid type",
            });
        }
    }
    if !body
        .get("parallel_tool_calls")
        .is_some_and(Value::is_boolean)
    {
        return Err(CodexOpenAiCompactRequestContractViolation {
            field: "parallel_tool_calls",
            reason: "Codex Compact parallel_tool_calls must be a boolean",
        });
    }
    Ok(())
}

pub fn apply_codex_openai_compact_terminal_headers(
    provider_request_headers: &mut BTreeMap<String, String>,
    provider_type: &str,
    provider_api_format: &str,
) {
    if codex_openai_endpoint_kind(provider_type, provider_api_format)
        != Some(CodexOpenAiEndpointKind::Compact)
    {
        return;
    }
    remove_btree_header(provider_request_headers, "x-client-request-id");
    remove_btree_header(provider_request_headers, "accept");
    remove_btree_header(provider_request_headers, "content-encoding");
}

fn apply_codex_model_request_capabilities(
    body_object: &mut serde_json::Map<String, Value>,
    provider_api_format: &str,
    capabilities: &CodexResponsesModelCapabilities,
    body_rules: Option<&Value>,
) {
    if is_openai_image_request(provider_api_format) {
        return;
    }
    if !body_rules_handle_path(body_rules, "parallel_tool_calls") {
        let requested = body_object
            .get("parallel_tool_calls")
            .and_then(Value::as_bool)
            .unwrap_or(capabilities.supports_parallel_tool_calls);
        body_object.insert(
            "parallel_tool_calls".to_string(),
            json!(
                requested
                    && capabilities.supports_parallel_tool_calls
                    && !capabilities.use_responses_lite
            ),
        );
    }

    if !is_openai_responses_compact_request(provider_api_format)
        && !body_rules_handle_path(body_rules, "include")
    {
        let include = if body_object.get("reasoning").is_some_and(Value::is_object) {
            json!([CODEX_REASONING_ENCRYPTED_CONTENT_INCLUDE])
        } else {
            json!([])
        };
        body_object.insert("include".to_string(), include);
    }

    if !body_rules_handle_path(body_rules, "text") {
        if capabilities.support_verbosity {
            if let Some(default_verbosity) = capabilities.default_verbosity.as_deref() {
                match body_object.get_mut("text") {
                    Some(Value::Object(text)) => {
                        text.entry("verbosity".to_string())
                            .or_insert_with(|| json!(default_verbosity));
                    }
                    None | Some(Value::Null) => {
                        body_object.insert(
                            "text".to_string(),
                            json!({ "verbosity": default_verbosity }),
                        );
                    }
                    Some(_) => {}
                }
            }
        } else if let Some(Value::Object(text)) = body_object.get_mut("text") {
            text.remove("verbosity");
            if text.is_empty() {
                body_object.remove("text");
            }
        }
    }
}

fn ensure_codex_reasoning_defaults(
    body_object: &mut serde_json::Map<String, Value>,
    capabilities: &CodexResponsesModelCapabilities,
    supports_reasoning_mode: bool,
    body_rules: Option<&Value>,
) {
    if body_rules_handle_path(body_rules, "reasoning") && !capabilities.use_responses_lite {
        let has_summary = body_object
            .get_mut("reasoning")
            .and_then(Value::as_object_mut)
            .and_then(|reasoning| {
                if !capabilities.supports_reasoning_summary_parameter
                    || reasoning
                        .get("summary")
                        .is_some_and(codex_reasoning_summary_is_disabled)
                {
                    reasoning.remove("summary");
                }
                reasoning.get("summary")
            })
            .is_some_and(|summary| !summary.is_null());
        if !has_summary {
            remove_codex_reasoning_summary_delivery(body_object);
        }
        return;
    }
    let reasoning = body_object
        .entry("reasoning".to_string())
        .or_insert_with(|| json!({}));
    if reasoning.is_null() {
        *reasoning = json!({});
    }
    let Some(reasoning_object) = reasoning.as_object_mut() else {
        return;
    };
    if reasoning_object.get("effort").is_none_or(Value::is_null) {
        let default_effort = if supports_reasoning_mode
            && reasoning_object
                .get("mode")
                .and_then(Value::as_str)
                .is_some_and(|mode| matches!(mode, "standard" | "pro"))
        {
            Some(CODEX_DEFAULT_REASONING_EFFORT)
        } else {
            capabilities.default_reasoning_effort.as_deref()
        };
        if let Some(default_effort) = default_effort {
            reasoning_object.insert("effort".to_string(), json!(default_effort));
        }
    }
    if !capabilities.supports_reasoning_summary_parameter
        || reasoning_object
            .get("summary")
            .is_some_and(codex_reasoning_summary_is_disabled)
    {
        reasoning_object.remove("summary");
    } else if !reasoning_object.contains_key("summary") {
        if let Some(summary) = capabilities.default_reasoning_summary.as_deref() {
            reasoning_object.insert("summary".to_string(), json!(summary));
        }
    }
    if capabilities.use_responses_lite {
        reasoning_object.insert("context".to_string(), json!("all_turns"));
    }
    let has_summary = reasoning_object
        .get("summary")
        .is_some_and(|summary| !summary.is_null());
    if !has_summary {
        remove_codex_reasoning_summary_delivery(body_object);
    }
}

fn codex_reasoning_summary_is_disabled(value: &Value) -> bool {
    value.is_null()
        || value
            .as_str()
            .is_some_and(|summary| summary.eq_ignore_ascii_case("none"))
}

fn remove_codex_reasoning_summary_delivery(body_object: &mut serde_json::Map<String, Value>) {
    let remove_stream_options = body_object
        .get_mut("stream_options")
        .and_then(Value::as_object_mut)
        .is_some_and(|stream_options| {
            stream_options.remove("reasoning_summary_delivery");
            stream_options.is_empty()
        });
    if remove_stream_options {
        body_object.remove("stream_options");
    }
}

fn is_codex_responses_lite_additional_tools_item(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|item_type| item_type == "additional_tools")
}

fn is_codex_responses_lite_static_additional_tools_item(value: &Value) -> bool {
    is_codex_responses_lite_additional_tools_item(value)
        && value.get("role").and_then(Value::as_str) == Some("developer")
        && value.get("tools").is_some_and(Value::is_array)
}

fn codex_tool_type_accepts_top_level_name(tool_type: &str) -> bool {
    matches!(tool_type, "function" | "custom" | "namespace")
}

/// Returns whether a tool is represented in the Responses Lite synthetic
/// `additional_tools` item and therefore becomes part of stored history.
///
/// Keep callers that compare raw client configuration with the synthetic wire
/// prefix on this predicate so hosted/server-executed tools do not create a
/// false configuration change.
pub fn codex_responses_lite_tool_is_client_executed(tool: &Value) -> bool {
    match tool.get("type").and_then(Value::as_str) {
        Some("function" | "custom" | "namespace") => true,
        Some("tool_search") => tool.get("execution").and_then(Value::as_str) == Some("client"),
        _ => false,
    }
}

fn retain_codex_client_executed_tools(additional_tools: &mut Value) {
    let Some(tools) = additional_tools
        .get_mut("tools")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    tools.retain(codex_responses_lite_tool_is_client_executed);
}

fn is_codex_responses_lite_instruction_item(value: &Value) -> bool {
    value
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|item_type| item_type == "message")
        && value
            .get("role")
            .and_then(Value::as_str)
            .is_some_and(|role| role == "developer")
        && value
            .get("content")
            .and_then(Value::as_array)
            .is_some_and(|content| {
                content.len() == 1
                    && content[0]
                        .get("type")
                        .and_then(Value::as_str)
                        .is_some_and(|item_type| item_type == "input_text")
                    && content[0].get("text").is_some_and(Value::is_string)
            })
}

fn strip_codex_responses_lite_image_details(item: &mut Value) {
    let Some(item_object) = item.as_object_mut() else {
        return;
    };
    let item_type = item_object
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let content = match item_type {
        "message" => item_object.get_mut("content"),
        "function_call_output" | "custom_tool_call_output" => item_object.get_mut("output"),
        _ => None,
    };
    let Some(content) = content.and_then(Value::as_array_mut) else {
        return;
    };
    for content_item in content {
        let Some(content_object) = content_item.as_object_mut() else {
            continue;
        };
        if content_object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|content_type| content_type == "input_image")
        {
            content_object.remove("detail");
        }
    }
}

fn codex_responses_lite_supports_context_management(context_management: Option<&Value>) -> bool {
    context_management.is_none_or(Value::is_null)
}

fn codex_responses_lite_supports_request_body(provider_request_body: Option<&Value>) -> bool {
    provider_request_body.is_none_or(|body| {
        codex_responses_lite_supports_context_management(body.get("context_management"))
    })
}

fn apply_codex_responses_lite_body_contract(
    body_object: &mut serde_json::Map<String, Value>,
    capabilities: &CodexResponsesModelCapabilities,
    websocket_continuation: bool,
) {
    if !capabilities.use_responses_lite {
        return;
    }

    let tools_are_valid = body_object
        .get("tools")
        .is_none_or(|tools| tools.is_null() || tools.is_array());
    let instructions_are_valid = body_object
        .get("instructions")
        .is_none_or(|instructions| instructions.is_null() || instructions.is_string());
    if !tools_are_valid || !instructions_are_valid {
        return;
    }
    if !body_object.get("input").is_some_and(Value::is_array) {
        return;
    }

    let top_level_tools = body_object
        .remove("tools")
        .and_then(|tools| tools.as_array().cloned())
        .map(|tools| {
            tools
                .into_iter()
                .filter(codex_responses_lite_tool_is_client_executed)
                .collect::<Vec<_>>()
        });
    let top_level_instructions = body_object
        .remove("instructions")
        .and_then(|instructions| instructions.as_str().map(ToOwned::to_owned))
        .filter(|instructions| !instructions.is_empty());
    let input = body_object
        .get_mut("input")
        .and_then(Value::as_array_mut)
        .expect("Responses Lite input was validated as an array");

    // `previous_response_id` already references the prior response's stored
    // input. The Codex client sends only the new input items on these turns;
    // replaying the current top-level tools/instructions as synthetic history
    // would permanently append another copy on every tool round-trip. Keep
    // the Lite wire requirements below, but do not synthesize static config
    // for a continuation.
    if websocket_continuation {
        // A few compatible clients send the already-normalized Lite prefix
        // instead of top-level fields. It is still inherited through
        // `previous_response_id`, so remove every repeated leading prefix
        // item. Limit this to the prefix: a later developer message can be
        // genuine incremental input and must not be deleted by shape alone.
        let mut repeated_prefix_len = 0;
        while input
            .get(repeated_prefix_len)
            .is_some_and(is_codex_responses_lite_static_additional_tools_item)
        {
            repeated_prefix_len += 1;
            if input
                .get(repeated_prefix_len)
                .is_some_and(is_codex_responses_lite_instruction_item)
            {
                repeated_prefix_len += 1;
            }
        }
        if repeated_prefix_len > 0 {
            input.drain(..repeated_prefix_len);
        }
        for item in input.iter_mut() {
            strip_codex_responses_lite_image_details(item);
        }
        body_object.insert("parallel_tool_calls".to_string(), json!(false));
        return;
    }

    let existing_additional_tools = input
        .iter()
        .position(is_codex_responses_lite_additional_tools_item)
        .map(|index| input.remove(index));
    let mut additional_tools = existing_additional_tools.unwrap_or_else(|| {
        json!({
            "type": "additional_tools",
            "role": "developer",
            "tools": [],
        })
    });
    if let Some(tools) = top_level_tools {
        if let Some(object) = additional_tools.as_object_mut() {
            object.insert("tools".to_string(), Value::Array(tools));
        }
    }
    retain_codex_client_executed_tools(&mut additional_tools);
    input.insert(0, additional_tools);

    if let Some(instructions) = top_level_instructions {
        if input
            .get(1)
            .is_some_and(is_codex_responses_lite_instruction_item)
        {
            input.remove(1);
        }
        input.insert(
            1,
            json!({
                "type": "message",
                "role": "developer",
                "content": [{
                    "type": "input_text",
                    "text": instructions,
                }],
            }),
        );
    }

    for item in input
        .iter_mut()
        .filter(|item| !is_codex_responses_lite_additional_tools_item(item))
    {
        strip_codex_responses_lite_image_details(item);
    }
    body_object.insert("parallel_tool_calls".to_string(), json!(false));
}

fn remove_empty_codex_instructions(body_object: &mut serde_json::Map<String, Value>) {
    if body_object
        .get("instructions")
        .is_some_and(|value| value.is_null() || value.as_str().is_some_and(str::is_empty))
    {
        body_object.remove("instructions");
    }
}

fn codex_tool_type_rejects_top_level_name(tool_type: &str) -> bool {
    let normalized = tool_type.trim().to_ascii_lowercase();
    !normalized.is_empty() && !codex_tool_type_accepts_top_level_name(normalized.as_str())
}

fn strip_codex_hosted_tool_names_for_backend(body_object: &mut serde_json::Map<String, Value>) {
    let Some(tools) = body_object.get_mut("tools").and_then(Value::as_array_mut) else {
        return;
    };

    for tool in tools {
        let Some(tool_object) = tool.as_object_mut() else {
            continue;
        };
        if tool_object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(codex_tool_type_rejects_top_level_name)
        {
            tool_object.remove("name");
        }
    }
}

fn strip_codex_hosted_tool_choice_name_for_backend(
    body_object: &mut serde_json::Map<String, Value>,
) {
    let Some(tool_choice_object) = body_object
        .get_mut("tool_choice")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    if tool_choice_object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(codex_tool_type_rejects_top_level_name)
    {
        tool_choice_object.remove("name");
    }
}

fn wrap_codex_responses_string_input_for_backend(
    body_object: &mut serde_json::Map<String, Value>,
    provider_api_format: &str,
) {
    if !aether_ai_formats::is_openai_responses_family_format(provider_api_format) {
        return;
    }
    let Some(text) = body_object
        .get("input")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return;
    };

    body_object.insert(
        "input".to_string(),
        json!([{
            "type": "message",
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": text,
            }],
        }]),
    );
}

fn non_empty_json_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn adapt_codex_prompt_cache_identity_for_backend(body_object: &mut serde_json::Map<String, Value>) {
    // The standard OpenAI contract permits arbitrary prompt_cache_key strings, while the Codex
    // backend's native session identity is UUID-shaped. Adapt only requests that carry a cache key
    // but do not already carry a native Codex session identity.
    let Some(prompt_cache_key) = body_object
        .get("prompt_cache_key")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
    else {
        return;
    };

    match body_object.get("client_metadata") {
        Some(Value::Object(metadata)) => {
            match metadata.get("session_id") {
                Some(Value::String(session_id)) if !session_id.trim().is_empty() => return,
                None | Some(Value::Null) => {}
                Some(_) => return,
            }
            match metadata.get("thread_id") {
                None | Some(Value::Null) => {}
                Some(Value::String(thread_id)) if !thread_id.trim().is_empty() => {}
                Some(_) => return,
            }
        }
        Some(Value::Null) | None => {}
        Some(_) => return,
    }

    let cache_identity = uuid::Uuid::parse_str(&prompt_cache_key)
        .unwrap_or_else(|_| {
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_URL,
                format!("{CODEX_PROMPT_CACHE_IDENTITY_NAMESPACE}{prompt_cache_key}").as_bytes(),
            )
        })
        .to_string();

    {
        let metadata = body_object
            .entry("client_metadata".to_string())
            .or_insert_with(|| json!({}));
        if metadata.is_null() {
            *metadata = json!({});
        }
        let Some(metadata) = metadata.as_object_mut() else {
            return;
        };
        metadata.insert(
            "session_id".to_string(),
            Value::String(cache_identity.clone()),
        );
        if metadata.get("thread_id").is_none_or(Value::is_null) {
            metadata.insert(
                "thread_id".to_string(),
                Value::String(cache_identity.clone()),
            );
        }
    }

    body_object.insert(
        "prompt_cache_key".to_string(),
        Value::String(cache_identity),
    );
}

fn valid_codex_identity_header(value: &str) -> Option<String> {
    let value = value.trim();
    if value.is_empty() || value.parse::<http::HeaderValue>().is_err() {
        return None;
    }
    Some(value.to_string())
}

fn codex_prompt_cache_header_identity(provider_request_body: &Value) -> Option<(String, String)> {
    let prompt_cache_key = non_empty_json_string(provider_request_body.get("prompt_cache_key"))?;

    match provider_request_body.get("client_metadata") {
        Some(Value::Object(metadata)) => match metadata.get("session_id") {
            Some(Value::String(session_id)) if !session_id.trim().is_empty() => {
                let session_id = valid_codex_identity_header(session_id)?;
                let thread_id = match metadata.get("thread_id") {
                    None | Some(Value::Null) => session_id.clone(),
                    Some(Value::String(thread_id)) if !thread_id.trim().is_empty() => {
                        valid_codex_identity_header(thread_id)?
                    }
                    Some(_) => return None,
                };
                return Some((session_id, thread_id));
            }
            None | Some(Value::Null) => {}
            Some(_) => return None,
        },
        Some(Value::Null) | None => {}
        Some(_) => return None,
    }

    let cache_identity = uuid::Uuid::parse_str(prompt_cache_key).ok()?.to_string();
    Some((cache_identity.clone(), cache_identity))
}

fn insert_btree_header_if_missing(
    headers: &mut BTreeMap<String, String>,
    header_name: &str,
    header_value: String,
) {
    if headers.iter().any(|(name, value)| {
        name.trim().eq_ignore_ascii_case(header_name) && !value.trim().is_empty()
    }) {
        return;
    }
    remove_btree_header(headers, header_name);
    headers.insert(header_name.to_string(), header_value);
}

pub fn apply_codex_openai_responses_identity_headers(
    provider_request_headers: &mut BTreeMap<String, String>,
    provider_request_body: &Value,
    provider_type: &str,
    provider_api_format: &str,
) {
    // Codex projects its body session identity into compatibility HTTP headers. Existing explicit
    // headers remain authoritative; only missing values are completed here.
    if !is_codex_openai_responses_request(provider_type, provider_api_format) {
        return;
    }
    let Some((session_id, thread_id)) = codex_prompt_cache_header_identity(provider_request_body)
    else {
        return;
    };
    insert_btree_header_if_missing(provider_request_headers, "session-id", session_id);
    insert_btree_header_if_missing(provider_request_headers, "thread-id", thread_id);
}

pub fn apply_codex_openai_responses_special_body_edits(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    body_rules: Option<&Value>,
    user_api_key_id: Option<&str>,
) {
    let provider_model = provider_request_body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    apply_codex_openai_responses_special_body_edits_with_source_model(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model.as_str(),
        provider_model.as_str(),
        body_rules,
        user_api_key_id,
    );
}

pub fn apply_codex_openai_responses_special_body_edits_with_source_model(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    body_rules: Option<&Value>,
    _user_api_key_id: Option<&str>,
) {
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        None,
        body_rules,
    );
}

pub fn apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    model_capabilities: Option<&CodexResponsesModelCapabilities>,
    body_rules: Option<&Value>,
) {
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities_inner(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        model_capabilities,
        body_rules,
        false,
    );
}

pub fn apply_codex_openai_responses_websocket_continuation_body_edits_with_source_model_and_capabilities(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    model_capabilities: Option<&CodexResponsesModelCapabilities>,
    body_rules: Option<&Value>,
) {
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities_inner(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        model_capabilities,
        body_rules,
        true,
    );
}

#[allow(clippy::too_many_arguments)]
fn apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities_inner(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    model_capabilities: Option<&CodexResponsesModelCapabilities>,
    body_rules: Option<&Value>,
    websocket_continuation: bool,
) {
    if !is_codex_openai_responses_request(provider_type, provider_api_format) {
        return;
    }

    let Some(body_object) = provider_request_body.as_object_mut() else {
        return;
    };

    wrap_codex_responses_string_input_for_backend(body_object, provider_api_format);
    for field in CODEX_OPENAI_RESPONSES_UNSUPPORTED_BODY_FIELDS {
        if !body_rules_handle_path(body_rules, field) {
            body_object.remove(*field);
        }
    }
    adapt_codex_prompt_cache_identity_for_backend(body_object);
    if is_openai_responses_compact_request(provider_api_format) {
        body_object.remove("store");
    } else if !body_rules_handle_path(body_rules, "store") {
        body_object.insert("store".to_string(), json!(false));
    }
    remove_empty_codex_instructions(body_object);
    strip_codex_hosted_tool_names_for_backend(body_object);
    strip_codex_hosted_tool_choice_name_for_backend(body_object);
    if !is_openai_responses_compact_request(provider_api_format) {
        body_object
            .entry("tool_choice".to_string())
            .or_insert_with(|| json!("auto"));
    }
    if !is_openai_responses_compact_request(provider_api_format)
        && codex_openai_responses_tool_choice_references_image_generation(body_object)
    {
        body_object.insert(
            "model".to_string(),
            json!(CODEX_OPENAI_IMAGE_INTERNAL_MODEL),
        );
        body_object.insert("stream".to_string(), json!(true));
        apply_codex_openai_image_tool_overrides(body_object);
        inject_codex_default_variation_prompt(body_object);
    }
    let effective_provider_model = body_object
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .unwrap_or(provider_model)
        .to_string();
    let bundled_capabilities;
    let capabilities = if effective_provider_model == CODEX_OPENAI_IMAGE_INTERNAL_MODEL {
        bundled_capabilities = resolve_codex_responses_model_capabilities(
            effective_provider_model.as_str(),
            source_model,
            None,
        );
        &bundled_capabilities
    } else if let Some(capabilities) = model_capabilities {
        capabilities
    } else {
        bundled_capabilities = resolve_codex_responses_model_capabilities(
            effective_provider_model.as_str(),
            source_model,
            None,
        );
        &bundled_capabilities
    };
    let standard_contract_capabilities = (capabilities.use_responses_lite
        && !codex_responses_lite_supports_context_management(
            body_object.get("context_management"),
        ))
    .then(|| CodexResponsesModelCapabilities {
        use_responses_lite: false,
        ..capabilities.clone()
    });
    let capabilities = standard_contract_capabilities
        .as_ref()
        .unwrap_or(capabilities);
    let supports_reasoning_mode =
        crate::formats::shared::model_directives::openai_model_resolves_to_gpt_5_6(
            effective_provider_model.as_str(),
            source_model,
        );
    ensure_codex_reasoning_defaults(
        body_object,
        capabilities,
        supports_reasoning_mode,
        body_rules,
    );
    apply_codex_model_request_capabilities(
        body_object,
        provider_api_format,
        capabilities,
        body_rules,
    );
    apply_codex_responses_lite_body_contract(body_object, capabilities, websocket_continuation);
    strip_codex_cache_control_fields(provider_request_body);
    apply_codex_openai_responses_compact_body_edits(
        provider_request_body,
        provider_type,
        provider_api_format,
    );
}

pub fn apply_codex_openai_responses_chat_body_edits(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    body_rules: Option<&Value>,
    user_api_key_id: Option<&str>,
) {
    let provider_model = provider_request_body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    apply_codex_openai_responses_chat_body_edits_with_source_model(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model.as_str(),
        provider_model.as_str(),
        body_rules,
        user_api_key_id,
    );
}

pub fn apply_codex_openai_responses_chat_body_edits_with_source_model(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    body_rules: Option<&Value>,
    _user_api_key_id: Option<&str>,
) {
    apply_codex_openai_responses_chat_body_edits_with_source_model_and_capabilities(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        None,
        body_rules,
    );
}

pub fn apply_codex_openai_responses_chat_body_edits_with_source_model_and_capabilities(
    provider_request_body: &mut Value,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    model_capabilities: Option<&CodexResponsesModelCapabilities>,
    body_rules: Option<&Value>,
) {
    apply_codex_openai_responses_special_body_edits_with_source_model_and_capabilities(
        provider_request_body,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        model_capabilities,
        body_rules,
    );

    if !is_codex_openai_responses_request(provider_type, provider_api_format) {
        return;
    }
    let Some(body_object) = provider_request_body.as_object_mut() else {
        return;
    };
    if let Some(prompt_cache_key) = body_object.remove("prompt_cache_key") {
        body_object.insert("prompt_cache_key".to_string(), prompt_cache_key);
    }
}

pub fn apply_codex_openai_responses_lite_header(
    provider_request_headers: &mut BTreeMap<String, String>,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
) {
    apply_codex_openai_responses_lite_header_with_capabilities(
        provider_request_headers,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        None,
    );
}

pub fn apply_codex_openai_responses_lite_header_with_capabilities(
    provider_request_headers: &mut BTreeMap<String, String>,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    model_capabilities: Option<&CodexResponsesModelCapabilities>,
) {
    apply_codex_openai_responses_lite_header_for_request_body_with_capabilities(
        provider_request_headers,
        None,
        provider_type,
        provider_api_format,
        provider_model,
        source_model,
        model_capabilities,
    );
}

pub fn apply_codex_openai_responses_lite_header_for_request_body_with_capabilities(
    provider_request_headers: &mut BTreeMap<String, String>,
    provider_request_body: Option<&Value>,
    provider_type: &str,
    provider_api_format: &str,
    provider_model: &str,
    source_model: &str,
    model_capabilities: Option<&CodexResponsesModelCapabilities>,
) {
    if !is_codex_openai_responses_request(provider_type, provider_api_format) {
        return;
    }
    remove_btree_header(provider_request_headers, CODEX_RESPONSES_LITE_HEADER);
    let bundled_capabilities;
    let capabilities = if let Some(capabilities) = model_capabilities {
        capabilities
    } else {
        bundled_capabilities =
            resolve_codex_responses_model_capabilities(provider_model, source_model, None);
        &bundled_capabilities
    };
    if capabilities.use_responses_lite
        && codex_responses_lite_supports_request_body(provider_request_body)
    {
        provider_request_headers
            .insert(CODEX_RESPONSES_LITE_HEADER.to_string(), "true".to_string());
    }
}

pub fn apply_codex_openai_special_headers(
    provider_request_headers: &mut BTreeMap<String, String>,
    provider_request_body: &Value,
    _original_headers: &http::HeaderMap,
    provider_type: &str,
    provider_api_format: &str,
    _request_id: Option<&str>,
    decrypted_auth_config_raw: Option<&str>,
) {
    let Some(endpoint_kind) = codex_openai_endpoint_kind(provider_type, provider_api_format) else {
        return;
    };

    let auth_identity = parse_codex_auth_identity(decrypted_auth_config_raw);
    let client_profile = codex_client_profile();

    remove_btree_header(provider_request_headers, "chatgpt-account-id");
    remove_btree_header(provider_request_headers, "x-openai-fedramp");
    if let Some(account_id) = auth_identity.account_id {
        provider_request_headers.insert("chatgpt-account-id".to_string(), account_id);
    }

    if auth_identity.is_fedramp {
        provider_request_headers.insert("x-openai-fedramp".to_string(), "true".to_string());
    }

    set_codex_client_header(
        provider_request_headers,
        "user-agent",
        &client_profile.user_agent,
    );
    set_codex_client_header(
        provider_request_headers,
        "originator",
        &client_profile.originator,
    );
    if endpoint_kind == CodexOpenAiEndpointKind::Search {
        remove_btree_header(provider_request_headers, CODEX_RESPONSES_LITE_HEADER);
        remove_btree_header(provider_request_headers, "openai-beta");
        if provider_request_headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("accept")
                && header_value_contains_media_type(value, "text/event-stream")
        }) {
            remove_btree_header(provider_request_headers, "accept");
        }
        return;
    }
    if matches!(
        endpoint_kind,
        CodexOpenAiEndpointKind::Images | CodexOpenAiEndpointKind::Live
    ) {
        if endpoint_kind == CodexOpenAiEndpointKind::Live {
            remove_btree_header(provider_request_headers, CODEX_RESPONSES_LITE_HEADER);
        }
        return;
    }

    let provider_model = provider_request_body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or_default();
    apply_codex_openai_responses_lite_header_for_request_body_with_capabilities(
        provider_request_headers,
        Some(provider_request_body),
        provider_type,
        provider_api_format,
        provider_model,
        provider_model,
        None,
    );

    apply_codex_openai_compact_terminal_headers(
        provider_request_headers,
        provider_type,
        provider_api_format,
    );
}

#[cfg(test)]
#[path = "codex/tests.rs"]
mod tests;
