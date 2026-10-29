use crate::image_capabilities::{
    openai_image_normalize_options_for_provider,
    openai_image_provider_max_generation_count_for_model,
};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AdminProviderOpenAiImageTestCapability {
    pub(crate) max_generation_count: u64,
}

impl AdminProviderOpenAiImageTestCapability {
    pub(crate) fn supports_generation_count(self, count: u64) -> bool {
        count >= 1 && count <= self.max_generation_count
    }
}

pub(crate) fn admin_provider_openai_image_test_capability(
    provider_type: &str,
    provider_model: Option<&str>,
) -> AdminProviderOpenAiImageTestCapability {
    AdminProviderOpenAiImageTestCapability {
        max_generation_count: openai_image_provider_max_generation_count_for_model(
            provider_type,
            provider_model,
        ),
    }
}

pub(crate) fn admin_provider_openai_image_normalize_options(
    provider_type: &str,
    provider_model: Option<&str>,
) -> crate::ai_serving::OpenAiImageNormalizeOptions {
    openai_image_normalize_options_for_provider(provider_type, provider_model)
}

pub(crate) fn admin_provider_model_test_capabilities_payload(
    provider_type: &str,
    model_id: &str,
    supports_image_generation: bool,
) -> Value {
    let provider_type = provider_type.trim();
    let model_id = model_id.trim();
    let openai_image = if supports_image_generation {
        Some(json!({
            "max_generation_count": admin_provider_openai_image_test_capability(provider_type, Some(model_id)).max_generation_count,
            "supports_generation": true,
            "supports_edit": false,
        }))
    } else {
        None
    };

    json!({
        "openai:image": openai_image,
    })
}

pub(crate) fn admin_provider_model_supports_image_generation(
    _provider_type: &str,
    _model_id: &str,
    fallback_supports_image_generation: bool,
) -> bool {
    fallback_supports_image_generation
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_image_models_report_null_image_test_capability() {
        let payload = admin_provider_model_test_capabilities_payload("openai", "gpt-5.5", false);

        assert!(payload["openai:image"].is_null());
    }

    #[test]
    fn dall_e_3_reports_its_model_specific_generation_limit() {
        let payload = admin_provider_model_test_capabilities_payload("openai", "dall-e-3", true);

        assert_eq!(payload["openai:image"]["max_generation_count"], 1);
    }
}
