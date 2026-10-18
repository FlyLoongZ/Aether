use aether_data_contracts::repository::billing::StoredBillingModelContext;
use aether_data_contracts::repository::usage::{
    extract_provider_cache_ttl_minutes_from_metadata, resolve_provider_cache_ttl_minutes,
    resolve_provider_service_tier_from_request_capture, CANCELLED_REQUEST_FEE_METADATA_KEY,
    USAGE_AVAILABLE_METADATA_KEY, USAGE_PRICING_AVAILABLE_METADATA_KEY,
};
use aether_data_contracts::DataLayerError;
use aether_usage_runtime::{UsageEvent, UsageEventType};
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use std::sync::Arc;

use crate::{
    BillingComputation, BillingModelPricingSnapshot, BillingService, BillingSnapshotStatus,
    BillingUsageInput,
};

const SETTLEMENT_SNAPSHOT_SCHEMA_VERSION: &str = "3.0";

#[async_trait]
pub trait BillingModelContextLookup: Send + Sync {
    async fn find_billing_model_context_by_model_id(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        model_id: &str,
    ) -> Result<Option<Arc<StoredBillingModelContext>>, DataLayerError> {
        let _ = (provider_id, provider_api_key_id, model_id);
        Ok(None)
    }

    async fn find_billing_model_context(
        &self,
        provider_id: &str,
        provider_api_key_id: Option<&str>,
        global_model_name: &str,
    ) -> Result<Option<Arc<StoredBillingModelContext>>, DataLayerError>;
}

pub async fn enrich_usage_event_with_billing(
    data: &dyn BillingModelContextLookup,
    event: &mut UsageEvent,
) -> Result<(), DataLayerError> {
    if matches!(event.event_type, UsageEventType::Cancelled) {
        event.data.total_cost_usd = Some(0.0);
        event.data.actual_total_cost_usd = Some(0.0);
        if let Some(metadata) = event
            .data
            .request_metadata
            .as_mut()
            .and_then(Value::as_object_mut)
        {
            metadata.remove(CANCELLED_REQUEST_FEE_METADATA_KEY);
        }
    }
    // Session transports such as Codex Live expose lifecycle telemetry but no
    // authoritative token/cost object. Do not run request-based pricing with
    // zero default tokens: that would turn "unknown" into a fabricated charge.
    if usage_is_explicitly_unavailable(event) {
        event.data.input_tokens = None;
        event.data.output_tokens = None;
        event.data.total_tokens = None;
        event.data.cache_creation_input_tokens = None;
        event.data.cache_creation_ephemeral_5m_input_tokens = None;
        event.data.cache_creation_ephemeral_1h_input_tokens = None;
        event.data.cache_read_input_tokens = None;
        event.data.cache_creation_cost_usd = None;
        event.data.cache_read_cost_usd = None;
        event.data.total_cost_usd = None;
        event.data.actual_total_cost_usd = None;
        return Ok(());
    }
    // Some protocols expose authoritative token totals with dimensions that
    // Aether's pricing schema cannot safely express yet (Realtime audio is the
    // first example). Preserve those tokens for observability, but never apply
    // ordinary text token prices to them.
    if usage_pricing_is_explicitly_unavailable(event) {
        clear_usage_costs(event);
        return Ok(());
    }
    if !matches!(
        event.event_type,
        UsageEventType::Completed | UsageEventType::Cancelled
    ) {
        event.data.total_cost_usd = Some(0.0);
        event.data.actual_total_cost_usd = Some(0.0);
        return Ok(());
    }

    let Some(provider_id) = event
        .data
        .provider_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if let Some(model_id) = event
        .data
        .model_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(context) = data
            .find_billing_model_context_by_model_id(
                provider_id,
                event.data.provider_api_key_id.as_deref(),
                model_id,
            )
            .await?
        {
            let pricing = map_pricing_context(&context);
            let computation = calculate_billing_computation(&pricing, event)?;
            apply_billing_computation(event, &pricing, computation)?;
            return Ok(());
        }
    }

    let mut first_no_rule = None;
    for lookup_name in billing_model_lookup_names(&event.data) {
        let Some(context) = data
            .find_billing_model_context(
                provider_id,
                event.data.provider_api_key_id.as_deref(),
                lookup_name,
            )
            .await?
        else {
            continue;
        };

        let pricing = map_pricing_context(&context);
        let computation = calculate_billing_computation(&pricing, event)?;
        if matches!(
            computation.cost_result.status,
            BillingSnapshotStatus::NoRule
        ) {
            first_no_rule.get_or_insert((pricing, computation));
            continue;
        }
        apply_billing_computation(event, &pricing, computation)?;
        return Ok(());
    }

    if let Some((pricing, computation)) = first_no_rule {
        apply_billing_computation(event, &pricing, computation)?;
    }
    Ok(())
}

fn usage_is_explicitly_unavailable(event: &UsageEvent) -> bool {
    event
        .data
        .request_metadata
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get(USAGE_AVAILABLE_METADATA_KEY))
        .and_then(Value::as_bool)
        == Some(false)
}

fn usage_pricing_is_explicitly_unavailable(event: &UsageEvent) -> bool {
    event
        .data
        .request_metadata
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get(USAGE_PRICING_AVAILABLE_METADATA_KEY))
        .and_then(Value::as_bool)
        == Some(false)
}

fn clear_usage_costs(event: &mut UsageEvent) {
    event.data.cache_creation_cost_usd = None;
    event.data.cache_read_cost_usd = None;
    event.data.total_cost_usd = None;
    event.data.actual_total_cost_usd = None;
}

fn billing_model_lookup_names(data: &aether_usage_runtime::UsageEventData) -> Vec<&str> {
    let mut names = Vec::new();
    for value in [data.target_model.as_deref(), Some(data.model.as_str())]
        .into_iter()
        .flatten()
    {
        let value = value.trim();
        if !value.is_empty() && !names.contains(&value) {
            names.push(value);
        }
    }
    names
}

fn calculate_billing_computation(
    pricing: &BillingModelPricingSnapshot,
    event: &UsageEvent,
) -> Result<BillingComputation, DataLayerError> {
    let failed =
        event.data.status_code.unwrap_or_default() >= 400 || event.data.error_message.is_some();
    let is_image_usage = usage_event_is_image_usage(&event.data);
    let image_count = if failed {
        0
    } else {
        usage_event_image_count(&event.data).unwrap_or(0)
    };
    let cancelled = matches!(event.event_type, UsageEventType::Cancelled);
    let request_count = if cancelled {
        1
    } else if failed {
        0
    } else if is_image_usage && image_count > 0 {
        image_count
    } else {
        1
    };
    let processing_tiers = usage_event_processing_tiers(&event.data);
    let mut input = BillingUsageInput {
        task_type: if is_image_usage {
            "image".to_string()
        } else {
            event
                .data
                .request_type
                .clone()
                .unwrap_or_else(|| "chat".to_string())
        },
        api_format: event
            .data
            .endpoint_api_format
            .clone()
            .or_else(|| event.data.api_format.clone()),
        requested_processing_tier: processing_tiers.requested,
        // The response-reported tier remains usage audit data, but it is not authoritative for
        // pricing. Settlement follows the final request that was sent upstream.
        actual_processing_tier: None,
        request_count,
        input_tokens: event.data.input_tokens.unwrap_or_default() as i64,
        output_tokens: event.data.output_tokens.unwrap_or_default() as i64,
        cache_creation_tokens: event.data.cache_creation_input_tokens.unwrap_or_default() as i64,
        cache_creation_ephemeral_5m_tokens: event
            .data
            .cache_creation_ephemeral_5m_input_tokens
            .unwrap_or_default() as i64,
        cache_creation_ephemeral_1h_tokens: event
            .data
            .cache_creation_ephemeral_1h_input_tokens
            .unwrap_or_default() as i64,
        cache_read_tokens: event.data.cache_read_input_tokens.unwrap_or_default() as i64,
        image_count,
        image_size: usage_event_dimension_string(&event.data, "image_size"),
        image_quality: usage_event_dimension_string(&event.data, "image_quality"),
        image_output_format: usage_event_dimension_string(&event.data, "image_output_format"),
        cache_ttl_minutes: usage_event_provider_cache_ttl_minutes(&event.data)
            .or(pricing.provider_api_key_cache_ttl_minutes),
    };

    if cancelled {
        input.input_tokens = 0;
        input.output_tokens = 0;
        input.cache_creation_tokens = 0;
        input.cache_creation_ephemeral_5m_tokens = 0;
        input.cache_creation_ephemeral_1h_tokens = 0;
        input.cache_read_tokens = 0;
        input.image_count = 0;
    }

    BillingService::new()
        .calculate(pricing, &input)
        .map_err(|err| {
            DataLayerError::UnexpectedValue(format!("billing calculation failed: {err}"))
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UsageEventProcessingTiers {
    requested: Option<String>,
}

fn usage_event_processing_tiers(
    data: &aether_usage_runtime::UsageEventData,
) -> UsageEventProcessingTiers {
    let requested = resolve_provider_service_tier_from_request_capture(
        data.provider_request_body.as_ref(),
        data.provider_request_body_state,
        data.request_metadata.as_ref(),
    );

    UsageEventProcessingTiers { requested }
}

fn usage_event_provider_cache_ttl_minutes(
    data: &aether_usage_runtime::UsageEventData,
) -> Option<i64> {
    resolve_provider_cache_ttl_minutes(
        data.endpoint_api_format
            .as_deref()
            .or(data.api_format.as_deref()),
        data.target_model.as_deref().or(Some(data.model.as_str())),
        Some(data.model.as_str()),
        data.provider_request_body.as_ref(),
    )
    .or_else(|| extract_provider_cache_ttl_minutes_from_metadata(data.request_metadata.as_ref()))
}

fn usage_event_is_image_usage(data: &aether_usage_runtime::UsageEventData) -> bool {
    data.request_type
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("image"))
        || api_format_endpoint_kind(data.endpoint_api_format.as_deref()) == Some("image")
        || api_format_endpoint_kind(data.api_format.as_deref()) == Some("image")
        || usage_event_image_count(data).is_some_and(|value| value > 0)
}

fn usage_event_image_count(data: &aether_usage_runtime::UsageEventData) -> Option<i64> {
    metadata_dimension_i64(data.request_metadata.as_ref(), "dimensions", "image_count")
        .or_else(|| {
            metadata_dimension_i64(
                data.request_metadata.as_ref(),
                "billing_dimensions",
                "image_count",
            )
        })
        .filter(|value| *value > 0)
}

fn usage_event_dimension_string(
    data: &aether_usage_runtime::UsageEventData,
    dimension_key: &str,
) -> Option<String> {
    metadata_dimension_string(data.request_metadata.as_ref(), "dimensions", dimension_key).or_else(
        || {
            metadata_dimension_string(
                data.request_metadata.as_ref(),
                "billing_dimensions",
                dimension_key,
            )
        },
    )
}

fn metadata_dimension_string(
    metadata: Option<&Value>,
    bag_key: &str,
    dimension_key: &str,
) -> Option<String> {
    metadata
        .and_then(Value::as_object)
        .and_then(|object| object.get(bag_key))
        .and_then(Value::as_object)
        .and_then(|object| object.get(dimension_key))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn metadata_dimension_i64(
    metadata: Option<&Value>,
    bag_key: &str,
    dimension_key: &str,
) -> Option<i64> {
    metadata
        .and_then(Value::as_object)
        .and_then(|object| object.get(bag_key))
        .and_then(Value::as_object)
        .and_then(|object| object.get(dimension_key))
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|number| i64::try_from(number).ok()))
        })
}

fn api_format_endpoint_kind(api_format: Option<&str>) -> Option<&str> {
    api_format
        .and_then(|value| value.split_once(':').map(|(_, kind)| kind))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn apply_billing_computation(
    event: &mut UsageEvent,
    pricing: &BillingModelPricingSnapshot,
    computation: BillingComputation,
) -> Result<(), DataLayerError> {
    let cancelled = matches!(event.event_type, UsageEventType::Cancelled);
    if cancelled
        && !computation
            .pricing_resolution
            .price_per_request
            .is_some_and(|price| price > 0.0)
    {
        return Ok(());
    }
    event.data.total_cost_usd = Some(computation.cost_result.cost);
    event.data.actual_total_cost_usd = Some(computation.actual_total_cost);
    merge_billing_snapshot_metadata(&mut event.data.request_metadata, pricing, &computation)?;
    if cancelled {
        if let Some(metadata) = event
            .data
            .request_metadata
            .as_mut()
            .and_then(Value::as_object_mut)
        {
            metadata.insert(
                CANCELLED_REQUEST_FEE_METADATA_KEY.to_string(),
                Value::Bool(true),
            );
        }
    }
    Ok(())
}

fn map_pricing_context(context: &StoredBillingModelContext) -> BillingModelPricingSnapshot {
    context.into()
}

fn merge_billing_snapshot_metadata(
    request_metadata: &mut Option<Value>,
    pricing: &BillingModelPricingSnapshot,
    computation: &BillingComputation,
) -> Result<(), DataLayerError> {
    let snapshot = &computation.cost_result.snapshot;
    let billing_snapshot = serde_json::to_value(snapshot).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("failed to serialize billing snapshot: {err}"))
    })?;
    let settlement_snapshot = build_settlement_snapshot(pricing, computation);

    let mut metadata = match request_metadata.take() {
        Some(Value::Object(object)) => object,
        _ => Map::new(),
    };
    metadata.insert("billing_snapshot".to_string(), billing_snapshot);
    metadata.insert(
        "settlement_snapshot_schema_version".to_string(),
        Value::from(SETTLEMENT_SNAPSHOT_SCHEMA_VERSION),
    );
    metadata.insert("settlement_snapshot".to_string(), settlement_snapshot);
    metadata.insert(
        "billing_dimensions".to_string(),
        Value::Object(snapshot.resolved_dimensions.clone().into_iter().collect()),
    );
    metadata.insert(
        "rate_multiplier".to_string(),
        Value::from(computation.rate_multiplier),
    );
    metadata.insert(
        "is_free_tier".to_string(),
        Value::from(computation.is_free_tier),
    );
    *request_metadata = Some(Value::Object(metadata));
    Ok(())
}

fn build_settlement_snapshot(
    pricing: &BillingModelPricingSnapshot,
    computation: &BillingComputation,
) -> Value {
    let snapshot = &computation.cost_result.snapshot;
    let resolution = &computation.pricing_resolution;
    json!({
        "schema_version": SETTLEMENT_SNAPSHOT_SCHEMA_VERSION,
        "pricing_snapshot": {
            "provider_id": pricing.provider_id.clone(),
            "provider_billing_type": pricing.provider_billing_type.clone(),
            "provider_api_key_id": pricing.provider_api_key_id.clone(),
            "global_model_id": pricing.global_model_id.clone(),
            "global_model_name": pricing.global_model_name.clone(),
            "model_id": pricing.model_id.clone(),
            "provider_model_name": pricing.model_provider_model_name.clone(),
            "requested_processing_tier": resolution.requested_processing_tier,
            "actual_processing_tier": resolution.actual_processing_tier,
            "billing_processing_tier": resolution.billing_processing_tier,
            "pricing_source": resolution.pricing_source(),
            "tiered_pricing_source": resolution.tiered_pricing_source.map(|source| source.as_str()),
            "processing_tier_price_multiplier": resolution.processing_tier_price_multiplier,
            "price_per_request_source": resolution.price_per_request_source.map(|source| source.as_str()),
            "tiered_pricing": resolution.tiered_pricing,
            "price_per_request": resolution.price_per_request,
            "rate_multiplier": computation.rate_multiplier,
            "is_free_tier": computation.is_free_tier,
        },
        "billing_plan_snapshot": {
            "rule_id": snapshot.rule_id.clone(),
            "rule_name": snapshot.rule_name.clone(),
            "scope": snapshot.scope.clone(),
            "expression": snapshot.expression.clone(),
            "engine_version": snapshot.engine_version.clone(),
        },
        "resolved_dimensions": snapshot.resolved_dimensions.clone(),
        "resolved_variables": snapshot.resolved_variables.clone(),
        "cost_breakdown": snapshot.cost_breakdown.clone(),
        "total_cost": snapshot.total_cost,
        "actual_total_cost": computation.actual_total_cost,
        "status": snapshot.status,
        "calculated_at": snapshot.calculated_at.clone(),
    })
}

#[cfg(test)]
#[path = "event_enrichment/tests.rs"]
mod tests;
