-- Retirement DDL touches hot tables; bound how long the upgrade can wait for the
-- table lock instead of inheriting the migration runner's unlimited wait.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

DROP VIEW IF EXISTS public.usage_analytics_facts_v1;
DROP VIEW IF EXISTS public.usage_billing_facts;

ALTER TABLE public.usage
    DROP COLUMN IF EXISTS input_tokens,
    DROP COLUMN IF EXISTS output_tokens,
    DROP COLUMN IF EXISTS cache_creation_input_tokens,
    DROP COLUMN IF EXISTS cache_read_input_tokens,
    DROP COLUMN IF EXISTS cache_creation_input_tokens_5m,
    DROP COLUMN IF EXISTS cache_creation_input_tokens_1h,
    DROP COLUMN IF EXISTS input_cost_usd,
    DROP COLUMN IF EXISTS output_cost_usd,
    DROP COLUMN IF EXISTS cache_creation_cost_usd,
    DROP COLUMN IF EXISTS cache_read_cost_usd,
    DROP COLUMN IF EXISTS total_cost_usd,
    DROP COLUMN IF EXISTS actual_total_cost_usd,
    DROP COLUMN IF EXISTS total_tokens;

CREATE OR REPLACE VIEW public.usage_billing_facts AS
 SELECT usage_rows.id,
    usage_rows.request_id,
    usage_rows.user_id,
    usage_rows.api_key_id,
    usage_rows.username,
    usage_rows.api_key_name,
    usage_rows.provider_name,
    usage_rows.model,
    usage_rows.target_model,
    usage_rows.provider_id,
    usage_rows.provider_endpoint_id,
    usage_rows.provider_api_key_id,
    usage_rows.request_type,
    usage_rows.api_format,
    usage_rows.api_family,
    usage_rows.endpoint_kind,
    usage_rows.endpoint_api_format,
    usage_rows.provider_api_family,
    usage_rows.provider_endpoint_kind,
    COALESCE(usage_rows.has_format_conversion, false) AS has_format_conversion,
    COALESCE(usage_rows.is_stream, false) AS is_stream,
    usage_rows.status_code,
    usage_rows.error_message,
    usage_rows.error_category,
    usage_rows.response_time_ms,
    usage_rows.first_byte_time_ms,
    usage_rows.status,
    COALESCE(settlement.billing_status, usage_rows.billing_status) AS billing_status,
    usage_rows.created_at,
    settlement.finalized_at AS finalized_at,
    GREATEST(COALESCE(settlement.billing_input_tokens, 0), 0)::bigint AS input_tokens,
    GREATEST(COALESCE(settlement.billing_effective_input_tokens, 0), 0)::bigint AS effective_input_tokens,
    GREATEST(COALESCE(settlement.billing_output_tokens, 0), 0)::bigint AS output_tokens,
    GREATEST(COALESCE(settlement.billing_cache_creation_tokens,
        COALESCE(settlement.billing_cache_creation_5m_tokens, 0)
        + COALESCE(settlement.billing_cache_creation_1h_tokens, 0), 0), 0)::bigint AS cache_creation_input_tokens,
    GREATEST(COALESCE(settlement.billing_cache_creation_5m_tokens, 0), 0)::bigint AS cache_creation_input_tokens_5m,
    GREATEST(COALESCE(settlement.billing_cache_creation_1h_tokens, 0), 0)::bigint AS cache_creation_input_tokens_1h,
    GREATEST(COALESCE(settlement.billing_cache_read_tokens, 0), 0)::bigint AS cache_read_input_tokens,
    GREATEST(COALESCE(
        CASE
            WHEN settlement.billing_effective_input_tokens IS NOT NULL THEN
                GREATEST(settlement.billing_effective_input_tokens, 0)
                + GREATEST(COALESCE(settlement.billing_output_tokens, 0), 0)
                + GREATEST(COALESCE(settlement.billing_cache_creation_tokens,
                    COALESCE(settlement.billing_cache_creation_5m_tokens, 0)
                    + COALESCE(settlement.billing_cache_creation_1h_tokens, 0), 0), 0)
                + GREATEST(COALESCE(settlement.billing_cache_read_tokens, 0), 0)
            WHEN settlement.billing_total_input_context IS NOT NULL THEN
                GREATEST(settlement.billing_total_input_context, 0)
                + GREATEST(COALESCE(settlement.billing_output_tokens, 0), 0)
        END, 0), 0)::bigint AS total_tokens,
    GREATEST(
        COALESCE(settlement.billing_total_input_context,
            COALESCE(settlement.billing_effective_input_tokens, 0)
            + COALESCE(settlement.billing_cache_creation_tokens,
                COALESCE(settlement.billing_cache_creation_5m_tokens, 0)
                + COALESCE(settlement.billing_cache_creation_1h_tokens, 0), 0)
            + COALESCE(settlement.billing_cache_read_tokens, 0)), 0)::bigint AS total_input_context,
    0::double precision AS input_cost_usd,
    0::double precision AS output_cost_usd,
    COALESCE(settlement.billing_cache_creation_cost_usd::double precision, 0) AS cache_creation_cost_usd,
    COALESCE(settlement.billing_cache_read_cost_usd::double precision, 0) AS cache_read_cost_usd,
    COALESCE(settlement.billing_total_cost_usd::double precision, 0) AS total_cost_usd,
    COALESCE(settlement.billing_actual_total_cost_usd::double precision, 0) AS actual_total_cost_usd,
    settlement.output_price_per_1m::double precision AS output_price_per_1m,
    settlement.input_price_per_1m::double precision AS input_price_per_1m,
    settlement.cache_creation_price_per_1m::double precision AS cache_creation_price_per_1m,
    settlement.cache_read_price_per_1m::double precision AS cache_read_price_per_1m,
    settlement.price_per_request::double precision AS price_per_request,
    settlement.billing_pricing_source,
    settlement.billing_rule_id,
    settlement.billing_rule_version,
    COALESCE(usage_rows.upstream_is_stream, COALESCE(usage_rows.is_stream, false)) AS upstream_is_stream
   FROM public.usage usage_rows
     LEFT JOIN public.usage_settlement_snapshots settlement ON settlement.request_id = usage_rows.request_id;

CREATE OR REPLACE VIEW public.usage_analytics_facts_v1 AS
 SELECT u.request_id,
    COALESCE(u.id, u.request_id) AS id,
    u.created_at,
        CASE
            WHEN ((identity.owner_id IS NOT NULL) AND (identity.is_standalone = false)) THEN identity.owner_id
            ELSE NULL::text
        END AS actor_user_id,
    identity.owner_id AS credential_owner_id,
        CASE
            WHEN (identity.owner_id IS NULL) THEN 'unknown'::text
            WHEN identity.is_standalone THEN 'standalone'::text
            WHEN (NOT identity.is_standalone) THEN 'employee'::text
            ELSE 'unknown'::text
        END AS attribution_kind,
        CASE
            WHEN (identity.owner_id IS NULL) THEN 'unknown'::text
            WHEN identity.is_standalone THEN 'standalone_key'::text
            WHEN (NOT identity.is_standalone) THEN 'user_account'::text
            ELSE 'unknown'::text
        END AS attribution_source,
    COALESCE(a.record_kind, 'request'::text) AS record_kind,
    a.parent_request_id,
    u.api_key_id,
    u.model,
    u.target_model,
    u.provider_id,
    u.provider_name,
    u.api_format,
    u.endpoint_kind,
    u.request_type,
    u.is_stream,
    u.has_format_conversion,
    u.status,
    u.status_code,
    u.error_category,
    u.failure_origin,
    u.failure_stage,
    u.failure_reason,
    u.failure_schema_version,
    u.response_time_ms,
    u.first_byte_time_ms,
    COALESCE(s.billing_status, u.billing_status) AS settlement_status,
    (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AS usage_available,
    ((COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND ((s.billing_total_cost_usd IS NOT NULL) OR ((COALESCE(s.billing_status, u.billing_status))::text = 'settled'::text))) AS pricing_available,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.input_tokens
            ELSE NULL::bigint
        END AS input_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.output_tokens
            ELSE NULL::bigint
        END AS output_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.total_tokens
            ELSE NULL::bigint
        END AS total_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.cache_read_input_tokens
            ELSE NULL::bigint
        END AS cache_read_input_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.cache_creation_input_tokens
            ELSE NULL::bigint
        END AS cache_creation_input_tokens,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND ((s.billing_total_cost_usd IS NOT NULL) OR ((COALESCE(s.billing_status, u.billing_status))::text = 'settled'::text))) THEN round(COALESCE((s.billing_total_cost_usd)::numeric, 0), 8)
            ELSE NULL::numeric
        END AS rated_amount,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND ((s.billing_actual_total_cost_usd IS NOT NULL) OR ((COALESCE(s.billing_status, u.billing_status))::text = 'settled'::text))) THEN public.usage_customer_billable_amount(metadata.value, COALESCE((s.billing_total_cost_usd)::numeric, 0), COALESCE((s.billing_actual_total_cost_usd)::numeric, 0))
            ELSE NULL::numeric
        END AS billable_amount,
    s.quota_covered_amount_usd AS quota_covered_amount,
    s.wallet_consumed_amount_usd AS wallet_consumed_amount,
    s.wallet_debit_amount_usd AS wallet_debit_amount,
    s.wallet_recharge_debit_usd AS wallet_recharge_debit_amount,
    s.wallet_gift_debit_usd AS wallet_gift_debit_amount,
    s.wallet_overdraft_usd AS wallet_overdraft_amount,
    s.allocation_status,
    s.finalized_at AS settled_at,
        CASE
            WHEN (s.billing_total_cost_usd IS NOT NULL) THEN 'settlement_snapshot'::text
            ELSE 'legacy_float'::text
        END AS amount_source,
    b.upstream_is_stream,
        CASE
            WHEN ((metadata.value #>> '{analytics_measurement,source}'::text[]) = ANY (ARRAY['reported'::text, 'estimated'::text, 'mixed'::text])) THEN (metadata.value #>> '{analytics_measurement,source}'::text[])
            ELSE 'unknown'::text
        END AS token_source,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (s.input_price_per_1m IS NOT NULL) AND (s.billing_cache_read_cost_usd IS NOT NULL)) THEN round((((s.input_price_per_1m)::numeric * (b.cache_read_input_tokens)::numeric) / (1000000)::numeric), 8)
            ELSE NULL::numeric
        END AS cache_estimated_full_cost_amount,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (s.input_price_per_1m IS NOT NULL) AND (s.billing_cache_read_cost_usd IS NOT NULL)) THEN round((s.billing_cache_read_cost_usd)::numeric, 8)
            ELSE NULL::numeric
        END AS cache_read_cost_amount,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (s.input_price_per_1m IS NOT NULL) AND (s.billing_cache_creation_cost_usd IS NOT NULL)) THEN round((s.billing_cache_creation_cost_usd)::numeric, 8)
            ELSE NULL::numeric
        END AS cache_creation_cost_amount
   FROM ((((((public.usage u
     CROSS JOIN LATERAL ( SELECT u.request_metadata AS value
         OFFSET 0) metadata)
     LEFT JOIN public.usage_settlement_snapshots s USING (request_id))
     LEFT JOIN public.usage_attribution_snapshots a USING (request_id))
     JOIN public.usage_billing_facts b USING (request_id))
     LEFT JOIN public.api_keys k ON (((k.id)::text = (u.api_key_id)::text)))
     CROSS JOIN LATERAL ( SELECT
                CASE
                    WHEN (a.request_id IS NOT NULL) THEN a.credential_owner_id
                    WHEN (EXISTS ( SELECT 1
                       FROM public.users
                      WHERE (((users.id)::text = (u.user_id)::text) AND (NOT users.is_deleted)))) THEN (u.user_id)::text
                    ELSE NULL::text
                END AS owner_id,
            COALESCE(k.is_standalone,
                CASE
                    WHEN (jsonb_typeof((metadata.value #> '{analytics_attribution,is_standalone}'::text[])) = 'boolean'::text) THEN ((metadata.value #>> '{analytics_attribution,is_standalone}'::text[]))::boolean
                    ELSE NULL::boolean
                END,
                CASE
                    WHEN (jsonb_typeof((metadata.value -> 'api_key_is_standalone'::text)) = 'boolean'::text) THEN ((metadata.value ->> 'api_key_is_standalone'::text))::boolean
                    ELSE NULL::boolean
                END,
                CASE
                    WHEN (a.attribution_source = 'user_account'::text) THEN false
                    WHEN (a.attribution_source = 'standalone_key'::text) THEN true
                    ELSE NULL::boolean
                END,
                CASE
                    WHEN (u.api_key_id IS NULL) THEN false
                    ELSE NULL::boolean
                END) AS is_standalone) identity);
