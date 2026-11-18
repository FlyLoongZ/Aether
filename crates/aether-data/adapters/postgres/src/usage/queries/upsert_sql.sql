INSERT INTO "usage" (
  id,
  request_id,
  user_id,
  api_key_id,
  username,
  api_key_name,
  provider_name,
  model,
  target_model,
  provider_id,
  provider_endpoint_id,
  provider_api_key_id,
  request_type,
  api_format,
  api_family,
  endpoint_kind,
  endpoint_api_format,
  provider_api_family,
  provider_endpoint_kind,
  has_format_conversion,
  is_stream,
  upstream_is_stream,
  input_tokens,
  output_tokens,
  total_tokens,
  cache_creation_input_tokens,
  cache_creation_input_tokens_5m,
  cache_creation_input_tokens_1h,
  cache_read_input_tokens,
  cache_creation_cost_usd,
  cache_read_cost_usd,
  total_cost_usd,
  actual_total_cost_usd,
  status_code,
  error_message,
  error_category,
  response_time_ms,
  first_byte_time_ms,
  status,
  billing_status,
  request_metadata,
  created_at,
  updated_at_unix_secs
) VALUES (
  $1,
  $2,
  $3,
  $4,
  $5,
  $6,
  $7,
  $8,
  $9,
  $10,
  $11,
  $12,
  $13,
  $14,
  $15,
  $16,
  $17,
  $18,
  $19,
  COALESCE($20, FALSE),
  COALESCE($21, FALSE),
  COALESCE(
    CASE
      WHEN ($40::jsonb->>'upstream_is_stream') IN ('true', 'false')
      THEN ($40::jsonb->>'upstream_is_stream')::boolean
      ELSE NULL
    END,
    COALESCE($21, FALSE)
  ),
  COALESCE($22, 0),
  COALESCE($23, 0),
  COALESCE(
    $24,
    COALESCE($22, 0)
      + COALESCE($23, 0)
      + COALESCE(
        NULLIF(COALESCE($25, 0), 0),
        COALESCE($26, 0) + COALESCE($27, 0),
        0
      )
      + COALESCE($28, 0)
  ),
  COALESCE($25, 0),
  COALESCE($26, 0),
  COALESCE($27, 0),
  COALESCE($28, 0),
  COALESCE($29, 0),
  $30,
  COALESCE($31, 0),
  $32,
  $33,
  $34,
  $35,
  $36,
  $37,
  $38,
  $39,
  $40::jsonb,
  COALESCE(TO_TIMESTAMP($41::double precision), NOW()),
  COALESCE(
    NULLIF($42::bigint, 0),
    CAST(EXTRACT(EPOCH FROM COALESCE(TO_TIMESTAMP($40::double precision), NOW())) AS BIGINT)
  )
)
ON CONFLICT (request_id)
DO UPDATE SET
  user_id = CASE WHEN "usage".billing_status = 'pending' THEN COALESCE(EXCLUDED.user_id, "usage".user_id) ELSE "usage".user_id END,
  api_key_id = CASE WHEN "usage".billing_status = 'pending' THEN COALESCE(EXCLUDED.api_key_id, "usage".api_key_id) ELSE "usage".api_key_id END,
  username = CASE WHEN "usage".billing_status = 'pending' THEN COALESCE(EXCLUDED.username, "usage".username) ELSE "usage".username END,
  api_key_name = CASE WHEN "usage".billing_status = 'pending' THEN COALESCE(EXCLUDED.api_key_name, "usage".api_key_name) ELSE "usage".api_key_name END,
  provider_name = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.provider_name, "usage".provider_name) ELSE "usage".provider_name END,
  model = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.model, "usage".model) ELSE "usage".model END,
  target_model = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN CASE WHEN EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN EXCLUDED.target_model ELSE COALESCE(EXCLUDED.target_model, "usage".target_model) END ELSE "usage".target_model END,
  provider_id = CASE WHEN ("usage".billing_status = 'pending' AND $43) OR ("usage".billing_status <> 'pending' AND "usage".provider_id IS NULL AND ("usage".provider_endpoint_id IS NULL OR "usage".provider_endpoint_id = EXCLUDED.provider_endpoint_id) AND ("usage".provider_api_key_id IS NULL OR "usage".provider_api_key_id = EXCLUDED.provider_api_key_id)) THEN COALESCE(EXCLUDED.provider_id, "usage".provider_id) ELSE "usage".provider_id END,
  provider_endpoint_id = CASE WHEN ("usage".billing_status = 'pending' AND $43) OR ("usage".billing_status <> 'pending' AND "usage".provider_endpoint_id IS NULL AND ("usage".provider_id IS NULL OR "usage".provider_id = EXCLUDED.provider_id) AND ("usage".provider_api_key_id IS NULL OR "usage".provider_api_key_id = EXCLUDED.provider_api_key_id)) THEN COALESCE(EXCLUDED.provider_endpoint_id, "usage".provider_endpoint_id) ELSE "usage".provider_endpoint_id END,
  provider_api_key_id = CASE WHEN ("usage".billing_status = 'pending' AND $43) OR ("usage".billing_status <> 'pending' AND "usage".provider_api_key_id IS NULL AND ("usage".provider_id IS NULL OR "usage".provider_id = EXCLUDED.provider_id) AND ("usage".provider_endpoint_id IS NULL OR "usage".provider_endpoint_id = EXCLUDED.provider_endpoint_id)) THEN COALESCE(EXCLUDED.provider_api_key_id, "usage".provider_api_key_id) ELSE "usage".provider_api_key_id END,
  request_type = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.request_type, "usage".request_type) ELSE "usage".request_type END,
  api_format = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.api_format, "usage".api_format) ELSE "usage".api_format END,
  api_family = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.api_family, "usage".api_family) ELSE "usage".api_family END,
  endpoint_kind = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.endpoint_kind, "usage".endpoint_kind) ELSE "usage".endpoint_kind END,
  endpoint_api_format = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.endpoint_api_format, "usage".endpoint_api_format) ELSE "usage".endpoint_api_format END,
  provider_api_family = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.provider_api_family, "usage".provider_api_family) ELSE "usage".provider_api_family END,
  provider_endpoint_kind = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.provider_endpoint_kind, "usage".provider_endpoint_kind) ELSE "usage".provider_endpoint_kind END,
  has_format_conversion = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.has_format_conversion, "usage".has_format_conversion) ELSE "usage".has_format_conversion END,
  is_stream = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.is_stream, "usage".is_stream) ELSE "usage".is_stream END,
  upstream_is_stream = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.upstream_is_stream, "usage".upstream_is_stream, "usage".is_stream, false) ELSE "usage".upstream_is_stream END,
  input_tokens = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".input_tokens, EXCLUDED.input_tokens) ELSE "usage".input_tokens END,
  output_tokens = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".output_tokens, EXCLUDED.output_tokens) ELSE "usage".output_tokens END,
  total_tokens = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".total_tokens, EXCLUDED.total_tokens) ELSE "usage".total_tokens END,
  cache_creation_input_tokens = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".cache_creation_input_tokens, EXCLUDED.cache_creation_input_tokens) ELSE "usage".cache_creation_input_tokens END,
  cache_creation_input_tokens_5m = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".cache_creation_input_tokens_5m, EXCLUDED.cache_creation_input_tokens_5m) ELSE "usage".cache_creation_input_tokens_5m END,
  cache_creation_input_tokens_1h = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".cache_creation_input_tokens_1h, EXCLUDED.cache_creation_input_tokens_1h) ELSE "usage".cache_creation_input_tokens_1h END,
  cache_read_input_tokens = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".cache_read_input_tokens, EXCLUDED.cache_read_input_tokens) ELSE "usage".cache_read_input_tokens END,
  cache_creation_cost_usd = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".cache_creation_cost_usd, EXCLUDED.cache_creation_cost_usd) ELSE "usage".cache_creation_cost_usd END,
  cache_read_cost_usd = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".cache_read_cost_usd, EXCLUDED.cache_read_cost_usd) ELSE "usage".cache_read_cost_usd END,
  total_cost_usd = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".total_cost_usd, EXCLUDED.total_cost_usd) ELSE "usage".total_cost_usd END,
  actual_total_cost_usd = CASE WHEN "usage".billing_status = 'pending' AND EXCLUDED.status IN ('completed', 'failed', 'cancelled') THEN GREATEST("usage".actual_total_cost_usd, EXCLUDED.actual_total_cost_usd) ELSE "usage".actual_total_cost_usd END,
  status_code = CASE WHEN "usage".billing_status = 'pending' THEN CASE
    WHEN "usage".status IN ('completed', 'failed', 'cancelled') AND EXCLUDED.status IN ('pending', 'streaming') THEN "usage".status_code
    WHEN "usage".status = 'streaming' AND EXCLUDED.status = 'pending' THEN "usage".status_code
    WHEN "usage".status = 'streaming' AND EXCLUDED.status = 'streaming' AND EXCLUDED.status_code IS NULL THEN "usage".status_code
    WHEN EXCLUDED.status IN ('pending', 'streaming', 'completed', 'cancelled') AND EXCLUDED.status_code IS NULL THEN NULL
    ELSE COALESCE(EXCLUDED.status_code, "usage".status_code)
  END ELSE "usage".status_code END,
  error_message = CASE WHEN "usage".billing_status = 'pending' THEN CASE
    WHEN "usage".status IN ('completed', 'failed', 'cancelled') AND EXCLUDED.status IN ('pending', 'streaming') THEN "usage".error_message
    WHEN "usage".status = 'streaming' AND EXCLUDED.status = 'pending' THEN "usage".error_message
    WHEN EXCLUDED.status IN ('pending', 'streaming', 'completed', 'cancelled') THEN EXCLUDED.error_message
    ELSE COALESCE(EXCLUDED.error_message, "usage".error_message)
  END ELSE "usage".error_message END,
  error_category = CASE WHEN "usage".billing_status = 'pending' THEN CASE
    WHEN "usage".status IN ('completed', 'failed', 'cancelled') AND EXCLUDED.status IN ('pending', 'streaming') THEN "usage".error_category
    WHEN "usage".status = 'streaming' AND EXCLUDED.status = 'pending' THEN "usage".error_category
    WHEN EXCLUDED.status IN ('pending', 'streaming', 'completed', 'cancelled') THEN EXCLUDED.error_category
    ELSE COALESCE(EXCLUDED.error_category, "usage".error_category)
  END ELSE "usage".error_category END,
  response_time_ms = CASE WHEN "usage".billing_status = 'pending' THEN CASE
    WHEN "usage".status IN ('completed', 'failed', 'cancelled') AND EXCLUDED.status IN ('pending', 'streaming') THEN "usage".response_time_ms
    WHEN EXCLUDED.response_time_ms IS NULL OR EXCLUDED.response_time_ms = 0 THEN COALESCE("usage".response_time_ms, EXCLUDED.response_time_ms)
    ELSE EXCLUDED.response_time_ms
  END ELSE "usage".response_time_ms END,
  first_byte_time_ms = CASE WHEN "usage".billing_status = 'pending' THEN CASE
    WHEN "usage".status IN ('completed', 'failed', 'cancelled') AND EXCLUDED.status IN ('pending', 'streaming') THEN "usage".first_byte_time_ms
    WHEN EXCLUDED.first_byte_time_ms IS NULL OR EXCLUDED.first_byte_time_ms = 0 THEN COALESCE("usage".first_byte_time_ms, EXCLUDED.first_byte_time_ms)
    ELSE EXCLUDED.first_byte_time_ms
  END ELSE "usage".first_byte_time_ms END,
  status = CASE WHEN "usage".billing_status = 'pending' THEN CASE
    WHEN "usage".status IN ('completed', 'failed', 'cancelled') AND EXCLUDED.status IN ('pending', 'streaming') THEN "usage".status
    WHEN "usage".status = 'streaming' AND EXCLUDED.status = 'pending' THEN "usage".status
    ELSE EXCLUDED.status
  END ELSE "usage".status END,
  billing_status = CASE WHEN "usage".billing_status = 'pending' THEN EXCLUDED.billing_status ELSE "usage".billing_status END,
  request_metadata = CASE WHEN "usage".billing_status = 'pending' AND $43 THEN COALESCE(EXCLUDED.request_metadata, "usage".request_metadata) ELSE "usage".request_metadata END,
  updated_at_unix_secs = CASE WHEN "usage".billing_status = 'pending' THEN
    GREATEST(
      COALESCE(NULLIF("usage".updated_at_unix_secs, 0), 0),
      COALESCE(NULLIF(EXCLUDED.updated_at_unix_secs, 0), 0),
      CAST(EXTRACT(EPOCH FROM "usage".created_at) AS BIGINT)
    )
  ELSE "usage".updated_at_unix_secs END
RETURNING
  request_id
