INSERT INTO usage_http_audits (
  request_id,
  request_headers,
  provider_request_headers,
  response_headers,
  client_response_headers,
  request_body_state,
  provider_request_body_state,
  response_body_state,
  client_response_body_state,
  body_capture_mode
) VALUES (
  $1,
  $2::jsonb,
  $3::jsonb,
  $4::jsonb,
  $5::jsonb,
  $6,
  $7,
  $8,
  $9,
  $10
)
ON CONFLICT (request_id)
DO UPDATE SET
  request_headers = COALESCE(EXCLUDED.request_headers, usage_http_audits.request_headers),
  provider_request_headers = COALESCE(
    EXCLUDED.provider_request_headers,
    usage_http_audits.provider_request_headers
  ),
  response_headers = COALESCE(EXCLUDED.response_headers, usage_http_audits.response_headers),
  client_response_headers = COALESCE(
    EXCLUDED.client_response_headers,
    usage_http_audits.client_response_headers
  ),
  request_body_state = COALESCE(
    EXCLUDED.request_body_state,
    usage_http_audits.request_body_state
  ),
  provider_request_body_state = COALESCE(
    EXCLUDED.provider_request_body_state,
    usage_http_audits.provider_request_body_state
  ),
  response_body_state = COALESCE(
    EXCLUDED.response_body_state,
    usage_http_audits.response_body_state
  ),
  client_response_body_state = COALESCE(
    EXCLUDED.client_response_body_state,
    usage_http_audits.client_response_body_state
  ),
  body_capture_mode = CASE
    WHEN EXCLUDED.body_capture_mode = 'none'
      AND (
        EXCLUDED.request_body_state = 'none'
        OR EXCLUDED.provider_request_body_state = 'none'
        OR EXCLUDED.response_body_state = 'none'
        OR EXCLUDED.client_response_body_state = 'none'
      )
      THEN 'none'
    ELSE COALESCE(
      NULLIF(EXCLUDED.body_capture_mode, 'none'),
      usage_http_audits.body_capture_mode,
      'none'
    )
  END,
  updated_at = NOW()
