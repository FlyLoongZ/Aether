INSERT INTO usage_body_blobs (
  body_ref,
  request_id,
  body_field,
  payload,
  encoding
) VALUES (
  $1,
  $2,
  $3,
  $4,
  $5
)
ON CONFLICT (body_ref)
DO UPDATE SET
  payload = EXCLUDED.payload,
  encoding = EXCLUDED.encoding,
  updated_at = NOW()
