INSERT INTO usage_body_blobs (
  body_ref,
  request_id,
  body_field,
  payload
) VALUES (
  $1,
  $2,
  $3,
  $4
)
ON CONFLICT (body_ref)
DO UPDATE SET
  payload = EXCLUDED.payload,
  updated_at = NOW()
