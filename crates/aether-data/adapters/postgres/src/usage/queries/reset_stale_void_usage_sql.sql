UPDATE "usage"
SET
  billing_status = 'pending'
WHERE request_id = $1
  AND billing_status = 'void'
  AND status IN ('failed', 'cancelled')
