-- Move the historical HTTP capture into the split tables and retire the old ones.
--
-- `usage_http_audits` holds one exchange per request, which is exactly what the
-- replacement tables hold too, so the copy is column to column. The retry trail
-- was never in this table: `request_candidates` has it.
--
-- `usage_body_blobs` is not migrated: bodies were captured for a much shorter
-- window than headers, so whatever is still there expires within days and is
-- not worth a second pass over a `bytea` table.
--
-- Both DROPs are in the same statement batch as the copy, so a failure leaves
-- the old tables intact rather than half-migrated.

INSERT INTO public.usage_http_headers (
    request_id,
    client_request,
    client_response,
    provider_request,
    provider_response,
    created_at
)
SELECT
    audits.request_id,
    audits.request_headers,
    audits.client_response_headers,
    audits.provider_request_headers,
    audits.response_headers,
    -- Swept by its own timestamp, so it has to be the request's time and not the
    -- time this backfill happens to run.
    usage.created_at
FROM public.usage_http_audits AS audits
JOIN public.usage AS usage ON usage.request_id = audits.request_id
WHERE usage.created_at IS NOT NULL
ON CONFLICT (request_id) DO NOTHING;

DROP TABLE IF EXISTS public.usage_body_blobs;
DROP TABLE IF EXISTS public.usage_http_audits;
