-- Copy legacy headers into the split table, then drop the retired capture tables.
-- Bodies are intentionally not migrated; they have a short retention window.
-- The copy is guarded so fresh databases without the legacy tables can run this.

DO $$
BEGIN
    -- A fresh database has only the split tables, so the copy is skipped there.
    -- If the legacy table exists but the split table does not, abort: silently
    -- skipping would still DROP the legacy headers below and lose them.
    IF to_regclass('public.usage_http_audits') IS NOT NULL THEN
        IF to_regclass('public.usage_http_headers') IS NULL THEN
            RAISE EXCEPTION
                'public.usage_http_audits exists but public.usage_http_headers is missing; apply schema migrations before this backfill';
        END IF;

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
    END IF;
END $$;

DROP TABLE IF EXISTS public.usage_body_blobs;
DROP TABLE IF EXISTS public.usage_http_audits;

-- A later schema cleanup may already have removed these compatibility columns;
-- only annotate the ones that still exist.
DO $$
DECLARE
    legacy_header RECORD;
BEGIN
    FOR legacy_header IN
        SELECT * FROM (VALUES
            ('request_headers', 'public.usage_http_headers.client_request'),
            ('provider_request_headers', 'public.usage_http_headers.provider_request'),
            ('response_headers', 'public.usage_http_headers.provider_response'),
            ('client_response_headers', 'public.usage_http_headers.client_response')
        ) AS owners(column_name, canonical_owner)
    LOOP
        IF EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'usage'
              AND column_name::text = legacy_header.column_name
        ) THEN
            EXECUTE format(
                'COMMENT ON COLUMN public.usage.%I IS %L',
                legacy_header.column_name,
                'DEPRECATED: HTTP audit owner moved to ' || legacy_header.canonical_owner
                    || '. Legacy compatibility only; do not write new values.'
            );
        END IF;
    END LOOP;
END $$;
