-- Normalize the request-record tables to `jsonb`.
--
-- `usage.request_metadata` is referenced by the overview triggers (in their
-- `UPDATE OF` column list) and by the `usage_analytics_facts_v1` view, and
-- PostgreSQL refuses to change the type of a column those objects depend on.
-- Drop them around the conversion and recreate them, the same way
-- 20260510000000 handles its dependent check constraint. The view is rebuilt
-- from its current definition so databases that already applied the later
-- overview migrations keep the view they have.
--
-- The trigger functions already read the column through jsonb operators
-- (`#>>`, `jsonb_typeof`), so their behaviour is unchanged.
DO $$
DECLARE
    view_definition text;
BEGIN
    IF to_regclass('public.usage_analytics_facts_v1') IS NOT NULL THEN
        view_definition := pg_get_viewdef('public.usage_analytics_facts_v1'::regclass, true);
        DROP VIEW public.usage_analytics_facts_v1;
    END IF;

    DROP TRIGGER IF EXISTS overview_usage_identity ON public.usage;
    DROP TRIGGER IF EXISTS overview_usage_failure ON public.usage;

    ALTER TABLE public.usage
        ALTER COLUMN request_headers TYPE jsonb USING request_headers::jsonb,
        ALTER COLUMN provider_request_headers TYPE jsonb USING provider_request_headers::jsonb,
        ALTER COLUMN response_headers TYPE jsonb USING response_headers::jsonb,
        ALTER COLUMN client_response_headers TYPE jsonb USING client_response_headers::jsonb,
        ALTER COLUMN request_body TYPE jsonb USING request_body::jsonb,
        ALTER COLUMN provider_request_body TYPE jsonb USING provider_request_body::jsonb,
        ALTER COLUMN response_body TYPE jsonb USING response_body::jsonb,
        ALTER COLUMN client_response_body TYPE jsonb USING client_response_body::jsonb,
        ALTER COLUMN request_metadata TYPE jsonb USING request_metadata::jsonb;

    IF to_regprocedure('public.overview_capture_usage_identity()') IS NOT NULL THEN
        CREATE TRIGGER overview_usage_identity
            AFTER INSERT OR UPDATE OF user_id, api_key_id, request_metadata ON public.usage
            FOR EACH ROW EXECUTE FUNCTION public.overview_capture_usage_identity();
    END IF;
    IF to_regprocedure('public.overview_capture_failure()') IS NOT NULL THEN
        CREATE TRIGGER overview_usage_failure
            BEFORE INSERT OR UPDATE OF request_metadata, status ON public.usage
            FOR EACH ROW EXECUTE FUNCTION public.overview_capture_failure();
    END IF;

    IF view_definition IS NOT NULL THEN
        EXECUTE 'CREATE VIEW public.usage_analytics_facts_v1 AS ' || view_definition;
    END IF;
END $$;

ALTER TABLE public.usage_http_audits
    ALTER COLUMN request_headers TYPE jsonb USING request_headers::jsonb,
    ALTER COLUMN provider_request_headers TYPE jsonb USING provider_request_headers::jsonb,
    ALTER COLUMN response_headers TYPE jsonb USING response_headers::jsonb,
    ALTER COLUMN client_response_headers TYPE jsonb USING client_response_headers::jsonb;

ALTER TABLE public.request_candidates
    ALTER COLUMN extra_data TYPE jsonb USING extra_data::jsonb,
    ALTER COLUMN required_capabilities TYPE jsonb USING required_capabilities::jsonb;
