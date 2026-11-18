-- Retirement DDL touches hot tables; bound how long the upgrade can wait for the
-- table lock instead of inheriting the migration runner's unlimited wait.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

-- Retire usage columns that no longer have a writer or reader.
--
-- Fresh databases already omit these columns from the bootstrap snapshot.
-- Upgraded databases keep them until this backfill runs, because schema
-- migrations run before backfills and dropping them there would break a
-- direct upgrade from a database that still needs the old definitions.
--
-- The columns are intentionally dropped without cascading. If a deployment
-- added an object that still depends on one of them, the backfill must fail
-- instead of silently removing that object.
DROP INDEX IF EXISTS public.usage_created_at_idx;

ALTER TABLE public.usage
    DROP COLUMN IF EXISTS input_output_total_tokens,
    DROP COLUMN IF EXISTS input_context_tokens,
    DROP COLUMN IF EXISTS cache_cost_usd,
    DROP COLUMN IF EXISTS cache_creation_cost_usd_5m,
    DROP COLUMN IF EXISTS cache_creation_cost_usd_1h,
    DROP COLUMN IF EXISTS request_cost_usd,
    DROP COLUMN IF EXISTS actual_input_cost_usd,
    DROP COLUMN IF EXISTS actual_output_cost_usd,
    DROP COLUMN IF EXISTS actual_cache_cost_usd,
    DROP COLUMN IF EXISTS actual_cache_creation_cost_usd,
    DROP COLUMN IF EXISTS actual_cache_creation_cost_usd_5m,
    DROP COLUMN IF EXISTS actual_cache_creation_cost_usd_1h,
    DROP COLUMN IF EXISTS actual_cache_read_cost_usd,
    DROP COLUMN IF EXISTS actual_request_cost_usd,
    DROP COLUMN IF EXISTS cache_creation_price_per_1m_5m,
    DROP COLUMN IF EXISTS cache_creation_price_per_1m_1h,
    DROP COLUMN IF EXISTS cache_creation_ephemeral_5m_input_tokens,
    DROP COLUMN IF EXISTS cache_creation_ephemeral_1h_input_tokens,
    DROP COLUMN IF EXISTS created_at_unix_ms,
    DROP COLUMN IF EXISTS request_headers,
    DROP COLUMN IF EXISTS provider_request_headers,
    DROP COLUMN IF EXISTS response_headers,
    DROP COLUMN IF EXISTS client_response_headers;
