-- Retirement DDL touches hot tables; bound how long the upgrade can wait for the
-- table lock instead of inheriting the migration runner's unlimited wait.
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

-- Retire settlement mirrors from usage.
--
-- usage_settlement_snapshots owns wallet state, rates, prices, and the
-- finalized timestamp. billing_status stays on usage because it is the write
-- arbitration state, not a settlement mirror.
--
-- Fresh databases already omit these columns from the bootstrap snapshot.
-- Upgraded databases keep them until this backfill runs, after the new
-- application code and canonical view no longer read them.
DROP INDEX IF EXISTS public.idx_usage_billing_finalized_wallet;
DROP INDEX IF EXISTS public.idx_usage_wallet_finalized;
DROP INDEX IF EXISTS public.ix_usage_wallet_id;
DROP INDEX IF EXISTS public.usage_wallet_id_idx;

ALTER TABLE public.usage
    DROP COLUMN IF EXISTS finalized_at,
    DROP COLUMN IF EXISTS wallet_id,
    DROP COLUMN IF EXISTS wallet_balance_before,
    DROP COLUMN IF EXISTS wallet_balance_after,
    DROP COLUMN IF EXISTS wallet_recharge_balance_before,
    DROP COLUMN IF EXISTS wallet_recharge_balance_after,
    DROP COLUMN IF EXISTS wallet_gift_balance_before,
    DROP COLUMN IF EXISTS wallet_gift_balance_after,
    DROP COLUMN IF EXISTS rate_multiplier,
    DROP COLUMN IF EXISTS input_price_per_1m,
    DROP COLUMN IF EXISTS output_price_per_1m,
    DROP COLUMN IF EXISTS cache_creation_price_per_1m,
    DROP COLUMN IF EXISTS cache_read_price_per_1m,
    DROP COLUMN IF EXISTS price_per_request;
