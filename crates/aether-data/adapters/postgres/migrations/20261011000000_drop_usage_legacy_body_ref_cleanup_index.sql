-- no-transaction
-- The legacy body-reference cleanup stage is gone: it was the only reader of
-- this partial index, and no query filters `usage.request_metadata` on the
-- `*_body_ref` keys any more. The keys themselves are still written and read by
-- the reference-capture path, so only the index is retired here.
--
-- Empty databases never build it: 20260715000000 is stamped by the empty
-- database snapshot, which no longer creates the index.
DROP INDEX CONCURRENTLY IF EXISTS public.idx_usage_legacy_body_ref_cleanup_created_at;
