-- PostgreSQL 17 relaxed the parallel-mode subtransaction restriction
-- ("Allow internal subtransactions in parallel mode", 2024-03-28); PG18's
-- parallel-safety rules exempt subtransactions used for error recovery. The
-- PG15/16 workaround in 20261008000000 is unnecessary on the PG18+ baseline.
-- Restore the original PARALLEL SAFE marking from 20261007000000.
ALTER FUNCTION public.usage_customer_billable_amount(jsonb, numeric, numeric) PARALLEL SAFE;
