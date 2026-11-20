-- no-transaction
-- Wallet daily ledgers and flows aggregate settled settlement snapshots by wallet and
-- finalized time. The usage mirrors that used to carry this index are gone, so the
-- authoritative table needs its own.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_usage_settlement_wallet_finalized
  ON public.usage_settlement_snapshots (wallet_id, finalized_at)
  WHERE wallet_id IS NOT NULL;
