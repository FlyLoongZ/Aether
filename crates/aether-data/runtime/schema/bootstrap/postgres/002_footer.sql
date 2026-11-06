-- Keep SQLx's unqualified migration-ledger writes on public for the rest of
-- this transaction. The bootstrap connection is discarded after committing.
SELECT pg_catalog.set_config('search_path', 'public', true);
