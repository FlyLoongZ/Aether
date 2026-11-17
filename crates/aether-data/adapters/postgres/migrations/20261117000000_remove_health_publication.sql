-- The public status page and the publication allowlist it served were removed,
-- so `health_publication_v1` is an orphaned configuration key. Drop it instead
-- of leaving a value in `system_configs` that no code reads any more and that
-- configuration export/import would keep carrying into new installations.
DELETE FROM system_configs
WHERE key = 'health_publication_v1';
