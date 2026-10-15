-- Captured usage bodies are stored as one zstd frame per request field pair, so
-- the blob row has to describe how `payload` is encoded and how large the
-- uncompressed body set is. `encoding` defaults to 0, which is what every
-- existing row holds: a raw, uncompressed, single body.
--
-- `logical_bytes` stays NULL for those legacy rows; readers fall back to
-- octet_length(payload) for them. Both columns are metadata-only, so the
-- ALTER does not rewrite the table.
ALTER TABLE public.usage_body_blobs
    ADD COLUMN IF NOT EXISTS encoding smallint NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS logical_bytes integer;
