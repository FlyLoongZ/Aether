-- Introduce the split HTTP capture tables.
--
-- `usage_http_audits` and `usage_body_blobs` mixed several retention windows
-- into one row: headers (months), bodies (days) and the body-state machine that
-- tracked which of them had already been cleared. Retention could only be
-- expressed as `UPDATE ... SET <columns> = NULL`, which never removes a row from
-- the sweep's predicate, so the sweep kept rewriting the same batch.
--
-- The replacements are one row per request each, differing only in retention
-- window, and are never updated to clear data:
--   * `usage_http_headers` -- the client pair plus the upstream pair
--   * `usage_http_bodies`  -- the same four as raw bytes
--
-- Each table holds the exchange the accounting row records, not a history of
-- retries: which candidates were tried and how each one ended is what
-- `request_candidates` is for, and it already carries the status, error and
-- latency of every attempt.
--
-- Bodies are the bytes that crossed the wire, plus a `*_encoding` marker saying
-- how to read them (json / text / sse / binary) and a `*_truncated` marker
-- saying the capture stopped early. Both tables ask PostgreSQL not to
-- compress their payload columns; the same shape is captured in
-- `crates/aether-data/runtime/schema/bootstrap/postgres/000_snapshot.sql`.
--
-- Both tables carry `created_at` from the request itself, so a plain
-- `DELETE ... WHERE created_at < cutoff` retires them with an index that
-- advances on its own.
--
-- The old tables are dropped by the accompanying data backfill, not here: this
-- migration runs while the previous release may still be writing them.

-- An empty database already gets both tables from the bootstrap snapshot, so
-- every statement below is a no-op there.
CREATE TABLE IF NOT EXISTS public.usage_http_headers (
    request_id character varying(128) NOT NULL,
    client_request jsonb,
    client_response jsonb,
    provider_request jsonb,
    provider_response jsonb,
    created_at timestamp with time zone NOT NULL
);

ALTER TABLE ONLY public.usage_http_headers
    ALTER COLUMN client_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_headers
    ALTER COLUMN client_response SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_headers
    ALTER COLUMN provider_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_headers
    ALTER COLUMN provider_response SET STORAGE EXTERNAL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'usage_http_headers_pkey'
    ) THEN
        ALTER TABLE ONLY public.usage_http_headers
            ADD CONSTRAINT usage_http_headers_pkey PRIMARY KEY (request_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'usage_http_headers_request_id_fkey'
    ) THEN
        ALTER TABLE ONLY public.usage_http_headers
            ADD CONSTRAINT usage_http_headers_request_id_fkey
            FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS ix_usage_http_headers_created_at
    ON public.usage_http_headers USING btree (created_at);

CREATE TABLE IF NOT EXISTS public.usage_http_bodies (
    request_id character varying(128) NOT NULL,
    client_request bytea,
    client_request_encoding character varying(8),
    client_request_truncated boolean NOT NULL DEFAULT false,
    client_response bytea,
    client_response_encoding character varying(8),
    client_response_truncated boolean NOT NULL DEFAULT false,
    provider_request bytea,
    provider_request_encoding character varying(8),
    provider_request_truncated boolean NOT NULL DEFAULT false,
    provider_response bytea,
    provider_response_encoding character varying(8),
    provider_response_truncated boolean NOT NULL DEFAULT false,
    created_at timestamp with time zone NOT NULL
);

ALTER TABLE ONLY public.usage_http_bodies
    ALTER COLUMN client_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_bodies
    ALTER COLUMN client_response SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_bodies
    ALTER COLUMN provider_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_bodies
    ALTER COLUMN provider_response SET STORAGE EXTERNAL;

-- Databases that already ran an earlier build of this branch hold the table
-- without the truncation columns; add them idempotently so the guarded
-- constraints below always find them.
ALTER TABLE ONLY public.usage_http_bodies
    ADD COLUMN IF NOT EXISTS client_request_truncated boolean NOT NULL DEFAULT false;
ALTER TABLE ONLY public.usage_http_bodies
    ADD COLUMN IF NOT EXISTS client_response_truncated boolean NOT NULL DEFAULT false;
ALTER TABLE ONLY public.usage_http_bodies
    ADD COLUMN IF NOT EXISTS provider_request_truncated boolean NOT NULL DEFAULT false;
ALTER TABLE ONLY public.usage_http_bodies
    ADD COLUMN IF NOT EXISTS provider_response_truncated boolean NOT NULL DEFAULT false;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'usage_http_bodies_pkey'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT usage_http_bodies_pkey PRIMARY KEY (request_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'usage_http_bodies_request_id_fkey'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT usage_http_bodies_request_id_fkey
            FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'client_request_encoding_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT client_request_encoding_pair_check
            CHECK ((client_request IS NULL) = (client_request_encoding IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'client_request_truncated_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT client_request_truncated_pair_check
            CHECK (NOT client_request_truncated OR client_request IS NOT NULL);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'client_request_encoding_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT client_request_encoding_check
            CHECK (client_request_encoding IN ('json', 'text', 'sse', 'binary'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'client_response_encoding_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT client_response_encoding_pair_check
            CHECK ((client_response IS NULL) = (client_response_encoding IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'client_response_truncated_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT client_response_truncated_pair_check
            CHECK (NOT client_response_truncated OR client_response IS NOT NULL);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'client_response_encoding_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT client_response_encoding_check
            CHECK (client_response_encoding IN ('json', 'text', 'sse', 'binary'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'provider_request_encoding_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT provider_request_encoding_pair_check
            CHECK ((provider_request IS NULL) = (provider_request_encoding IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'provider_request_truncated_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT provider_request_truncated_pair_check
            CHECK (NOT provider_request_truncated OR provider_request IS NOT NULL);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'provider_request_encoding_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT provider_request_encoding_check
            CHECK (provider_request_encoding IN ('json', 'text', 'sse', 'binary'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'provider_response_encoding_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT provider_response_encoding_pair_check
            CHECK ((provider_response IS NULL) = (provider_response_encoding IS NULL));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'provider_response_truncated_pair_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT provider_response_truncated_pair_check
            CHECK (NOT provider_response_truncated OR provider_response IS NOT NULL);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'provider_response_encoding_check'
    ) THEN
        ALTER TABLE ONLY public.usage_http_bodies
            ADD CONSTRAINT provider_response_encoding_check
            CHECK (provider_response_encoding IN ('json', 'text', 'sse', 'binary'));
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS ix_usage_http_bodies_created_at
    ON public.usage_http_bodies USING btree (created_at);
