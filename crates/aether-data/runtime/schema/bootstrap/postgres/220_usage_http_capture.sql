-- Split HTTP capture out of the request-accounting tables.
--
-- Both tables hold one row per request and are append-only: a row is inserted
-- once and retired by a plain DELETE on its own `created_at`, never by
-- `UPDATE ... SET NULL`. That is what keeps retention cheap -- the sweep folds
-- nothing back into the predicate and needs no cursor -- and it is why the two
-- capture kinds are split by retention window instead of by column: headers live
-- for months, bodies for days.
--
-- Bodies are stored as the bytes that actually crossed the wire. The four
-- `*_encoding` columns say how to read them (json / text / sse / binary) because
-- a `bytea` alone cannot: an SSE frame stream stays raw instead of being
-- rewritten into a chunk array, and a binary body is no longer inflated by
-- base64. The four `*_truncated` columns carry the byte-level cut so a reloaded
-- row knows the capture stopped early without consulting `request_metadata`;
-- `EXTERNAL` turns off PostgreSQL's compression: these values are
-- handed to the application's own storage path, so pglz would only burn CPU.
-- The CHECK constraints keep the payload, its encoding and its truncation flag
-- in step: a body without an encoding, an encoding without a body, or a
-- truncation flag without bytes is a bug the engine can catch on write.

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

ALTER TABLE ONLY public.usage_http_headers
    ADD CONSTRAINT usage_http_headers_pkey PRIMARY KEY (request_id);
CREATE INDEX IF NOT EXISTS ix_usage_http_headers_created_at
    ON public.usage_http_headers USING btree (created_at);
ALTER TABLE ONLY public.usage_http_headers
    ADD CONSTRAINT usage_http_headers_request_id_fkey
    FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;


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

ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT client_request_encoding_pair_check
    CHECK ((client_request IS NULL) = (client_request_encoding IS NULL));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT client_request_truncated_pair_check
    CHECK (NOT client_request_truncated OR client_request IS NOT NULL);
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT client_request_encoding_check
    CHECK (client_request_encoding IN ('json', 'text', 'sse', 'binary'));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT client_response_encoding_pair_check
    CHECK ((client_response IS NULL) = (client_response_encoding IS NULL));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT client_response_truncated_pair_check
    CHECK (NOT client_response_truncated OR client_response IS NOT NULL);
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT client_response_encoding_check
    CHECK (client_response_encoding IN ('json', 'text', 'sse', 'binary'));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT provider_request_encoding_pair_check
    CHECK ((provider_request IS NULL) = (provider_request_encoding IS NULL));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT provider_request_truncated_pair_check
    CHECK (NOT provider_request_truncated OR provider_request IS NOT NULL);
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT provider_request_encoding_check
    CHECK (provider_request_encoding IN ('json', 'text', 'sse', 'binary'));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT provider_response_encoding_pair_check
    CHECK ((provider_response IS NULL) = (provider_response_encoding IS NULL));
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT provider_response_truncated_pair_check
    CHECK (NOT provider_response_truncated OR provider_response IS NOT NULL);
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT provider_response_encoding_check
    CHECK (provider_response_encoding IN ('json', 'text', 'sse', 'binary'));

ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT usage_http_bodies_pkey PRIMARY KEY (request_id);
CREATE INDEX IF NOT EXISTS ix_usage_http_bodies_created_at
    ON public.usage_http_bodies USING btree (created_at);
ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT usage_http_bodies_request_id_fkey
    FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;
