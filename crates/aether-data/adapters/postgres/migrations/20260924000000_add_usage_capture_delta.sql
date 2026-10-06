ALTER TABLE public.usage_body_blobs ADD COLUMN IF NOT EXISTS encoding integer NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'public.usage_body_blobs'::regclass AND conname = 'usage_body_blobs_encoding_check') THEN
        ALTER TABLE public.usage_body_blobs ADD CONSTRAINT usage_body_blobs_encoding_check CHECK (
            encoding = 0 OR (
                encoding = 1 AND body_field IN ('provider_request_body', 'client_response_body')
                AND octet_length(payload) >= 76
                AND substring(payload FROM 1 FOR 4) = decode('41444c31', 'hex')
            )
        );
    END IF;
END;
$$;
