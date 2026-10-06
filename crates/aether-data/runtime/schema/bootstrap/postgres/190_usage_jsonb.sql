ALTER TABLE public.usage
    ALTER COLUMN request_headers TYPE jsonb USING request_headers::jsonb,
    ALTER COLUMN provider_request_headers TYPE jsonb USING provider_request_headers::jsonb,
    ALTER COLUMN response_headers TYPE jsonb USING response_headers::jsonb,
    ALTER COLUMN client_response_headers TYPE jsonb USING client_response_headers::jsonb,
    ALTER COLUMN request_body TYPE jsonb USING request_body::jsonb,
    ALTER COLUMN provider_request_body TYPE jsonb USING provider_request_body::jsonb,
    ALTER COLUMN response_body TYPE jsonb USING response_body::jsonb,
    ALTER COLUMN client_response_body TYPE jsonb USING client_response_body::jsonb,
    ALTER COLUMN request_metadata TYPE jsonb USING request_metadata::jsonb;

ALTER TABLE public.usage_http_audits
    ALTER COLUMN request_headers TYPE jsonb USING request_headers::jsonb,
    ALTER COLUMN provider_request_headers TYPE jsonb USING provider_request_headers::jsonb,
    ALTER COLUMN response_headers TYPE jsonb USING response_headers::jsonb,
    ALTER COLUMN client_response_headers TYPE jsonb USING client_response_headers::jsonb;

ALTER TABLE public.request_candidates
    ALTER COLUMN extra_data TYPE jsonb USING extra_data::jsonb,
    ALTER COLUMN required_capabilities TYPE jsonb USING required_capabilities::jsonb;
