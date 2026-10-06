UPDATE public.users
SET allowed_providers_mode = 'unrestricted'
WHERE allowed_providers_mode = 'specific'
  AND (
    allowed_providers IS NULL
    OR jsonb_typeof(allowed_providers::jsonb) = 'null'
    OR CASE
      WHEN jsonb_typeof(allowed_providers::jsonb) = 'array' THEN
        jsonb_array_length(allowed_providers::jsonb) = 0
      ELSE FALSE
    END
  );

UPDATE public.users
SET allowed_api_formats_mode = 'unrestricted'
WHERE allowed_api_formats_mode = 'specific'
  AND (
    allowed_api_formats IS NULL
    OR jsonb_typeof(allowed_api_formats::jsonb) = 'null'
    OR CASE
      WHEN jsonb_typeof(allowed_api_formats::jsonb) = 'array' THEN
        jsonb_array_length(allowed_api_formats::jsonb) = 0
      ELSE FALSE
    END
  );

UPDATE public.users
SET allowed_models_mode = 'unrestricted'
WHERE allowed_models_mode = 'specific'
  AND (
    allowed_models IS NULL
    OR jsonb_typeof(allowed_models::jsonb) = 'null'
    OR CASE
      WHEN jsonb_typeof(allowed_models::jsonb) = 'array' THEN
        jsonb_array_length(allowed_models::jsonb) = 0
      ELSE FALSE
    END
  );
