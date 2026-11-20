-- Retire the AI video generation surface (openai:video / gemini:video / xai:video).
--
-- The table no longer exists in the baseline fragments or the empty-database snapshot;
-- this migration only retires it on databases provisioned before video support was
-- removed. Every statement is idempotent so the migration stays harmless when replayed
-- against a freshly bootstrapped database.

DROP TABLE IF EXISTS public.video_tasks;

-- Management tokens lose the removed permission scope. A token that only held video
-- permissions cannot be given a replacement scope on the operator's behalf: granting a
-- different permission silently widens what the token can do. Such tokens are disabled and
-- logged so they can be reissued with an explicit permission set.
DO $$
DECLARE
    affected text;
BEGIN
    SELECT string_agg(tokens.id || ' (' || tokens.name || ')', ', ' ORDER BY tokens.id)
      INTO affected
      FROM public.management_tokens AS tokens
     WHERE jsonb_typeof(tokens.permissions) = 'array'
       AND tokens.permissions ?| ARRAY[
           'admin:video_tasks:read',
           'admin:video_tasks:write',
           'admin:video_tasks:admin'
       ]
       AND NOT EXISTS (
           SELECT 1
             FROM jsonb_array_elements_text(tokens.permissions) AS permission
            WHERE permission NOT LIKE 'admin:video_tasks:%'
       );
    IF affected IS NOT NULL THEN
        RAISE NOTICE
            'video support removal disabled management tokens that only held video permissions: %',
            affected;
    END IF;
END
$$;

WITH filtered AS (
    SELECT
        id,
        COALESCE(
            (
                SELECT jsonb_agg(permission ORDER BY permission)
                FROM jsonb_array_elements_text(permissions) AS permission
                WHERE permission NOT LIKE 'admin:video_tasks:%'
            ),
            '[]'::jsonb
        ) AS remaining
    FROM public.management_tokens
    WHERE jsonb_typeof(permissions) = 'array'
      AND permissions ?| ARRAY[
          'admin:video_tasks:read',
          'admin:video_tasks:write',
          'admin:video_tasks:admin'
      ]
)
UPDATE public.management_tokens AS tokens
SET permissions = filtered.remaining,
    is_active = CASE
        WHEN jsonb_array_length(filtered.remaining) = 0 THEN false
        ELSE tokens.is_active
    END
FROM filtered
WHERE tokens.id = filtered.id;

-- Video-only billing configuration.
DELETE FROM public.billing_rules
WHERE lower(btrim(task_type)) = 'video';

DELETE FROM public.dimension_collectors
WHERE lower(btrim(task_type)) = 'video'
   OR upper(btrim(api_format)) IN ('OPENAI:VIDEO', 'GEMINI:VIDEO');

-- Global models whose provider models all served a removed video format would be left
-- behind as enabled shells with no provider model at all. The provider model rows are the
-- evidence for that, so capture the affected ids before they are deleted; `models` keeps a
-- plain foreign key on `global_model_id`, so the parents can only be deleted afterwards.
DROP TABLE IF EXISTS pg_temp.retired_video_global_models;
CREATE TEMPORARY TABLE retired_video_global_models (
    id character varying(36) PRIMARY KEY
);
INSERT INTO retired_video_global_models (id)
SELECT model.global_model_id
FROM public.models AS model
WHERE model.global_model_id IS NOT NULL
GROUP BY model.global_model_id
HAVING bool_or(lower(btrim(model.api_format)) IN ('openai:video', 'gemini:video'))
   AND NOT bool_or(
           model.api_format IS NULL
           OR lower(btrim(model.api_format)) NOT IN ('openai:video', 'gemini:video')
       );

-- Video-only provider endpoints and provider models. Endpoint rows are scoped to a single
-- api_format, so removing them cannot take another surface's configuration with them.
DELETE FROM public.provider_endpoints
WHERE lower(btrim(api_format)) IN ('openai:video', 'gemini:video');

DELETE FROM public.models
WHERE lower(btrim(api_format)) IN ('openai:video', 'gemini:video');

DELETE FROM public.global_models AS global_model
USING retired_video_global_models AS retired
WHERE global_model.id = retired.id;

DROP TABLE retired_video_global_models;

-- Per-format grants on API keys, provider keys and user groups.
UPDATE public.api_keys
SET allowed_api_formats = COALESCE(
        (
            SELECT jsonb_agg(api_format ORDER BY ordinality)
            FROM jsonb_array_elements_text(allowed_api_formats)
                WITH ORDINALITY AS entries(api_format, ordinality)
            WHERE api_format NOT IN ('openai:video', 'gemini:video')
        ),
        '[]'::jsonb
    )
WHERE jsonb_typeof(allowed_api_formats) = 'array'
  AND allowed_api_formats ?| ARRAY['openai:video', 'gemini:video'];

-- User groups carry their own per-format grant list, so they need the same filtering. A
-- group restricted to the removed formats keeps an empty list, which the "specific" mode
-- already treats as deny-all; it is not widened to any other surface.
UPDATE public.user_groups
SET allowed_api_formats = COALESCE(
        (
            SELECT jsonb_agg(api_format ORDER BY ordinality)
            FROM jsonb_array_elements_text(allowed_api_formats)
                WITH ORDINALITY AS entries(api_format, ordinality)
            WHERE api_format NOT IN ('openai:video', 'gemini:video')
        ),
        '[]'::jsonb
    )
WHERE jsonb_typeof(allowed_api_formats) = 'array'
  AND allowed_api_formats ?| ARRAY['openai:video', 'gemini:video'];

-- Per-format provider credential settings.
UPDATE public.provider_api_keys
SET api_formats = COALESCE(
        (
            SELECT jsonb_agg(api_format ORDER BY ordinality)
            FROM jsonb_array_elements_text(api_formats)
                WITH ORDINALITY AS entries(api_format, ordinality)
            WHERE api_format NOT IN ('openai:video', 'gemini:video')
        ),
        '[]'::jsonb
    )
WHERE jsonb_typeof(api_formats) = 'array'
  AND api_formats ?| ARRAY['openai:video', 'gemini:video'];

UPDATE public.provider_api_keys
SET auth_type_by_format = auth_type_by_format - 'openai:video' - 'gemini:video',
    rate_multipliers = rate_multipliers - 'openai:video' - 'gemini:video',
    health_by_format = health_by_format - 'openai:video' - 'gemini:video',
    circuit_breaker_by_format = circuit_breaker_by_format - 'openai:video' - 'gemini:video',
    allow_auth_channel_mismatch_formats =
        allow_auth_channel_mismatch_formats - 'openai:video' - 'gemini:video',
    global_priority_by_format =
        global_priority_by_format - 'openai:video' - 'gemini:video'
WHERE (jsonb_typeof(auth_type_by_format) = 'object'
        AND auth_type_by_format ?| ARRAY['openai:video', 'gemini:video'])
   OR (jsonb_typeof(rate_multipliers) = 'object'
        AND rate_multipliers ?| ARRAY['openai:video', 'gemini:video'])
   OR (jsonb_typeof(health_by_format) = 'object'
        AND health_by_format ?| ARRAY['openai:video', 'gemini:video'])
   OR (jsonb_typeof(circuit_breaker_by_format) = 'object'
        AND circuit_breaker_by_format ?| ARRAY['openai:video', 'gemini:video'])
   OR (jsonb_typeof(allow_auth_channel_mismatch_formats) = 'object'
        AND allow_auth_channel_mismatch_formats ?| ARRAY['openai:video', 'gemini:video'])
   OR (jsonb_typeof(global_priority_by_format) = 'object'
        AND global_priority_by_format ?| ARRAY['openai:video', 'gemini:video']);

-- Global model configuration keeps a public per-resolution video price map under
-- config.billing.video; drop that bookkeeping with the surface it priced.
UPDATE public.global_models
SET config = config #- '{billing,video}'
WHERE jsonb_typeof(config) = 'object'
  AND config #> '{billing,video}' IS NOT NULL;
