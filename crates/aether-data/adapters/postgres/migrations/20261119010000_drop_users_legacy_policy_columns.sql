-- Per-user policy columns are dropped, so restricted users silently fall back to their
-- group-derived policy (usually wider). Report the affected accounts before the drop so
-- the upgrade log carries that fact.
DO $$
DECLARE
    affected bigint;
BEGIN
    -- Fresh databases were bootstrapped after the columns were removed, so the
    -- notice must stay a no-op when the legacy shape is already gone.
    IF NOT EXISTS (
        SELECT 1
          FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'users'
           AND column_name = 'allowed_providers_mode'
    ) THEN
        RETURN;
    END IF;
    SELECT COUNT(*) INTO affected
      FROM public.users
     WHERE COALESCE(allowed_providers_mode, 'inherit') NOT IN ('inherit', 'unrestricted')
        OR COALESCE(allowed_models_mode, 'inherit') NOT IN ('inherit', 'unrestricted')
        OR COALESCE(rate_limit_mode, 'inherit') NOT IN ('inherit', 'system');
    IF affected > 0 THEN
        RAISE NOTICE
            'dropping per-user policy columns widens % user(s) to their group policy',
            affected;
    END IF;
END
$$;

ALTER TABLE public.users
    DROP COLUMN IF EXISTS allowed_providers,
    DROP COLUMN IF EXISTS allowed_providers_mode,
    DROP COLUMN IF EXISTS allowed_api_formats,
    DROP COLUMN IF EXISTS allowed_api_formats_mode,
    DROP COLUMN IF EXISTS allowed_models,
    DROP COLUMN IF EXISTS allowed_models_mode,
    DROP COLUMN IF EXISTS rate_limit,
    DROP COLUMN IF EXISTS rate_limit_mode;
