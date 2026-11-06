-- Seed rows that fresh databases need before serving traffic.
INSERT INTO public.user_groups (
    id, name, normalized_name, description, priority,
    allowed_providers_mode, allowed_api_formats_mode, allowed_models_mode, rate_limit_mode
) VALUES (
    '00000000-0000-0000-0000-000000000001', 'Default', 'default',
    'Default group for all users', 0,
    'unrestricted', 'unrestricted', 'unrestricted', 'system'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO public.system_configs (id, key, value, description)
VALUES (
    '00000000-0000-0000-0000-000000000002', 'default_user_group_id',
    '"00000000-0000-0000-0000-000000000001"'::jsonb,
    'Default user group'
)
ON CONFLICT (key) DO NOTHING;

INSERT INTO public.dashboard_stats_total (shard)
SELECT generate_series(0, 15)
ON CONFLICT (shard) DO NOTHING;

INSERT INTO public.dashboard_stats_state (singleton, stats_since)
VALUES (TRUE, clock_timestamp())
ON CONFLICT (singleton) DO NOTHING;
