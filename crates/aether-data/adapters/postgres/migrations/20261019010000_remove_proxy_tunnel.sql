-- Remove the tunnel-only proxy node concept. Manual HTTP/SOCKS proxy nodes
-- remain and keep the proxy_nodes table.
--
-- Two shapes can reach this migration: legacy/upgraded databases and the
-- snapshot bootstrap still carry the pre-removal columns, while a database
-- bootstrapped from a regenerated snapshot already carries the trimmed ones.
-- Every step is therefore guarded so the migration stays a no-op instead of
-- failing on "column does not exist".

DO $mig$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'proxy_nodes'
      AND column_name = 'is_manual'
  ) THEN
    DELETE FROM public.proxy_nodes WHERE is_manual = FALSE;
  END IF;
END $mig$;

DROP TABLE IF EXISTS public.proxy_node_events;
DROP TABLE IF EXISTS public.proxy_node_metrics_1m;
DROP TABLE IF EXISTS public.proxy_node_metrics_1h;

DO $mig$ BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'proxy_nodes'
      AND column_name = 'tunnel_generation'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'proxy_nodes'
      AND column_name = 'node_generation'
  ) THEN
    ALTER TABLE public.proxy_nodes
      RENAME COLUMN tunnel_generation TO node_generation;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'proxy_nodes'
      AND column_name = 'last_heartbeat_at'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'proxy_nodes'
      AND column_name = 'last_used_at'
  ) THEN
    ALTER TABLE public.proxy_nodes
      RENAME COLUMN last_heartbeat_at TO last_used_at;
  END IF;

  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'usage_counter_deltas'
      AND column_name = 'target_tunnel_generation'
  ) AND NOT EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'usage_counter_deltas'
      AND column_name = 'target_node_generation'
  ) THEN
    ALTER TABLE public.usage_counter_deltas
      RENAME COLUMN target_tunnel_generation TO target_node_generation;
  END IF;
END $mig$;

ALTER TABLE public.proxy_nodes
    DROP COLUMN IF EXISTS is_manual,
    DROP COLUMN IF EXISTS tunnel_mode,
    DROP COLUMN IF EXISTS tunnel_connected,
    DROP COLUMN IF EXISTS tunnel_connected_at,
    DROP COLUMN IF EXISTS hardware_info,
    DROP COLUMN IF EXISTS estimated_max_concurrency,
    DROP COLUMN IF EXISTS heartbeat_interval,
    DROP COLUMN IF EXISTS active_connections,
    DROP COLUMN IF EXISTS proxy_metadata,
    DROP COLUMN IF EXISTS remote_config,
    DROP COLUMN IF EXISTS config_version;

DELETE FROM system_configs
WHERE key IN (
    'proxy_node_metrics_1m_retention_days',
    'proxy_node_metrics_1h_retention_days'
);
