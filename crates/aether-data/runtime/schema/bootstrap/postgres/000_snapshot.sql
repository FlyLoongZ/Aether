-- Schema state at migration 20261103000000: derived from pg_dump of a
-- PostgreSQL 18.x reference database and then post-processed (see
-- ../../README.md).
-- Retired capture and proxy tables are intentionally omitted.
-- Requires PostgreSQL 18 or newer: this is pg_dump output from an 18.x
-- server, which is not guaranteed to restore into an older major version.
--
-- Keep the migration runner's statement/lock deadlines in effect: they are set
-- by MigrationTimeouts::apply immediately before this snapshot runs. Only the
-- idle timeout and the server-wide transaction deadline are relaxed here; the
-- client-side with_deadline() guard still bounds the whole bootstrap.
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: authsource; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.authsource AS ENUM (
    'local',
    'ldap',
    'oauth'
);


--
-- Name: providerbillingtype; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.providerbillingtype AS ENUM (
    'monthly_quota',
    'pay_as_you_go',
    'free_tier'
);


--
-- Name: proxynodestatus; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.proxynodestatus AS ENUM (
    'online',
    'unhealthy',
    'offline'
);


--
-- Name: userrole; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.userrole AS ENUM (
    'admin',
    'user',
    'audit_admin'
);


--
-- Name: dashboard_apply_pending(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_apply_pending() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE p record; anonymous_request record; old_fact public.dashboard_request_contributions%ROWTYPE;
  fact jsonb; next_metrics jsonb; next_at timestamptz; next_actor text;
  shard_id smallint; old_bucket timestamptz; next_bucket timestamptz; purged boolean; anonymize boolean;
  detail_cutoff timestamptz := date_trunc('minute',clock_timestamp()-INTERVAL '35 days');
BEGIN
  IF current_setting('aether.dashboard_restore',true)='on' THEN
    DELETE FROM public.dashboard_stats_pending WHERE transaction_id=NEW.transaction_id;
    DELETE FROM public.dashboard_user_anonymization_pending WHERE transaction_id=NEW.transaction_id;
    RETURN NULL;
  END IF;
  SELECT EXISTS (SELECT 1 FROM public.dashboard_user_anonymization_pending
    WHERE transaction_id=NEW.transaction_id) INTO anonymize;
  -- Lock all touched shards in one order before touching any ledger or minute.
  -- This also serializes concurrent usage/settlement updates for the same request.
  PERFORM t.shard FROM public.dashboard_stats_total t
    WHERE t.shard IN (SELECT (hashtextextended(request_id,0) & 15)::smallint
      FROM public.dashboard_stats_pending WHERE transaction_id=NEW.transaction_id)
      OR anonymize
    ORDER BY t.shard FOR UPDATE;
  FOR p IN SELECT * FROM public.dashboard_stats_pending
    WHERE transaction_id=NEW.transaction_id ORDER BY request_id LOOP
    shard_id := (hashtextextended(p.request_id,0) & 15)::smallint;
    fact := public.dashboard_request_fact(p.request_id);
    purged := NOT EXISTS(SELECT 1 FROM public.usage WHERE request_id=p.request_id);
    -- A purge preserves the last contribution, including an update in this tx.
    fact := COALESCE(fact,p.deleted_fact);
    IF fact IS NULL THEN
      IF purged THEN DELETE FROM public.dashboard_request_contributions WHERE request_id=p.request_id; END IF;
      CONTINUE;
    END IF;
    next_metrics := fact->'metrics';
    next_at := (fact->>'created_at')::timestamptz;
    next_actor := CASE WHEN COALESCE((next_metrics->>'request_count')::bigint,0)>0
      THEN (SELECT id FROM public.users WHERE id=fact->>'actor_user_id' AND NOT is_deleted) END;
    SELECT * INTO old_fact FROM public.dashboard_request_contributions WHERE request_id=p.request_id;
    IF FOUND AND old_fact.created_at=next_at
      AND old_fact.actor_user_id IS NOT DISTINCT FROM next_actor
      AND old_fact.metrics=next_metrics THEN
      IF purged THEN DELETE FROM public.dashboard_request_contributions WHERE request_id=p.request_id; END IF;
      CONTINUE;
    END IF;
    next_bucket := date_trunc('minute',next_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    IF old_fact.request_id IS NOT NULL THEN
      old_bucket := date_trunc('minute',old_fact.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
      PERFORM public.dashboard_ensure_activity_minute(old_bucket,shard_id);
      UPDATE public.dashboard_activity_minute SET request_count=request_count-COALESCE((old_fact.metrics->>'request_count')::bigint,0)
        WHERE bucket_start=old_bucket AND shard=shard_id;
      UPDATE public.dashboard_stats_total SET metrics=public.dashboard_metrics_add(metrics,old_fact.metrics,-1) WHERE shard=shard_id;
      IF old_bucket>=detail_cutoff THEN
        UPDATE public.dashboard_stats_minute SET metrics=public.dashboard_metrics_add(metrics,old_fact.metrics,-1)
          WHERE bucket_start=old_bucket AND shard=shard_id;
      END IF;
      UPDATE public.dashboard_activity_hour SET request_count=request_count-COALESCE((old_fact.metrics->>'request_count')::bigint,0)
        WHERE bucket_start=date_trunc('hour',old_fact.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AND shard=shard_id;
      IF old_fact.actor_user_id IS NOT NULL AND old_bucket>=detail_cutoff THEN
        UPDATE public.dashboard_actor_minute SET request_count=request_count-1
          WHERE bucket_start=old_bucket AND shard=shard_id AND actor_user_id=old_fact.actor_user_id;
      END IF;
    END IF;
    PERFORM public.dashboard_ensure_activity_minute(next_bucket,shard_id);
    INSERT INTO public.dashboard_activity_minute(bucket_start,shard,request_count)
      VALUES(next_bucket,shard_id,COALESCE((next_metrics->>'request_count')::bigint,0))
      ON CONFLICT(bucket_start,shard) DO UPDATE
        SET request_count=dashboard_activity_minute.request_count+EXCLUDED.request_count;
    UPDATE public.dashboard_stats_total SET metrics=public.dashboard_metrics_add(metrics,next_metrics) WHERE shard=shard_id;
    IF next_bucket>=detail_cutoff THEN
      INSERT INTO public.dashboard_stats_minute(bucket_start,shard,metrics) VALUES(next_bucket,shard_id,next_metrics)
        ON CONFLICT(bucket_start,shard) DO UPDATE
        SET metrics=public.dashboard_metrics_add(dashboard_stats_minute.metrics,EXCLUDED.metrics);
    END IF;
    INSERT INTO public.dashboard_activity_hour(bucket_start,shard,request_count)
      VALUES(date_trunc('hour',next_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',shard_id,COALESCE((next_metrics->>'request_count')::bigint,0))
      ON CONFLICT(bucket_start,shard) DO UPDATE SET request_count=dashboard_activity_hour.request_count+EXCLUDED.request_count;
    IF next_actor IS NOT NULL AND next_bucket>=detail_cutoff THEN
      INSERT INTO public.dashboard_actor_minute(bucket_start,shard,actor_user_id,request_count)
        VALUES(next_bucket,shard_id,next_actor,1)
        ON CONFLICT(bucket_start,shard,actor_user_id) DO UPDATE SET request_count=dashboard_actor_minute.request_count+1;
    END IF;
    IF purged THEN
      DELETE FROM public.dashboard_request_contributions WHERE request_id=p.request_id;
    ELSE
      INSERT INTO public.dashboard_request_contributions(request_id,created_at,actor_user_id,metrics)
      VALUES(p.request_id,next_at,next_actor,next_metrics)
      ON CONFLICT(request_id) DO UPDATE SET created_at=EXCLUDED.created_at,
        actor_user_id=EXCLUDED.actor_user_id,metrics=EXCLUDED.metrics;
    END IF;
  END LOOP;
  -- Flush identity removal only after request corrections, under the same shard
  -- locks. This also covers requests whose source usage was already purged.
  -- Surviving contribution identities were corrected above by request_id; do
  -- not scan the lifetime contribution table while holding the shard locks.
  IF anonymize THEN
    -- A writer can commit between the user's deletion statement and this flush.
    -- Its new attribution was invisible to the user's earlier trigger. Reuse
    -- the actor index to find only those requests, then clear each ledger by PK.
    -- Do not lock attribution rows here: writers lock them before their shard.
    FOR anonymous_request IN
      SELECT a.request_id, pending_user.user_id
      FROM public.dashboard_user_anonymization_pending pending_user
      JOIN public.usage_attribution_snapshots a ON a.actor_user_id=pending_user.user_id
      WHERE pending_user.transaction_id=NEW.transaction_id
    LOOP
      UPDATE public.dashboard_request_contributions SET actor_user_id=NULL
        WHERE request_id=anonymous_request.request_id AND actor_user_id=anonymous_request.user_id;
    END LOOP;
    DELETE FROM public.dashboard_actor_minute a USING public.dashboard_user_anonymization_pending pending_user
      WHERE pending_user.transaction_id=NEW.transaction_id AND a.actor_user_id=pending_user.user_id;
  END IF;
  DELETE FROM public.dashboard_stats_pending WHERE transaction_id=NEW.transaction_id;
  DELETE FROM public.dashboard_user_anonymization_pending WHERE transaction_id=NEW.transaction_id;
  RETURN NULL;
END $$;


--
-- Name: dashboard_enqueue_request(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_enqueue_request() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE rid text;
BEGIN
  IF current_setting('aether.dashboard_restore',true)='on' THEN RETURN COALESCE(NEW,OLD); END IF;
  rid := CASE WHEN TG_OP = 'DELETE' THEN OLD.request_id ELSE NEW.request_id END;
  -- Fast path: an update to a pre-activation request must never scan/replay it.
  IF TG_TABLE_NAME = 'usage' THEN
    IF (CASE WHEN TG_OP = 'DELETE' THEN OLD.created_at ELSE NEW.created_at END)
       < (SELECT stats_since FROM public.dashboard_stats_state WHERE singleton) THEN
      RETURN COALESCE(NEW, OLD);
    END IF;
  END IF;
  INSERT INTO public.dashboard_stats_pending(transaction_id, request_id, deleted_fact)
    VALUES(txid_current(), rid,
      CASE WHEN TG_TABLE_NAME = 'usage' AND TG_OP = 'DELETE'
        THEN public.dashboard_request_fact(rid) END)
  ON CONFLICT (transaction_id, request_id) DO UPDATE
    SET deleted_fact = COALESCE(EXCLUDED.deleted_fact, dashboard_stats_pending.deleted_fact);
  RETURN COALESCE(NEW, OLD);
END $$;


--
-- Name: dashboard_enqueue_user_anonymization(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_enqueue_user_anonymization() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF current_setting('aether.dashboard_restore',true)='on' THEN RETURN NULL; END IF;
  IF TG_OP='DELETE' OR NEW.is_deleted THEN
    INSERT INTO public.dashboard_user_anonymization_pending(transaction_id,user_id)
      VALUES(txid_current(),OLD.id) ON CONFLICT DO NOTHING;
  END IF;
  RETURN NULL;
END $$;


--
-- Name: dashboard_ensure_activity_minute(timestamp with time zone, smallint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_ensure_activity_minute(p_bucket timestamp with time zone, p_shard smallint) RETURNS void
    LANGUAGE sql
    AS $$
  INSERT INTO public.dashboard_activity_minute(bucket_start,shard,request_count)
    SELECT bucket_start,shard,COALESCE((metrics->>'request_count')::bigint,0)
    FROM public.dashboard_stats_minute WHERE bucket_start=p_bucket AND shard=p_shard
    ON CONFLICT(bucket_start,shard) DO NOTHING
$$;


--
-- Name: dashboard_metrics_add(jsonb, jsonb, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_metrics_add(a jsonb, b jsonb, direction integer DEFAULT 1) RETURNS jsonb
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
  SELECT COALESCE(jsonb_object_agg(key, value), '{}'::jsonb) FROM (
    SELECT key, sum(value) AS value FROM (
      SELECT key, value::numeric FROM jsonb_each_text(COALESCE(a, '{}'::jsonb))
      UNION ALL
      SELECT key, value::numeric * direction FROM jsonb_each_text(COALESCE(b, '{}'::jsonb))
    ) entries GROUP BY key
  ) sums
$$;


--
-- Name: dashboard_record_user_event(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_record_user_event() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE added bigint := 0; removed bigint := 0; rid text; at timestamptz := clock_timestamp();
BEGIN
  IF current_setting('aether.dashboard_restore',true)='on' THEN RETURN COALESCE(NEW,OLD); END IF;
  IF TG_OP='INSERT' THEN added:=1; rid:=NEW.id;
  ELSIF TG_OP='DELETE' THEN removed:=(NOT OLD.is_deleted)::integer; rid:=OLD.id;
  ELSE removed:=(NOT OLD.is_deleted AND NEW.is_deleted)::integer; rid:=NEW.id;
  END IF;
  IF added=0 AND removed=0 THEN RETURN COALESCE(NEW,OLD); END IF;
  IF NOT EXISTS(SELECT 1 FROM public.dashboard_stats_state WHERE singleton AND stats_since<=at) THEN RETURN COALESCE(NEW,OLD); END IF;
  INSERT INTO public.dashboard_user_events_minute(bucket_start,shard,created_count,deleted_count)
    VALUES(date_trunc('minute',at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC',
      (hashtextextended(rid,0) & 15)::smallint,added,removed)
    ON CONFLICT(bucket_start,shard) DO UPDATE SET
      created_count=dashboard_user_events_minute.created_count+EXCLUDED.created_count,
      deleted_count=dashboard_user_events_minute.deleted_count+EXCLUDED.deleted_count;
  RETURN COALESCE(NEW,OLD);
END $$;


--
-- Name: dashboard_request_fact(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.dashboard_request_fact(p_request_id text) RETURNS jsonb
    LANGUAGE sql STABLE
    AS $$
  SELECT jsonb_build_object('created_at', f.created_at,
    'actor_user_id', f.actor_user_id,
    'metrics', CASE WHEN f.record_kind = 'session' THEN '{}'::jsonb ELSE
      jsonb_build_object(
        'request_count', 1,
        'input_tokens', COALESCE(f.input_tokens,0),
        'output_tokens', COALESCE(f.output_tokens,0),
        'total_tokens', COALESCE(f.total_tokens,0),
        'usage_available_count', f.usage_available::integer,
        'pricing_available_count', (f.billable_amount IS NOT NULL)::integer,
        'billable_amount', COALESCE(f.billable_amount,0),
        'cache_read_tokens', COALESCE(f.cache_read_input_tokens,0),
        'cache_creation_tokens', COALESCE(f.cache_creation_input_tokens,0),
        'cache_input_tokens', CASE WHEN f.usage_available THEN COALESCE(b.total_input_context,0) ELSE 0 END,
        'first_byte_sum_ms', COALESCE(f.first_byte_time_ms,0),
        'first_byte_sample_count', (f.first_byte_time_ms IS NOT NULL)::integer,
        'response_sum_ms', COALESCE(f.response_time_ms,0),
        'response_sample_count', (f.response_time_ms IS NOT NULL)::integer,
        'stream_requests', COALESCE(f.upstream_is_stream,false)::integer,
        'standard_requests', (NOT COALESCE(f.upstream_is_stream,false))::integer
      ) END)
  FROM public.usage_analytics_facts_v1 f
  JOIN public.usage_billing_facts b USING (request_id)
  WHERE f.request_id = p_request_id
    AND f.created_at >= (SELECT stats_since FROM public.dashboard_stats_state WHERE singleton)
$$;


--
-- Name: overview_anonymize_user(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.overview_anonymize_user() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF TG_OP = 'DELETE' OR NEW.is_deleted THEN
    UPDATE public.usage_attribution_snapshots SET actor_user_id = NULL, credential_owner_id = NULL,
      attribution_kind = 'unknown', attribution_source = 'unknown', attribution_revision = attribution_revision + 10
    WHERE actor_user_id = OLD.id OR credential_owner_id = OLD.id;
    UPDATE public.usage SET request_metadata = (request_metadata::jsonb #- '{analytics_attribution,actor_user_id}')::json
    WHERE request_metadata #>> '{analytics_attribution,actor_user_id}' = OLD.id;
    DELETE FROM public.stats_overview_hourly WHERE dimensions->>'actor_user_id'=OLD.id OR dimensions->>'credential_owner_id'=OLD.id;
    DELETE FROM public.stats_overview_daily WHERE dimensions->>'actor_user_id'=OLD.id OR dimensions->>'credential_owner_id'=OLD.id;
  END IF;
  RETURN NULL;
END $$;


--
-- Name: overview_capture_failure(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.overview_capture_failure() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.status = 'completed' THEN
    NEW.failure_origin := NULL; NEW.failure_stage := NULL; NEW.failure_reason := NULL;
    NEW.failure_schema_version := NULL; RETURN NEW;
  END IF;
  IF NEW.request_metadata #>> '{analytics_failure,origin}' IN ('client','gateway','upstream','transport','unknown') THEN
    NEW.failure_origin := NEW.request_metadata #>> '{analytics_failure,origin}';
    NEW.failure_stage := NEW.request_metadata #>> '{analytics_failure,stage}';
    NEW.failure_reason := NEW.request_metadata #>> '{analytics_failure,reason}';
    NEW.failure_schema_version := 1;
  END IF;
  RETURN NEW;
END $$;


--
-- Name: overview_capture_usage_identity(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.overview_capture_usage_identity() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE actor text; owner text; kind text; source text; standalone boolean;
BEGIN
  SELECT id INTO owner FROM public.users WHERE id=NEW.user_id AND NOT is_deleted;
  SELECT COALESCE(k.is_standalone,
    CASE WHEN jsonb_typeof(NEW.request_metadata::jsonb #> '{analytics_attribution,is_standalone}')='boolean'
      THEN (NEW.request_metadata #>> '{analytics_attribution,is_standalone}')::boolean END,
    CASE WHEN jsonb_typeof(NEW.request_metadata::jsonb->'api_key_is_standalone')='boolean'
      THEN (NEW.request_metadata->>'api_key_is_standalone')::boolean END,
    CASE WHEN a.attribution_source='user_account' THEN false
      WHEN a.attribution_source='standalone_key' THEN true END,
    CASE WHEN NEW.api_key_id IS NULL THEN false END)
  INTO standalone FROM (SELECT 1) seed
  LEFT JOIN public.api_keys k ON k.id=NEW.api_key_id
  LEFT JOIN public.usage_attribution_snapshots a ON a.request_id=NEW.request_id;
  kind := CASE WHEN owner IS NULL THEN 'unknown' WHEN standalone THEN 'standalone'
    WHEN NOT standalone THEN 'employee' ELSE 'unknown' END;
  actor := CASE WHEN kind='employee' THEN owner END;
  source := CASE kind WHEN 'employee' THEN 'user_account' WHEN 'standalone' THEN 'standalone_key' ELSE 'unknown' END;
  INSERT INTO public.usage_attribution_snapshots(request_id, actor_user_id, credential_owner_id,
    attribution_kind, attribution_source, record_kind, parent_request_id, attribution_revision)
  VALUES (NEW.request_id, actor, owner, kind, source,
    COALESCE(NEW.request_metadata #>> '{analytics_attribution,record_kind}', 'request'),
    NEW.request_metadata #>> '{analytics_attribution,parent_request_id}', 2)
  ON CONFLICT (request_id) DO UPDATE SET actor_user_id = EXCLUDED.actor_user_id,
    credential_owner_id = EXCLUDED.credential_owner_id, attribution_kind = EXCLUDED.attribution_kind,
    attribution_source = EXCLUDED.attribution_source,
    record_kind = COALESCE(NEW.request_metadata #>> '{analytics_attribution,record_kind}', usage_attribution_snapshots.record_kind),
    parent_request_id = COALESCE(EXCLUDED.parent_request_id, usage_attribution_snapshots.parent_request_id),
    attribution_revision = EXCLUDED.attribution_revision, recorded_at = NOW()
  WHERE usage_attribution_snapshots.attribution_revision <= EXCLUDED.attribution_revision;
  RETURN NULL;
END $$;


--
-- Name: overview_delete_attribution(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.overview_delete_attribution() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  DELETE FROM public.usage_attribution_snapshots WHERE request_id = OLD.request_id;
  RETURN OLD;
END $$;


--
-- Name: overview_mark_usage_bucket(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.overview_mark_usage_bucket() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE old_time timestamptz; new_time timestamptz; bucket record;
BEGIN
  IF TG_TABLE_NAME = 'usage' THEN
    IF TG_OP <> 'INSERT' THEN old_time := OLD.created_at; END IF;
    IF TG_OP <> 'DELETE' THEN new_time := NEW.created_at; END IF;
  ELSE
    IF TG_OP <> 'INSERT' THEN
      SELECT created_at INTO old_time FROM public.usage WHERE request_id = OLD.request_id;
    END IF;
    IF TG_OP <> 'DELETE' THEN
      SELECT created_at INTO new_time FROM public.usage WHERE request_id = NEW.request_id;
    END IF;
  END IF;
  FOR bucket IN
    SELECT DISTINCT g, date_trunc(g, t AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' AS starts
    FROM unnest(ARRAY[old_time, new_time]) t CROSS JOIN unnest(ARRAY['day','hour']) g
    WHERE t IS NOT NULL ORDER BY g, starts
  LOOP
    INSERT INTO public.stats_overview_dirty_events
      (transaction_id, projection_version, granularity, bucket_start, unrecoverable)
    VALUES (txid_current(), 'overview-v2', bucket.g, bucket.starts,
      TG_TABLE_NAME = 'usage' AND TG_OP = 'DELETE')
    ON CONFLICT (transaction_id, projection_version, granularity, bucket_start)
    DO UPDATE SET unrecoverable = stats_overview_dirty_events.unrecoverable OR EXCLUDED.unrecoverable;
  END LOOP;
  RETURN NULL;
END $$;


--
-- Name: usage_customer_billable_amount(jsonb, numeric, numeric); Type: FUNCTION; Schema: public; Owner: -
--

CREATE OR REPLACE FUNCTION public.usage_customer_billable_amount(metadata jsonb, base_cost numeric, legacy_cost numeric) RETURNS numeric
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
    AS $_$
DECLARE factor jsonb; multiplier numeric; amount numeric;
  factor_name text; factor_value jsonb; factor_number double precision;
  expected_multiplier double precision := 1.0; factor_count integer := 0;
  has_zero boolean := false;
BEGIN
  IF metadata ? 'billing_multiplier_snapshot' THEN
    IF jsonb_typeof(metadata->'billing_multiplier_snapshot') <> 'object'
      OR metadata #> '{billing_multiplier_snapshot,version}' IS DISTINCT FROM '1'::jsonb
      OR jsonb_typeof(metadata #> '{billing_multiplier_snapshot,factors}') IS DISTINCT FROM 'object'
    THEN RETURN NULL; END IF;
    factor := metadata #> '{billing_multiplier_snapshot,multiplier}';
    FOR factor_name, factor_value IN
      SELECT key, value FROM jsonb_each(metadata #> '{billing_multiplier_snapshot,factors}') ORDER BY key COLLATE "C"
    LOOP
      factor_count := factor_count + 1;
      IF factor_count > 16 OR factor_name = '' OR length(factor_name) > 64
        OR factor_name !~ '^[A-Za-z0-9_]+$'
        OR jsonb_typeof(factor_value) IS DISTINCT FROM 'number'
      THEN RETURN NULL; END IF;
      factor_number := factor_value::text::double precision;
      IF factor_number < 0 OR factor_number > 1.7976931348623157e308::double precision
      THEN RETURN NULL; END IF;
      has_zero := has_zero OR factor_number = 0;
    END LOOP;
    -- Rust short-circuits zero before multiplying any of the other factors.
    IF has_zero THEN expected_multiplier := 0;
    ELSE
      FOR factor_name, factor_value IN
        SELECT key, value FROM jsonb_each(metadata #> '{billing_multiplier_snapshot,factors}') ORDER BY key COLLATE "C"
      LOOP
        factor_number := factor_value::text::double precision;
        BEGIN
          expected_multiplier := expected_multiplier * factor_number;
        EXCEPTION WHEN numeric_value_out_of_range THEN
          -- PostgreSQL raises on float underflow; Rust rounds that product to 0.
          IF expected_multiplier::numeric * factor_number::numeric > 1.7976931348623157e308::numeric
          THEN RETURN NULL; END IF;
          expected_multiplier := 0;
        END;
      END LOOP;
    END IF;
  ELSIF metadata ? 'routing_group_billing_multiplier' THEN
    factor := metadata->'routing_group_billing_multiplier';
    expected_multiplier := NULL;
  ELSE
    RETURN CASE WHEN legacy_cost NOT IN ('NaN'::numeric,'Infinity'::numeric,'-Infinity'::numeric)
      THEN round(legacy_cost,8) END;
  END IF;
  IF jsonb_typeof(factor) IS DISTINCT FROM 'number' THEN RETURN NULL; END IF;
  multiplier := factor::text::numeric;
  factor_number := factor::text::double precision;
  IF factor_number < 0
    OR factor_number > 1.7976931348623157e308::double precision
    OR (expected_multiplier IS NOT NULL AND factor_number <> expected_multiplier)
    OR multiplier < 0 OR multiplier > 1.7976931348623157e308::numeric
    OR base_cost IS NULL OR base_cost < 0
    OR base_cost IN ('NaN'::numeric,'Infinity'::numeric,'-Infinity'::numeric)
  THEN RETURN NULL; END IF;
  amount := base_cost * multiplier;
  IF amount > 1.7976931348623157e308::numeric THEN RETURN NULL; END IF;
  RETURN round(amount,8);
EXCEPTION WHEN numeric_value_out_of_range OR invalid_text_representation THEN
  -- Corrupt captured pricing must not abort an entire analytics query.
  RETURN NULL;
END $_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admin_user_wallet_balance_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admin_user_wallet_balance_batches (
    admin_user_id character varying(64) NOT NULL,
    idempotency_key character varying(128) NOT NULL,
    request_fingerprint character varying(64) NOT NULL,
    target_user_ids jsonb NOT NULL,
    missing_user_ids jsonb NOT NULL,
    warnings jsonb NOT NULL,
    user_outcomes jsonb NOT NULL,
    created_at_unix_secs bigint NOT NULL,
    updated_at_unix_secs bigint NOT NULL
);


--
-- Name: announcement_reads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.announcement_reads (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    announcement_id character varying(36) NOT NULL,
    read_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: announcements; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.announcements (
    id character varying(36) NOT NULL,
    title character varying(200) NOT NULL,
    content text NOT NULL,
    type character varying(20) DEFAULT 'info'::character varying,
    priority integer DEFAULT 0,
    author_id character varying(36),
    is_active boolean DEFAULT true,
    is_pinned boolean DEFAULT false,
    start_time timestamp with time zone,
    end_time timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    requires_ack boolean DEFAULT false NOT NULL
);


--
-- Name: api_key_provider_mappings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_key_provider_mappings (
    id character varying(36) NOT NULL,
    api_key_id character varying(36) NOT NULL,
    provider_id character varying(36) NOT NULL,
    priority_adjustment integer DEFAULT 0,
    weight_multiplier double precision DEFAULT '1'::double precision,
    is_enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.api_keys (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    key_hash character varying(64) NOT NULL,
    key_encrypted text,
    name character varying(100),
    key_prefix character varying(64),
    status character varying(64) DEFAULT 'active'::character varying NOT NULL,
    total_requests bigint DEFAULT 0,
    total_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    is_standalone boolean DEFAULT false NOT NULL,
    allowed_providers jsonb,
    allowed_api_formats jsonb,
    allowed_models jsonb,
    rate_limit integer DEFAULT 100,
    concurrent_limit integer,
    force_capabilities jsonb,
    is_active boolean DEFAULT true NOT NULL,
    last_used_at timestamp with time zone,
    expires_at timestamp with time zone,
    auto_delete_on_expiry boolean DEFAULT false NOT NULL,
    metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    is_locked boolean DEFAULT false NOT NULL,
    total_tokens bigint DEFAULT '0'::bigint NOT NULL,
    feature_settings jsonb,
    ip_rules jsonb,
    CONSTRAINT ck_api_keys_standalone_not_locked CHECK (((NOT is_standalone) OR (NOT is_locked)))
);


--
-- Name: audit_logs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_logs (
    id character varying(36) NOT NULL,
    event_type character varying(50) NOT NULL,
    user_id character varying(36),
    api_key_id character varying(36),
    description text NOT NULL,
    ip_address character varying(45),
    user_agent character varying(500),
    request_id character varying(100),
    event_metadata jsonb,
    status_code integer,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: auth_modules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.auth_modules (
    id character varying(36) NOT NULL,
    module_type character varying(128) NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    config jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: background_task_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.background_task_events (
    id character varying(64) NOT NULL,
    run_id character varying(64) NOT NULL,
    event_type character varying(64) NOT NULL,
    message text NOT NULL,
    payload_json jsonb,
    created_at_unix_secs bigint NOT NULL
);


--
-- Name: background_task_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.background_task_runs (
    id character varying(64) NOT NULL,
    task_key character varying(200) NOT NULL,
    kind character varying(32) NOT NULL,
    trigger character varying(64) NOT NULL,
    status character varying(32) NOT NULL,
    attempt integer DEFAULT 0 NOT NULL,
    max_attempts integer DEFAULT 0 NOT NULL,
    owner_instance character varying(200),
    progress_percent integer DEFAULT 0 NOT NULL,
    progress_message text,
    payload_json jsonb,
    result_json jsonb,
    error_message text,
    cancel_requested boolean DEFAULT false NOT NULL,
    created_by character varying(200),
    created_at_unix_secs bigint NOT NULL,
    started_at_unix_secs bigint,
    finished_at_unix_secs bigint,
    updated_at_unix_secs bigint NOT NULL
);


--
-- Name: billing_plans; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_plans (
    id character varying(64) NOT NULL,
    title character varying(128) NOT NULL,
    description text,
    price_amount numeric(20,8) NOT NULL,
    price_currency character varying(16) DEFAULT 'CNY'::character varying NOT NULL,
    duration_unit character varying(32) NOT NULL,
    duration_value bigint NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    sort_order bigint DEFAULT 0 NOT NULL,
    max_active_per_user bigint DEFAULT 1 NOT NULL,
    entitlements_json jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    purchase_limit_scope character varying(32) DEFAULT 'active_period'::character varying NOT NULL
);


--
-- Name: billing_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.billing_rules (
    id character varying(36) NOT NULL,
    global_model_id character varying(36),
    model_id character varying(36),
    name character varying(100) NOT NULL,
    task_type character varying(20) DEFAULT 'chat'::character varying NOT NULL,
    expression text NOT NULL,
    variables jsonb DEFAULT '{}'::jsonb NOT NULL,
    dimension_mappings jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_billing_rules_model_ref CHECK ((((global_model_id IS NOT NULL) AND (model_id IS NULL)) OR ((global_model_id IS NULL) AND (model_id IS NOT NULL))))
);


--
-- Name: dashboard_activity_hour; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_activity_hour (
    bucket_start timestamp with time zone NOT NULL,
    shard smallint NOT NULL,
    request_count bigint NOT NULL,
    CONSTRAINT dashboard_activity_hour_shard_check CHECK (((shard >= 0) AND (shard < 16)))
);


--
-- Name: dashboard_activity_minute; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_activity_minute (
    bucket_start timestamp with time zone NOT NULL,
    shard smallint NOT NULL,
    request_count bigint NOT NULL,
    CONSTRAINT dashboard_activity_minute_shard_check CHECK (((shard >= 0) AND (shard < 16)))
);


--
-- Name: dashboard_actor_minute; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_actor_minute (
    bucket_start timestamp with time zone NOT NULL,
    shard smallint NOT NULL,
    actor_user_id character varying(255) NOT NULL,
    request_count bigint NOT NULL,
    CONSTRAINT dashboard_actor_minute_shard_check CHECK (((shard >= 0) AND (shard < 16)))
);


--
-- Name: dashboard_request_contributions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_request_contributions (
    request_id character varying(100) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    actor_user_id character varying(255),
    metrics jsonb NOT NULL
);


--
-- Name: dashboard_stats_minute; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_stats_minute (
    bucket_start timestamp with time zone NOT NULL,
    shard smallint NOT NULL,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT dashboard_stats_minute_shard_check CHECK (((shard >= 0) AND (shard < 16)))
);


--
-- Name: dashboard_stats_pending; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_stats_pending (
    transaction_id bigint NOT NULL,
    request_id character varying(100) NOT NULL,
    deleted_fact jsonb
);


--
-- Name: dashboard_stats_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_stats_state (
    singleton boolean DEFAULT true NOT NULL,
    stats_since timestamp with time zone NOT NULL,
    contributions_cleanup_cursor character varying(100),
    CONSTRAINT dashboard_stats_state_singleton_check CHECK (singleton)
);


--
-- Name: dashboard_stats_total; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_stats_total (
    shard smallint NOT NULL,
    metrics jsonb DEFAULT '{}'::jsonb NOT NULL,
    CONSTRAINT dashboard_stats_total_shard_check CHECK (((shard >= 0) AND (shard < 16)))
);


--
-- Name: dashboard_user_anonymization_pending; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_user_anonymization_pending (
    transaction_id bigint NOT NULL,
    user_id character varying(255) NOT NULL
);


--
-- Name: dashboard_user_events_minute; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dashboard_user_events_minute (
    bucket_start timestamp with time zone NOT NULL,
    shard smallint NOT NULL,
    created_count bigint DEFAULT 0 NOT NULL,
    deleted_count bigint DEFAULT 0 NOT NULL,
    CONSTRAINT dashboard_user_events_minute_shard_check CHECK (((shard >= 0) AND (shard < 16)))
);


--
-- Name: dimension_collectors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dimension_collectors (
    id character varying(36) NOT NULL,
    api_format character varying(50) NOT NULL,
    task_type character varying(20) NOT NULL,
    dimension_name character varying(100) NOT NULL,
    source_type character varying(20) NOT NULL,
    source_path character varying(200),
    value_type character varying(20) DEFAULT 'float'::character varying NOT NULL,
    transform_expression text,
    default_value character varying(100),
    priority integer DEFAULT 0 NOT NULL,
    is_enabled boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_dimension_collectors_source_config CHECK (((((source_type)::text = 'computed'::text) AND (source_path IS NULL) AND (transform_expression IS NOT NULL)) OR (((source_type)::text <> 'computed'::text) AND (source_path IS NOT NULL))))
);


--
-- Name: entitlement_usage_ledgers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entitlement_usage_ledgers (
    id character varying(64) NOT NULL,
    user_entitlement_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    request_id character varying(128) NOT NULL,
    amount_usd numeric(20,8) NOT NULL,
    balance_before numeric(20,8) NOT NULL,
    balance_after numeric(20,8) NOT NULL,
    usage_date character varying(16) NOT NULL,
    created_at timestamp with time zone NOT NULL
);


--
-- Name: gemini_file_mappings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.gemini_file_mappings (
    id character varying(36) NOT NULL,
    file_name character varying(512) NOT NULL,
    key_id character varying(36) NOT NULL,
    user_id character varying(36),
    display_name character varying(512),
    mime_type character varying(255),
    source_hash character varying(64),
    created_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL
);


--
-- Name: global_models; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.global_models (
    id character varying(36) NOT NULL,
    name character varying(100) NOT NULL,
    display_name character varying(100),
    enabled boolean DEFAULT true NOT NULL,
    default_price_per_request numeric(20,8),
    default_tiered_pricing jsonb,
    supported_capabilities jsonb,
    is_active boolean DEFAULT true NOT NULL,
    usage_count bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    config jsonb,
    metadata jsonb
);


--
-- Name: ldap_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ldap_configs (
    id integer NOT NULL,
    server_url character varying(255) NOT NULL,
    bind_dn text NOT NULL,
    bind_password_encrypted text,
    base_dn text NOT NULL,
    user_search_filter text DEFAULT '(uid={username})'::character varying NOT NULL,
    username_attr character varying(50) DEFAULT 'uid'::character varying NOT NULL,
    email_attr character varying(50) DEFAULT 'mail'::character varying NOT NULL,
    display_name_attr character varying(50) DEFAULT 'cn'::character varying NOT NULL,
    is_enabled boolean DEFAULT false NOT NULL,
    is_exclusive boolean DEFAULT false NOT NULL,
    use_starttls boolean DEFAULT false NOT NULL,
    connect_timeout integer DEFAULT 10 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    singleton_key integer DEFAULT 1 NOT NULL,
    CONSTRAINT ldap_configs_singleton_key_check CHECK ((singleton_key = 1))
);


--
-- Name: ldap_configs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ldap_configs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ldap_configs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ldap_configs_id_seq OWNED BY public.ldap_configs.id;


--
-- Name: management_tokens; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.management_tokens (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    token_hash character varying(64) NOT NULL,
    token_prefix character varying(12),
    name character varying(100) NOT NULL,
    description text,
    allowed_ips jsonb,
    permissions jsonb,
    expires_at timestamp with time zone,
    last_used_at timestamp with time zone,
    last_used_ip character varying(45),
    usage_count bigint DEFAULT 0 NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_allowed_ips_not_empty CHECK (
CASE
    WHEN ((allowed_ips IS NULL) OR (allowed_ips = 'null'::jsonb)) THEN true
    WHEN (jsonb_typeof(allowed_ips) = 'array'::text) THEN (jsonb_array_length(allowed_ips) > 0)
    ELSE false
END)
);


--
-- Name: models; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.models (
    id character varying(36) NOT NULL,
    provider_id character varying(36) NOT NULL,
    global_model_id character varying(36),
    provider_model_name character varying(200) NOT NULL,
    global_model_name character varying(255),
    api_format character varying(128),
    enabled boolean DEFAULT true NOT NULL,
    price_per_request numeric(20,8),
    tiered_pricing jsonb,
    supports_vision boolean,
    supports_function_calling boolean,
    supports_streaming boolean,
    supports_extended_thinking boolean,
    supports_image_generation boolean,
    is_active boolean DEFAULT true NOT NULL,
    is_available boolean DEFAULT true,
    config jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    provider_model_mappings jsonb,
    metadata jsonb
);


--
-- Name: oauth_providers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.oauth_providers (
    provider_type character varying(50) NOT NULL,
    display_name character varying(100) NOT NULL,
    client_id text NOT NULL,
    client_secret_encrypted text,
    authorization_url_override character varying(500),
    token_url_override character varying(500),
    userinfo_url_override character varying(500),
    scopes jsonb,
    redirect_uri character varying(500) NOT NULL,
    frontend_callback_url character varying(500) NOT NULL,
    attribute_mapping jsonb,
    extra_config jsonb,
    is_enabled boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    icon_url character varying(500)
);


--
-- Name: payment_callbacks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payment_callbacks (
    id character varying(36) NOT NULL,
    payment_order_id character varying(36),
    payment_method character varying(30) NOT NULL,
    callback_key character varying(128) NOT NULL,
    order_no character varying(64),
    gateway_order_id character varying(128),
    payload_hash character varying(128),
    signature_valid boolean DEFAULT false NOT NULL,
    status character varying(20) DEFAULT 'received'::character varying NOT NULL,
    payload jsonb,
    error_message text,
    created_at timestamp with time zone NOT NULL,
    processed_at timestamp with time zone
);


--
-- Name: payment_gateway_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payment_gateway_configs (
    provider character varying(64) NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    endpoint_url character varying(512) NOT NULL,
    callback_base_url character varying(512),
    merchant_id character varying(128) NOT NULL,
    merchant_key_encrypted text,
    pay_currency character varying(16) DEFAULT 'CNY'::character varying NOT NULL,
    usd_exchange_rate numeric(18,8) DEFAULT 7.2 NOT NULL,
    min_recharge_usd numeric(20,8) DEFAULT 1 NOT NULL,
    channels_json jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: payment_orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payment_orders (
    id character varying(36) NOT NULL,
    order_no character varying(64) NOT NULL,
    wallet_id character varying(36) NOT NULL,
    user_id character varying(36),
    amount_usd numeric(20,8) NOT NULL,
    pay_amount numeric(20,2),
    pay_currency character varying(3),
    exchange_rate numeric(18,8),
    refunded_amount_usd numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    refundable_amount_usd numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    payment_method character varying(30) NOT NULL,
    gateway_order_id character varying(128),
    gateway_response jsonb,
    status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    created_at timestamp with time zone NOT NULL,
    paid_at timestamp with time zone,
    credited_at timestamp with time zone,
    expires_at timestamp with time zone,
    payment_provider character varying(64),
    payment_channel character varying(64),
    order_kind character varying(64) DEFAULT 'wallet_recharge'::character varying NOT NULL,
    product_id character varying(64),
    product_snapshot jsonb,
    fulfillment_status character varying(64) DEFAULT 'pending'::character varying NOT NULL,
    fulfillment_error text
);


--
-- Name: pool_member_scores; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pool_member_scores (
    id character varying(192) NOT NULL,
    pool_kind character varying(64) NOT NULL,
    pool_id character varying(64) NOT NULL,
    member_kind character varying(64) NOT NULL,
    member_id character varying(64) NOT NULL,
    capability character varying(64) NOT NULL,
    scope_kind character varying(64) NOT NULL,
    scope_id character varying(128),
    score double precision DEFAULT 0 NOT NULL,
    hard_state character varying(64) DEFAULT 'unknown'::character varying NOT NULL,
    score_version bigint DEFAULT 1 NOT NULL,
    score_reason jsonb NOT NULL,
    last_ranked_at bigint,
    last_scheduled_at bigint,
    last_success_at bigint,
    last_failure_at bigint,
    failure_count bigint DEFAULT 0 NOT NULL,
    last_probe_attempt_at bigint,
    last_probe_success_at bigint,
    last_probe_failure_at bigint,
    probe_failure_count bigint DEFAULT 0 NOT NULL,
    probe_status character varying(64) DEFAULT 'never'::character varying NOT NULL,
    updated_at bigint NOT NULL
);


--
-- Name: provider_api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_api_keys (
    id character varying(36) NOT NULL,
    api_key text,
    encrypted_key text,
    name character varying(100) NOT NULL,
    note character varying(500),
    internal_priority integer DEFAULT 50,
    rpm_limit integer,
    allowed_models jsonb,
    capabilities jsonb,
    learned_rpm_limit integer,
    concurrent_429_count integer DEFAULT 0 NOT NULL,
    rpm_429_count integer DEFAULT 0 NOT NULL,
    last_429_at timestamp with time zone,
    last_429_type character varying(50),
    last_rpm_peak integer,
    adjustment_history jsonb,
    utilization_samples jsonb,
    last_probe_increase_at timestamp with time zone,
    cache_ttl_minutes integer DEFAULT 5 NOT NULL,
    max_probe_interval_minutes integer DEFAULT 32 NOT NULL,
    request_count bigint DEFAULT 0,
    success_count bigint DEFAULT 0,
    error_count bigint DEFAULT 0,
    total_response_time_ms bigint DEFAULT 0,
    last_used_at timestamp with time zone,
    last_error_at timestamp with time zone,
    last_error_msg text,
    is_active boolean DEFAULT true NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    provider_id character varying(36) NOT NULL,
    api_formats jsonb,
    auth_type_by_format jsonb,
    rate_multipliers jsonb,
    health_by_format jsonb,
    circuit_breaker_by_format jsonb,
    auto_fetch_models boolean DEFAULT false NOT NULL,
    last_models_fetch_at timestamp with time zone,
    last_models_fetch_error text,
    locked_models jsonb,
    global_priority_by_format jsonb,
    model_include_patterns jsonb,
    model_exclude_patterns jsonb,
    auth_type character varying(20) DEFAULT 'api_key'::character varying NOT NULL,
    auth_config text,
    upstream_metadata jsonb,
    oauth_invalid_at timestamp with time zone,
    oauth_invalid_reason character varying(255),
    proxy jsonb,
    fingerprint jsonb,
    total_tokens bigint NOT NULL,
    total_cost_usd numeric(20,8) NOT NULL,
    status_snapshot jsonb,
    status character varying(64) DEFAULT 'active'::character varying NOT NULL,
    weight bigint DEFAULT 1 NOT NULL,
    metadata jsonb,
    allow_auth_channel_mismatch_formats jsonb,
    concurrent_limit integer
);


--
-- Name: provider_endpoints; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_endpoints (
    id character varying(36) NOT NULL,
    provider_id character varying(36) NOT NULL,
    name character varying(255),
    api_format character varying(50),
    base_url character varying(500) NOT NULL,
    max_retries integer DEFAULT 3,
    enabled boolean DEFAULT true NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    weight bigint DEFAULT 1 NOT NULL,
    custom_path character varying(200),
    config jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    proxy jsonb,
    header_rules jsonb,
    format_acceptance_config jsonb,
    api_family character varying(50),
    endpoint_kind character varying(50),
    body_rules jsonb,
    health_score double precision DEFAULT 1.0 NOT NULL,
    metadata jsonb
);


--
-- Name: provider_expenses; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_expenses (
    id text NOT NULL,
    client_request_id text NOT NULL,
    provider_id text NOT NULL,
    provider_name text NOT NULL,
    kind text NOT NULL,
    amount numeric(20,8) NOT NULL,
    currency text NOT NULL,
    paid_at timestamp with time zone NOT NULL,
    period_start timestamp with time zone,
    period_end timestamp with time zone,
    note text,
    external_reference text,
    created_by text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    voided_at timestamp with time zone,
    voided_by text,
    CONSTRAINT provider_expenses_amount_check CHECK ((amount > (0)::numeric)),
    CONSTRAINT provider_expenses_currency_check CHECK ((currency ~ '^[A-Z]{3}$'::text)),
    CONSTRAINT provider_expenses_kind_check CHECK ((kind = ANY (ARRAY['recharge'::text, 'subscription'::text, 'other'::text]))),
    CONSTRAINT provider_expenses_period_check CHECK ((((period_start IS NULL) AND (period_end IS NULL)) OR ((period_start IS NOT NULL) AND (period_end IS NOT NULL) AND (period_start < period_end))))
);


--
-- Name: provider_usage_tracking; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.provider_usage_tracking (
    id character varying(36) NOT NULL,
    provider_id character varying(36) NOT NULL,
    window_start timestamp with time zone NOT NULL,
    window_end timestamp with time zone NOT NULL,
    total_requests integer DEFAULT 0,
    successful_requests integer DEFAULT 0,
    failed_requests integer DEFAULT 0,
    avg_response_time_ms double precision DEFAULT '0'::double precision,
    total_response_time_ms double precision DEFAULT '0'::double precision,
    total_cost_usd double precision DEFAULT '0'::double precision,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: providers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.providers (
    id character varying(36) NOT NULL,
    name character varying(100) NOT NULL,
    description text,
    website character varying(500),
    billing_type public.providerbillingtype DEFAULT 'pay_as_you_go'::public.providerbillingtype NOT NULL,
    monthly_quota_usd numeric(20,8),
    monthly_used_usd numeric(20,8) DEFAULT '0'::double precision,
    quota_reset_day integer DEFAULT 30,
    quota_last_reset_at timestamp with time zone,
    quota_expires_at timestamp with time zone,
    enabled boolean DEFAULT true NOT NULL,
    priority bigint DEFAULT 0 NOT NULL,
    provider_priority integer DEFAULT 100,
    is_active boolean DEFAULT true NOT NULL,
    concurrent_limit integer,
    config jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    max_retries integer,
    proxy jsonb,
    stream_first_byte_timeout double precision,
    request_timeout double precision,
    keep_priority_on_conversion boolean DEFAULT false NOT NULL,
    enable_format_conversion boolean DEFAULT true NOT NULL,
    provider_type character varying(20) DEFAULT 'custom'::character varying NOT NULL
);


--
-- Name: proxy_nodes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.proxy_nodes (
    id character varying(36) NOT NULL,
    name character varying(100) NOT NULL,
    ip character varying(512) NOT NULL,
    port integer NOT NULL,
    region character varying(100),
    status public.proxynodestatus DEFAULT 'online'::public.proxynodestatus NOT NULL,
    registered_by character varying(36),
    last_used_at timestamp with time zone,
    total_requests bigint DEFAULT 0 NOT NULL,
    avg_latency_ms double precision,
    proxy_url character varying(500),
    proxy_username character varying(255),
    proxy_password text,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    failed_requests bigint DEFAULT '0'::bigint NOT NULL,
    dns_failures bigint DEFAULT '0'::bigint NOT NULL,
    stream_errors bigint DEFAULT '0'::bigint NOT NULL,
    node_generation character varying(64) CONSTRAINT proxy_nodes_tunnel_generation_not_null NOT NULL
);


--
-- Name: redeem_code_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.redeem_code_batches (
    id character varying(36) NOT NULL,
    name character varying(120) NOT NULL,
    amount_usd numeric(20,8) NOT NULL,
    currency character varying(3) DEFAULT 'USD'::character varying NOT NULL,
    balance_bucket character varying(20) DEFAULT 'gift'::character varying NOT NULL,
    total_count integer NOT NULL,
    status character varying(20) DEFAULT 'active'::character varying NOT NULL,
    description text,
    created_by character varying(36),
    expires_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT ck_redeem_code_batches_amount_positive CHECK ((amount_usd > (0)::numeric)),
    CONSTRAINT ck_redeem_code_batches_total_count_positive CHECK ((total_count > 0))
);


--
-- Name: redeem_codes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.redeem_codes (
    id character varying(36) NOT NULL,
    batch_id character varying(36) NOT NULL,
    code_hash character varying(64) NOT NULL,
    code_prefix character varying(8) NOT NULL,
    code_suffix character varying(8) NOT NULL,
    status character varying(20) DEFAULT 'active'::character varying NOT NULL,
    redeemed_by_user_id character varying(36),
    redeemed_wallet_id character varying(36),
    redeemed_payment_order_id character varying(36),
    redeemed_at timestamp with time zone,
    disabled_by character varying(36),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: referral_rewards; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.referral_rewards (
    id character varying(64) NOT NULL,
    referral_id character varying(64) NOT NULL,
    inviter_user_id character varying(64) NOT NULL,
    invitee_user_id character varying(64) NOT NULL,
    reward_type character varying(32) NOT NULL,
    trigger_point character varying(64) NOT NULL,
    source_order_id character varying(64),
    idempotency_key character varying(128) NOT NULL,
    amount_usd numeric(20,8) NOT NULL,
    status character varying(32) DEFAULT 'pending'::character varying NOT NULL,
    wallet_transaction_id character varying(64),
    reversed_amount_usd numeric(20,8) DEFAULT 0 NOT NULL,
    pending_reversal_amount_usd numeric(20,8) DEFAULT 0 NOT NULL,
    failure_reason text,
    admin_operator_id character varying(64),
    admin_note text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: refund_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.refund_requests (
    id character varying(36) NOT NULL,
    refund_no character varying(64) NOT NULL,
    wallet_id character varying(36) NOT NULL,
    user_id character varying(36),
    payment_order_id character varying(36),
    source_type character varying(30) NOT NULL,
    source_id character varying(100),
    refund_mode character varying(30) NOT NULL,
    amount_usd numeric(20,8) NOT NULL,
    status character varying(30) DEFAULT 'pending_approval'::character varying NOT NULL,
    reason text,
    requested_by character varying(36),
    approved_by character varying(36),
    processed_by character varying(36),
    gateway_refund_id character varying(128),
    payout_method character varying(50),
    payout_reference character varying(255),
    payout_proof jsonb,
    failure_reason text,
    idempotency_key character varying(128),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    processed_at timestamp with time zone,
    completed_at timestamp with time zone
);


--
-- Name: request_candidates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.request_candidates (
    id character varying(36) NOT NULL,
    request_id character varying(100) NOT NULL,
    user_id character varying(36),
    api_key_id character varying(36),
    candidate_index integer NOT NULL,
    retry_index integer DEFAULT 0 NOT NULL,
    provider_id character varying(36),
    endpoint_id character varying(36),
    key_id character varying(36),
    status character varying(20) NOT NULL,
    skip_reason text,
    is_cached boolean DEFAULT false,
    status_code integer,
    error_type character varying(50),
    error_message text,
    latency_ms integer,
    concurrent_requests integer,
    extra_data jsonb,
    required_capabilities jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    username character varying(100),
    api_key_name character varying(200)
);


--
-- Name: routing_group_bindings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routing_group_bindings (
    id character varying(64) NOT NULL,
    group_id character varying(64) NOT NULL,
    subject_type character varying(32) NOT NULL,
    subject_id character varying(64) NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    allow_explicit_select boolean DEFAULT true NOT NULL,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);


--
-- Name: routing_group_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routing_group_versions (
    id character varying(64) NOT NULL,
    group_id character varying(64) NOT NULL,
    version bigint NOT NULL,
    config_json jsonb NOT NULL,
    created_at bigint NOT NULL,
    created_by character varying(64)
);


--
-- Name: routing_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.routing_groups (
    id character varying(64) NOT NULL,
    name character varying(255) NOT NULL,
    description text,
    enabled boolean DEFAULT true NOT NULL,
    is_system_default boolean DEFAULT false NOT NULL,
    config_json jsonb NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    published_at bigint,
    sort_order bigint DEFAULT 0 NOT NULL
);


--
-- Name: schema_backfills; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_backfills (
    version bigint NOT NULL,
    description text NOT NULL,
    success boolean DEFAULT true NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint DEFAULT 0 NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_bucket_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_bucket_state (
    projection_version text NOT NULL,
    granularity text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    source_revision bigint DEFAULT 0 NOT NULL,
    built_revision bigint DEFAULT '-1'::integer NOT NULL,
    coverage_status text DEFAULT 'unbuilt'::text NOT NULL,
    built_at timestamp with time zone,
    last_error text,
    last_failed_at timestamp with time zone
);


--
-- Name: stats_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    success_requests integer DEFAULT 0 NOT NULL,
    error_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    actual_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    input_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    output_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    avg_response_time_ms double precision DEFAULT '0'::double precision NOT NULL,
    fallback_count integer DEFAULT 0 NOT NULL,
    unique_models integer DEFAULT 0 NOT NULL,
    unique_providers integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    is_complete boolean DEFAULT false NOT NULL,
    aggregated_at timestamp with time zone,
    p50_response_time_ms integer,
    p90_response_time_ms integer,
    p99_response_time_ms integer,
    p50_first_byte_time_ms integer,
    p90_first_byte_time_ms integer,
    p99_first_byte_time_ms integer,
    effective_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_input_context bigint DEFAULT '0'::bigint NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_hit_total_requests bigint DEFAULT 0 NOT NULL,
    cache_hit_requests bigint DEFAULT 0 NOT NULL,
    completed_total_requests bigint DEFAULT 0 NOT NULL,
    completed_cache_hit_requests bigint DEFAULT 0 NOT NULL,
    completed_input_tokens bigint DEFAULT 0 NOT NULL,
    completed_cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    completed_cache_read_tokens bigint DEFAULT 0 NOT NULL,
    completed_total_input_context bigint DEFAULT 0 NOT NULL,
    completed_cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    completed_cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    settled_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    settled_total_requests bigint DEFAULT 0 NOT NULL,
    settled_input_tokens bigint DEFAULT 0 NOT NULL,
    settled_output_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_read_tokens bigint DEFAULT 0 NOT NULL,
    settled_first_finalized_at_unix_secs bigint,
    settled_last_finalized_at_unix_secs bigint,
    billing_cost numeric(20,8)
);


--
-- Name: stats_daily_api_key; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_api_key (
    id character varying(36) NOT NULL,
    api_key_id character varying(36),
    date timestamp with time zone NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    success_requests integer DEFAULT 0 NOT NULL,
    error_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    api_key_name character varying(200)
);


--
-- Name: stats_daily_cost_savings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_cost_savings (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_daily_cost_savings_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_cost_savings_model (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_daily_cost_savings_model_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_cost_savings_model_provider (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    provider_name character varying(100) NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_daily_cost_savings_model_provi_cache_read_tokens_not_null NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_daily_cost_savings_model_provide_cache_read_cost_not_null NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_daily_cost_savings_model_pro_cache_creation_cost_not_null NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_daily_cost_savings_model_pro_estimated_full_cost_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_daily_cost_savings_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_cost_savings_provider (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    provider_name character varying(100) NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_daily_error; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_error (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    error_category character varying(50) NOT NULL,
    provider_name character varying(100),
    model character varying(100),
    count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: stats_daily_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_model (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    total_requests integer NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    cache_creation_tokens bigint NOT NULL,
    cache_read_tokens bigint NOT NULL,
    total_cost numeric(20,8) NOT NULL,
    avg_response_time_ms double precision NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint NOT NULL
);


--
-- Name: stats_daily_model_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_model_provider (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    provider_name character varying(100) NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    total_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_daily_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_daily_provider (
    id character varying(36) NOT NULL,
    date timestamp with time zone NOT NULL,
    provider_name character varying(100) NOT NULL,
    total_requests integer NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    cache_creation_tokens bigint NOT NULL,
    cache_read_tokens bigint NOT NULL,
    total_cost numeric(20,8) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: stats_hourly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_hourly (
    id character varying(36) NOT NULL,
    hour_utc timestamp with time zone NOT NULL,
    total_requests integer NOT NULL,
    success_requests integer NOT NULL,
    error_requests integer NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    cache_creation_tokens bigint NOT NULL,
    cache_read_tokens bigint NOT NULL,
    total_cost numeric(20,8) NOT NULL,
    actual_total_cost numeric(20,8) NOT NULL,
    avg_response_time_ms double precision NOT NULL,
    is_complete boolean NOT NULL,
    aggregated_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    cache_hit_total_requests bigint DEFAULT 0 NOT NULL,
    cache_hit_requests bigint DEFAULT 0 NOT NULL,
    completed_total_requests bigint DEFAULT 0 NOT NULL,
    completed_cache_hit_requests bigint DEFAULT 0 NOT NULL,
    completed_input_tokens bigint DEFAULT 0 NOT NULL,
    completed_cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    completed_cache_read_tokens bigint DEFAULT 0 NOT NULL,
    completed_total_input_context bigint DEFAULT 0 NOT NULL,
    completed_cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    completed_cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    settled_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    settled_total_requests bigint DEFAULT 0 NOT NULL,
    settled_input_tokens bigint DEFAULT 0 NOT NULL,
    settled_output_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_read_tokens bigint DEFAULT 0 NOT NULL,
    settled_first_finalized_at_unix_secs bigint,
    settled_last_finalized_at_unix_secs bigint
);


--
-- Name: stats_hourly_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_hourly_model (
    id character varying(36) NOT NULL,
    hour_utc timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    total_requests integer NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    total_cost numeric(20,8) NOT NULL,
    avg_response_time_ms double precision NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL
);


--
-- Name: stats_hourly_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_hourly_provider (
    id character varying(36) NOT NULL,
    hour_utc timestamp with time zone NOT NULL,
    provider_name character varying(100) NOT NULL,
    total_requests integer NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    total_cost numeric(20,8) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: stats_hourly_user; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_hourly_user (
    id character varying(36) NOT NULL,
    hour_utc timestamp with time zone NOT NULL,
    user_id character varying(36) NOT NULL,
    total_requests integer NOT NULL,
    success_requests integer NOT NULL,
    error_requests integer NOT NULL,
    input_tokens bigint NOT NULL,
    output_tokens bigint NOT NULL,
    total_cost numeric(20,8) NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    actual_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    settled_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    settled_total_requests bigint DEFAULT 0 NOT NULL,
    settled_input_tokens bigint DEFAULT 0 NOT NULL,
    settled_output_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_read_tokens bigint DEFAULT 0 NOT NULL,
    settled_first_finalized_at_unix_secs bigint,
    settled_last_finalized_at_unix_secs bigint
);


--
-- Name: stats_hourly_user_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_hourly_user_model (
    id character varying(36) NOT NULL,
    hour_utc timestamp with time zone NOT NULL,
    user_id character varying(36) NOT NULL,
    model character varying(100) NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_overview_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_overview_daily (
    projection_version text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    dimensions jsonb NOT NULL,
    metrics jsonb NOT NULL
);


--
-- Name: stats_overview_dirty_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_overview_dirty_events (
    transaction_id bigint NOT NULL,
    projection_version text NOT NULL,
    granularity text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    unrecoverable boolean DEFAULT false NOT NULL
);


--
-- Name: stats_overview_hourly; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_overview_hourly (
    projection_version text NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    dimensions jsonb NOT NULL,
    metrics jsonb NOT NULL
);


--
-- Name: stats_summary; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_summary (
    id character varying(36) NOT NULL,
    cutoff_date timestamp with time zone NOT NULL,
    all_time_requests integer DEFAULT 0 NOT NULL,
    all_time_success_requests integer DEFAULT 0 NOT NULL,
    all_time_error_requests integer DEFAULT 0 NOT NULL,
    all_time_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    all_time_actual_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    total_users integer DEFAULT 0 NOT NULL,
    active_users integer DEFAULT 0 NOT NULL,
    total_api_keys integer DEFAULT 0 NOT NULL,
    active_api_keys integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_daily; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily (
    id character varying(36) NOT NULL,
    user_id character varying(36),
    date timestamp with time zone NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    success_requests integer DEFAULT 0 NOT NULL,
    error_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    username character varying(100),
    actual_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    effective_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_input_context bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint NOT NULL,
    settled_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    settled_total_requests bigint DEFAULT 0 NOT NULL,
    settled_input_tokens bigint DEFAULT 0 NOT NULL,
    settled_output_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_creation_tokens bigint DEFAULT 0 NOT NULL,
    settled_cache_read_tokens bigint DEFAULT 0 NOT NULL,
    settled_first_finalized_at_unix_secs bigint,
    settled_last_finalized_at_unix_secs bigint
);


--
-- Name: stats_user_daily_api_format; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_api_format (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    api_format character varying(50) NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    success_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    effective_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_input_context bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_api_format_cache_creation_ephemeral_5_not_null NOT NULL,
    cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_api_format_cache_creation_ephemeral_1_not_null NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    actual_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    successful_response_time_sum_ms double precision DEFAULT '0'::double precision CONSTRAINT stats_user_daily_api_format_successful_response_time_s_not_null NOT NULL,
    successful_response_time_samples bigint DEFAULT 0 CONSTRAINT stats_user_daily_api_forma_successful_response_time_s_not_null1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_daily_cost_savings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_cost_savings (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_daily_cost_savings_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_cost_savings_model (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_mode_cache_creation_cost_not_null NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_mode_estimated_full_cost_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_daily_cost_savings_model_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_cost_savings_model_provider (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    provider_name character varying(100) CONSTRAINT stats_user_daily_cost_savings_model_prov_provider_name_not_null NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_cost_savings_model__cache_read_tokens_not_null NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_model_pr_cache_read_cost_not_null NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_mod_cache_creation_cost_not_null1 NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_mod_estimated_full_cost_not_null1 NOT NULL,
    created_at timestamp with time zone DEFAULT now() CONSTRAINT stats_user_daily_cost_savings_model_provide_created_at_not_null NOT NULL,
    updated_at timestamp with time zone DEFAULT now() CONSTRAINT stats_user_daily_cost_savings_model_provide_updated_at_not_null NOT NULL
);


--
-- Name: stats_user_daily_cost_savings_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_cost_savings_provider (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    provider_name character varying(100) NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_cost_savings_provid_cache_read_tokens_not_null NOT NULL,
    cache_read_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    cache_creation_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_prov_cache_creation_cost_not_null NOT NULL,
    estimated_full_cost numeric(20,8) DEFAULT '0'::double precision CONSTRAINT stats_user_daily_cost_savings_prov_estimated_full_cost_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_daily_model; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_model (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    success_requests integer DEFAULT 0 NOT NULL,
    effective_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_input_context bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_model_cache_creation_ephemeral_5m_tok_not_null NOT NULL,
    cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_model_cache_creation_ephemeral_1h_tok_not_null NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    actual_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    successful_response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    successful_response_time_samples bigint DEFAULT 0 CONSTRAINT stats_user_daily_model_successful_response_time_sample_not_null NOT NULL
);


--
-- Name: stats_user_daily_model_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_model_provider (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    model character varying(100) NOT NULL,
    provider_name character varying(100) NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    total_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_daily_provider; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_daily_provider (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    date timestamp with time zone NOT NULL,
    provider_name character varying(100) NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    success_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    effective_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_input_context bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_provider_cache_creation_ephemeral_5m__not_null NOT NULL,
    cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint CONSTRAINT stats_user_daily_provider_cache_creation_ephemeral_1h__not_null NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    actual_total_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    response_time_sum_ms double precision DEFAULT '0'::double precision NOT NULL,
    response_time_samples bigint DEFAULT 0 NOT NULL,
    successful_response_time_sum_ms double precision DEFAULT '0'::double precision CONSTRAINT stats_user_daily_provider_successful_response_time_sum_not_null NOT NULL,
    successful_response_time_samples bigint DEFAULT 0 CONSTRAINT stats_user_daily_provider_successful_response_time_sam_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: stats_user_summary; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stats_user_summary (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    username character varying(100),
    cutoff_date timestamp with time zone NOT NULL,
    all_time_requests integer DEFAULT 0 NOT NULL,
    all_time_success_requests integer DEFAULT 0 NOT NULL,
    all_time_error_requests integer DEFAULT 0 NOT NULL,
    all_time_input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    all_time_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    all_time_actual_cost numeric(20,8) DEFAULT '0'::double precision NOT NULL,
    active_days integer DEFAULT 0 NOT NULL,
    first_active_date timestamp with time zone,
    last_active_date timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: system_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.system_configs (
    id character varying(36) NOT NULL,
    key character varying(100) NOT NULL,
    value jsonb NOT NULL,
    description text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: usage; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage (
    id character varying(36) NOT NULL,
    user_id character varying(36),
    api_key_id character varying(36),
    request_id character varying(100) NOT NULL,
    provider_name character varying(100) NOT NULL,
    model character varying(100) NOT NULL,
    target_model character varying(100),
    provider_id character varying(36),
    provider_endpoint_id character varying(36),
    provider_api_key_id character varying(36),
    input_tokens integer DEFAULT 0,
    output_tokens integer DEFAULT 0,
    input_output_total_tokens integer DEFAULT 0,
    cache_creation_input_tokens integer DEFAULT 0,
    cache_read_input_tokens integer DEFAULT 0,
    input_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    output_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    cache_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    cache_creation_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    cache_read_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    request_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    total_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    actual_input_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    actual_output_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    actual_cache_creation_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    actual_cache_read_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    actual_request_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    actual_total_cost_usd numeric(20,8) DEFAULT '0'::double precision,
    rate_multiplier numeric(10,6) DEFAULT '1'::double precision,
    input_price_per_1m numeric(20,8),
    output_price_per_1m numeric(20,8),
    cache_creation_price_per_1m numeric(20,8),
    cache_read_price_per_1m numeric(20,8),
    price_per_request numeric(20,8),
    request_type character varying(50),
    api_format character varying(50),
    is_stream boolean DEFAULT false,
    status_code integer,
    error_message text,
    response_time_ms integer,
    status character varying(20) DEFAULT 'completed'::character varying NOT NULL,
    request_headers jsonb,
    provider_request_headers jsonb,
    response_headers jsonb,
    request_metadata jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    first_byte_time_ms integer,
    client_response_headers jsonb,
    endpoint_api_format character varying(50),
    has_format_conversion boolean DEFAULT false,
    billing_status character varying(20) DEFAULT 'pending'::character varying NOT NULL,
    finalized_at timestamp with time zone,
    error_category character varying(50),
    api_family character varying(50),
    endpoint_kind character varying(50),
    provider_api_family character varying(50),
    provider_endpoint_kind character varying(50),
    cache_creation_input_tokens_5m integer DEFAULT 0 NOT NULL,
    cache_creation_input_tokens_1h integer DEFAULT 0 NOT NULL,
    cache_creation_ephemeral_5m_input_tokens integer DEFAULT 0 NOT NULL,
    cache_creation_ephemeral_1h_input_tokens integer DEFAULT 0 NOT NULL,
    wallet_id character varying(36),
    wallet_balance_before numeric(20,8),
    wallet_balance_after numeric(20,8),
    wallet_recharge_balance_before numeric(20,8),
    wallet_recharge_balance_after numeric(20,8),
    wallet_gift_balance_before numeric(20,8),
    wallet_gift_balance_after numeric(20,8),
    username character varying(100),
    api_key_name character varying(200),
    input_context_tokens integer DEFAULT 0 NOT NULL,
    total_tokens integer DEFAULT 0 NOT NULL,
    cache_creation_cost_usd_5m numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    cache_creation_cost_usd_1h numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    actual_cache_creation_cost_usd_5m numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    actual_cache_creation_cost_usd_1h numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    actual_cache_cost_usd numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    cache_creation_price_per_1m_5m numeric(20,8),
    cache_creation_price_per_1m_1h numeric(20,8),
    created_at_unix_ms bigint DEFAULT 0 NOT NULL,
    updated_at_unix_secs bigint DEFAULT 0 NOT NULL,
    upstream_is_stream boolean,
    failure_origin text,
    failure_stage text,
    failure_reason text,
    failure_schema_version integer
)
WITH (autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000');


--
-- Name: COLUMN usage.input_tokens; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.input_tokens IS 'DEPRECATED: billing dimension mirror. Use public.usage_settlement_snapshots.billing_input_tokens or public.usage_billing_facts.input_tokens.';


--
-- Name: COLUMN usage.output_tokens; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.output_tokens IS 'DEPRECATED: billing dimension mirror. Use public.usage_settlement_snapshots.billing_output_tokens or public.usage_billing_facts.output_tokens.';


--
-- Name: COLUMN usage.cache_creation_input_tokens; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_creation_input_tokens IS 'DEPRECATED: billing dimension mirror. Use public.usage_settlement_snapshots.billing_cache_creation_tokens or public.usage_billing_facts.cache_creation_input_tokens.';


--
-- Name: COLUMN usage.cache_read_input_tokens; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_read_input_tokens IS 'DEPRECATED: billing dimension mirror. Use public.usage_settlement_snapshots.billing_cache_read_tokens or public.usage_billing_facts.cache_read_input_tokens.';


--
-- Name: COLUMN usage.cache_creation_cost_usd; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_creation_cost_usd IS 'DEPRECATED: billing cost mirror. Use public.usage_settlement_snapshots.billing_cache_creation_cost_usd or public.usage_billing_facts.cache_creation_cost_usd.';


--
-- Name: COLUMN usage.cache_read_cost_usd; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_read_cost_usd IS 'DEPRECATED: billing cost mirror. Use public.usage_settlement_snapshots.billing_cache_read_cost_usd or public.usage_billing_facts.cache_read_cost_usd.';


--
-- Name: COLUMN usage.total_cost_usd; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.total_cost_usd IS 'DEPRECATED: billing cost mirror. Use public.usage_settlement_snapshots.billing_total_cost_usd or public.usage_billing_facts.total_cost_usd.';


--
-- Name: COLUMN usage.actual_total_cost_usd; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.actual_total_cost_usd IS 'DEPRECATED: billing cost mirror. Use public.usage_settlement_snapshots.billing_actual_total_cost_usd or public.usage_billing_facts.actual_total_cost_usd.';


--
-- Name: COLUMN usage.rate_multiplier; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.rate_multiplier IS 'DEPRECATED: settlement pricing owner moved to public.usage_settlement_snapshots.rate_multiplier. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.input_price_per_1m; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.input_price_per_1m IS 'DEPRECATED: settlement pricing owner moved to public.usage_settlement_snapshots.input_price_per_1m. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.output_price_per_1m; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.output_price_per_1m IS 'DEPRECATED: settlement pricing owner moved to public.usage_settlement_snapshots.output_price_per_1m. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.cache_creation_price_per_1m; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_creation_price_per_1m IS 'DEPRECATED: settlement pricing owner moved to public.usage_settlement_snapshots.cache_creation_price_per_1m. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.cache_read_price_per_1m; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_read_price_per_1m IS 'DEPRECATED: settlement pricing owner moved to public.usage_settlement_snapshots.cache_read_price_per_1m. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.price_per_request; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.price_per_request IS 'DEPRECATED: settlement pricing owner moved to public.usage_settlement_snapshots.price_per_request. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.request_headers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.request_headers IS 'DEPRECATED: HTTP audit owner moved to public.usage_http_headers.client_request. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.provider_request_headers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.provider_request_headers IS 'DEPRECATED: HTTP audit owner moved to public.usage_http_headers.provider_request. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.response_headers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.response_headers IS 'DEPRECATED: HTTP audit owner moved to public.usage_http_headers.provider_response. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.client_response_headers; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.client_response_headers IS 'DEPRECATED: HTTP audit owner moved to public.usage_http_headers.client_response. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.billing_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.billing_status IS 'DEPRECATED: authoritative owner moved to public.usage_settlement_snapshots.billing_status. Compatibility/index mirror only; do not write new values.';


--
-- Name: COLUMN usage.finalized_at; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.finalized_at IS 'DEPRECATED: authoritative owner moved to public.usage_settlement_snapshots.finalized_at. Compatibility/index mirror only; do not write new values.';


--
-- Name: COLUMN usage.cache_creation_input_tokens_5m; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_creation_input_tokens_5m IS 'DEPRECATED: billing dimension mirror. Use public.usage_settlement_snapshots.billing_cache_creation_5m_tokens or public.usage_billing_facts.cache_creation_input_tokens_5m.';


--
-- Name: COLUMN usage.cache_creation_input_tokens_1h; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.cache_creation_input_tokens_1h IS 'DEPRECATED: billing dimension mirror. Use public.usage_settlement_snapshots.billing_cache_creation_1h_tokens or public.usage_billing_facts.cache_creation_input_tokens_1h.';


--
-- Name: COLUMN usage.wallet_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_id IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_id. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.wallet_balance_before; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_balance_before IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_balance_before. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.wallet_balance_after; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_balance_after IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_balance_after. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.wallet_recharge_balance_before; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_recharge_balance_before IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_recharge_balance_before. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.wallet_recharge_balance_after; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_recharge_balance_after IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_recharge_balance_after. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.wallet_gift_balance_before; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_gift_balance_before IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_gift_balance_before. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.wallet_gift_balance_after; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.wallet_gift_balance_after IS 'DEPRECATED: settlement owner moved to public.usage_settlement_snapshots.wallet_gift_balance_after. Legacy compatibility only; do not write new values.';


--
-- Name: COLUMN usage.username; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.username IS 'DEPRECATED: display cache only. Prefer join-time lookup from user/auth records. Legacy compatibility only.';


--
-- Name: COLUMN usage.api_key_name; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.api_key_name IS 'DEPRECATED: display cache only. Prefer join-time lookup from API key records. Legacy compatibility only.';


--
-- Name: COLUMN usage.total_tokens; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.total_tokens IS 'DEPRECATED: billing dimension mirror. Use public.usage_billing_facts.total_tokens.';


--
-- Name: COLUMN usage.upstream_is_stream; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage.upstream_is_stream IS 'Resolved upstream stream mode from request_metadata.upstream_is_stream, falling back to is_stream for legacy rows.';


--
-- Name: usage_attribution_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_attribution_snapshots (
    request_id text NOT NULL,
    actor_user_id text,
    credential_owner_id text,
    attribution_kind text DEFAULT 'unknown'::text NOT NULL,
    attribution_source text DEFAULT 'unknown'::text NOT NULL,
    record_kind text DEFAULT 'request'::text NOT NULL,
    parent_request_id text,
    schema_version integer DEFAULT 1 NOT NULL,
    attribution_revision bigint DEFAULT 1 NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: usage_settlement_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_settlement_snapshots (
    request_id character varying(100) NOT NULL,
    billing_status character varying(20) NOT NULL,
    wallet_id character varying(36),
    wallet_balance_before numeric(20,8),
    wallet_balance_after numeric(20,8),
    wallet_recharge_balance_before numeric(20,8),
    wallet_recharge_balance_after numeric(20,8),
    wallet_gift_balance_before numeric(20,8),
    wallet_gift_balance_after numeric(20,8),
    provider_monthly_used_usd numeric(20,8),
    finalized_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    billing_snapshot_schema_version character varying(20),
    billing_snapshot_status character varying(20),
    rate_multiplier numeric(10,6),
    is_free_tier boolean,
    input_price_per_1m numeric(20,8),
    output_price_per_1m numeric(20,8),
    cache_creation_price_per_1m numeric(20,8),
    cache_read_price_per_1m numeric(20,8),
    price_per_request numeric(20,8),
    settlement_snapshot_schema_version character varying(20),
    settlement_snapshot jsonb,
    billing_dimensions jsonb,
    billing_input_tokens bigint,
    billing_effective_input_tokens bigint,
    billing_output_tokens bigint,
    billing_cache_creation_tokens bigint,
    billing_cache_creation_5m_tokens bigint,
    billing_cache_creation_1h_tokens bigint,
    billing_cache_read_tokens bigint,
    billing_total_input_context bigint,
    billing_cache_creation_cost_usd numeric(20,8),
    billing_cache_read_cost_usd numeric(20,8),
    billing_total_cost_usd numeric(20,8),
    billing_actual_total_cost_usd numeric(20,8),
    billing_pricing_source character varying(50),
    billing_rule_id character varying(100),
    billing_rule_version character varying(50),
    quota_covered_amount_usd numeric(20,8),
    wallet_consumed_amount_usd numeric(20,8),
    wallet_debit_amount_usd numeric(20,8),
    wallet_recharge_debit_usd numeric(20,8),
    wallet_gift_debit_usd numeric(20,8),
    wallet_overdraft_usd numeric(20,8),
    allocation_schema_version integer,
    allocation_status text
)
WITH (autovacuum_analyze_scale_factor='0.02', autovacuum_analyze_threshold='10000');


--
-- Name: usage_billing_facts; Type: VIEW; Schema: public; Owner: -
--

CREATE OR REPLACE VIEW public.usage_billing_facts AS
 SELECT usage_rows.id,
    usage_rows.request_id,
    usage_rows.user_id,
    usage_rows.api_key_id,
    usage_rows.username,
    usage_rows.api_key_name,
    usage_rows.provider_name,
    usage_rows.model,
    usage_rows.target_model,
    usage_rows.provider_id,
    usage_rows.provider_endpoint_id,
    usage_rows.provider_api_key_id,
    usage_rows.request_type,
    usage_rows.api_format,
    usage_rows.api_family,
    usage_rows.endpoint_kind,
    usage_rows.endpoint_api_format,
    usage_rows.provider_api_family,
    usage_rows.provider_endpoint_kind,
    COALESCE(usage_rows.has_format_conversion, false) AS has_format_conversion,
    COALESCE(usage_rows.is_stream, false) AS is_stream,
    usage_rows.status_code,
    usage_rows.error_message,
    usage_rows.error_category,
    usage_rows.response_time_ms,
    usage_rows.first_byte_time_ms,
    usage_rows.status,
    COALESCE(settlement.billing_status, usage_rows.billing_status) AS billing_status,
    usage_rows.created_at,
    COALESCE(settlement.finalized_at, usage_rows.finalized_at) AS finalized_at,
    GREATEST(COALESCE(settlement.billing_input_tokens, (usage_rows.input_tokens)::bigint, (0)::bigint), (0)::bigint) AS input_tokens,
    GREATEST(COALESCE(settlement.billing_effective_input_tokens, (
        CASE
            WHEN (GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) <= 0) THEN 0
            WHEN ((split_part(lower((COALESCE(COALESCE(usage_rows.endpoint_api_format, usage_rows.api_format), ''::character varying))::text), ':'::text, 1) = 'openai'::text) AND ((GREATEST(COALESCE(usage_rows.cache_creation_input_tokens, 0), 0) > 0) OR (GREATEST(COALESCE(usage_rows.cache_creation_input_tokens_5m, 0), 0) > 0) OR (GREATEST(COALESCE(usage_rows.cache_creation_input_tokens_1h, 0), 0) > 0) OR (GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0) > 0))) THEN GREATEST(((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) - GREATEST(
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END, 0)) - GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0)), 0)
            WHEN ((split_part(lower((COALESCE(COALESCE(usage_rows.endpoint_api_format, usage_rows.api_format), ''::character varying))::text), ':'::text, 1) = ANY (ARRAY['gemini'::text, 'google'::text])) AND (GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0) > 0)) THEN GREATEST((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) - GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0)), 0)
            ELSE GREATEST(COALESCE(usage_rows.input_tokens, 0), 0)
        END)::bigint), (0)::bigint) AS effective_input_tokens,
    GREATEST(COALESCE(settlement.billing_output_tokens, (usage_rows.output_tokens)::bigint, (0)::bigint), (0)::bigint) AS output_tokens,
    GREATEST(COALESCE(settlement.billing_cache_creation_tokens,
        CASE
            WHEN ((settlement.billing_cache_creation_5m_tokens IS NOT NULL) OR (settlement.billing_cache_creation_1h_tokens IS NOT NULL)) THEN (COALESCE(settlement.billing_cache_creation_5m_tokens, (0)::bigint) + COALESCE(settlement.billing_cache_creation_1h_tokens, (0)::bigint))
            ELSE NULL::bigint
        END, (
        CASE
            WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
            ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
        END)::bigint, (0)::bigint), (0)::bigint) AS cache_creation_input_tokens,
    GREATEST(COALESCE(settlement.billing_cache_creation_5m_tokens, (usage_rows.cache_creation_input_tokens_5m)::bigint, (0)::bigint), (0)::bigint) AS cache_creation_input_tokens_5m,
    GREATEST(COALESCE(settlement.billing_cache_creation_1h_tokens, (usage_rows.cache_creation_input_tokens_1h)::bigint, (0)::bigint), (0)::bigint) AS cache_creation_input_tokens_1h,
    GREATEST(COALESCE(settlement.billing_cache_read_tokens, (usage_rows.cache_read_input_tokens)::bigint, (0)::bigint), (0)::bigint) AS cache_read_input_tokens,
    GREATEST(COALESCE(
        CASE
            WHEN (settlement.billing_effective_input_tokens IS NOT NULL) THEN (((GREATEST(settlement.billing_effective_input_tokens, (0)::bigint) + GREATEST(COALESCE(settlement.billing_output_tokens, (usage_rows.output_tokens)::bigint, (0)::bigint), (0)::bigint)) + GREATEST(COALESCE(settlement.billing_cache_creation_tokens,
            CASE
                WHEN ((settlement.billing_cache_creation_5m_tokens IS NOT NULL) OR (settlement.billing_cache_creation_1h_tokens IS NOT NULL)) THEN (COALESCE(settlement.billing_cache_creation_5m_tokens, (0)::bigint) + COALESCE(settlement.billing_cache_creation_1h_tokens, (0)::bigint))
                ELSE NULL::bigint
            END, (
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END)::bigint, (0)::bigint), (0)::bigint)) + GREATEST(COALESCE(settlement.billing_cache_read_tokens, (usage_rows.cache_read_input_tokens)::bigint, (0)::bigint), (0)::bigint))
            WHEN (settlement.billing_total_input_context IS NOT NULL) THEN (GREATEST(settlement.billing_total_input_context, (0)::bigint) + GREATEST(COALESCE(settlement.billing_output_tokens, (usage_rows.output_tokens)::bigint, (0)::bigint), (0)::bigint))
            ELSE NULL::bigint
        END, (NULLIF(GREATEST(COALESCE(usage_rows.total_tokens, 0), 0), 0))::bigint, (
        CASE
            WHEN (split_part(lower((COALESCE(COALESCE(usage_rows.endpoint_api_format, usage_rows.api_format), ''::character varying))::text), ':'::text, 1) = ANY (ARRAY['openai'::text, 'gemini'::text, 'google'::text])) THEN (GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) + GREATEST(COALESCE(usage_rows.output_tokens, 0), 0))
            ELSE (((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) + GREATEST(COALESCE(usage_rows.output_tokens, 0), 0)) + GREATEST(
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END, 0)) + GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0))
        END)::bigint, (0)::bigint), (0)::bigint) AS total_tokens,
    GREATEST(COALESCE(
        CASE
            WHEN (settlement.billing_effective_input_tokens IS NOT NULL) THEN ((GREATEST(settlement.billing_effective_input_tokens, (0)::bigint) + GREATEST(COALESCE(settlement.billing_cache_creation_tokens,
            CASE
                WHEN ((settlement.billing_cache_creation_5m_tokens IS NOT NULL) OR (settlement.billing_cache_creation_1h_tokens IS NOT NULL)) THEN (COALESCE(settlement.billing_cache_creation_5m_tokens, (0)::bigint) + COALESCE(settlement.billing_cache_creation_1h_tokens, (0)::bigint))
                ELSE NULL::bigint
            END, (
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END)::bigint, (0)::bigint), (0)::bigint)) + GREATEST(COALESCE(settlement.billing_cache_read_tokens, (usage_rows.cache_read_input_tokens)::bigint, (0)::bigint), (0)::bigint))
            ELSE NULL::bigint
        END, settlement.billing_total_input_context, (
        CASE
            WHEN (split_part(lower((COALESCE(COALESCE(usage_rows.endpoint_api_format, usage_rows.api_format), ''::character varying))::text), ':'::text, 1) = ANY (ARRAY['claude'::text, 'anthropic'::text])) THEN ((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) +
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END) + GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0))
            WHEN (split_part(lower((COALESCE(COALESCE(usage_rows.endpoint_api_format, usage_rows.api_format), ''::character varying))::text), ':'::text, 1) = 'openai'::text) THEN ((
            CASE
                WHEN (GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) <= 0) THEN 0
                WHEN ((GREATEST(COALESCE(usage_rows.cache_creation_input_tokens, 0), 0) <= 0) AND (GREATEST(COALESCE(usage_rows.cache_creation_input_tokens_5m, 0), 0) <= 0) AND (GREATEST(COALESCE(usage_rows.cache_creation_input_tokens_1h, 0), 0) <= 0) AND (GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0) <= 0)) THEN GREATEST(COALESCE(usage_rows.input_tokens, 0), 0)
                ELSE GREATEST(((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) - GREATEST(
                CASE
                    WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                    ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
                END, 0)) - GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0)), 0)
            END + GREATEST(
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END, 0)) + GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0))
            WHEN (split_part(lower((COALESCE(COALESCE(usage_rows.endpoint_api_format, usage_rows.api_format), ''::character varying))::text), ':'::text, 1) = ANY (ARRAY['gemini'::text, 'google'::text])) THEN (
            CASE
                WHEN (GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) <= 0) THEN 0
                WHEN (GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0) <= 0) THEN GREATEST(COALESCE(usage_rows.input_tokens, 0), 0)
                ELSE GREATEST((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) - GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0)), 0)
            END + GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0))
            ELSE ((GREATEST(COALESCE(usage_rows.input_tokens, 0), 0) +
            CASE
                WHEN ((COALESCE(usage_rows.cache_creation_input_tokens, 0) = 0) AND ((COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0)) > 0)) THEN (COALESCE(usage_rows.cache_creation_input_tokens_5m, 0) + COALESCE(usage_rows.cache_creation_input_tokens_1h, 0))
                ELSE COALESCE(usage_rows.cache_creation_input_tokens, 0)
            END) + GREATEST(COALESCE(usage_rows.cache_read_input_tokens, 0), 0))
        END)::bigint, (0)::bigint), (0)::bigint) AS total_input_context,
    COALESCE((usage_rows.input_cost_usd)::double precision, (0)::double precision) AS input_cost_usd,
    COALESCE((usage_rows.output_cost_usd)::double precision, (0)::double precision) AS output_cost_usd,
    COALESCE((settlement.billing_cache_creation_cost_usd)::double precision, (usage_rows.cache_creation_cost_usd)::double precision, (0)::double precision) AS cache_creation_cost_usd,
    COALESCE((settlement.billing_cache_read_cost_usd)::double precision, (usage_rows.cache_read_cost_usd)::double precision, (0)::double precision) AS cache_read_cost_usd,
    COALESCE((settlement.billing_total_cost_usd)::double precision, (usage_rows.total_cost_usd)::double precision, (0)::double precision) AS total_cost_usd,
    COALESCE((settlement.billing_actual_total_cost_usd)::double precision, (usage_rows.actual_total_cost_usd)::double precision, (0)::double precision) AS actual_total_cost_usd,
    COALESCE((settlement.output_price_per_1m)::double precision, (usage_rows.output_price_per_1m)::double precision) AS output_price_per_1m,
    COALESCE((settlement.input_price_per_1m)::double precision, (usage_rows.input_price_per_1m)::double precision) AS input_price_per_1m,
    COALESCE((settlement.cache_creation_price_per_1m)::double precision, (usage_rows.cache_creation_price_per_1m)::double precision) AS cache_creation_price_per_1m,
    COALESCE((settlement.cache_read_price_per_1m)::double precision, (usage_rows.cache_read_price_per_1m)::double precision) AS cache_read_price_per_1m,
    COALESCE((settlement.price_per_request)::double precision, (usage_rows.price_per_request)::double precision) AS price_per_request,
    settlement.billing_pricing_source,
    settlement.billing_rule_id,
    settlement.billing_rule_version,
    COALESCE(usage_rows.upstream_is_stream, COALESCE(usage_rows.is_stream, false)) AS upstream_is_stream
   FROM (public.usage usage_rows
     LEFT JOIN public.usage_settlement_snapshots settlement ON (((settlement.request_id)::text = (usage_rows.request_id)::text)));


--
-- Name: VIEW usage_billing_facts; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON VIEW public.usage_billing_facts IS 'Canonical billing read model. Token/cost fields prefer usage_settlement_snapshots.billing_* and fall back to deprecated usage mirrors for legacy rows.';


--
-- Name: COLUMN usage_billing_facts.upstream_is_stream; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.usage_billing_facts.upstream_is_stream IS 'Resolved upstream stream mode from public.usage.upstream_is_stream, falling back to usage.is_stream for legacy rows.';


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    id character varying(36) NOT NULL,
    external_id character varying(255),
    email character varying(255),
    username character varying(100) NOT NULL,
    password_hash character varying(255),
    role public.userrole DEFAULT 'user'::public.userrole NOT NULL,
    allowed_providers jsonb,
    allowed_api_formats jsonb,
    allowed_models jsonb,
    model_capability_settings jsonb,
    is_active boolean DEFAULT true NOT NULL,
    is_deleted boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_login_at timestamp with time zone,
    auth_source public.authsource DEFAULT 'local'::public.authsource NOT NULL,
    ldap_dn character varying(512),
    ldap_username character varying(255),
    email_verified boolean NOT NULL,
    rate_limit integer,
    metadata jsonb,
    allowed_providers_mode text DEFAULT 'unrestricted'::text NOT NULL,
    allowed_api_formats_mode text DEFAULT 'unrestricted'::text NOT NULL,
    allowed_models_mode text DEFAULT 'unrestricted'::text NOT NULL,
    rate_limit_mode text DEFAULT 'system'::text NOT NULL,
    feature_settings jsonb,
    privacy_policy_accepted_version character varying(64),
    privacy_policy_accepted_at timestamp with time zone,
    security_version bigint DEFAULT 0 NOT NULL
);


--
-- Name: usage_analytics_facts_v1; Type: VIEW; Schema: public; Owner: -
--

CREATE OR REPLACE VIEW public.usage_analytics_facts_v1 AS
 SELECT u.request_id,
    COALESCE(u.id, u.request_id) AS id,
    u.created_at,
        CASE
            WHEN ((identity.owner_id IS NOT NULL) AND (identity.is_standalone = false)) THEN identity.owner_id
            ELSE NULL::text
        END AS actor_user_id,
    identity.owner_id AS credential_owner_id,
        CASE
            WHEN (identity.owner_id IS NULL) THEN 'unknown'::text
            WHEN identity.is_standalone THEN 'standalone'::text
            WHEN (NOT identity.is_standalone) THEN 'employee'::text
            ELSE 'unknown'::text
        END AS attribution_kind,
        CASE
            WHEN (identity.owner_id IS NULL) THEN 'unknown'::text
            WHEN identity.is_standalone THEN 'standalone_key'::text
            WHEN (NOT identity.is_standalone) THEN 'user_account'::text
            ELSE 'unknown'::text
        END AS attribution_source,
    COALESCE(a.record_kind, 'request'::text) AS record_kind,
    a.parent_request_id,
    u.api_key_id,
    u.model,
    u.target_model,
    u.provider_id,
    u.provider_name,
    u.api_format,
    u.endpoint_kind,
    u.request_type,
    u.is_stream,
    u.has_format_conversion,
    u.status,
    u.status_code,
    u.error_category,
    u.failure_origin,
    u.failure_stage,
    u.failure_reason,
    u.failure_schema_version,
    u.response_time_ms,
    u.first_byte_time_ms,
    COALESCE(s.billing_status, u.billing_status) AS settlement_status,
    (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AS usage_available,
    ((COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND ((s.billing_total_cost_usd IS NOT NULL) OR ((COALESCE(s.billing_status, u.billing_status))::text = 'settled'::text))) AS pricing_available,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.input_tokens
            ELSE NULL::bigint
        END AS input_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.output_tokens
            ELSE NULL::bigint
        END AS output_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.total_tokens
            ELSE NULL::bigint
        END AS total_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.cache_read_input_tokens
            ELSE NULL::bigint
        END AS cache_read_input_tokens,
        CASE
            WHEN (COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) THEN b.cache_creation_input_tokens
            ELSE NULL::bigint
        END AS cache_creation_input_tokens,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND ((s.billing_total_cost_usd IS NOT NULL) OR ((COALESCE(s.billing_status, u.billing_status))::text = 'settled'::text))) THEN round(COALESCE((s.billing_total_cost_usd)::numeric, (u.total_cost_usd)::numeric), 8)
            ELSE NULL::numeric
        END AS rated_amount,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND ((s.billing_actual_total_cost_usd IS NOT NULL) OR ((COALESCE(s.billing_status, u.billing_status))::text = 'settled'::text))) THEN public.usage_customer_billable_amount(metadata.value, COALESCE((s.billing_total_cost_usd)::numeric, (u.total_cost_usd)::numeric), COALESCE((s.billing_actual_total_cost_usd)::numeric, (u.actual_total_cost_usd)::numeric))
            ELSE NULL::numeric
        END AS billable_amount,
    s.quota_covered_amount_usd AS quota_covered_amount,
    s.wallet_consumed_amount_usd AS wallet_consumed_amount,
    s.wallet_debit_amount_usd AS wallet_debit_amount,
    s.wallet_recharge_debit_usd AS wallet_recharge_debit_amount,
    s.wallet_gift_debit_usd AS wallet_gift_debit_amount,
    s.wallet_overdraft_usd AS wallet_overdraft_amount,
    s.allocation_status,
    s.finalized_at AS settled_at,
        CASE
            WHEN (s.billing_total_cost_usd IS NOT NULL) THEN 'settlement_snapshot'::text
            ELSE 'legacy_float'::text
        END AS amount_source,
    b.upstream_is_stream,
        CASE
            WHEN ((metadata.value #>> '{analytics_measurement,source}'::text[]) = ANY (ARRAY['reported'::text, 'estimated'::text, 'mixed'::text])) THEN (metadata.value #>> '{analytics_measurement,source}'::text[])
            ELSE 'unknown'::text
        END AS token_source,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (s.input_price_per_1m IS NOT NULL) AND (s.billing_cache_read_cost_usd IS NOT NULL)) THEN round((((s.input_price_per_1m)::numeric * (b.cache_read_input_tokens)::numeric) / (1000000)::numeric), 8)
            ELSE NULL::numeric
        END AS cache_estimated_full_cost_amount,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (s.input_price_per_1m IS NOT NULL) AND (s.billing_cache_read_cost_usd IS NOT NULL)) THEN round((s.billing_cache_read_cost_usd)::numeric, 8)
            ELSE NULL::numeric
        END AS cache_read_cost_amount,
        CASE
            WHEN ((COALESCE((metadata.value -> 'usage_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (COALESCE((metadata.value -> 'usage_pricing_available'::text), 'true'::jsonb) <> 'false'::jsonb) AND (s.input_price_per_1m IS NOT NULL) AND (s.billing_cache_creation_cost_usd IS NOT NULL)) THEN round((s.billing_cache_creation_cost_usd)::numeric, 8)
            ELSE NULL::numeric
        END AS cache_creation_cost_amount
   FROM ((((((public.usage u
     CROSS JOIN LATERAL ( SELECT u.request_metadata AS value
         OFFSET 0) metadata)
     LEFT JOIN public.usage_settlement_snapshots s USING (request_id))
     LEFT JOIN public.usage_attribution_snapshots a USING (request_id))
     JOIN public.usage_billing_facts b USING (request_id))
     LEFT JOIN public.api_keys k ON (((k.id)::text = (u.api_key_id)::text)))
     CROSS JOIN LATERAL ( SELECT
                CASE
                    WHEN (a.request_id IS NOT NULL) THEN a.credential_owner_id
                    WHEN (EXISTS ( SELECT 1
                       FROM public.users
                      WHERE (((users.id)::text = (u.user_id)::text) AND (NOT users.is_deleted)))) THEN (u.user_id)::text
                    ELSE NULL::text
                END AS owner_id,
            COALESCE(k.is_standalone,
                CASE
                    WHEN (jsonb_typeof((metadata.value #> '{analytics_attribution,is_standalone}'::text[])) = 'boolean'::text) THEN ((metadata.value #>> '{analytics_attribution,is_standalone}'::text[]))::boolean
                    ELSE NULL::boolean
                END,
                CASE
                    WHEN (jsonb_typeof((metadata.value -> 'api_key_is_standalone'::text)) = 'boolean'::text) THEN ((metadata.value ->> 'api_key_is_standalone'::text))::boolean
                    ELSE NULL::boolean
                END,
                CASE
                    WHEN (a.attribution_source = 'user_account'::text) THEN false
                    WHEN (a.attribution_source = 'standalone_key'::text) THEN true
                    ELSE NULL::boolean
                END,
                CASE
                    WHEN (u.api_key_id IS NULL) THEN false
                    ELSE NULL::boolean
                END) AS is_standalone) identity);


--
-- Name: usage_cost_reservations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_cost_reservations (
    request_id character varying(128) NOT NULL,
    subject_id character varying(128) NOT NULL,
    reservation_token character varying(128) NOT NULL,
    admitted_at timestamp with time zone NOT NULL,
    reserved_cost_units bigint NOT NULL,
    actual_cost_units bigint,
    state character varying(20) NOT NULL,
    reservation_expires_at timestamp with time zone NOT NULL,
    retain_until timestamp with time zone NOT NULL,
    finalized_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT usage_cost_reservations_actual_cost_units_check CHECK (((actual_cost_units IS NULL) OR (actual_cost_units >= 0))),
    CONSTRAINT usage_cost_reservations_expiry_check CHECK ((reservation_expires_at > admitted_at)),
    CONSTRAINT usage_cost_reservations_lifecycle_check CHECK (((((state)::text = 'reserved'::text) AND (actual_cost_units IS NULL) AND (finalized_at IS NULL)) OR (((state)::text = 'finalized'::text) AND (actual_cost_units IS NOT NULL) AND (finalized_at IS NOT NULL)) OR (((state)::text = 'released'::text) AND (actual_cost_units IS NOT NULL) AND (actual_cost_units = 0) AND (finalized_at IS NOT NULL)))),
    CONSTRAINT usage_cost_reservations_reserved_cost_units_check CHECK ((reserved_cost_units >= 0)),
    CONSTRAINT usage_cost_reservations_retention_check CHECK ((retain_until >= reservation_expires_at)),
    CONSTRAINT usage_cost_reservations_state_check CHECK (((state)::text = ANY ((ARRAY['reserved'::character varying, 'finalized'::character varying, 'released'::character varying])::text[])))
);


--
-- Name: usage_counter_deltas; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_counter_deltas (
    id character varying(36) NOT NULL,
    request_id character varying(128) NOT NULL,
    kind character varying(64) NOT NULL,
    target_id text NOT NULL,
    request_count_delta bigint DEFAULT 0 NOT NULL,
    total_requests_delta bigint DEFAULT 0 NOT NULL,
    success_count_delta bigint DEFAULT 0 NOT NULL,
    error_count_delta bigint DEFAULT 0 NOT NULL,
    dns_failures_delta bigint DEFAULT 0 NOT NULL,
    stream_errors_delta bigint DEFAULT 0 NOT NULL,
    total_tokens_delta bigint DEFAULT 0 NOT NULL,
    total_cost_usd_delta double precision DEFAULT 0 NOT NULL,
    total_response_time_ms_delta bigint DEFAULT 0 NOT NULL,
    last_used_at_unix_secs bigint,
    last_used_ip text,
    candidate_last_used_at_unix_secs bigint,
    removed_last_used_at_unix_secs bigint,
    usage_created_at_unix_secs bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    processed_at timestamp with time zone,
    target_node_generation character varying(64),
    CONSTRAINT usage_counter_deltas_kind_check CHECK (((kind)::text = ANY ((ARRAY['api_key'::character varying, 'provider_api_key'::character varying, 'model'::character varying, 'provider_monthly'::character varying, 'proxy_node'::character varying, 'management_token'::character varying, 'api_key_last_used'::character varying])::text[])))
);


--
-- Name: usage_http_bodies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_http_bodies (
    request_id character varying(128) NOT NULL,
    client_request bytea,
    client_request_encoding character varying(8),
    client_request_truncated boolean DEFAULT false NOT NULL,
    client_response bytea,
    client_response_encoding character varying(8),
    client_response_truncated boolean DEFAULT false NOT NULL,
    provider_request bytea,
    provider_request_encoding character varying(8),
    provider_request_truncated boolean DEFAULT false NOT NULL,
    provider_response bytea,
    provider_response_encoding character varying(8),
    provider_response_truncated boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT client_request_encoding_check CHECK (client_request_encoding IN ('json', 'text', 'sse', 'binary')),
    CONSTRAINT client_request_encoding_pair_check CHECK ((client_request IS NULL) = (client_request_encoding IS NULL)),
    CONSTRAINT client_request_truncated_pair_check CHECK (NOT client_request_truncated OR client_request IS NOT NULL),
    CONSTRAINT client_response_encoding_check CHECK (client_response_encoding IN ('json', 'text', 'sse', 'binary')),
    CONSTRAINT client_response_encoding_pair_check CHECK ((client_response IS NULL) = (client_response_encoding IS NULL)),
    CONSTRAINT client_response_truncated_pair_check CHECK (NOT client_response_truncated OR client_response IS NOT NULL),
    CONSTRAINT provider_request_encoding_check CHECK (provider_request_encoding IN ('json', 'text', 'sse', 'binary')),
    CONSTRAINT provider_request_encoding_pair_check CHECK ((provider_request IS NULL) = (provider_request_encoding IS NULL)),
    CONSTRAINT provider_request_truncated_pair_check CHECK (NOT provider_request_truncated OR provider_request IS NOT NULL),
    CONSTRAINT provider_response_encoding_check CHECK (provider_response_encoding IN ('json', 'text', 'sse', 'binary')),
    CONSTRAINT provider_response_encoding_pair_check CHECK ((provider_response IS NULL) = (provider_response_encoding IS NULL)),
    CONSTRAINT provider_response_truncated_pair_check CHECK (NOT provider_response_truncated OR provider_response IS NOT NULL)
);
ALTER TABLE ONLY public.usage_http_bodies ALTER COLUMN client_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_bodies ALTER COLUMN client_response SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_bodies ALTER COLUMN provider_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_bodies ALTER COLUMN provider_response SET STORAGE EXTERNAL;


--
-- Name: usage_http_headers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_http_headers (
    request_id character varying(128) NOT NULL,
    client_request jsonb,
    client_response jsonb,
    provider_request jsonb,
    provider_response jsonb,
    created_at timestamp with time zone NOT NULL
);
ALTER TABLE ONLY public.usage_http_headers ALTER COLUMN client_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_headers ALTER COLUMN client_response SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_headers ALTER COLUMN provider_request SET STORAGE EXTERNAL;
ALTER TABLE ONLY public.usage_http_headers ALTER COLUMN provider_response SET STORAGE EXTERNAL;


--
-- Name: usage_request_admissions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_request_admissions (
    request_id character varying(128) NOT NULL,
    subject_id character varying(128) NOT NULL,
    event_token character varying(128) NOT NULL,
    admitted_at timestamp with time zone NOT NULL,
    retain_until timestamp with time zone NOT NULL,
    state character varying(20) NOT NULL,
    released_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT usage_request_admissions_lifecycle_check CHECK (((((state)::text = 'active'::text) AND (released_at IS NULL)) OR (((state)::text = 'released'::text) AND (released_at IS NOT NULL) AND (released_at >= admitted_at)))),
    CONSTRAINT usage_request_admissions_retention_check CHECK ((retain_until > admitted_at)),
    CONSTRAINT usage_request_admissions_state_check CHECK (((state)::text = ANY ((ARRAY['active'::character varying, 'released'::character varying])::text[])))
);


--
-- Name: usage_routing_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.usage_routing_snapshots (
    request_id character varying(100) NOT NULL,
    candidate_id character varying(160),
    candidate_index integer,
    key_name character varying(255),
    planner_kind character varying(120),
    route_family character varying(80),
    route_kind character varying(80),
    execution_path character varying(80),
    local_execution_runtime_miss_reason character varying(120),
    selected_provider_id character varying(100),
    selected_endpoint_id character varying(100),
    selected_provider_api_key_id character varying(100),
    has_format_conversion boolean,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: user_group_members; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_group_members (
    group_id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: user_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_groups (
    id character varying(36) NOT NULL,
    name character varying(100) NOT NULL,
    normalized_name character varying(100) NOT NULL,
    description text,
    priority integer DEFAULT 0 NOT NULL,
    allowed_providers jsonb,
    allowed_providers_mode text DEFAULT 'inherit'::text NOT NULL,
    allowed_api_formats jsonb,
    allowed_api_formats_mode text DEFAULT 'inherit'::text NOT NULL,
    allowed_models jsonb,
    allowed_models_mode text DEFAULT 'inherit'::text NOT NULL,
    rate_limit integer,
    rate_limit_mode text DEFAULT 'inherit'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT user_groups_allowed_api_formats_mode_check CHECK ((allowed_api_formats_mode = ANY (ARRAY['inherit'::text, 'unrestricted'::text, 'specific'::text, 'deny_all'::text]))),
    CONSTRAINT user_groups_allowed_models_mode_check CHECK ((allowed_models_mode = ANY (ARRAY['inherit'::text, 'unrestricted'::text, 'specific'::text, 'deny_all'::text]))),
    CONSTRAINT user_groups_allowed_providers_mode_check CHECK ((allowed_providers_mode = ANY (ARRAY['inherit'::text, 'unrestricted'::text, 'specific'::text, 'deny_all'::text]))),
    CONSTRAINT user_groups_rate_limit_mode_check CHECK ((rate_limit_mode = ANY (ARRAY['inherit'::text, 'system'::text, 'custom'::text])))
);


--
-- Name: user_invite_codes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_invite_codes (
    user_id character varying(64) NOT NULL,
    invite_code character varying(64) NOT NULL,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: user_model_usage_counts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_model_usage_counts (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    model character varying(100) NOT NULL,
    usage_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: user_oauth_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_oauth_links (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    provider_type character varying(50) NOT NULL,
    provider_user_id character varying(255) NOT NULL,
    provider_username character varying(255),
    provider_email character varying(255),
    extra_data jsonb,
    linked_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
    last_login_at timestamp with time zone
);


--
-- Name: user_plan_entitlements; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_plan_entitlements (
    id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    plan_id character varying(64) NOT NULL,
    payment_order_id character varying(64) NOT NULL,
    status character varying(64) DEFAULT 'active'::character varying NOT NULL,
    starts_at timestamp with time zone NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    entitlements_snapshot jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: user_preferences; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_preferences (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    avatar_url character varying(500),
    bio text,
    default_provider_id character varying(36),
    theme character varying(20) DEFAULT 'light'::character varying,
    language character varying(10) DEFAULT 'zh-CN'::character varying,
    timezone character varying(50) DEFAULT 'Asia/Shanghai'::character varying,
    email_notifications boolean DEFAULT true,
    usage_alerts boolean DEFAULT true,
    announcement_notifications boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    allow_wallet_overage boolean DEFAULT false NOT NULL
);


--
-- Name: user_referrals; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_referrals (
    id character varying(64) NOT NULL,
    inviter_user_id character varying(64) NOT NULL,
    invitee_user_id character varying(64) NOT NULL,
    invite_code_snapshot character varying(64) NOT NULL,
    source_json jsonb,
    first_paid_order_id character varying(64),
    first_paid_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: user_sessions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_sessions (
    id character varying(36) NOT NULL,
    user_id character varying(36) NOT NULL,
    client_device_id character varying(128) NOT NULL,
    device_label character varying(120),
    device_type character varying(20) DEFAULT 'unknown'::character varying NOT NULL,
    browser_name character varying(50),
    browser_version character varying(50),
    os_name character varying(50),
    os_version character varying(50),
    device_model character varying(100),
    ip_address character varying(45),
    user_agent character varying(1000),
    client_hints jsonb,
    refresh_token_hash character varying(64) NOT NULL,
    prev_refresh_token_hash character varying(64),
    rotated_at timestamp with time zone,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    revoke_reason character varying(100),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    security_version bigint DEFAULT 0 NOT NULL
);


--
-- Name: video_tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.video_tasks (
    id character varying(36) NOT NULL,
    external_task_id character varying(200),
    user_id character varying(36),
    api_key_id character varying(36),
    provider_id character varying(36),
    endpoint_id character varying(36),
    key_id character varying(36),
    client_api_format character varying(50) NOT NULL,
    provider_api_format character varying(50) NOT NULL,
    format_converted boolean DEFAULT false,
    model character varying(100) NOT NULL,
    prompt text NOT NULL,
    original_request_body jsonb,
    converted_request_body jsonb,
    duration_seconds integer DEFAULT 4,
    resolution character varying(20) DEFAULT '720p'::character varying,
    aspect_ratio character varying(10) DEFAULT '16:9'::character varying,
    size character varying(20),
    status character varying(20) DEFAULT 'pending'::character varying,
    progress_percent integer DEFAULT 0,
    progress_message character varying(500),
    video_url character varying(2000),
    video_urls jsonb,
    thumbnail_url character varying(2000),
    video_size_bytes bigint,
    video_expires_at timestamp with time zone,
    stored_video_path character varying(500),
    storage_provider character varying(50),
    error_code character varying(50),
    error_message text,
    retry_count integer DEFAULT 0,
    max_retries integer DEFAULT 3,
    poll_interval_seconds integer DEFAULT 10,
    next_poll_at timestamp with time zone,
    poll_count integer DEFAULT 0,
    max_poll_count integer DEFAULT 360,
    remixed_from_task_id character varying(36),
    webhook_url character varying(500),
    webhook_sent boolean DEFAULT false,
    webhook_sent_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    submitted_at timestamp with time zone,
    completed_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    request_metadata jsonb,
    request_id character varying(100) NOT NULL,
    short_id character varying(16) NOT NULL,
    video_duration_seconds double precision,
    username character varying(100),
    api_key_name character varying(200)
);


--
-- Name: wallet_daily_usage_ledgers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wallet_daily_usage_ledgers (
    id character varying(36) NOT NULL,
    wallet_id character varying(36) NOT NULL,
    billing_date date NOT NULL,
    billing_timezone character varying(64) NOT NULL,
    total_cost_usd numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    total_requests integer DEFAULT 0 NOT NULL,
    input_tokens bigint DEFAULT '0'::bigint NOT NULL,
    output_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_creation_tokens bigint DEFAULT '0'::bigint NOT NULL,
    cache_read_tokens bigint DEFAULT '0'::bigint NOT NULL,
    first_finalized_at timestamp with time zone,
    last_finalized_at timestamp with time zone,
    aggregated_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: wallet_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wallet_transactions (
    id character varying(36) NOT NULL,
    wallet_id character varying(36) NOT NULL,
    category character varying(20) NOT NULL,
    reason_code character varying(40) NOT NULL,
    amount numeric(20,8) NOT NULL,
    balance_before numeric(20,8) NOT NULL,
    balance_after numeric(20,8) NOT NULL,
    recharge_balance_before numeric(20,8) NOT NULL,
    recharge_balance_after numeric(20,8) NOT NULL,
    gift_balance_before numeric(20,8) NOT NULL,
    gift_balance_after numeric(20,8) NOT NULL,
    link_type character varying(30),
    link_id character varying(100),
    operator_id character varying(36),
    description text,
    created_at timestamp with time zone NOT NULL,
    CONSTRAINT ck_wallet_tx_balance_after_consistent CHECK ((balance_after = (recharge_balance_after + gift_balance_after))),
    CONSTRAINT ck_wallet_tx_balance_before_consistent CHECK ((balance_before = (recharge_balance_before + gift_balance_before)))
);


--
-- Name: wallets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.wallets (
    id character varying(36) NOT NULL,
    user_id character varying(36),
    api_key_id character varying(36),
    balance numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    gift_balance numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    limit_mode character varying(20) DEFAULT 'finite'::character varying NOT NULL,
    currency character varying(3) DEFAULT 'USD'::character varying NOT NULL,
    status character varying(20) DEFAULT 'active'::character varying NOT NULL,
    total_recharged numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    total_consumed numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    total_refunded numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    total_adjusted numeric(20,8) DEFAULT '0'::numeric NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT ck_wallet_single_owner CHECK ((((user_id IS NOT NULL) AND (api_key_id IS NULL)) OR ((user_id IS NULL) AND (api_key_id IS NOT NULL)) OR ((user_id IS NULL) AND (api_key_id IS NULL)))),
    CONSTRAINT ck_wallets_gift_balance_non_negative CHECK ((gift_balance >= (0)::numeric))
);


--
-- Name: ldap_configs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ldap_configs ALTER COLUMN id SET DEFAULT nextval('public.ldap_configs_id_seq'::regclass);


--
-- Name: admin_user_wallet_balance_batches admin_user_wallet_balance_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_user_wallet_balance_batches
    ADD CONSTRAINT admin_user_wallet_balance_batches_pkey PRIMARY KEY (admin_user_id, idempotency_key);


--
-- Name: announcement_reads announcement_reads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.announcement_reads
    ADD CONSTRAINT announcement_reads_pkey PRIMARY KEY (id);


--
-- Name: announcements announcements_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.announcements
    ADD CONSTRAINT announcements_pkey PRIMARY KEY (id);


--
-- Name: api_key_provider_mappings api_key_provider_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_provider_mappings
    ADD CONSTRAINT api_key_provider_mappings_pkey PRIMARY KEY (id);


--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);


--
-- Name: audit_logs audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_logs
    ADD CONSTRAINT audit_logs_pkey PRIMARY KEY (id);


--
-- Name: auth_modules auth_modules_module_type_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auth_modules
    ADD CONSTRAINT auth_modules_module_type_key UNIQUE (module_type);


--
-- Name: auth_modules auth_modules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auth_modules
    ADD CONSTRAINT auth_modules_pkey PRIMARY KEY (id);


--
-- Name: background_task_events background_task_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.background_task_events
    ADD CONSTRAINT background_task_events_pkey PRIMARY KEY (id);


--
-- Name: background_task_runs background_task_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.background_task_runs
    ADD CONSTRAINT background_task_runs_pkey PRIMARY KEY (id);


--
-- Name: billing_plans billing_plans_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_plans
    ADD CONSTRAINT billing_plans_pkey PRIMARY KEY (id);


--
-- Name: billing_rules billing_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_rules
    ADD CONSTRAINT billing_rules_pkey PRIMARY KEY (id);


--
-- Name: dashboard_activity_hour dashboard_activity_hour_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_activity_hour
    ADD CONSTRAINT dashboard_activity_hour_pkey PRIMARY KEY (bucket_start, shard);


--
-- Name: dashboard_activity_minute dashboard_activity_minute_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_activity_minute
    ADD CONSTRAINT dashboard_activity_minute_pkey PRIMARY KEY (bucket_start, shard);


--
-- Name: dashboard_actor_minute dashboard_actor_minute_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_actor_minute
    ADD CONSTRAINT dashboard_actor_minute_pkey PRIMARY KEY (bucket_start, shard, actor_user_id);


--
-- Name: dashboard_request_contributions dashboard_request_contributions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_request_contributions
    ADD CONSTRAINT dashboard_request_contributions_pkey PRIMARY KEY (request_id);


--
-- Name: dashboard_stats_minute dashboard_stats_minute_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_stats_minute
    ADD CONSTRAINT dashboard_stats_minute_pkey PRIMARY KEY (bucket_start, shard);


--
-- Name: dashboard_stats_pending dashboard_stats_pending_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_stats_pending
    ADD CONSTRAINT dashboard_stats_pending_pkey PRIMARY KEY (transaction_id, request_id);


--
-- Name: dashboard_stats_state dashboard_stats_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_stats_state
    ADD CONSTRAINT dashboard_stats_state_pkey PRIMARY KEY (singleton);


--
-- Name: dashboard_stats_total dashboard_stats_total_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_stats_total
    ADD CONSTRAINT dashboard_stats_total_pkey PRIMARY KEY (shard);


--
-- Name: dashboard_user_anonymization_pending dashboard_user_anonymization_pending_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_user_anonymization_pending
    ADD CONSTRAINT dashboard_user_anonymization_pending_pkey PRIMARY KEY (transaction_id, user_id);


--
-- Name: dashboard_user_events_minute dashboard_user_events_minute_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dashboard_user_events_minute
    ADD CONSTRAINT dashboard_user_events_minute_pkey PRIMARY KEY (bucket_start, shard);


--
-- Name: dimension_collectors dimension_collectors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dimension_collectors
    ADD CONSTRAINT dimension_collectors_pkey PRIMARY KEY (id);


--
-- Name: entitlement_usage_ledgers entitlement_usage_ledgers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entitlement_usage_ledgers
    ADD CONSTRAINT entitlement_usage_ledgers_pkey PRIMARY KEY (id);


--
-- Name: gemini_file_mappings gemini_file_mappings_file_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gemini_file_mappings
    ADD CONSTRAINT gemini_file_mappings_file_name_key UNIQUE (file_name);


--
-- Name: gemini_file_mappings gemini_file_mappings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gemini_file_mappings
    ADD CONSTRAINT gemini_file_mappings_pkey PRIMARY KEY (id);


--
-- Name: global_models global_models_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.global_models
    ADD CONSTRAINT global_models_pkey PRIMARY KEY (id);


--
-- Name: ldap_configs ldap_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ldap_configs
    ADD CONSTRAINT ldap_configs_pkey PRIMARY KEY (id);


--
-- Name: ldap_configs ldap_configs_singleton_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ldap_configs
    ADD CONSTRAINT ldap_configs_singleton_key_key UNIQUE (singleton_key);


--
-- Name: management_tokens management_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.management_tokens
    ADD CONSTRAINT management_tokens_pkey PRIMARY KEY (id);


--
-- Name: models models_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.models
    ADD CONSTRAINT models_pkey PRIMARY KEY (id);


--
-- Name: oauth_providers oauth_providers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.oauth_providers
    ADD CONSTRAINT oauth_providers_pkey PRIMARY KEY (provider_type);


--
-- Name: payment_callbacks payment_callbacks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_callbacks
    ADD CONSTRAINT payment_callbacks_pkey PRIMARY KEY (id);


--
-- Name: payment_gateway_configs payment_gateway_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_gateway_configs
    ADD CONSTRAINT payment_gateway_configs_pkey PRIMARY KEY (provider);


--
-- Name: payment_orders payment_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_orders
    ADD CONSTRAINT payment_orders_pkey PRIMARY KEY (id);


--
-- Name: pool_member_scores pool_member_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pool_member_scores
    ADD CONSTRAINT pool_member_scores_pkey PRIMARY KEY (id);


--
-- Name: provider_api_keys provider_api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_api_keys
    ADD CONSTRAINT provider_api_keys_pkey PRIMARY KEY (id);


--
-- Name: provider_endpoints provider_endpoints_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_endpoints
    ADD CONSTRAINT provider_endpoints_pkey PRIMARY KEY (id);


--
-- Name: provider_expenses provider_expenses_client_request_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_expenses
    ADD CONSTRAINT provider_expenses_client_request_id_key UNIQUE (client_request_id);


--
-- Name: provider_expenses provider_expenses_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_expenses
    ADD CONSTRAINT provider_expenses_pkey PRIMARY KEY (id);


--
-- Name: provider_usage_tracking provider_usage_tracking_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_usage_tracking
    ADD CONSTRAINT provider_usage_tracking_pkey PRIMARY KEY (id);


--
-- Name: providers providers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.providers
    ADD CONSTRAINT providers_pkey PRIMARY KEY (id);


--
-- Name: proxy_nodes proxy_nodes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_nodes
    ADD CONSTRAINT proxy_nodes_pkey PRIMARY KEY (id);


--
-- Name: redeem_code_batches redeem_code_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redeem_code_batches
    ADD CONSTRAINT redeem_code_batches_pkey PRIMARY KEY (id);


--
-- Name: redeem_codes redeem_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redeem_codes
    ADD CONSTRAINT redeem_codes_pkey PRIMARY KEY (id);


--
-- Name: referral_rewards referral_rewards_idempotency_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.referral_rewards
    ADD CONSTRAINT referral_rewards_idempotency_key_key UNIQUE (idempotency_key);


--
-- Name: referral_rewards referral_rewards_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.referral_rewards
    ADD CONSTRAINT referral_rewards_pkey PRIMARY KEY (id);


--
-- Name: refund_requests refund_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refund_requests
    ADD CONSTRAINT refund_requests_pkey PRIMARY KEY (id);


--
-- Name: request_candidates request_candidates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_candidates
    ADD CONSTRAINT request_candidates_pkey PRIMARY KEY (id);


--
-- Name: routing_group_bindings routing_group_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_group_bindings
    ADD CONSTRAINT routing_group_bindings_pkey PRIMARY KEY (id);


--
-- Name: routing_group_versions routing_group_versions_group_version_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_group_versions
    ADD CONSTRAINT routing_group_versions_group_version_key UNIQUE (group_id, version);


--
-- Name: routing_group_versions routing_group_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_group_versions
    ADD CONSTRAINT routing_group_versions_pkey PRIMARY KEY (id);


--
-- Name: routing_groups routing_groups_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_groups
    ADD CONSTRAINT routing_groups_name_key UNIQUE (name);


--
-- Name: routing_groups routing_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.routing_groups
    ADD CONSTRAINT routing_groups_pkey PRIMARY KEY (id);


--
-- Name: schema_backfills schema_backfills_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_backfills
    ADD CONSTRAINT schema_backfills_pkey PRIMARY KEY (version);


--
-- Name: stats_bucket_state stats_bucket_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_bucket_state
    ADD CONSTRAINT stats_bucket_state_pkey PRIMARY KEY (projection_version, granularity, bucket_start);


--
-- Name: stats_daily_api_key stats_daily_api_key_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_api_key
    ADD CONSTRAINT stats_daily_api_key_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_cost_savings_model stats_daily_cost_savings_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings_model
    ADD CONSTRAINT stats_daily_cost_savings_model_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_cost_savings_model_provider stats_daily_cost_savings_model_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings_model_provider
    ADD CONSTRAINT stats_daily_cost_savings_model_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_cost_savings stats_daily_cost_savings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings
    ADD CONSTRAINT stats_daily_cost_savings_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_cost_savings_provider stats_daily_cost_savings_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings_provider
    ADD CONSTRAINT stats_daily_cost_savings_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_error stats_daily_error_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_error
    ADD CONSTRAINT stats_daily_error_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_model stats_daily_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_model
    ADD CONSTRAINT stats_daily_model_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_model_provider stats_daily_model_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_model_provider
    ADD CONSTRAINT stats_daily_model_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_daily stats_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily
    ADD CONSTRAINT stats_daily_pkey PRIMARY KEY (id);


--
-- Name: stats_daily_provider stats_daily_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_provider
    ADD CONSTRAINT stats_daily_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_hourly_model stats_hourly_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_model
    ADD CONSTRAINT stats_hourly_model_pkey PRIMARY KEY (id);


--
-- Name: stats_hourly stats_hourly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly
    ADD CONSTRAINT stats_hourly_pkey PRIMARY KEY (id);


--
-- Name: stats_hourly_provider stats_hourly_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_provider
    ADD CONSTRAINT stats_hourly_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_hourly_user_model stats_hourly_user_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_user_model
    ADD CONSTRAINT stats_hourly_user_model_pkey PRIMARY KEY (id);


--
-- Name: stats_hourly_user stats_hourly_user_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_user
    ADD CONSTRAINT stats_hourly_user_pkey PRIMARY KEY (id);


--
-- Name: stats_overview_daily stats_overview_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_overview_daily
    ADD CONSTRAINT stats_overview_daily_pkey PRIMARY KEY (projection_version, bucket_start, dimensions);


--
-- Name: stats_overview_dirty_events stats_overview_dirty_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_overview_dirty_events
    ADD CONSTRAINT stats_overview_dirty_events_pkey PRIMARY KEY (transaction_id, projection_version, granularity, bucket_start);


--
-- Name: stats_overview_hourly stats_overview_hourly_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_overview_hourly
    ADD CONSTRAINT stats_overview_hourly_pkey PRIMARY KEY (projection_version, bucket_start, dimensions);


--
-- Name: stats_summary stats_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_summary
    ADD CONSTRAINT stats_summary_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_api_format stats_user_daily_api_format_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_api_format
    ADD CONSTRAINT stats_user_daily_api_format_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_cost_savings_model stats_user_daily_cost_savings_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings_model
    ADD CONSTRAINT stats_user_daily_cost_savings_model_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_cost_savings_model_provider stats_user_daily_cost_savings_model_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings_model_provider
    ADD CONSTRAINT stats_user_daily_cost_savings_model_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_cost_savings stats_user_daily_cost_savings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings
    ADD CONSTRAINT stats_user_daily_cost_savings_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_cost_savings_provider stats_user_daily_cost_savings_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings_provider
    ADD CONSTRAINT stats_user_daily_cost_savings_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_model stats_user_daily_model_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_model
    ADD CONSTRAINT stats_user_daily_model_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_model_provider stats_user_daily_model_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_model_provider
    ADD CONSTRAINT stats_user_daily_model_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily stats_user_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily
    ADD CONSTRAINT stats_user_daily_pkey PRIMARY KEY (id);


--
-- Name: stats_user_daily_provider stats_user_daily_provider_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_provider
    ADD CONSTRAINT stats_user_daily_provider_pkey PRIMARY KEY (id);


--
-- Name: stats_user_summary stats_user_summary_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_summary
    ADD CONSTRAINT stats_user_summary_pkey PRIMARY KEY (id);


--
-- Name: system_configs system_configs_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_configs
    ADD CONSTRAINT system_configs_key_key UNIQUE (key);


--
-- Name: system_configs system_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.system_configs
    ADD CONSTRAINT system_configs_pkey PRIMARY KEY (id);


--
-- Name: api_key_provider_mappings uq_apikey_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_provider_mappings
    ADD CONSTRAINT uq_apikey_provider UNIQUE (api_key_id, provider_id);


--
-- Name: entitlement_usage_ledgers uq_entitlement_usage_request; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entitlement_usage_ledgers
    ADD CONSTRAINT uq_entitlement_usage_request UNIQUE (user_entitlement_id, request_id);


--
-- Name: management_tokens uq_management_tokens_user_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.management_tokens
    ADD CONSTRAINT uq_management_tokens_user_name UNIQUE (user_id, name);


--
-- Name: user_oauth_links uq_oauth_provider_user; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_oauth_links
    ADD CONSTRAINT uq_oauth_provider_user UNIQUE (provider_type, provider_user_id);


--
-- Name: payment_callbacks uq_payment_callbacks_callback_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_callbacks
    ADD CONSTRAINT uq_payment_callbacks_callback_key UNIQUE (callback_key);


--
-- Name: payment_orders uq_payment_orders_order_no; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_orders
    ADD CONSTRAINT uq_payment_orders_order_no UNIQUE (order_no);


--
-- Name: models uq_provider_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.models
    ADD CONSTRAINT uq_provider_model UNIQUE (provider_id, provider_model_name);


--
-- Name: proxy_nodes uq_proxy_node_ip_port; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.proxy_nodes
    ADD CONSTRAINT uq_proxy_node_ip_port UNIQUE (ip, port);


--
-- Name: redeem_codes uq_redeem_codes_code_hash; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redeem_codes
    ADD CONSTRAINT uq_redeem_codes_code_hash UNIQUE (code_hash);


--
-- Name: refund_requests uq_refund_requests_idempotency_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refund_requests
    ADD CONSTRAINT uq_refund_requests_idempotency_key UNIQUE (idempotency_key);


--
-- Name: refund_requests uq_refund_requests_refund_no; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refund_requests
    ADD CONSTRAINT uq_refund_requests_refund_no UNIQUE (refund_no);


--
-- Name: request_candidates uq_request_candidate_with_retry; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_candidates
    ADD CONSTRAINT uq_request_candidate_with_retry UNIQUE (request_id, candidate_index, retry_index);


--
-- Name: stats_daily_api_key uq_stats_daily_api_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_api_key
    ADD CONSTRAINT uq_stats_daily_api_key UNIQUE (api_key_id, date);


--
-- Name: stats_daily_cost_savings uq_stats_daily_cost_savings_date; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings
    ADD CONSTRAINT uq_stats_daily_cost_savings_date UNIQUE (date);


--
-- Name: stats_daily_cost_savings_model uq_stats_daily_cost_savings_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings_model
    ADD CONSTRAINT uq_stats_daily_cost_savings_model UNIQUE (date, model);


--
-- Name: stats_daily_cost_savings_model_provider uq_stats_daily_cost_savings_model_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings_model_provider
    ADD CONSTRAINT uq_stats_daily_cost_savings_model_provider UNIQUE (date, model, provider_name);


--
-- Name: stats_daily_cost_savings_provider uq_stats_daily_cost_savings_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_cost_savings_provider
    ADD CONSTRAINT uq_stats_daily_cost_savings_provider UNIQUE (date, provider_name);


--
-- Name: stats_daily_error uq_stats_daily_error; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_error
    ADD CONSTRAINT uq_stats_daily_error UNIQUE (date, error_category, provider_name, model);


--
-- Name: stats_daily_model uq_stats_daily_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_model
    ADD CONSTRAINT uq_stats_daily_model UNIQUE (date, model);


--
-- Name: stats_daily_model_provider uq_stats_daily_model_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_model_provider
    ADD CONSTRAINT uq_stats_daily_model_provider UNIQUE (date, model, provider_name);


--
-- Name: stats_daily_provider uq_stats_daily_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_daily_provider
    ADD CONSTRAINT uq_stats_daily_provider UNIQUE (date, provider_name);


--
-- Name: stats_hourly uq_stats_hourly_hour; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly
    ADD CONSTRAINT uq_stats_hourly_hour UNIQUE (hour_utc);


--
-- Name: stats_hourly_model uq_stats_hourly_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_model
    ADD CONSTRAINT uq_stats_hourly_model UNIQUE (hour_utc, model);


--
-- Name: stats_hourly_provider uq_stats_hourly_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_provider
    ADD CONSTRAINT uq_stats_hourly_provider UNIQUE (hour_utc, provider_name);


--
-- Name: stats_hourly_user uq_stats_hourly_user; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_user
    ADD CONSTRAINT uq_stats_hourly_user UNIQUE (hour_utc, user_id);


--
-- Name: stats_hourly_user_model uq_stats_hourly_user_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_hourly_user_model
    ADD CONSTRAINT uq_stats_hourly_user_model UNIQUE (hour_utc, user_id, model);


--
-- Name: stats_user_daily uq_stats_user_daily; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily
    ADD CONSTRAINT uq_stats_user_daily UNIQUE (user_id, date);


--
-- Name: stats_user_daily_api_format uq_stats_user_daily_api_format_user_date_api_format; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_api_format
    ADD CONSTRAINT uq_stats_user_daily_api_format_user_date_api_format UNIQUE (user_id, date, api_format);


--
-- Name: stats_user_daily_cost_savings uq_stats_user_daily_cost_savings; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings
    ADD CONSTRAINT uq_stats_user_daily_cost_savings UNIQUE (user_id, date);


--
-- Name: stats_user_daily_cost_savings_model uq_stats_user_daily_cost_savings_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings_model
    ADD CONSTRAINT uq_stats_user_daily_cost_savings_model UNIQUE (user_id, date, model);


--
-- Name: stats_user_daily_cost_savings_model_provider uq_stats_user_daily_cost_savings_model_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings_model_provider
    ADD CONSTRAINT uq_stats_user_daily_cost_savings_model_provider UNIQUE (user_id, date, model, provider_name);


--
-- Name: stats_user_daily_cost_savings_provider uq_stats_user_daily_cost_savings_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_cost_savings_provider
    ADD CONSTRAINT uq_stats_user_daily_cost_savings_provider UNIQUE (user_id, date, provider_name);


--
-- Name: stats_user_daily_model_provider uq_stats_user_daily_model_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_model_provider
    ADD CONSTRAINT uq_stats_user_daily_model_provider UNIQUE (user_id, date, model, provider_name);


--
-- Name: stats_user_daily_model uq_stats_user_daily_model_user_date_model; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_model
    ADD CONSTRAINT uq_stats_user_daily_model_user_date_model UNIQUE (user_id, date, model);


--
-- Name: stats_user_daily_provider uq_stats_user_daily_provider_user_date_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_daily_provider
    ADD CONSTRAINT uq_stats_user_daily_provider_user_date_provider UNIQUE (user_id, date, provider_name);


--
-- Name: stats_user_summary uq_stats_user_summary_user_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.stats_user_summary
    ADD CONSTRAINT uq_stats_user_summary_user_id UNIQUE (user_id);


--
-- Name: announcement_reads uq_user_announcement; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.announcement_reads
    ADD CONSTRAINT uq_user_announcement UNIQUE (user_id, announcement_id);


--
-- Name: user_model_usage_counts uq_user_model_usage_count; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_model_usage_counts
    ADD CONSTRAINT uq_user_model_usage_count UNIQUE (user_id, model);


--
-- Name: user_oauth_links uq_user_oauth_provider; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_oauth_links
    ADD CONSTRAINT uq_user_oauth_provider UNIQUE (user_id, provider_type);


--
-- Name: video_tasks uq_video_tasks_request_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_tasks
    ADD CONSTRAINT uq_video_tasks_request_id UNIQUE (request_id);


--
-- Name: wallet_daily_usage_ledgers uq_wallet_daily_usage_ledgers_wallet_date_tz; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_daily_usage_ledgers
    ADD CONSTRAINT uq_wallet_daily_usage_ledgers_wallet_date_tz UNIQUE (wallet_id, billing_date, billing_timezone);


--
-- Name: wallets uq_wallets_api_key_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT uq_wallets_api_key_id UNIQUE (api_key_id);


--
-- Name: wallets uq_wallets_user_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT uq_wallets_user_id UNIQUE (user_id);


--
-- Name: usage_attribution_snapshots usage_attribution_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_attribution_snapshots
    ADD CONSTRAINT usage_attribution_snapshots_pkey PRIMARY KEY (request_id);


--
-- Name: usage_cost_reservations usage_cost_reservations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_cost_reservations
    ADD CONSTRAINT usage_cost_reservations_pkey PRIMARY KEY (reservation_token);


--
-- Name: usage_counter_deltas usage_counter_deltas_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_counter_deltas
    ADD CONSTRAINT usage_counter_deltas_pkey PRIMARY KEY (id);


--
-- Name: usage_http_bodies usage_http_bodies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT usage_http_bodies_pkey PRIMARY KEY (request_id);


--
-- Name: usage_http_headers usage_http_headers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_http_headers
    ADD CONSTRAINT usage_http_headers_pkey PRIMARY KEY (request_id);


--
-- Name: usage usage_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage
    ADD CONSTRAINT usage_pkey PRIMARY KEY (id);


--
-- Name: usage_request_admissions usage_request_admissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_request_admissions
    ADD CONSTRAINT usage_request_admissions_pkey PRIMARY KEY (event_token);


--
-- Name: usage_routing_snapshots usage_routing_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_routing_snapshots
    ADD CONSTRAINT usage_routing_snapshots_pkey PRIMARY KEY (request_id);


--
-- Name: usage_settlement_snapshots usage_settlement_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_settlement_snapshots
    ADD CONSTRAINT usage_settlement_snapshots_pkey PRIMARY KEY (request_id);


--
-- Name: user_group_members user_group_members_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_group_members
    ADD CONSTRAINT user_group_members_pkey PRIMARY KEY (group_id, user_id);


--
-- Name: user_groups user_groups_normalized_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_groups
    ADD CONSTRAINT user_groups_normalized_name_key UNIQUE (normalized_name);


--
-- Name: user_groups user_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_groups
    ADD CONSTRAINT user_groups_pkey PRIMARY KEY (id);


--
-- Name: user_invite_codes user_invite_codes_invite_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_invite_codes
    ADD CONSTRAINT user_invite_codes_invite_code_key UNIQUE (invite_code);


--
-- Name: user_invite_codes user_invite_codes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_invite_codes
    ADD CONSTRAINT user_invite_codes_pkey PRIMARY KEY (user_id);


--
-- Name: user_model_usage_counts user_model_usage_counts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_model_usage_counts
    ADD CONSTRAINT user_model_usage_counts_pkey PRIMARY KEY (id);


--
-- Name: user_oauth_links user_oauth_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_oauth_links
    ADD CONSTRAINT user_oauth_links_pkey PRIMARY KEY (id);


--
-- Name: user_plan_entitlements user_plan_entitlements_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_plan_entitlements
    ADD CONSTRAINT user_plan_entitlements_pkey PRIMARY KEY (id);


--
-- Name: user_preferences user_preferences_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_preferences
    ADD CONSTRAINT user_preferences_pkey PRIMARY KEY (id);


--
-- Name: user_preferences user_preferences_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_preferences
    ADD CONSTRAINT user_preferences_user_id_key UNIQUE (user_id);


--
-- Name: user_referrals user_referrals_invitee_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_referrals
    ADD CONSTRAINT user_referrals_invitee_user_id_key UNIQUE (invitee_user_id);


--
-- Name: user_referrals user_referrals_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_referrals
    ADD CONSTRAINT user_referrals_pkey PRIMARY KEY (id);


--
-- Name: user_sessions user_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_sessions
    ADD CONSTRAINT user_sessions_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: video_tasks video_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_tasks
    ADD CONSTRAINT video_tasks_pkey PRIMARY KEY (id);


--
-- Name: wallet_daily_usage_ledgers wallet_daily_usage_ledgers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_daily_usage_ledgers
    ADD CONSTRAINT wallet_daily_usage_ledgers_pkey PRIMARY KEY (id);


--
-- Name: wallet_transactions wallet_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_transactions
    ADD CONSTRAINT wallet_transactions_pkey PRIMARY KEY (id);


--
-- Name: wallets wallets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallets
    ADD CONSTRAINT wallets_pkey PRIMARY KEY (id);


--
-- Name: idx_apikey_provider_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_apikey_provider_enabled ON public.api_key_provider_mappings USING btree (api_key_id, is_enabled);


--
-- Name: idx_background_task_events_run_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_events_run_id ON public.background_task_events USING btree (run_id, created_at_unix_secs);


--
-- Name: idx_background_task_runs_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_runs_created_at ON public.background_task_runs USING btree (created_at_unix_secs DESC);


--
-- Name: idx_background_task_runs_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_runs_kind ON public.background_task_runs USING btree (kind);


--
-- Name: idx_background_task_runs_kind_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_runs_kind_created ON public.background_task_runs USING btree (kind, created_at_unix_secs DESC, updated_at_unix_secs DESC);


--
-- Name: idx_background_task_runs_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_runs_status ON public.background_task_runs USING btree (status);


--
-- Name: idx_background_task_runs_status_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_runs_status_created ON public.background_task_runs USING btree (status, created_at_unix_secs DESC, updated_at_unix_secs DESC);


--
-- Name: idx_background_task_runs_task_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_background_task_runs_task_key ON public.background_task_runs USING btree (task_key);


--
-- Name: idx_billing_plans_enabled_sort; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_billing_plans_enabled_sort ON public.billing_plans USING btree (enabled, sort_order);


--
-- Name: idx_endpoint_format_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_endpoint_format_active ON public.provider_endpoints USING btree (api_format, is_active);


--
-- Name: idx_entitlement_usage_entitlement_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_entitlement_usage_entitlement_date ON public.entitlement_usage_ledgers USING btree (user_entitlement_id, usage_date);


--
-- Name: idx_entitlement_usage_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_entitlement_usage_user_date ON public.entitlement_usage_ledgers USING btree (user_id, usage_date);


--
-- Name: idx_gemini_file_mappings_expires; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_gemini_file_mappings_expires ON public.gemini_file_mappings USING btree (expires_at);


--
-- Name: idx_gemini_file_mappings_source_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_gemini_file_mappings_source_hash ON public.gemini_file_mappings USING btree (source_hash);


--
-- Name: idx_management_tokens_is_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_management_tokens_is_active ON public.management_tokens USING btree (is_active);


--
-- Name: idx_management_tokens_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_management_tokens_user_id ON public.management_tokens USING btree (user_id);


--
-- Name: idx_model_provider_model_aliases_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_model_provider_model_aliases_gin ON public.models USING gin (provider_model_mappings jsonb_path_ops) WHERE (is_active = true);


--
-- Name: idx_model_provider_model_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_model_provider_model_name ON public.models USING btree (provider_model_name) WHERE (is_active = true);


--
-- Name: idx_payment_callbacks_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_callbacks_created ON public.payment_callbacks USING btree (created_at);


--
-- Name: idx_payment_callbacks_gateway_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_callbacks_gateway_order ON public.payment_callbacks USING btree (gateway_order_id);


--
-- Name: idx_payment_callbacks_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_callbacks_order ON public.payment_callbacks USING btree (order_no);


--
-- Name: idx_payment_orders_gateway_order_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_gateway_order_id ON public.payment_orders USING btree (gateway_order_id);


--
-- Name: idx_payment_orders_kind_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_kind_status ON public.payment_orders USING btree (order_kind, status);


--
-- Name: idx_payment_orders_product; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_product ON public.payment_orders USING btree (product_id);


--
-- Name: idx_payment_orders_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_status ON public.payment_orders USING btree (status);


--
-- Name: idx_payment_orders_status_credited_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_status_credited_user ON public.payment_orders USING btree (status, credited_at, user_id);


--
-- Name: idx_payment_orders_user_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_user_created ON public.payment_orders USING btree (user_id, created_at);


--
-- Name: idx_payment_orders_wallet_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_payment_orders_wallet_created ON public.payment_orders USING btree (wallet_id, created_at);


--
-- Name: idx_provider_api_keys_provider_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_active ON public.provider_api_keys USING btree (provider_id, is_active);


--
-- Name: idx_provider_api_keys_provider_active_priority_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_active_priority_id ON public.provider_api_keys USING btree (provider_id, internal_priority, id) WHERE (is_active IS TRUE);


--
-- Name: idx_provider_api_keys_provider_created_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_created_at_desc ON public.provider_api_keys USING btree (provider_id, created_at DESC NULLS LAST, name, id);


--
-- Name: idx_provider_api_keys_provider_default_sort; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_default_sort ON public.provider_api_keys USING btree (provider_id, internal_priority, name, id);


--
-- Name: idx_provider_api_keys_provider_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_id ON public.provider_api_keys USING btree (provider_id);


--
-- Name: idx_provider_api_keys_provider_last_used_at_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_last_used_at_desc ON public.provider_api_keys USING btree (provider_id, last_used_at DESC NULLS LAST, name, id);


--
-- Name: idx_provider_api_keys_provider_name_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_api_keys_provider_name_id ON public.provider_api_keys USING btree (provider_id, name, id);


--
-- Name: idx_provider_endpoints_provider_api_format; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_endpoints_provider_api_format ON public.provider_endpoints USING btree (provider_id, api_format);


--
-- Name: idx_provider_family_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_family_kind ON public.provider_endpoints USING btree (provider_id, api_family, endpoint_kind);


--
-- Name: idx_provider_window; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_provider_window ON public.provider_usage_tracking USING btree (provider_id, window_start);


--
-- Name: idx_rc_provider_status_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_rc_provider_status_created ON public.request_candidates USING btree (provider_id, status, created_at);


--
-- Name: idx_rc_request_id_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_rc_request_id_status ON public.request_candidates USING btree (request_id, status);


--
-- Name: idx_redeem_code_batches_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_redeem_code_batches_status ON public.redeem_code_batches USING btree (status, created_at);


--
-- Name: idx_redeem_codes_batch_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_redeem_codes_batch_created ON public.redeem_codes USING btree (batch_id, created_at);


--
-- Name: idx_redeem_codes_redeemed_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_redeem_codes_redeemed_order ON public.redeem_codes USING btree (redeemed_payment_order_id);


--
-- Name: idx_redeem_codes_redeemed_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_redeem_codes_redeemed_user ON public.redeem_codes USING btree (redeemed_by_user_id, redeemed_at);


--
-- Name: idx_redeem_codes_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_redeem_codes_status ON public.redeem_codes USING btree (status, updated_at);


--
-- Name: idx_referral_rewards_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_referral_rewards_created ON public.referral_rewards USING btree (created_at DESC);


--
-- Name: idx_referral_rewards_inviter_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_referral_rewards_inviter_created ON public.referral_rewards USING btree (inviter_user_id, created_at DESC);


--
-- Name: idx_referral_rewards_inviter_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_referral_rewards_inviter_status ON public.referral_rewards USING btree (inviter_user_id, status, created_at DESC);


--
-- Name: idx_referral_rewards_source_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_referral_rewards_source_order ON public.referral_rewards USING btree (source_order_id);


--
-- Name: idx_refund_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_refund_status ON public.refund_requests USING btree (status);


--
-- Name: idx_refund_user_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_refund_user_created ON public.refund_requests USING btree (user_id, created_at);


--
-- Name: idx_refund_wallet_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_refund_wallet_created ON public.refund_requests USING btree (wallet_id, created_at);


--
-- Name: idx_request_candidates_api_key_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_api_key_created ON public.request_candidates USING btree (api_key_id, created_at, id);


--
-- Name: idx_request_candidates_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_created_at ON public.request_candidates USING btree (created_at);


--
-- Name: idx_request_candidates_endpoint_status_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_endpoint_status_created ON public.request_candidates USING btree (endpoint_id, status, created_at DESC, id);


--
-- Name: idx_request_candidates_provider_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_provider_created ON public.request_candidates USING btree (provider_id, created_at DESC, id);


--
-- Name: idx_request_candidates_provider_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_provider_id ON public.request_candidates USING btree (provider_id);


--
-- Name: idx_request_candidates_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_request_id ON public.request_candidates USING btree (request_id);


--
-- Name: idx_request_candidates_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_request_candidates_status ON public.request_candidates USING btree (status);


--
-- Name: idx_schema_backfills_applied_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_schema_backfills_applied_at ON public.schema_backfills USING btree (applied_at DESC);


--
-- Name: idx_stats_daily_api_key_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_api_key_date ON public.stats_daily_api_key USING btree (date);


--
-- Name: idx_stats_daily_api_key_date_cost; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_api_key_date_cost ON public.stats_daily_api_key USING btree (date, total_cost);


--
-- Name: idx_stats_daily_api_key_date_requests; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_api_key_date_requests ON public.stats_daily_api_key USING btree (date, total_requests);


--
-- Name: idx_stats_daily_api_key_key_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_api_key_key_date ON public.stats_daily_api_key USING btree (api_key_id, date);


--
-- Name: idx_stats_daily_cost_savings_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_date ON public.stats_daily_cost_savings USING btree (date);


--
-- Name: idx_stats_daily_cost_savings_model_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_model_date ON public.stats_daily_cost_savings_model USING btree (date);


--
-- Name: idx_stats_daily_cost_savings_model_date_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_model_date_model ON public.stats_daily_cost_savings_model USING btree (date, model);


--
-- Name: idx_stats_daily_cost_savings_model_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_model_provider_date ON public.stats_daily_cost_savings_model_provider USING btree (date);


--
-- Name: idx_stats_daily_cost_savings_model_provider_date_dims; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_model_provider_date_dims ON public.stats_daily_cost_savings_model_provider USING btree (date, model, provider_name);


--
-- Name: idx_stats_daily_cost_savings_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_provider_date ON public.stats_daily_cost_savings_provider USING btree (date);


--
-- Name: idx_stats_daily_cost_savings_provider_date_provider; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_cost_savings_provider_date_provider ON public.stats_daily_cost_savings_provider USING btree (date, provider_name);


--
-- Name: idx_stats_daily_error_category; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_error_category ON public.stats_daily_error USING btree (date, error_category);


--
-- Name: idx_stats_daily_error_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_error_date ON public.stats_daily_error USING btree (date);


--
-- Name: idx_stats_daily_model_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_model_date ON public.stats_daily_model USING btree (date);


--
-- Name: idx_stats_daily_model_date_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_model_date_model ON public.stats_daily_model USING btree (date, model);


--
-- Name: idx_stats_daily_model_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_model_provider_date ON public.stats_daily_model_provider USING btree (date);


--
-- Name: idx_stats_daily_model_provider_date_model_provider; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_model_provider_date_model_provider ON public.stats_daily_model_provider USING btree (date, model, provider_name);


--
-- Name: idx_stats_daily_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_provider_date ON public.stats_daily_provider USING btree (date);


--
-- Name: idx_stats_daily_provider_date_provider; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_daily_provider_date_provider ON public.stats_daily_provider USING btree (date, provider_name);


--
-- Name: idx_stats_hourly_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_hour ON public.stats_hourly USING btree (hour_utc);


--
-- Name: idx_stats_hourly_model_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_model_hour ON public.stats_hourly_model USING btree (hour_utc);


--
-- Name: idx_stats_hourly_model_model_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_model_model_hour ON public.stats_hourly_model USING btree (model, hour_utc);


--
-- Name: idx_stats_hourly_provider_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_provider_hour ON public.stats_hourly_provider USING btree (hour_utc);


--
-- Name: idx_stats_hourly_user_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_user_hour ON public.stats_hourly_user USING btree (hour_utc);


--
-- Name: idx_stats_hourly_user_model_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_user_model_hour ON public.stats_hourly_user_model USING btree (hour_utc);


--
-- Name: idx_stats_hourly_user_model_user_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_user_model_user_hour ON public.stats_hourly_user_model USING btree (user_id, hour_utc);


--
-- Name: idx_stats_hourly_user_user_hour; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_hourly_user_user_hour ON public.stats_hourly_user USING btree (user_id, hour_utc);


--
-- Name: idx_stats_user_daily_api_format_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_api_format_date ON public.stats_user_daily_api_format USING btree (date);


--
-- Name: idx_stats_user_daily_api_format_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_api_format_user_id ON public.stats_user_daily_api_format USING btree (user_id);


--
-- Name: idx_stats_user_daily_cost_savings_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_date ON public.stats_user_daily_cost_savings USING btree (date);


--
-- Name: idx_stats_user_daily_cost_savings_model_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_model_date ON public.stats_user_daily_cost_savings_model USING btree (date);


--
-- Name: idx_stats_user_daily_cost_savings_model_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_model_provider_date ON public.stats_user_daily_cost_savings_model_provider USING btree (date);


--
-- Name: idx_stats_user_daily_cost_savings_model_provider_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_model_provider_user_date ON public.stats_user_daily_cost_savings_model_provider USING btree (user_id, date);


--
-- Name: idx_stats_user_daily_cost_savings_model_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_model_user_date ON public.stats_user_daily_cost_savings_model USING btree (user_id, date);


--
-- Name: idx_stats_user_daily_cost_savings_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_provider_date ON public.stats_user_daily_cost_savings_provider USING btree (date);


--
-- Name: idx_stats_user_daily_cost_savings_provider_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_provider_user_date ON public.stats_user_daily_cost_savings_provider USING btree (user_id, date);


--
-- Name: idx_stats_user_daily_cost_savings_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_cost_savings_user_date ON public.stats_user_daily_cost_savings USING btree (user_id, date);


--
-- Name: idx_stats_user_daily_model_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_model_date ON public.stats_user_daily_model USING btree (date);


--
-- Name: idx_stats_user_daily_model_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_model_provider_date ON public.stats_user_daily_model_provider USING btree (date);


--
-- Name: idx_stats_user_daily_model_provider_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_model_provider_user_date ON public.stats_user_daily_model_provider USING btree (user_id, date);


--
-- Name: idx_stats_user_daily_model_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_model_user_id ON public.stats_user_daily_model USING btree (user_id);


--
-- Name: idx_stats_user_daily_provider_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_provider_date ON public.stats_user_daily_provider USING btree (date);


--
-- Name: idx_stats_user_daily_provider_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_provider_user_id ON public.stats_user_daily_provider USING btree (user_id);


--
-- Name: idx_stats_user_daily_user_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_daily_user_date ON public.stats_user_daily USING btree (user_id, date);


--
-- Name: idx_stats_user_summary_cutoff_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_stats_user_summary_cutoff_date ON public.stats_user_summary USING btree (cutoff_date);


--
-- Name: idx_usage_api_family; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_api_family ON public.usage USING btree (api_family);


--
-- Name: idx_usage_api_format_created_id_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_api_format_created_id_desc ON public.usage USING btree (api_format, created_at DESC, id);


--
-- Name: idx_usage_apikey_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_apikey_created ON public.usage USING btree (api_key_id, created_at);


--
-- Name: idx_usage_billing_finalized_wallet; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_billing_finalized_wallet ON public.usage USING btree (billing_status, finalized_at, wallet_id);


--
-- Name: idx_usage_billing_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_billing_status ON public.usage USING btree (billing_status);


--
-- Name: idx_usage_created_id_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_created_id_desc ON public.usage USING btree (created_at DESC, id);


--
-- Name: idx_usage_endpoint_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_endpoint_kind ON public.usage USING btree (endpoint_kind);


--
-- Name: idx_usage_error_category; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_error_category ON public.usage USING btree (error_category);


--
-- Name: idx_usage_family_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_family_kind ON public.usage USING btree (api_family, endpoint_kind);


--
-- Name: idx_usage_model_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_model_created ON public.usage USING btree (model, created_at);


--
-- Name: idx_usage_monitoring_errors_created_id_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_monitoring_errors_created_id_desc ON public.usage USING btree (created_at DESC, id) WHERE ((lower(btrim((COALESCE(status, ''::character varying))::text)) = ANY (ARRAY['failed'::text, 'error'::text])) OR ((error_category IS NOT NULL) AND (btrim((error_category)::text) <> ''::text)) OR ((btrim((COALESCE(status, ''::character varying))::text) = ''::text) AND ((COALESCE(status_code, 0) >= 400) OR ((error_message IS NOT NULL) AND (btrim(error_message) <> ''::text)))));


--
-- Name: idx_usage_provider_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_provider_created ON public.usage USING btree (provider_name, created_at);


--
-- Name: idx_usage_provider_key; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_provider_key ON public.usage USING btree (provider_id, provider_api_key_id);


--
-- Name: idx_usage_provider_model_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_provider_model_created ON public.usage USING btree (provider_name, model, created_at);


--
-- Name: idx_usage_settlement_dashboard_cover_v2; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_settlement_dashboard_cover_v2 ON public.usage_settlement_snapshots USING btree (request_id) INCLUDE (billing_input_tokens, billing_effective_input_tokens, billing_output_tokens, billing_cache_creation_tokens, billing_cache_creation_5m_tokens, billing_cache_creation_1h_tokens, billing_cache_read_tokens, billing_total_input_context, billing_cache_creation_cost_usd, billing_cache_read_cost_usd, billing_total_cost_usd, billing_actual_total_cost_usd, input_price_per_1m, billing_status, allocation_status);


--
-- Name: idx_usage_stale_pending_created_request; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_stale_pending_created_request ON public.usage USING btree (created_at, request_id) WHERE ((status)::text = ANY ((ARRAY['pending'::character varying, 'streaming'::character varying])::text[]));


--
-- Name: idx_usage_status_created_id_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_status_created_id_desc ON public.usage USING btree (status, created_at DESC, id);


--
-- Name: idx_usage_status_user_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_status_user_created ON public.usage USING btree (status, user_id, created_at);


--
-- Name: idx_usage_user_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_user_created ON public.usage USING btree (user_id, created_at);


--
-- Name: idx_usage_user_created_id_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_user_created_id_desc ON public.usage USING btree (user_id, created_at DESC, id);


--
-- Name: idx_usage_wallet_finalized; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_usage_wallet_finalized ON public.usage USING btree (wallet_id, finalized_at);


--
-- Name: idx_user_model_usage_model; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_model_usage_model ON public.user_model_usage_counts USING btree (model);


--
-- Name: idx_user_model_usage_user; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_model_usage_user ON public.user_model_usage_counts USING btree (user_id);


--
-- Name: idx_user_plan_entitlements_order; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_plan_entitlements_order ON public.user_plan_entitlements USING btree (payment_order_id);


--
-- Name: idx_user_plan_entitlements_user_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_plan_entitlements_user_active ON public.user_plan_entitlements USING btree (user_id, status, expires_at);


--
-- Name: idx_user_referrals_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_referrals_created ON public.user_referrals USING btree (created_at DESC);


--
-- Name: idx_user_referrals_invite_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_referrals_invite_code ON public.user_referrals USING btree (invite_code_snapshot);


--
-- Name: idx_user_referrals_inviter; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_referrals_inviter ON public.user_referrals USING btree (inviter_user_id, created_at DESC);


--
-- Name: idx_user_sessions_user_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_sessions_user_active ON public.user_sessions USING btree (user_id, revoked_at, expires_at);


--
-- Name: idx_user_sessions_user_device; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_user_sessions_user_device ON public.user_sessions USING btree (user_id, client_device_id);


--
-- Name: idx_video_tasks_due_poll; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_video_tasks_due_poll ON public.video_tasks USING btree (status, next_poll_at, updated_at) WHERE (next_poll_at IS NOT NULL);


--
-- Name: idx_video_tasks_external_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_video_tasks_external_id ON public.video_tasks USING btree (external_task_id);


--
-- Name: idx_video_tasks_next_poll; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_video_tasks_next_poll ON public.video_tasks USING btree (next_poll_at);


--
-- Name: idx_video_tasks_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_video_tasks_request_id ON public.video_tasks USING btree (request_id);


--
-- Name: idx_video_tasks_user_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_video_tasks_user_status ON public.video_tasks USING btree (user_id, status);


--
-- Name: idx_wallet_daily_usage_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallet_daily_usage_date ON public.wallet_daily_usage_ledgers USING btree (billing_date);


--
-- Name: idx_wallet_daily_usage_wallet_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallet_daily_usage_wallet_date ON public.wallet_daily_usage_ledgers USING btree (wallet_id, billing_date);


--
-- Name: idx_wallet_tx_category_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallet_tx_category_created ON public.wallet_transactions USING btree (category, created_at);


--
-- Name: idx_wallet_tx_link; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallet_tx_link ON public.wallet_transactions USING btree (link_type, link_id);


--
-- Name: idx_wallet_tx_reason_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallet_tx_reason_created ON public.wallet_transactions USING btree (reason_code, created_at);


--
-- Name: idx_wallet_tx_wallet_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallet_tx_wallet_created ON public.wallet_transactions USING btree (wallet_id, created_at);


--
-- Name: idx_wallets_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_wallets_status ON public.wallets USING btree (status);


--
-- Name: idx_window_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_window_time ON public.provider_usage_tracking USING btree (window_start, window_end);


--
-- Name: ix_announcement_reads_announcement_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_announcement_reads_announcement_id ON public.announcement_reads USING btree (announcement_id);


--
-- Name: ix_announcement_reads_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_announcement_reads_id ON public.announcement_reads USING btree (id);


--
-- Name: ix_announcements_author_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_announcements_author_id ON public.announcements USING btree (author_id);


--
-- Name: ix_announcements_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_announcements_created_at ON public.announcements USING btree (created_at);


--
-- Name: ix_announcements_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_announcements_id ON public.announcements USING btree (id);


--
-- Name: ix_announcements_is_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_announcements_is_active ON public.announcements USING btree (is_active);


--
-- Name: ix_api_key_provider_mappings_api_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_api_key_provider_mappings_api_key_id ON public.api_key_provider_mappings USING btree (api_key_id);


--
-- Name: ix_api_key_provider_mappings_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_api_key_provider_mappings_id ON public.api_key_provider_mappings USING btree (id);


--
-- Name: ix_api_key_provider_mappings_provider_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_api_key_provider_mappings_provider_id ON public.api_key_provider_mappings USING btree (provider_id);


--
-- Name: ix_api_keys_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_api_keys_id ON public.api_keys USING btree (id);


--
-- Name: ix_api_keys_key_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_api_keys_key_hash ON public.api_keys USING btree (key_hash);


--
-- Name: ix_api_keys_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_api_keys_user_id ON public.api_keys USING btree (user_id);


--
-- Name: ix_audit_logs_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_logs_created_at ON public.audit_logs USING btree (created_at);


--
-- Name: ix_audit_logs_event_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_logs_event_type ON public.audit_logs USING btree (event_type);


--
-- Name: ix_audit_logs_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_logs_id ON public.audit_logs USING btree (id);


--
-- Name: ix_audit_logs_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_logs_request_id ON public.audit_logs USING btree (request_id);


--
-- Name: ix_audit_logs_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_audit_logs_user_id ON public.audit_logs USING btree (user_id);


--
-- Name: ix_gemini_file_mappings_file_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_gemini_file_mappings_file_name ON public.gemini_file_mappings USING btree (file_name);


--
-- Name: ix_gemini_file_mappings_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gemini_file_mappings_id ON public.gemini_file_mappings USING btree (id);


--
-- Name: ix_gemini_file_mappings_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gemini_file_mappings_key_id ON public.gemini_file_mappings USING btree (key_id);


--
-- Name: ix_gemini_file_mappings_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_gemini_file_mappings_user_id ON public.gemini_file_mappings USING btree (user_id);


--
-- Name: ix_global_models_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_global_models_id ON public.global_models USING btree (id);


--
-- Name: ix_global_models_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_global_models_name ON public.global_models USING btree (name);


--
-- Name: ix_global_models_usage_count; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_global_models_usage_count ON public.global_models USING btree (usage_count);


--
-- Name: ix_management_tokens_token_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_management_tokens_token_hash ON public.management_tokens USING btree (token_hash);


--
-- Name: ix_models_global_model_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_models_global_model_id ON public.models USING btree (global_model_id);


--
-- Name: ix_models_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_models_id ON public.models USING btree (id);


--
-- Name: ix_payment_callbacks_payment_order_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_payment_callbacks_payment_order_id ON public.payment_callbacks USING btree (payment_order_id);


--
-- Name: ix_provider_api_keys_circuit_breaker_by_format; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_api_keys_circuit_breaker_by_format ON public.provider_api_keys USING gin (circuit_breaker_by_format);


--
-- Name: ix_provider_api_keys_health_by_format; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_api_keys_health_by_format ON public.provider_api_keys USING gin (health_by_format);


--
-- Name: ix_provider_api_keys_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_api_keys_id ON public.provider_api_keys USING btree (id);


--
-- Name: ix_provider_endpoints_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_endpoints_id ON public.provider_endpoints USING btree (id);


--
-- Name: ix_provider_expenses_paid_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_expenses_paid_at ON public.provider_expenses USING btree (paid_at, id);


--
-- Name: ix_provider_expenses_provider_paid_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_expenses_provider_paid_at ON public.provider_expenses USING btree (provider_id, paid_at);


--
-- Name: ix_provider_usage_tracking_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_usage_tracking_id ON public.provider_usage_tracking USING btree (id);


--
-- Name: ix_provider_usage_tracking_provider_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_usage_tracking_provider_id ON public.provider_usage_tracking USING btree (provider_id);


--
-- Name: ix_provider_usage_tracking_window_start; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_provider_usage_tracking_window_start ON public.provider_usage_tracking USING btree (window_start);


--
-- Name: ix_providers_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_providers_id ON public.providers USING btree (id);


--
-- Name: ix_providers_name; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_providers_name ON public.providers USING btree (name);


--
-- Name: ix_proxy_nodes_registered_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_proxy_nodes_registered_by ON public.proxy_nodes USING btree (registered_by);


--
-- Name: ix_refund_requests_approved_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_refund_requests_approved_by ON public.refund_requests USING btree (approved_by);


--
-- Name: ix_refund_requests_payment_order_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_refund_requests_payment_order_id ON public.refund_requests USING btree (payment_order_id);


--
-- Name: ix_refund_requests_processed_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_refund_requests_processed_by ON public.refund_requests USING btree (processed_by);


--
-- Name: ix_refund_requests_requested_by; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_refund_requests_requested_by ON public.refund_requests USING btree (requested_by);


--
-- Name: ix_request_candidates_api_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_request_candidates_api_key_id ON public.request_candidates USING btree (api_key_id);


--
-- Name: ix_request_candidates_endpoint_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_request_candidates_endpoint_id ON public.request_candidates USING btree (endpoint_id);


--
-- Name: ix_request_candidates_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_request_candidates_key_id ON public.request_candidates USING btree (key_id);


--
-- Name: ix_request_candidates_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_request_candidates_request_id ON public.request_candidates USING btree (request_id);


--
-- Name: ix_request_candidates_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_request_candidates_user_id ON public.request_candidates USING btree (user_id);


--
-- Name: ix_stats_bucket_state_dirty; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_stats_bucket_state_dirty ON public.stats_bucket_state USING btree (bucket_start) WHERE (source_revision > built_revision);


--
-- Name: ix_stats_daily_date; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_stats_daily_date ON public.stats_daily USING btree (date);


--
-- Name: ix_stats_overview_dirty_events_bucket; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_stats_overview_dirty_events_bucket ON public.stats_overview_dirty_events USING btree (projection_version, granularity, bucket_start);


--
-- Name: ix_stats_user_daily_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_stats_user_daily_date ON public.stats_user_daily USING btree (date);


--
-- Name: ix_system_configs_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_system_configs_id ON public.system_configs USING btree (id);


--
-- Name: ix_usage_analytics_actor_metadata; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_analytics_actor_metadata ON public.usage USING btree (((request_metadata #>> '{analytics_attribution,actor_user_id}'::text[]))) WHERE ((request_metadata #>> '{analytics_attribution,actor_user_id}'::text[]) IS NOT NULL);


--
-- Name: ix_usage_attribution_actor_request; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_attribution_actor_request ON public.usage_attribution_snapshots USING btree (actor_user_id, request_id);


--
-- Name: ix_usage_attribution_owner_request; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_attribution_owner_request ON public.usage_attribution_snapshots USING btree (credential_owner_id, request_id);


--
-- Name: ix_usage_counter_deltas_processed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_counter_deltas_processed ON public.usage_counter_deltas USING btree (processed_at, created_at, id) WHERE (processed_at IS NOT NULL);


--
-- Name: ix_usage_counter_deltas_request_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_counter_deltas_request_kind ON public.usage_counter_deltas USING btree (request_id, kind, target_id);


--
-- Name: ix_usage_counter_deltas_unprocessed; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_counter_deltas_unprocessed ON public.usage_counter_deltas USING btree (created_at, id) WHERE (processed_at IS NULL);


--
-- Name: ix_usage_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_created_at ON public.usage USING btree (created_at);


--
-- Name: ix_usage_http_bodies_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_http_bodies_created_at ON public.usage_http_bodies USING btree (created_at);


--
-- Name: ix_usage_http_headers_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_http_headers_created_at ON public.usage_http_headers USING btree (created_at);


--
-- Name: ix_usage_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_id ON public.usage USING btree (id);


--
-- Name: ix_usage_provider_api_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_provider_api_key_id ON public.usage USING btree (provider_api_key_id);


--
-- Name: ix_usage_provider_endpoint_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_provider_endpoint_id ON public.usage USING btree (provider_endpoint_id);


--
-- Name: ix_usage_request_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_usage_request_id ON public.usage USING btree (request_id);


--
-- Name: ix_usage_routing_snapshots_candidate_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_routing_snapshots_candidate_id ON public.usage_routing_snapshots USING btree (candidate_id);


--
-- Name: ix_usage_routing_snapshots_route_family_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_routing_snapshots_route_family_kind ON public.usage_routing_snapshots USING btree (route_family, route_kind);


--
-- Name: ix_usage_settlement_snapshots_billing_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_settlement_snapshots_billing_status ON public.usage_settlement_snapshots USING btree (billing_status);


--
-- Name: ix_usage_settlement_snapshots_pricing_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_settlement_snapshots_pricing_source ON public.usage_settlement_snapshots USING btree (billing_pricing_source);


--
-- Name: ix_usage_settlement_snapshots_schema_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_settlement_snapshots_schema_version ON public.usage_settlement_snapshots USING btree (settlement_snapshot_schema_version);


--
-- Name: ix_usage_settlement_snapshots_wallet_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_settlement_snapshots_wallet_id ON public.usage_settlement_snapshots USING btree (wallet_id);


--
-- Name: ix_usage_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_status ON public.usage USING btree (status);


--
-- Name: ix_usage_wallet_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_usage_wallet_id ON public.usage USING btree (wallet_id);


--
-- Name: ix_user_oauth_links_provider_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_user_oauth_links_provider_type ON public.user_oauth_links USING btree (provider_type);


--
-- Name: ix_user_oauth_links_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_user_oauth_links_user_id ON public.user_oauth_links USING btree (user_id);


--
-- Name: ix_user_preferences_default_provider_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_user_preferences_default_provider_id ON public.user_preferences USING btree (default_provider_id);


--
-- Name: ix_user_preferences_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_user_preferences_id ON public.user_preferences USING btree (id);


--
-- Name: ix_user_sessions_client_device_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_user_sessions_client_device_id ON public.user_sessions USING btree (client_device_id);


--
-- Name: ix_user_sessions_user_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_user_sessions_user_id ON public.user_sessions USING btree (user_id);


--
-- Name: ix_users_email; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_users_email ON public.users USING btree (email);


--
-- Name: ix_users_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_users_id ON public.users USING btree (id);


--
-- Name: ix_users_ldap_dn; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_users_ldap_dn ON public.users USING btree (ldap_dn);


--
-- Name: ix_users_ldap_username; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_users_ldap_username ON public.users USING btree (ldap_username);


--
-- Name: ix_users_username; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_users_username ON public.users USING btree (username);


--
-- Name: ix_video_tasks_api_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_video_tasks_api_key_id ON public.video_tasks USING btree (api_key_id);


--
-- Name: ix_video_tasks_endpoint_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_video_tasks_endpoint_id ON public.video_tasks USING btree (endpoint_id);


--
-- Name: ix_video_tasks_key_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_video_tasks_key_id ON public.video_tasks USING btree (key_id);


--
-- Name: ix_video_tasks_provider_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_video_tasks_provider_id ON public.video_tasks USING btree (provider_id);


--
-- Name: ix_video_tasks_remixed_from_task_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_video_tasks_remixed_from_task_id ON public.video_tasks USING btree (remixed_from_task_id);


--
-- Name: ix_video_tasks_short_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ix_video_tasks_short_id ON public.video_tasks USING btree (short_id);


--
-- Name: ix_wallet_transactions_operator_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_wallet_transactions_operator_id ON public.wallet_transactions USING btree (operator_id);


--
-- Name: pool_member_scores_member_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pool_member_scores_member_idx ON public.pool_member_scores USING btree (pool_kind, pool_id, member_kind, member_id);


--
-- Name: pool_member_scores_probe_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pool_member_scores_probe_idx ON public.pool_member_scores USING btree (pool_kind, pool_id, probe_status, last_probe_success_at);


--
-- Name: pool_member_scores_rank_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pool_member_scores_rank_idx ON public.pool_member_scores USING btree (pool_kind, pool_id, capability, scope_kind, scope_id, hard_state, score DESC);


--
-- Name: pool_member_scores_scheduler_account_rank_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pool_member_scores_scheduler_account_rank_idx ON public.pool_member_scores USING btree (pool_kind, pool_id, capability, scope_kind, score DESC, last_ranked_at DESC NULLS LAST, member_id, id) WHERE ((scope_id IS NULL) AND ((hard_state)::text = ANY ((ARRAY['available'::character varying, 'unknown'::character varying])::text[])));


--
-- Name: pool_member_scores_updated_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX pool_member_scores_updated_at_idx ON public.pool_member_scores USING btree (updated_at);


--
-- Name: routing_group_bindings_group_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX routing_group_bindings_group_id_idx ON public.routing_group_bindings USING btree (group_id);


--
-- Name: routing_group_bindings_subject_default_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX routing_group_bindings_subject_default_key ON public.routing_group_bindings USING btree (subject_type, subject_id) WHERE (is_default = true);


--
-- Name: routing_group_bindings_subject_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX routing_group_bindings_subject_idx ON public.routing_group_bindings USING btree (subject_type, subject_id);


--
-- Name: routing_group_versions_group_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX routing_group_versions_group_id_idx ON public.routing_group_versions USING btree (group_id);


--
-- Name: routing_groups_enabled_sort_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX routing_groups_enabled_sort_idx ON public.routing_groups USING btree (enabled DESC, sort_order, name, id);


--
-- Name: routing_groups_one_system_default_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX routing_groups_one_system_default_key ON public.routing_groups USING btree (is_system_default) WHERE (is_system_default = true);


--
-- Name: routing_groups_system_default_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX routing_groups_system_default_idx ON public.routing_groups USING btree (is_system_default, enabled);


--
-- Name: uq_billing_rules_global_model_task; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_billing_rules_global_model_task ON public.billing_rules USING btree (global_model_id, task_type) WHERE ((is_enabled = true) AND (global_model_id IS NOT NULL));


--
-- Name: uq_billing_rules_model_task; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_billing_rules_model_task ON public.billing_rules USING btree (model_id, task_type) WHERE ((is_enabled = true) AND (model_id IS NOT NULL));


--
-- Name: uq_dimension_collectors_enabled; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_dimension_collectors_enabled ON public.dimension_collectors USING btree (api_format, task_type, dimension_name, priority) WHERE (is_enabled = true);


--
-- Name: uq_payment_orders_payment_method_gateway_order_id; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_payment_orders_payment_method_gateway_order_id ON public.payment_orders USING btree (payment_method, gateway_order_id);


--
-- Name: usage_cost_reservations_request_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_cost_reservations_request_id_idx ON public.usage_cost_reservations USING btree (request_id);


--
-- Name: usage_cost_reservations_reservation_expires_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_cost_reservations_reservation_expires_at_idx ON public.usage_cost_reservations USING btree (reservation_expires_at);


--
-- Name: usage_cost_reservations_retain_until_token_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_cost_reservations_retain_until_token_idx ON public.usage_cost_reservations USING btree (retain_until, reservation_token);


--
-- Name: usage_cost_reservations_subject_admitted_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_cost_reservations_subject_admitted_at_idx ON public.usage_cost_reservations USING btree (subject_id, admitted_at);


--
-- Name: usage_request_admissions_retain_until_token_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_request_admissions_retain_until_token_idx ON public.usage_request_admissions USING btree (retain_until, event_token);


--
-- Name: usage_request_admissions_subject_admitted_at_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX usage_request_admissions_subject_admitted_at_idx ON public.usage_request_admissions USING btree (subject_id, admitted_at);


--
-- Name: user_group_members_user_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX user_group_members_user_id_idx ON public.user_group_members USING btree (user_id);


--
-- Name: user_groups_priority_name_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX user_groups_priority_name_idx ON public.user_groups USING btree (priority DESC, name, id);


--
-- Name: usage aaa_dashboard_usage_deleted; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER aaa_dashboard_usage_deleted BEFORE DELETE ON public.usage FOR EACH ROW EXECUTE FUNCTION public.dashboard_enqueue_request();


--
-- Name: usage_attribution_snapshots dashboard_attribution_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER dashboard_attribution_changed AFTER INSERT OR UPDATE ON public.usage_attribution_snapshots FOR EACH ROW EXECUTE FUNCTION public.dashboard_enqueue_request();


--
-- Name: dashboard_stats_pending dashboard_flush_pending; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER dashboard_flush_pending AFTER INSERT ON public.dashboard_stats_pending DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.dashboard_apply_pending();


--
-- Name: dashboard_user_anonymization_pending dashboard_flush_user_anonymization; Type: TRIGGER; Schema: public; Owner: -
--

CREATE CONSTRAINT TRIGGER dashboard_flush_user_anonymization AFTER INSERT ON public.dashboard_user_anonymization_pending DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.dashboard_apply_pending();


--
-- Name: usage_settlement_snapshots dashboard_settlement_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER dashboard_settlement_changed AFTER INSERT OR DELETE OR UPDATE ON public.usage_settlement_snapshots FOR EACH ROW EXECUTE FUNCTION public.dashboard_enqueue_request();


--
-- Name: usage dashboard_usage_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER dashboard_usage_changed AFTER INSERT OR UPDATE ON public.usage FOR EACH ROW EXECUTE FUNCTION public.dashboard_enqueue_request();


--
-- Name: users dashboard_user_anonymize; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER dashboard_user_anonymize AFTER DELETE OR UPDATE OF is_deleted ON public.users FOR EACH ROW EXECUTE FUNCTION public.dashboard_enqueue_user_anonymization();


--
-- Name: users dashboard_user_changed; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER dashboard_user_changed AFTER INSERT OR DELETE OR UPDATE OF is_deleted ON public.users FOR EACH ROW EXECUTE FUNCTION public.dashboard_record_user_event();


--
-- Name: usage_attribution_snapshots overview_attribution_dirty; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_attribution_dirty AFTER INSERT OR DELETE OR UPDATE ON public.usage_attribution_snapshots FOR EACH ROW EXECUTE FUNCTION public.overview_mark_usage_bucket();


--
-- Name: usage_settlement_snapshots overview_settlement_dirty; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_settlement_dirty AFTER INSERT OR DELETE OR UPDATE ON public.usage_settlement_snapshots FOR EACH ROW EXECUTE FUNCTION public.overview_mark_usage_bucket();


--
-- Name: usage overview_usage_delete_attribution; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_usage_delete_attribution BEFORE DELETE ON public.usage FOR EACH ROW EXECUTE FUNCTION public.overview_delete_attribution();


--
-- Name: usage overview_usage_dirty; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_usage_dirty AFTER INSERT OR DELETE OR UPDATE ON public.usage FOR EACH ROW EXECUTE FUNCTION public.overview_mark_usage_bucket();


--
-- Name: usage overview_usage_failure; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_usage_failure BEFORE INSERT OR UPDATE OF request_metadata, status ON public.usage FOR EACH ROW EXECUTE FUNCTION public.overview_capture_failure();


--
-- Name: usage overview_usage_identity; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_usage_identity AFTER INSERT OR UPDATE OF user_id, api_key_id, request_metadata ON public.usage FOR EACH ROW EXECUTE FUNCTION public.overview_capture_usage_identity();


--
-- Name: users overview_user_anonymize; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER overview_user_anonymize AFTER DELETE OR UPDATE OF is_deleted ON public.users FOR EACH ROW EXECUTE FUNCTION public.overview_anonymize_user();


--
-- Name: announcement_reads announcement_reads_announcement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.announcement_reads
    ADD CONSTRAINT announcement_reads_announcement_id_fkey FOREIGN KEY (announcement_id) REFERENCES public.announcements(id) ON DELETE CASCADE;


--
-- Name: announcement_reads announcement_reads_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.announcement_reads
    ADD CONSTRAINT announcement_reads_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: api_key_provider_mappings api_key_provider_mappings_api_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_provider_mappings
    ADD CONSTRAINT api_key_provider_mappings_api_key_id_fkey FOREIGN KEY (api_key_id) REFERENCES public.api_keys(id) ON DELETE CASCADE;


--
-- Name: api_key_provider_mappings api_key_provider_mappings_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_key_provider_mappings
    ADD CONSTRAINT api_key_provider_mappings_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: api_keys api_keys_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: background_task_events background_task_events_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.background_task_events
    ADD CONSTRAINT background_task_events_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.background_task_runs(id) ON DELETE CASCADE;


--
-- Name: billing_rules billing_rules_global_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_rules
    ADD CONSTRAINT billing_rules_global_model_id_fkey FOREIGN KEY (global_model_id) REFERENCES public.global_models(id) ON DELETE CASCADE;


--
-- Name: billing_rules billing_rules_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.billing_rules
    ADD CONSTRAINT billing_rules_model_id_fkey FOREIGN KEY (model_id) REFERENCES public.models(id) ON DELETE CASCADE;


--
-- Name: entitlement_usage_ledgers entitlement_usage_ledgers_user_entitlement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entitlement_usage_ledgers
    ADD CONSTRAINT entitlement_usage_ledgers_user_entitlement_id_fkey FOREIGN KEY (user_entitlement_id) REFERENCES public.user_plan_entitlements(id) ON DELETE CASCADE;


--
-- Name: provider_api_keys fk_provider_api_keys_provider; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_api_keys
    ADD CONSTRAINT fk_provider_api_keys_provider FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: gemini_file_mappings gemini_file_mappings_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gemini_file_mappings
    ADD CONSTRAINT gemini_file_mappings_key_id_fkey FOREIGN KEY (key_id) REFERENCES public.provider_api_keys(id) ON DELETE CASCADE;


--
-- Name: gemini_file_mappings gemini_file_mappings_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.gemini_file_mappings
    ADD CONSTRAINT gemini_file_mappings_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: management_tokens management_tokens_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.management_tokens
    ADD CONSTRAINT management_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: models models_global_model_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.models
    ADD CONSTRAINT models_global_model_id_fkey FOREIGN KEY (global_model_id) REFERENCES public.global_models(id);


--
-- Name: models models_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.models
    ADD CONSTRAINT models_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id);


--
-- Name: payment_callbacks payment_callbacks_payment_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_callbacks
    ADD CONSTRAINT payment_callbacks_payment_order_id_fkey FOREIGN KEY (payment_order_id) REFERENCES public.payment_orders(id) ON DELETE SET NULL;


--
-- Name: payment_orders payment_orders_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payment_orders
    ADD CONSTRAINT payment_orders_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id) ON DELETE RESTRICT;


--
-- Name: provider_endpoints provider_endpoints_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_endpoints
    ADD CONSTRAINT provider_endpoints_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: provider_usage_tracking provider_usage_tracking_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.provider_usage_tracking
    ADD CONSTRAINT provider_usage_tracking_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: redeem_codes redeem_codes_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redeem_codes
    ADD CONSTRAINT redeem_codes_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.redeem_code_batches(id) ON DELETE CASCADE;


--
-- Name: redeem_codes redeem_codes_redeemed_payment_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redeem_codes
    ADD CONSTRAINT redeem_codes_redeemed_payment_order_id_fkey FOREIGN KEY (redeemed_payment_order_id) REFERENCES public.payment_orders(id) ON DELETE SET NULL;


--
-- Name: redeem_codes redeem_codes_redeemed_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.redeem_codes
    ADD CONSTRAINT redeem_codes_redeemed_wallet_id_fkey FOREIGN KEY (redeemed_wallet_id) REFERENCES public.wallets(id) ON DELETE SET NULL;


--
-- Name: referral_rewards referral_rewards_referral_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.referral_rewards
    ADD CONSTRAINT referral_rewards_referral_id_fkey FOREIGN KEY (referral_id) REFERENCES public.user_referrals(id) ON DELETE CASCADE;


--
-- Name: referral_rewards referral_rewards_source_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.referral_rewards
    ADD CONSTRAINT referral_rewards_source_order_id_fkey FOREIGN KEY (source_order_id) REFERENCES public.payment_orders(id) ON DELETE SET NULL;


--
-- Name: refund_requests refund_requests_payment_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refund_requests
    ADD CONSTRAINT refund_requests_payment_order_id_fkey FOREIGN KEY (payment_order_id) REFERENCES public.payment_orders(id) ON DELETE SET NULL;


--
-- Name: refund_requests refund_requests_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.refund_requests
    ADD CONSTRAINT refund_requests_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id) ON DELETE RESTRICT;


--
-- Name: request_candidates request_candidates_endpoint_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_candidates
    ADD CONSTRAINT request_candidates_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES public.provider_endpoints(id) ON DELETE CASCADE;


--
-- Name: request_candidates request_candidates_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.request_candidates
    ADD CONSTRAINT request_candidates_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id) ON DELETE CASCADE;


--
-- Name: usage_cost_reservations usage_cost_reservations_subject_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_cost_reservations
    ADD CONSTRAINT usage_cost_reservations_subject_id_fkey FOREIGN KEY (subject_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: usage_http_bodies usage_http_bodies_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_http_bodies
    ADD CONSTRAINT usage_http_bodies_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;


--
-- Name: usage_http_headers usage_http_headers_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_http_headers
    ADD CONSTRAINT usage_http_headers_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;


--
-- Name: usage_request_admissions usage_request_admissions_subject_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_request_admissions
    ADD CONSTRAINT usage_request_admissions_subject_id_fkey FOREIGN KEY (subject_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: usage_routing_snapshots usage_routing_snapshots_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_routing_snapshots
    ADD CONSTRAINT usage_routing_snapshots_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;


--
-- Name: usage_settlement_snapshots usage_settlement_snapshots_request_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.usage_settlement_snapshots
    ADD CONSTRAINT usage_settlement_snapshots_request_id_fkey FOREIGN KEY (request_id) REFERENCES public.usage(request_id) ON DELETE CASCADE;


--
-- Name: user_group_members user_group_members_group_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_group_members
    ADD CONSTRAINT user_group_members_group_id_fkey FOREIGN KEY (group_id) REFERENCES public.user_groups(id) ON DELETE CASCADE;


--
-- Name: user_group_members user_group_members_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_group_members
    ADD CONSTRAINT user_group_members_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_invite_codes user_invite_codes_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_invite_codes
    ADD CONSTRAINT user_invite_codes_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_oauth_links user_oauth_links_provider_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_oauth_links
    ADD CONSTRAINT user_oauth_links_provider_type_fkey FOREIGN KEY (provider_type) REFERENCES public.oauth_providers(provider_type) ON DELETE CASCADE;


--
-- Name: user_oauth_links user_oauth_links_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_oauth_links
    ADD CONSTRAINT user_oauth_links_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_plan_entitlements user_plan_entitlements_payment_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_plan_entitlements
    ADD CONSTRAINT user_plan_entitlements_payment_order_id_fkey FOREIGN KEY (payment_order_id) REFERENCES public.payment_orders(id) ON DELETE RESTRICT;


--
-- Name: user_plan_entitlements user_plan_entitlements_plan_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_plan_entitlements
    ADD CONSTRAINT user_plan_entitlements_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES public.billing_plans(id) ON DELETE RESTRICT;


--
-- Name: user_preferences user_preferences_default_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_preferences
    ADD CONSTRAINT user_preferences_default_provider_id_fkey FOREIGN KEY (default_provider_id) REFERENCES public.providers(id);


--
-- Name: user_preferences user_preferences_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_preferences
    ADD CONSTRAINT user_preferences_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: user_referrals user_referrals_first_paid_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_referrals
    ADD CONSTRAINT user_referrals_first_paid_order_id_fkey FOREIGN KEY (first_paid_order_id) REFERENCES public.payment_orders(id) ON DELETE SET NULL;


--
-- Name: user_sessions user_sessions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_sessions
    ADD CONSTRAINT user_sessions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id) ON DELETE CASCADE;


--
-- Name: video_tasks video_tasks_endpoint_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_tasks
    ADD CONSTRAINT video_tasks_endpoint_id_fkey FOREIGN KEY (endpoint_id) REFERENCES public.provider_endpoints(id);


--
-- Name: video_tasks video_tasks_key_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_tasks
    ADD CONSTRAINT video_tasks_key_id_fkey FOREIGN KEY (key_id) REFERENCES public.provider_api_keys(id) ON DELETE SET NULL;


--
-- Name: video_tasks video_tasks_provider_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_tasks
    ADD CONSTRAINT video_tasks_provider_id_fkey FOREIGN KEY (provider_id) REFERENCES public.providers(id);


--
-- Name: video_tasks video_tasks_remixed_from_task_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.video_tasks
    ADD CONSTRAINT video_tasks_remixed_from_task_id_fkey FOREIGN KEY (remixed_from_task_id) REFERENCES public.video_tasks(id);


--
-- Name: wallet_daily_usage_ledgers wallet_daily_usage_ledgers_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_daily_usage_ledgers
    ADD CONSTRAINT wallet_daily_usage_ledgers_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id) ON DELETE CASCADE;


--
-- Name: wallet_transactions wallet_transactions_wallet_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.wallet_transactions
    ADD CONSTRAINT wallet_transactions_wallet_id_fkey FOREIGN KEY (wallet_id) REFERENCES public.wallets(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
