-- Normalize every remaining `json` column to `jsonb`.
--
-- The request-record tables (usage, usage_http_audits, request_candidates) were
-- already converted by 20260923120000_normalize_usage_jsonb.sql. This migration
-- finishes the job for the rest of the catalog so the whole schema uses jsonb,
-- which supports equality, GIN indexes and the jsonb_* operator family.
--
-- The conversions are idempotent: re-running them against jsonb columns is a no-op.

ALTER TABLE public.api_keys
    ALTER COLUMN allowed_providers TYPE jsonb USING allowed_providers::jsonb,
    ALTER COLUMN allowed_api_formats TYPE jsonb USING allowed_api_formats::jsonb,
    ALTER COLUMN allowed_models TYPE jsonb USING allowed_models::jsonb,
    ALTER COLUMN force_capabilities TYPE jsonb USING force_capabilities::jsonb,
    ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;

ALTER TABLE public.audit_logs
    ALTER COLUMN event_metadata TYPE jsonb USING event_metadata::jsonb;

ALTER TABLE public.auth_modules
    ALTER COLUMN config TYPE jsonb USING config::jsonb;

ALTER TABLE public.global_models
    ALTER COLUMN default_tiered_pricing TYPE jsonb USING default_tiered_pricing::jsonb,
    ALTER COLUMN supported_capabilities TYPE jsonb USING supported_capabilities::jsonb,
    ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;

ALTER TABLE public.models
    ALTER COLUMN tiered_pricing TYPE jsonb USING tiered_pricing::jsonb,
    ALTER COLUMN config TYPE jsonb USING config::jsonb,
    ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;

ALTER TABLE public.oauth_providers
    ALTER COLUMN scopes TYPE jsonb USING scopes::jsonb,
    ALTER COLUMN attribute_mapping TYPE jsonb USING attribute_mapping::jsonb,
    ALTER COLUMN extra_config TYPE jsonb USING extra_config::jsonb;

ALTER TABLE public.provider_api_keys
    ALTER COLUMN allowed_models TYPE jsonb USING allowed_models::jsonb,
    ALTER COLUMN capabilities TYPE jsonb USING capabilities::jsonb,
    ALTER COLUMN adjustment_history TYPE jsonb USING adjustment_history::jsonb,
    ALTER COLUMN utilization_samples TYPE jsonb USING utilization_samples::jsonb,
    ALTER COLUMN api_formats TYPE jsonb USING api_formats::jsonb,
    ALTER COLUMN auth_type_by_format TYPE jsonb USING auth_type_by_format::jsonb,
    ALTER COLUMN allow_auth_channel_mismatch_formats TYPE jsonb USING allow_auth_channel_mismatch_formats::jsonb,
    ALTER COLUMN rate_multipliers TYPE jsonb USING rate_multipliers::jsonb,
    ALTER COLUMN locked_models TYPE jsonb USING locked_models::jsonb,
    ALTER COLUMN global_priority_by_format TYPE jsonb USING global_priority_by_format::jsonb,
    ALTER COLUMN model_include_patterns TYPE jsonb USING model_include_patterns::jsonb,
    ALTER COLUMN model_exclude_patterns TYPE jsonb USING model_exclude_patterns::jsonb,
    ALTER COLUMN proxy TYPE jsonb USING proxy::jsonb,
    ALTER COLUMN fingerprint TYPE jsonb USING fingerprint::jsonb,
    ALTER COLUMN status_snapshot TYPE jsonb USING status_snapshot::jsonb,
    ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;

ALTER TABLE public.provider_endpoints
    ALTER COLUMN config TYPE jsonb USING config::jsonb,
    ALTER COLUMN header_rules TYPE jsonb USING header_rules::jsonb,
    ALTER COLUMN format_acceptance_config TYPE jsonb USING format_acceptance_config::jsonb,
    ALTER COLUMN body_rules TYPE jsonb USING body_rules::jsonb,
    ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;

ALTER TABLE public.providers
    ALTER COLUMN config TYPE jsonb USING config::jsonb;

ALTER TABLE public.proxy_node_events
    ALTER COLUMN event_metadata TYPE jsonb USING event_metadata::jsonb;

ALTER TABLE public.proxy_nodes
    ALTER COLUMN remote_config TYPE jsonb USING remote_config::jsonb,
    ALTER COLUMN hardware_info TYPE jsonb USING hardware_info::jsonb,
    ALTER COLUMN proxy_metadata TYPE jsonb USING proxy_metadata::jsonb;

ALTER TABLE public.user_oauth_links
    ALTER COLUMN extra_data TYPE jsonb USING extra_data::jsonb;

ALTER TABLE public.user_sessions
    ALTER COLUMN client_hints TYPE jsonb USING client_hints::jsonb;

ALTER TABLE public.system_configs
    ALTER COLUMN value TYPE jsonb USING value::jsonb;

ALTER TABLE public.users
    ALTER COLUMN allowed_providers TYPE jsonb USING allowed_providers::jsonb,
    ALTER COLUMN allowed_api_formats TYPE jsonb USING allowed_api_formats::jsonb,
    ALTER COLUMN allowed_models TYPE jsonb USING allowed_models::jsonb,
    ALTER COLUMN model_capability_settings TYPE jsonb USING model_capability_settings::jsonb,
    ALTER COLUMN metadata TYPE jsonb USING metadata::jsonb;

ALTER TABLE public.user_groups
    ALTER COLUMN allowed_providers TYPE jsonb USING allowed_providers::jsonb,
    ALTER COLUMN allowed_api_formats TYPE jsonb USING allowed_api_formats::jsonb,
    ALTER COLUMN allowed_models TYPE jsonb USING allowed_models::jsonb;

ALTER TABLE public.video_tasks
    ALTER COLUMN original_request_body TYPE jsonb USING original_request_body::jsonb,
    ALTER COLUMN converted_request_body TYPE jsonb USING converted_request_body::jsonb,
    ALTER COLUMN video_urls TYPE jsonb USING video_urls::jsonb,
    ALTER COLUMN request_metadata TYPE jsonb USING request_metadata::jsonb;

