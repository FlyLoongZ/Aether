# Breaking Changes: Migration Compatibility Removal

This release removes the remaining compatibility paths for pre-migration
deployments. Removed inputs fail explicitly instead of silently falling back to
a default.

## Configuration

- PostgreSQL connection strings are read only from `DATABASE_URL`.
  `AETHER_DATABASE_URL` and `AETHER_GATEWAY_DATA_POSTGRES_URL` are no longer
  accepted.
- The gateway data encryption key is read only from `ENCRYPTION_KEY`.
  `AETHER_GATEWAY_DATA_ENCRYPTION_KEY` is no longer accepted.
  `AETHER_BACKUP_ENCRYPTION_KEY` remains the optional independent S3 backup key.
- `AETHER_GATEWAY_AUTO_PREPARE_DATABASE` and `--auto-prepare-database` are
  removed. Use `AETHER_GATEWAY_DATABASE_MODE=auto|verify-only` or
  `--database-mode`.
- `update.sh` only discovers `docker-compose.yml`; the former
  `docker-compose.single-node.yml` fallback is removed.


## Secrets

- New secret writes use the `aether-secret:v1` envelope: AES-256-GCM with a
  PBKDF2-HMAC-SHA256 (600k) root key, HKDF-SHA256 per-message keys, and
  associated data binding the envelope version, key id, and typed field
  context. `AETHER_SECRET`-style business prefixes are no longer added to new
  writes.
- Existing `aether-*-v2/v3/v4` prefixes and legacy Python-Fernet ciphertexts
  remain readable. They are lazy-migrated only on the existing read paths that
  already rewrite a record; there is no destructive bulk rewrite.
- System configuration secrets (`system_config`, SMTP, LDAP bind, Bark device
  key) are now written as `aether-secret:v1`; plaintext legacy values continue
  to migrate on read.
- S3 backup files now use the `v3` envelope and a 600k + HKDF-derived root key.
  Existing `v2` backup envelopes remain readable until the legacy Fernet
  compatibility layer is removed.

## Deployment and control plane

- The gateway runs in single-node mode only. `AETHER_GATEWAY_DEPLOYMENT_TOPOLOGY`,
  `AETHER_GATEWAY_NODE_ROLE`, split frontdoor/background roles, isolated
  background database pools, and distributed request/WebSocket gates are
  removed. `AETHER_GATEWAY_BACKGROUND_DB_MAX_CONNECTIONS` and
  `AETHER_GATEWAY_DISTRIBUTED_*` are no longer read.
- The `/api/internal/gateway/*` control plane and its
  `AETHER_INTERNAL_GATEWAY_AUTH_SECRET` HMAC secret are removed. Management and
  operational routes remain on their public/admin endpoints.
- The standalone execution-runtime server, its Unix/TCP serving entry points,
  and the `execution_runtime_harness` example are removed. Execution runs
  in-process.
- The macOS/launchd installer path is removed. `install.sh` supports Linux only;
  macOS users must use Docker or build from source.

## Schema maintenance

- The logical-schema compiler, `crates/aether-data/schema`,
  `runtime/schema/logical`, `runtime/schema/generated`,
  `runtime/schema/overrides`, and `compose_schema.sh` are removed.
  Baseline and bootstrap SQL are maintained directly as ordered fragments under
  `runtime/schema/drivers/postgres` and `runtime/schema/bootstrap/postgres`.

## Management tokens

- Tokens whose `permissions` value is SQL `NULL` are rejected during
  authentication. Legacy tokens must be reissued with explicit permissions;
  the former frozen full-permission fallback is removed.

## Users

- User exports use version `1.7`. Versions `1.5`, `1.6` and `1.7` are
  importable; per-user policy fields in older payloads are ignored.
- User imports may carry `password_hash` and API key material. Credentials are
  validated before use (bcrypt password hash, canonical lowercase SHA-256 key
  hash, plaintext matching the key hash) and are written as-is; payloads that
  declare `credential_state=not_exported` while carrying credentials are
  rejected. Payloads without credentials still import a tombstone, so the
  account must reset its password.
- Overwrite imports no longer disable existing API keys. A user export without
  credential material carries a tombstone hash and an `is_active` flag that says
  nothing about the key running in this deployment, so an existing key keeps its
  current hash and active state; only newly created keys are forced inactive
  when their credentials were not exported.
- Per-user access policies are no longer supported. User authorization is
  derived from user groups only.
- The `public.users` columns `allowed_providers`, `allowed_providers_mode`,
  `allowed_api_formats`, `allowed_api_formats_mode`, `allowed_models`,
  `allowed_models_mode`, `rate_limit`, and `rate_limit_mode` are dropped.
- Admin user responses no longer include top-level per-user policy fields;
  `effective_policy` remains the read-only, group-derived policy view.

## Request candidate traces

- Legacy candidate trace values `tunnel`, `tunnel_affinity`, `local_tunnel`,
  `control_execute_sync`, `control_execute_stream`, and
  `tunnel_affinity_forward` are no longer retained or displayed. The legacy
  `api_key_concurrency_limit_reached` skip reason is also removed.

## Video tasks

- AI video generation support is removed. The `openai:video`, `gemini:video`, and
  `xai:video` surfaces, `/v1/videos*`, `/openai/v1/videos*`, `/v1beta/operations*`,
  and `:predictLongRunning` are no longer served; the admin video-task APIs,
  `admin:video_tasks:*` permissions, and the async video-task poller are gone.
- Migration `20261119030000_remove_video_support` drops `public.video_tasks` and
  cleans video endpoints, provider models, billing rules/collectors, per-format
  credential grants, and video pricing from global model configs. Management tokens
  that only carried `admin:video_tasks:*` fall back to `admin:monitoring:read`.
- The same migration removes the video formats from `user_groups.allowed_api_formats`
  and deletes global models whose provider models all served video formats. Global
  models that still have another format attached are untouched (a legacy provider
  model row without an `api_format` also keeps its global model).
- Deleting the video provider endpoints also deletes the `request_candidates` rows that
  referenced them (`request_candidates_endpoint_id_fkey` is `ON DELETE CASCADE`), so
  candidate traces for historical video requests are gone. This matches what the admin
  endpoint delete does today. Usage rows keep their video `api_format` and cost history,
  because the usage identity foreign keys were decoupled in
  `20260522000000_decouple_usage_identity_foreign_keys`, and `usage.provider_endpoint_id`
  is a denormalized attribution column that intentionally survives endpoint deletion.
- `AETHER_GATEWAY_VIDEO_TASK_POLLER_INTERVAL_MS`,
  `AETHER_GATEWAY_VIDEO_TASK_POLLER_BATCH_SIZE`, and
  `AETHER_GATEWAY_VIDEO_TASK_STORE_PATH` are removed together with their CLI flags.

## Backups

- Backup restores accept the v2 and v3 AES-256-GCM envelopes with a key ID.
  Version 1 backups, the keyring `legacy_v1` field, and legacy v1 key
  candidates are no longer supported.
- New backups use the v3 envelope and a 600k + HKDF-derived root key.
- Automatic migration of legacy plaintext backup objects is removed.
  Pre-existing plaintext objects are left untouched for explicit operator
  cleanup.

## Configuration import

- Admin config exports accept only version `2.3`.
- User exports use `1.7`; versions `1.5` and `1.6` remain importable for
  recovery. Versions `1.3` and `1.4` are rejected.
- Legacy OAuth provider normalization for exports `2.0`-`2.2` is removed.

## Local proxy fallback

- The removed `local_proxy_passthrough_removed` execution path is gone. An
  unmatched local route now returns the ordinary `404 Route not found`
  response instead of `501`.

## Routing

- The `global_key` routing priority value is rejected. Routing groups always
  rank providers before keys. `disabled_providers` and
  `key_global_priority_by_format` remain supported.

## Usage

- Usage reads no longer coalesce deprecated `usage.*` mirror columns with the
  split audit/settlement tables. Historical rows without canonical satellite
  rows may display empty billing state.
- The retired legacy accounting columns (`input_output_total_tokens`,
  `input_context_tokens`, the unused split cost/price fields, and
  `created_at_unix_ms`), the legacy `usage` HTTP header mirrors, and the
  settlement wallet/rate/price/timestamp mirrors are dropped from
  `public.usage`. Fresh databases drop them while replaying the retirement
  migrations; upgraded databases drop them in the same retirement backfills.
- `usage_billing_facts` now reads wallet, rate, price, and finalized timestamp
  fields only from `usage_settlement_snapshots`. `billing_status` remains on
  `usage` as the write-arbitration state rather than a settlement mirror.
- The deprecated billing mirrors (`input_tokens`, `output_tokens`,
  `total_tokens`, the cache token variants, `input_cost_usd`, `output_cost_usd`,
  `cache_creation_cost_usd`, `cache_read_cost_usd`, `total_cost_usd`, and
  `actual_total_cost_usd`) are dropped from `public.usage`. Token and cost
  values are read only from `usage_settlement_snapshots`. Two kinds of
  historical row therefore display zero or empty values after the upgrade:
  rows with **no** settlement snapshot, and rows whose snapshot exists but kept
  `billing_*` columns `NULL` (early settlements only wrote the wallet fields).
  The drop runs before the values could be copied into the snapshot, so both
  cases are irreversible; the mirror columns were the last copy.
- `username` and `api_key_name` remain as historical display snapshots so
  audit rows stay readable after users or API keys are deleted.

## Wallet

- Wallet daily usage reads now use `APP_TIMEZONE` (the maintenance timezone)
  instead of the fixed `Asia/Shanghai` calendar. Historical ledger rows written
  under a different timezone are not rewritten, so those dates may no longer
  appear in the current calendar view.
- Wallet today windows, flow grouping, and zero-value today entries all use the
  configured timezone, including DST-aware local-midnight boundaries.

## Format conversion

- The public `convert_request` / `convert_response` legacy exports are removed.
  External callers must use the audited `*_pure` API and apply model/stream
  edits explicitly.

## Protocol state

- `/v1/responses`, `/v1/responses/compact`, `/v1/chat/completions` and
  `/v1/alpha/search` reject `store=true`, `background=true` and any
  `conversation` value. An explicit `store:null` is rewritten to `store:false`
  before dispatch so it cannot fall back to the upstream server-side default.
- Responses WebSocket events apply the same rule; a `store:null` event is
  normalized to `store:false`.
- Routing a Responses request to a non-Responses upstream now fails closed when
  the body carries `previous_response_id` (HTTP 400 /
  `LossyConversionBlocked`). It is no longer dropped silently, which used to
  return 200 with the conversation context lost.
- A WebSocket connection that resumes another connection's
  `previous_response_id` is rejected with `previous_response_not_found` and logs
  a `responses_websocket_previous_response_rejected` warning. Same-socket
  continuation keeps working.

