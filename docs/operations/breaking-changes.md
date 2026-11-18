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

## Video tasks

- The `python-sync-report` truth-source mode and
  `AETHER_GATEWAY_VIDEO_TASK_TRUTH_SOURCE_MODE` are removed. Video tasks always
  use the Rust-authoritative path with local polling and reads.
- The `aether-video-tasks-v1` encrypted file envelope is rejected. Only the v2
  envelope is readable.

## Backups

- Backup restores accept only the v2 AES-256-GCM envelope with a key ID.
  Version 1 backups, the keyring `legacy_v1` field, and legacy v1 key
  candidates are no longer supported.
- Legacy plaintext backup objects are still migrated to encrypted objects by
  the backup task; that security migration is independent of v1 restore.

## Configuration import

- Admin config exports accept only version `2.3`.
- User exports accept `1.5` (recovery backups) and `1.6`; versions `1.3` and
  `1.4` are rejected.
- Legacy OAuth provider normalization for exports `2.0`-`2.2` is removed.

## Routing

- The `global_key` routing priority value is rejected. Routing groups always
  rank providers before keys. `disabled_providers` and
  `key_global_priority_by_format` remain supported.

## Usage

- Usage reads no longer coalesce deprecated `usage.*` mirror columns with the
  split audit/settlement tables. Historical rows without canonical satellite
  rows may display empty billing state.
- The deprecated columns are not dropped by this change; a separate schema
  cleanup is required.

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

