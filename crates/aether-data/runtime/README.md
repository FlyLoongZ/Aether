# aether-data

`aether-data` is the runtime data-access crate. It owns concrete SQL/database
drivers, concrete repository implementations, migration/backfill/export
workflows, and the composition layer that wires those pieces into the rest of
the application.

It does not own the cross-crate DTO contracts. Shared repository records and
errors that are consumed by scheduler, billing, admin, usage runtime, and video
task crates live in the sibling `../contracts` crate (`aether-data-contracts`).

## Directory Map

| Path | Responsibility |
|---|---|
| `src/database.rs` | PostgreSQL pool configuration and URL validation. |
| `src/config.rs` | Data-layer config for the PostgreSQL backend and repository wiring. |
| `src/maintenance.rs` | Maintenance DTOs and aggregation summaries used by backend dispatch and runtime maintenance entrypoints. |
| `src/driver/postgres.rs` | Thin compatibility facades for the selected adapter crates. Low-level pools, transactions, and leases live in `aether-data-postgres`. |
| `src/repository` | Compatibility modules that re-export contracts and selected SQL adapters, plus concrete in-memory implementations. Driver-specific request-path SQL lives in the adapter crates. |
| `src/backend` | Composition root. Builds concrete driver backends and exposes app-facing read/write/worker/lock/lease handles. |
| `src/backend/{maintenance,stats,wallet,system}.rs` | Backend-owned maintenance, aggregation, wallet ledger, and system config workflows that are not normal request-path repositories. |
| `src/lifecycle/migrate.rs` and `src/lifecycle/migrate/*` | Compatibility entry points and Postgres snapshot bootstrap adapter. |
| `src/lifecycle/backfill.rs` | Backfill entry points and backfill discovery. |
| `src/lifecycle/export.rs` | Cross-database export/import workflows. |
| `../adapters/postgres/migrations` | Executable `sqlx` migrations owned and embedded by each adapter crate. |
| `schema` | Schema fragment workspace used to assemble the baseline migration and bootstrap snapshot. |
| `schema/drivers/postgres` | Human-maintained driver fragments composed into the executable baseline migration. |
| `schema/bootstrap/postgres` | Postgres bootstrap fragments, seed rows, and SQLx search-path footer. `build.rs` composes them into the runtime embedded artifact. |
| `backfills/postgres` | Executable backfill SQL grouped by driver. |

## Layering Rules

The crate is easiest to read as five layers:

1. Contracts: DTOs, input structs, repository traits, and `DataLayerError`.
   Prefer `aether-data-contracts` for anything that another crate needs to
   compile against.
2. Driver primitives: `aether-data-postgres` connect to infrastructure and expose pools, runners,
   and executable migrations;
   the matching `src/driver/*.rs` files only preserve the existing import paths.
3. Repository implementations: `aether-data-postgres` translate
   contract types to driver-specific SQL. `src/repository/<domain>` keeps the
   compatibility import path and owns the in-memory implementation where one
   exists.
4. Backend composition: `backend` builds the PostgreSQL backend from config and wires
   repository implementations into app-facing handles. Backend-owned runtime
   maintenance workflows live in focused backend modules rather than in the
   driver pool files.
5. Maintenance workflows: `lifecycle/migrate`, `lifecycle/backfill`,
   `lifecycle/export`, and `schema` manage database lifecycle outside normal
   request handling.

Do not add domain queries to low-level pool modules. Do not add driver selection
logic inside individual repository implementations. Keep cross-crate contracts
out of `aether-data` unless they are implementation-only.

## Repository Layout

Most domain repositories use this shape:

```text
src/repository/<domain>/
  mod.rs      # exports trait/type names and concrete implementations
  types.rs    # implementation-local DTOs when they are not already in contracts
  postgres.rs # Postgres implementation
  memory.rs   # tests/dev in-memory implementation
```

Use explicit driver filenames for repository implementations. Do not introduce
new generic `sql.rs` modules for driver-specific code.

## SQL Driver Policy

The repository/backend boundary supports PostgreSQL only. Repository contracts
remain independent of SQLx, while physical SQL, pools, and migrations belong to
`aether-data-postgres`.

| Logical type | PostgreSQL |
|---|---|
| `json` | `json` or `jsonb` |
| `bool` | `boolean` |
| `time_unix` | `bigint` or timestamp |
| `money_decimal` | `numeric` / legacy double |

### Feature Matrix

`aether-data` enables PostgreSQL by default. PostgreSQL is the only production
driver; in-memory repositories stay behind `cfg(test)` / `test-support` for
repository tests.

```bash
cargo check -p aether-data
```

Database URLs must use `postgres:` or `postgresql:`. Unsupported drivers and
URL schemes fail explicitly instead of falling back to another backend.

## Schema Maintenance

Executable migrations live under each adapter crate's `migrations/` directory.
Moving their physical files does not change SQLx migration versions or deployed
database history because SQLx records migration version and checksum, not the
workspace source path.

The large baseline SQL files are maintained as ordered fragments under
`schema/drivers/postgres`. The executable baseline migration
`../adapters/postgres/migrations/20260403000000_baseline.sql` must stay byte-for-byte
equal to the composition of `schema/drivers/postgres/baseline/manifest.txt`;
`src/lifecycle/migrate/tests.rs` enforces that equality. The bootstrap snapshot is
composed automatically by `build.rs` from `schema/bootstrap/postgres/manifest.txt`.

There is no logical-schema compiler or generated SQL directory. When adding a
table:

1. Update `schema/drivers/postgres` and the executable baseline migration together,
   preserving the composition order in `manifest.txt`.
2. Update `schema/bootstrap/postgres` when the empty-database snapshot changes;
   `build.rs` rebuilds the embedded artifact.
3. Add a new incremental migration under `../adapters/postgres/migrations` for
   existing databases. Do not modify already-applied migration checksums.
4. Add or update repository contracts in `aether-data-contracts` if other crates
   need the new shape.
5. Add the Postgres repository implementation and wire it through
   `src/backend/read.rs` or `src/backend/write.rs`.
6. Update this README when ownership or schema maintenance rules change.

## Known Cleanup Targets

These are intentionally staged to keep the data-layer refactor reviewable:

1. Retire the compatibility re-export paths once downstream crates use
   `aether-data-contracts` for contracts and `aether-data` only as the runtime
   composition facade.
2. Group repository domains once file-level names are stable. Likely groups:
   identity, auth config, provider catalog, runtime tasks, wallet/billing,
   usage, stats, and proxy nodes.
3. Continue shrinking Postgres stats SQL modules where useful by moving shared
   SQL fragments and row-mapping helpers behind focused `backend/stats/*`
   modules.
4. Split very large `usage`, `wallet`, and stats implementations into focused
   query/mapping/read/write/test modules before considering any additional
   crates. New crate boundaries should follow stable ownership, not file size
   alone.
