# aether-data

`aether-data` is the runtime data-access crate. It owns the PostgreSQL backend
composition, migration/backfill workflows, and the layer that wires repository
implementations into application-facing handles.

It does not own the cross-crate DTO contracts. Shared repository records and
errors that are consumed by scheduler, billing, admin, and usage runtime live in
the sibling `../contracts` crate (`aether-data-contracts`).
Test-only in-memory repositories live in the sibling `../memory` crate
(`aether-data-memory`) and are consumed as dev-dependencies.

## Directory Map

| Path | Responsibility |
|---|---|
| `src/config.rs` | Data-layer config for the PostgreSQL backend and repository wiring. |
| `src/maintenance.rs` | Maintenance DTOs and aggregation summaries used by backend dispatch and runtime maintenance entrypoints. |
| `src/backend` | Composition root. Builds the PostgreSQL backend and exposes app-facing read/write/worker/lock/lease handles. |
| `src/backend/{maintenance,stats,wallet,system}.rs` | Backend-owned maintenance, aggregation, wallet ledger, and system config workflows that are not normal request-path repositories. |
| `src/lifecycle/migrate.rs` and `src/lifecycle/migrate/*` | Migration entry points and the Postgres snapshot bootstrap adapter. |
| `src/lifecycle/backfill.rs` | Backfill entry points and backfill discovery. |
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
2. Driver primitives: `aether-data-postgres` connects to infrastructure and
   exposes pools, runners, and executable migrations.
3. Repository implementations: `aether-data-postgres` translates contract
   types to driver-specific SQL. Test doubles live in `aether-data-memory`.
4. Backend composition: `backend` builds the PostgreSQL backend from config and wires
   repository implementations into app-facing handles. Backend-owned runtime
   maintenance workflows live in focused backend modules rather than in the
   driver pool files.
5. Maintenance workflows: `lifecycle/migrate`, `lifecycle/backfill`, and `schema`
   manage database lifecycle outside normal request handling.

Do not add domain queries to low-level pool modules. Do not add driver selection
logic inside individual repository implementations. Keep cross-crate contracts
out of `aether-data` unless they are implementation-only.

## Repository Layout

Repository code is split by responsibility rather than kept behind a
compatibility module in `aether-data`:

```text
../contracts/src/repository/<domain>/  # DTOs, repository traits, shared helpers
../adapters/postgres/src/<domain>.rs   # Postgres implementations
../memory/src/<domain>.rs              # test-only in-memory implementations
```

Use explicit driver filenames for repository implementations. Do not introduce
new generic `sql.rs` modules for driver-specific code, and do not add a new
compatibility re-export module to `aether-data`.

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

PostgreSQL is the only production driver. In-memory repositories live in
`aether-data-memory` and are consumed as dev-dependencies; `aether-data` has no
test-support feature.

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

1. Group repository domains once file-level names are stable. Likely groups:
   identity, auth config, provider catalog, runtime tasks, wallet/billing,
   usage, stats, and proxy nodes.
2. Continue shrinking Postgres stats SQL modules where useful by moving shared
   SQL fragments and row-mapping helpers behind focused `backend/stats/*`
   modules.
3. Split very large `usage`, `wallet`, and stats implementations into focused
   query/mapping/read/write/test modules before considering any additional
   crates. New crate boundaries should follow stable ownership, not file size
   alone.
