# Aether Schema Fragments

This directory holds the ordered SQL fragments used to assemble the Postgres
baseline migration and the empty-database bootstrap snapshot.

| Path | Role |
|---|---|
| `drivers/postgres/` | Source fragments for `adapters/postgres/migrations/20260403000000_baseline.sql`. |
| `bootstrap/postgres/` | Source fragments composed by `aether-data`'s `build.rs` into the embedded bootstrap snapshot. |

The executable baseline migration must stay byte-for-byte equal to the
composition of `drivers/postgres/baseline/manifest.txt`;
`src/lifecycle/migrate/tests.rs` enforces that equality. Incremental migrations
live directly under `adapters/postgres/migrations/` and are not composed here.

There is no logical-schema source tree or generated SQL directory. Edit driver
fragments and the executable baseline together, preserve manifest ordering, and
do not change checksums of migrations that have already been applied.
