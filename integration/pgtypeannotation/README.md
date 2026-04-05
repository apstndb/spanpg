# PostgreSQL dialect `TypeAnnotation` probe

This **nested Go module** exercises the Cloud Spanner Go client against a **PostgreSQL-dialect** database:

- Query parameters bound as `spanner.PGNumeric` / `spanner.PGJsonB` (encoded with `TypeAnnotation` on the wire).
- `RowIterator.Metadata.RowType` after the first `Next()` — column types should carry `PG_NUMERIC` / `PG_JSONB` annotations.

It validates client and server `TypeAnnotation` behavior for PostgreSQL-oriented Spanner usage. Formatting helpers live in [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue); catalog-style names in [`github.com/apstndb/spanpg`](https://github.com/apstndb/spanpg).

SQL uses PostgreSQL placeholder syntax (`$1` with params keyed `p1`, …), matching `cloud.google.com/go/spanner` integration tests.

## Module layout and `replace`

`go.mod` pins `github.com/apstndb/spanpg` and uses:

```text
replace github.com/apstndb/spanpg => ../..
```

so this submodule always builds against the **checkout root**. Heavy test-only dependencies (e.g. [`github.com/apstndb/spanemuboost`](https://github.com/apstndb/spanemuboost)) stay out of the **root** `spanpg` `go.mod`.

Optional local multi-module workflow:

```sh
cp go.work.example ../../go.work   # repo root; edit if needed
go work sync
```

## Requirements

- Go version follows this submodule’s `go.mod` (may differ from the parent `spanpg` module; the GitHub Actions integration job uses `go-version-file: integration/pgtypeannotation/go.mod`).
- For the full integration test: network access to Spanner API, **or** Docker (default path uses spanemuboost / testcontainers).

## Real Cloud Spanner

1. Enable the Spanner API and have an **existing instance** (this test does not create paid instances).
2. Application Default Credentials (`gcloud auth application-default login` or workload identity).
3. Run:

```sh
export SPANVALUE_PROJECT_ID=your-project
export SPANVALUE_INSTANCE_ID=your-instance
go test ./... -count=1 -v
```

The test creates a temporary PostgreSQL-dialect database and drops it in cleanup.

## Default: emulator via spanemuboost

With **no** `SPANVALUE_*` or `SPANNER_EMULATOR_HOST` env vars, the test runs the Cloud Spanner emulator inside Docker using [`github.com/apstndb/spanemuboost`](https://github.com/apstndb/spanemuboost) (`SetupEmulatorWithClients` + `DatabaseDialect_POSTGRESQL` + `WithRandomDatabaseID()`).

From the **spanpg repository root**:

```sh
make test-integration
```

Or from this directory:

```sh
go test ./... -count=1 -v
```

Requires a working Docker (or compatible) runtime for testcontainers.

## Quick check without Docker

Runs only lightweight tests (including the parent-module link check):

```sh
go test -short ./...
```

## Manual emulator (no Docker)

If you already run the emulator yourself:

```sh
export SPANNER_EMULATOR_HOST=localhost:9010
go test ./... -count=1 -v
```

If `CreateDatabase` with `DatabaseDialect_POSTGRESQL` fails, the test **skips** — use a recent emulator build with PostgreSQL support.

## What is asserted

- `ResultSetMetadata.row_type.fields[0].type.code` is `NUMERIC` or `JSON`.
- `type_annotation` is `PG_NUMERIC` or `PG_JSONB`.
- Row values round-trip into `spanner.PGNumeric` / `spanner.PGJsonB`.
