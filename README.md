# spanpg

[![Go Reference](https://pkg.go.dev/badge/github.com/apstndb/spanpg.svg)](https://pkg.go.dev/github.com/apstndb/spanpg)

Experimental, optional layer for **PostgreSQL dialect** ergonomics around [Cloud Spanner](https://cloud.google.com/spanner) — built on top of:

- [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue) — `GenericColumnValue` formatting and constructors
- [`github.com/apstndb/spantype`](https://github.com/apstndb/spantype) — `google.spanner.v1.Type` string rendering

The module API is **unstable** until declared otherwise.

## API (experimental)

- **`FormatPostgreSQLType`** — render a `google.spanner.v1.Type` using PostgreSQL-dialect spellings (including `STRUCT<…>` and `json` / `jsonb` by annotation).
- **`StatementParamKey` / `PostgreSQLPlaceholder`** — pair `$n` placeholders with `spanner.Statement.Params` keys (`p1`, `p2`, …), matching the client convention (see [`integration/pgtypeannotation`](./integration/pgtypeannotation/README.md)).
- **`PostgreSQLLiteralFormatConfig` / `FormatColumnPostgreSQLLiteral` / `FormatRowPostgreSQLLiteral`** — format `GenericColumnValue` / `*spanner.Row` values as PostgreSQL literal SQL and reject Spanner types the PostgreSQL interface does not support.
- **`FormatColumnSimple`** — delegates to [`spanvalue.SimpleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SimpleFormatConfig) for readable column text.

## Status

`go.mod` pins [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue) **v0.2.1** and [`github.com/apstndb/spantype`](https://github.com/apstndb/spantype) **v0.3.11**. The API remains **unstable** until declared otherwise.

## Integration tests

The nested module [`integration/pgtypeannotation`](./integration/pgtypeannotation/README.md) runs PostgreSQL-dialect Spanner client probes (emulator or real instance). It is **not** part of root `go test ./...`; use `make test-integration` (see [`Makefile`](./Makefile) and [`.github/workflows/go.yml`](./.github/workflows/go.yml)).

## Development

```shell
go test ./...
go vet ./...
```

## License

MIT — see [LICENSE](./LICENSE).
