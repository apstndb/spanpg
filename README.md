# spanpg

Experimental, optional layer for **PostgreSQL dialect** ergonomics around [Cloud Spanner](https://cloud.google.com/spanner) — built on top of:

- [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue) — `GenericColumnValue` formatting and constructors
- [`github.com/apstndb/spantype`](https://github.com/apstndb/spantype) — `google.spanner.v1.Type` string rendering

The module API is **unstable** until declared otherwise.

## API (experimental)

- **`PostgreSQLCatalogTypeName`** — PostgreSQL catalog-style names for annotated scalars (`numeric`, `jsonb`, `oid`).
- **`StatementParamKey` / `PostgreSQLPlaceholder`** — pair `$n` placeholders with `spanner.Statement.Params` keys (`p1`, `p2`, …), matching the client convention ([integration notes](https://github.com/apstndb/spanvalue/pull/45)).
- **`FormatColumnSimple`** — delegates to [`spanvalue.SimpleFormatConfig`](https://pkg.go.dev/github.com/apstndb/spanvalue#SimpleFormatConfig) for readable column text.

## Status

`go.mod` pins [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue) **v0.2.1** and [`github.com/apstndb/spantype`](https://github.com/apstndb/spantype) **v0.3.11**. The API remains **unstable** until declared otherwise.

## Development

```shell
go test ./...
go vet ./...
```

## License

MIT — see [LICENSE](./LICENSE).
