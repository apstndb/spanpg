# spanpg

Experimental, optional layer for **PostgreSQL dialect** ergonomics around [Cloud Spanner](https://cloud.google.com/spanner) — built on top of:

- [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue) — `GenericColumnValue` formatting and constructors
- [`github.com/apstndb/spantype`](https://github.com/apstndb/spantype) — `google.spanner.v1.Type` string rendering

The module API is **unstable** until declared otherwise.

## Status

Scaffold only: `go.mod` pins [`github.com/apstndb/spanvalue`](https://github.com/apstndb/spanvalue) **v0.2.1** (PostgreSQL `TypeAnnotation` value formatting and `gcvctor` helpers) and [`github.com/apstndb/spantype`](https://github.com/apstndb/spantype) **v0.3.11** (`Type` string rendering). Package docs and a place for future code (e.g. display adapters, driver helpers).

## Development

```shell
go test ./...
go vet ./...
```

## License

MIT — see [LICENSE](./LICENSE).
