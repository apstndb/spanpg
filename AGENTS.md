# Agent instructions for `spanpg`

Go library (**MIT**, experimental): the **PostgreSQL-dialect adapter layer**
for Cloud Spanner, on top of [spanvalue](https://github.com/apstndb/spanvalue)
(formatting), [spancodec](https://github.com/apstndb/spancodec) (Go ↔ GCV;
spanpg adapts it via per-call `EncodeOption` injection, never forks it),
and [spantype](https://github.com/apstndb/spantype). Architecture rule: **the
cores stay dialect-neutral** — spanvalue's recorded boundary is "PG table
cells: spanpg, not spanvalue", and spancodec stays a client mirror; ALL
PG-dialect knowledge lands here. spanpg is a leaf module (nothing depends on
it), so iterate aggressively, but every formatting/encoding claim must be
**pinned by a probe** (see Verification).

## API map

- `FormatPostgreSQLType` — `sppb.Type` → PG spelling (`bool`, `bigint`,
  `float8`/`float4`, `numeric`, `text`, `bytea`, `timestamptz`, `date`,
  `jsonb` (only with PG_JSONB annotation), `interval`, `uuid`, `oid`,
  `T[]`). PROTO/ENUM deliberately render as "proto"/"enum" (never mislabel
  wire types); STRUCT rendering is provisional GoogleSQL syntax.
- `PostgreSQLLiteralFormatConfig` / `FormatRowPostgreSQLLiteral` /
  `FormatColumnPostgreSQLLiteral` — PG literal SQL via a spanvalue
  `NewFormatConfig` build (scalar-formatter switch + reject and
  wire-string plugins).
  Rejections (`ErrUnsupportedPostgreSQLType`): PROTO, ENUM, STRUCT, and
  JSON without PG_JSONB annotation, recursively through ARRAY.
- `StatementParamKey` / `PostgreSQLPlaceholder` — `$n` ↔ `p1..pn` pairing
  (client convention for the PG interface). `PositionalParams` /
  `InsertStatement` build on them: ordered values (plain Go or GCV) →
  Statement params / `INSERT INTO … VALUES ($1..$n)` with
  `spanvalue.QuoteIdentifier` PostgreSQL quoting. No UPDATE/UPSERT builders
  yet; generic fragment helpers stay in spanvalue (apstndb/spanvalue#79).
- `EncodeOptions` — spancodec dialect adapter: `big.Rat`/`*big.Rat`/
  `NullNumeric` → PG_NUMERIC, `NullJSON` → PG_JSONB (encoders + `WithGoType`
  for static inference, slice types included). REQUIRED, not cosmetic: the PG
  backend rejects un-annotated NUMERIC/JSON params (probe: Unimplemented
  "Unsupported GoogleSQL Type: NUMERIC"). `PGNumeric`/`PGJsonB` pass through
  unregistered. No `DecodeOptions`: client decode checks TypeCode only, so
  PG_NUMERIC/PG_JSONB columns decode natively (NaN needs `PGNumeric`, not
  `big.Rat`). All pinned in `integration/pgtypeannotation`.

## Verification (core discipline)

The nested module **`integration/pgtypeannotation`** runs probes against a
POSTGRESQL-dialect Spanner database: the emulator via spanemuboost/Docker by
default, real Cloud Spanner with `SPANPG_PGTYPEANNOTATION_CLOUD=1` +
`SPANVALUE_*` env. `mise run test-integration` (NOT part of
`mise run check`). PG-dialect behaviors must be pinned there, not assumed:
TypeAnnotation on params/metadata, and the **literal round-trip harness**
(format a GCV → execute `SELECT <literal>` → compare the returned GCV).
When adding or changing a literal form, add a round-trip case first.

## Go floors

Root module: **go 1.24.0** — hold transitive pins at MVS minimums like
spanvalue; do NOT let `go get` inflate the floor. The integration module is
probes-only (constrains no downstream) and rides newer floors as its deps
require (currently 1.25, pulled by spanemuboost@latest).

## Commands

`mise.toml` owns tasks/tools; prefer `mise run check`; `mise run
test-integration` for probes; Makefile delegates. CI runs the same tasks via
jdx/mise-action.

## Conventions

Versioning: stay on v0; breaking = minor, otherwise patch. GitHub Releases
are the per-version truth (record minimum spanvalue/spancodec versions
there); never re-tag. English only on github.com. Docs placement: godoc +
runnable Examples over README. Roadmap umbrella: issue #3.
