// Package spanpg provides experimental PostgreSQL-dialect helpers around Cloud Spanner:
// [FormatPostgreSQLType] for type strings, placeholder/param-key pairing for PostgreSQL SQL text,
// PostgreSQL literal formatting presets built on [github.com/apstndb/spanvalue], and thin
// bridges to existing spanvalue formatting.
//
// Behavioral notes (query parameters use [cloud.google.com/go/spanner.PGNumeric] /
// [cloud.google.com/go/spanner.PGJsonB]; row metadata exposes TypeAnnotation on column
// types) are covered by the nested module `integration/pgtypeannotation` in this repository
// (see that directory’s README).
//
// Stable [cloud.google.com/go/spanner.GenericColumnValue] construction remains in
// [github.com/apstndb/spanvalue/gcvctor]; [google.spanner.v1.Type] string rendering
// remains in [github.com/apstndb/spantype]. PostgreSQL-dialect spellings for
// [cloud.google.com/go/spanner/apiv1/spannerpb.Type] are available via [FormatPostgreSQLType]
// (see https://docs.cloud.google.com/spanner/docs/reference/postgresql/data-types
// and https://docs.cloud.google.com/spanner/docs/reference/dialect-differences).
package spanpg
