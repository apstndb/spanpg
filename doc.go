// Package spanpg provides experimental PostgreSQL-dialect helpers around Cloud Spanner:
// catalog-style type spellings, placeholder/param-key pairing for PostgreSQL SQL text,
// and thin bridges to [github.com/apstndb/spanvalue] formatting.
//
// Behavioral notes (query parameters use [cloud.google.com/go/spanner.PGNumeric] /
// [cloud.google.com/go/spanner.PGJsonB]; row metadata exposes TypeAnnotation on column
// types) are covered by the nested module `integration/pgtypeannotation` in this repository
// (see that directory’s README).
//
// Stable [cloud.google.com/go/spanner.GenericColumnValue] construction remains in
// [github.com/apstndb/spanvalue/gcvctor]; [google.spanner.v1.Type] string rendering
// remains in [github.com/apstndb/spantype]. PostgreSQL catalog spellings for
// [cloud.google.com/go/spanner/apiv1/spannerpb.Type] values are available via
// [FormatPostgreSQLType] (see https://docs.cloud.google.com/spanner/docs/reference/postgresql/data-types
// and https://docs.cloud.google.com/spanner/docs/reference/dialect-differences).
package spanpg
