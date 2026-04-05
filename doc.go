// Package spanpg provides experimental PostgreSQL-dialect helpers around Cloud Spanner:
// catalog-style type spellings, placeholder/param-key pairing for PostgreSQL SQL text,
// and thin bridges to [github.com/apstndb/spanvalue] formatting.
//
// Behavioral notes (query parameters use [cloud.google.com/go/spanner.PGNumeric] /
// [cloud.google.com/go/spanner.PGJsonB]; row metadata exposes TypeAnnotation on column
// types) are integration-tested in [github.com/apstndb/spanvalue] — see
// https://github.com/apstndb/spanvalue/pull/45.
//
// Stable [cloud.google.com/go/spanner.GenericColumnValue] construction remains in
// [github.com/apstndb/spanvalue/gcvctor]; [google.spanner.v1.Type] string rendering
// remains in [github.com/apstndb/spantype].
package spanpg
