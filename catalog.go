package spanpg

import sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

// PostgreSQLCatalogTypeName returns a conventional PostgreSQL type spelling for scalar
// [sppb.Type] values that carry well-known PostgreSQL dialect TypeAnnotation markers
// (for example NUMERIC with PG_NUMERIC → "numeric", JSON with PG_JSONB → "jsonb").
//
// It returns ("", false) when the type does not map to a single catalog-style name:
// nil type, ARRAY or STRUCT, UNSPECIFIED annotation, or an unmapped code/annotation pair.
//
// Display-oriented helpers like this are intentionally not part of [github.com/apstndb/spanvalue];
// see also [github.com/apstndb/spantype.FormatType] for Spanner-style type strings.
func PostgreSQLCatalogTypeName(typ *sppb.Type) (name string, ok bool) {
	if typ == nil {
		return "", false
	}
	switch typ.GetCode() {
	case sppb.TypeCode_ARRAY, sppb.TypeCode_STRUCT:
		return "", false
	}
	ann := typ.GetTypeAnnotation()
	switch typ.GetCode() {
	case sppb.TypeCode_NUMERIC:
		if ann == sppb.TypeAnnotationCode_PG_NUMERIC {
			return "numeric", true
		}
	case sppb.TypeCode_JSON:
		if ann == sppb.TypeAnnotationCode_PG_JSONB {
			return "jsonb", true
		}
	case sppb.TypeCode_INT64:
		if ann == sppb.TypeAnnotationCode_PG_OID {
			return "oid", true
		}
	}
	return "", false
}
