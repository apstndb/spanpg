package spanpg

import (
	"fmt"
	"strings"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
)

// FormatPostgreSQLType renders a [sppb.Type] using PostgreSQL-dialect spellings from
// https://docs.cloud.google.com/spanner/docs/reference/postgresql/data-types
// (supported types table: bool, bytea, date, float4, float8, bigint, interval, jsonb,
// numeric, timestamptz, text, uuid, oid, and array declarations).
// [sppb.TypeCode_JSON] is formatted as "jsonb" only when the type carries
// [sppb.TypeAnnotationCode_PG_JSONB]; otherwise it is formatted as "json" (non-PG JSON
// shapes are not defined in the PostgreSQL data-types table today, but the label keeps
// the distinction from PG_JSONB wire types).
//
// [sppb.TypeCode_PROTO] and [sppb.TypeCode_ENUM] are not PostgreSQL-interface types:
// the PostgreSQL dialect does not support Protocol Buffers, so column values of these
// types are not returned in normal use. Dialect documentation may mention BYTEA or
// TEXT with CHECK constraints as migration substitutes for GoogleSQL; those are not
// the same as PROTO or ENUM on the wire. This function therefore formats them as
// "proto" and "enum"—not as "bytea" or "text"—so wire types are never mislabeled if
// they appear in metadata or in a future extension.
//
// [sppb.TypeCode_STRUCT] is formatted as GoogleSQL STRUCT<…> declaration syntax. When
// reviewing or updating this behavior, read Working with STRUCT objects from
// https://docs.cloud.google.com/spanner/docs/structs (e.g. dkcli get
// docs.cloud.google.com/spanner/docs/structs) rather than ad-hoc page fetches. That page
// notes that the STRUCT data type is not supported in the PostgreSQL interface for
// Spanner, so the returned string is not a PostgreSQL-interface type name—it is the
// Spanner SQL form (named fields: STRUCT<name type, …>; unnamed: STRUCT<type, …>)
// with field types spelled using the PostgreSQL data-type names above, not Spanner
// keywords such as STRING.
func FormatPostgreSQLType(typ *sppb.Type) string {
	if typ == nil {
		return ""
	}
	return formatPostgreSQLTypeImpl(typ)
}

func formatPostgreSQLTypeImpl(typ *sppb.Type) string {
	switch typ.GetCode() {
	case sppb.TypeCode_ARRAY:
		elem := typ.GetArrayElementType()
		if elem == nil {
			return "array/*nil element type*/"
		}
		return formatPostgreSQLTypeImpl(elem) + "[]"

	case sppb.TypeCode_STRUCT:
		st := typ.GetStructType()
		if st == nil || len(st.GetFields()) == 0 {
			return "STRUCT<>"
		}
		var b strings.Builder
		b.WriteString("STRUCT<")
		for i, f := range st.GetFields() {
			if i > 0 {
				b.WriteString(", ")
			}
			ft := formatPostgreSQLTypeImpl(f.GetType())
			if n := f.GetName(); n != "" {
				b.WriteString(n)
				b.WriteByte(' ')
				b.WriteString(ft)
			} else {
				b.WriteString(ft)
			}
		}
		b.WriteByte('>')
		return b.String()

	case sppb.TypeCode_BOOL:
		return "bool"

	case sppb.TypeCode_INT64:
		if typ.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_OID {
			return "oid"
		}
		return "bigint"

	case sppb.TypeCode_FLOAT32:
		return "float4"

	case sppb.TypeCode_FLOAT64:
		return "float8"

	case sppb.TypeCode_STRING:
		return "text"

	case sppb.TypeCode_BYTES:
		return "bytea"

	case sppb.TypeCode_TIMESTAMP:
		return "timestamptz"

	case sppb.TypeCode_DATE:
		return "date"

	case sppb.TypeCode_NUMERIC:
		return "numeric"

	case sppb.TypeCode_JSON:
		if typ.GetTypeAnnotation() == sppb.TypeAnnotationCode_PG_JSONB {
			return "jsonb"
		}
		return "json"

	case sppb.TypeCode_INTERVAL:
		return "interval"

	case sppb.TypeCode_UUID:
		return "uuid"

	case sppb.TypeCode_PROTO:
		return "proto"

	case sppb.TypeCode_ENUM:
		return "enum"

	case sppb.TypeCode_TYPE_CODE_UNSPECIFIED:
		return "unknown"

	default:
		return fmt.Sprintf("unknown(%d)", typ.GetCode())
	}
}
