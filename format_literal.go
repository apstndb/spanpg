package spanpg

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanvalue"
	"google.golang.org/protobuf/types/known/structpb"
)

var postgresqlLiteralFormatConfig = PostgreSQLLiteralFormatConfig()

// ErrUnsupportedPostgreSQLType reports a Spanner type that cannot be rendered as
// executable PostgreSQL-dialect SQL because the interface does not support it.
var ErrUnsupportedPostgreSQLType = errors.New("unsupported PostgreSQL type")

// PostgreSQLLiteralFormatConfig returns a new spanvalue.FormatConfig that produces
// parseable PostgreSQL-dialect literal expressions for scalar values plus ARRAY constructors.
// It rejects Spanner-specific types that the PostgreSQL interface does not support
// (for example PROTO, ENUM, and STRUCT) instead of emitting invalid SQL.
//
// NULL values—scalar NULL and NULL arrays alike—render as the bare keyword NULL,
// not CAST(NULL AS <type>). Bare NULL is valid wherever the surrounding context fixes
// the type (INSERT column lists, comparisons against typed columns), which is the
// intended use of this config (for example SQL INSERT export through
// github.com/apstndb/spanvalue/writer). In context-free positions such as a bare
// SELECT NULL the backend has no type to infer, so callers needing a typed NULL
// expression must wrap it themselves (CAST(NULL AS bigint), ...). Both behaviors are
// pinned by the literal round-trip harness in integration/pgtypeannotation
// (pgliteral_roundtrip_test.go), which executes every literal form this config emits
// against a POSTGRESQL-dialect Spanner database.
//
// Known backend canonicalizations (probed by the same harness): timestamptz literals
// are parsed with microsecond precision—sub-microsecond digits are rounded, and a
// nanosecond-precision value at the maximum timestamp (9999-12-31T23:59:59.999999999Z)
// rounds out of range and fails—and jsonb text is normalized (key order, whitespace,
// unicode escapes).
func PostgreSQLLiteralFormatConfig() *spanvalue.FormatConfig {
	return &spanvalue.FormatConfig{
		NullString: spanvalue.LiteralFormatConfig().NullString,
		FormatArray: func(typ *sppb.Type, _ bool, elemStrings []string) (string, error) {
			return fmt.Sprintf("CAST(ARRAY[%s] AS %s)", strings.Join(elemStrings, ", "), FormatPostgreSQLType(typ)), nil
		},
		// STRUCT values are rejected by rejectUnsupportedPostgreSQLLiteralType before these
		// callbacks can run; they are still set so the config passes
		// spanvalue.FormatConfig.Validate and fails loudly if the plugin is ever bypassed.
		FormatStruct: spanvalue.FormatStruct{
			FormatStructField: func(_ *spanvalue.FormatConfig, field *sppb.StructType_Field, _ *structpb.Value) (string, error) {
				return "", fmt.Errorf("%w: STRUCT field %q", ErrUnsupportedPostgreSQLType, field.GetName())
			},
			FormatStructParen: func(typ *sppb.Type, _ bool, _ []string) (string, error) {
				return "", fmt.Errorf("%w: %s", ErrUnsupportedPostgreSQLType, typ.String())
			},
		},
		FormatComplexPlugins: []spanvalue.FormatComplexFunc{
			rejectUnsupportedPostgreSQLLiteralType,
			formatPostgreSQLWireStringLiteral,
		},
		FormatNullable: formatNullableValuePostgresqlLiteral,
	}
}

// formatPostgreSQLWireStringLiteral formats scalar INTERVAL and PG_JSONB values by
// quoting the GenericColumnValue wire string directly instead of re-serializing the
// decoded Go value. Re-serialization loses information for these types (pinned by the
// integration/pgtypeannotation round-trip harness):
//
//   - spanner.Interval.String() pads fractional seconds to 3-digit groups
//     ("PT6.5S" wire becomes "PT6.500S"), diverging from the server-canonical wire form.
//   - spanner.PGJsonB decoding round-trips JSON numbers through float64 by default,
//     so re-marshaling corrupts large integers (12345678901234567890 became
//     12345678901234567000) and re-applies Go's HTML escaping (the u003c Unicode
//     escape for <).
//
// The wire string is authoritative for both types, so it is embedded as-is.
// NULL values fall through to NullString handling.
func formatPostgreSQLWireStringLiteral(_ spanvalue.Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	if spanvalue.IsNull(value) {
		return "", spanvalue.ErrFallthrough
	}
	switch value.Type.GetCode() {
	case sppb.TypeCode_INTERVAL:
		// handled below
	case sppb.TypeCode_JSON:
		if value.Type.GetTypeAnnotation() != sppb.TypeAnnotationCode_PG_JSONB {
			return "", spanvalue.ErrFallthrough
		}
	default:
		return "", spanvalue.ErrFallthrough
	}
	wire, ok := value.Value.GetKind().(*structpb.Value_StringValue)
	if !ok {
		// The type is known; the payload is wrong — spanvalue's malformed-wire
		// class (apstndb/spanvalue#216), not ErrUnknownType.
		return "", fmt.Errorf("%w: %s value encoded as %T, want string_value",
			spanvalue.ErrMalformedWire, FormatPostgreSQLType(value.Type), value.Value.GetKind())
	}
	return postgresqlCast(postgresqlStringLiteral(wire.StringValue), FormatPostgreSQLType(value.Type)), nil
}

// FormatRowPostgreSQLLiteral formats a row using PostgreSQLLiteralFormatConfig.
func FormatRowPostgreSQLLiteral(value *spanner.Row) ([]string, error) {
	return postgresqlLiteralFormatConfig.FormatRow(value)
}

// FormatColumnPostgreSQLLiteral formats a top-level column using PostgreSQLLiteralFormatConfig.
func FormatColumnPostgreSQLLiteral(value spanner.GenericColumnValue) (string, error) {
	return postgresqlLiteralFormatConfig.FormatToplevelColumn(value)
}

func unsupportedPostgreSQLType(typ *sppb.Type) bool {
	if typ == nil {
		return false
	}
	switch typ.GetCode() {
	case sppb.TypeCode_PROTO, sppb.TypeCode_ENUM, sppb.TypeCode_STRUCT:
		return true
	case sppb.TypeCode_JSON:
		return typ.GetTypeAnnotation() != sppb.TypeAnnotationCode_PG_JSONB
	case sppb.TypeCode_ARRAY:
		return unsupportedPostgreSQLType(typ.GetArrayElementType())
	default:
		return false
	}
}

func rejectUnsupportedPostgreSQLLiteralType(_ spanvalue.Formatter, value spanner.GenericColumnValue, _ bool) (string, error) {
	if !unsupportedPostgreSQLType(value.Type) {
		return "", spanvalue.ErrFallthrough
	}
	return "", fmt.Errorf("%w: %s", ErrUnsupportedPostgreSQLType, value.Type.String())
}

func postgresqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func postgresqlCast(expr string, typ string) string {
	return fmt.Sprintf("CAST(%s AS %s)", expr, typ)
}

func postgresqlFloatLiteral(v float64, bits int) string {
	typ := "float8"
	if bits == 32 {
		typ = "float4"
	}
	switch {
	case math.IsNaN(v):
		return postgresqlCast(postgresqlStringLiteral("NaN"), typ)
	case math.IsInf(v, 1):
		return postgresqlCast(postgresqlStringLiteral("Infinity"), typ)
	case math.IsInf(v, -1):
		return postgresqlCast(postgresqlStringLiteral("-Infinity"), typ)
	default:
		return postgresqlCast(strconv.FormatFloat(v, 'g', -1, bits), typ)
	}
}

func formatNullableValuePostgresqlLiteral(value spanvalue.NullableValue) (string, error) {
	switch v := value.(type) {
	case spanner.NullString:
		return postgresqlStringLiteral(v.StringVal), nil
	case spanner.NullBool:
		return strconv.FormatBool(v.Bool), nil
	case spanvalue.NullBytes:
		return postgresqlCast(postgresqlStringLiteral(`\x`+hex.EncodeToString(v)), "bytea"), nil
	case spanner.NullFloat32:
		return postgresqlFloatLiteral(float64(v.Float32), 32), nil
	case spanner.NullFloat64:
		return postgresqlFloatLiteral(v.Float64, 64), nil
	case spanner.NullInt64:
		return strconv.FormatInt(v.Int64, 10), nil
	case spanner.NullNumeric:
		return postgresqlCast(postgresqlStringLiteral(spanner.NumericString(&v.Numeric)), "numeric"), nil
	case spanner.PGNumeric:
		return postgresqlCast(postgresqlStringLiteral(v.Numeric), "numeric"), nil
	case spanner.NullTime:
		return postgresqlCast(postgresqlStringLiteral(v.Time.UTC().Format(time.RFC3339Nano)), "timestamptz"), nil
	case spanner.NullDate:
		return postgresqlCast(postgresqlStringLiteral(v.Date.String()), "date"), nil
	case spanner.NullJSON:
		return "", fmt.Errorf("%w: %T", ErrUnsupportedPostgreSQLType, v)
	// PGJsonB and NullInterval are normally handled upstream by
	// formatPostgreSQLWireStringLiteral (wire string is authoritative; decoded
	// re-serialization pads interval fractions and corrupts large JSON integers).
	// These branches remain as fallbacks for callers that reuse this FormatNullable
	// without the plugin chain.
	case spanner.PGJsonB:
		b, err := json.Marshal(v.Value)
		if err != nil {
			return "", err
		}
		return postgresqlCast(postgresqlStringLiteral(string(b)), "jsonb"), nil
	case spanner.NullInterval:
		return postgresqlCast(postgresqlStringLiteral(v.String()), "interval"), nil
	case spanner.NullUUID:
		return postgresqlCast(postgresqlStringLiteral(v.String()), "uuid"), nil
	default:
		return "", fmt.Errorf("%w: %T", spanvalue.ErrUnknownType, v)
	}
}
