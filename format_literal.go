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
)

var postgresqlLiteralFormatConfig = PostgreSQLLiteralFormatConfig()

// ErrUnsupportedPostgreSQLType reports a Spanner type that cannot be rendered as
// executable PostgreSQL-dialect SQL because the interface does not support it.
var ErrUnsupportedPostgreSQLType = errors.New("unsupported PostgreSQL type")

// PostgreSQLLiteralFormatConfig returns a new spanvalue.FormatConfig that produces
// parseable PostgreSQL-dialect literal expressions for scalar values plus ARRAY constructors.
// It rejects Spanner-specific types that the PostgreSQL interface does not support
// (for example PROTO, ENUM, and STRUCT) instead of emitting invalid SQL.
func PostgreSQLLiteralFormatConfig() *spanvalue.FormatConfig {
	return &spanvalue.FormatConfig{
		NullString: spanvalue.LiteralFormatConfig().NullString,
		FormatArray: func(typ *sppb.Type, _ bool, elemStrings []string) (string, error) {
			return fmt.Sprintf("CAST(ARRAY[%s] AS %s)", strings.Join(elemStrings, ", "), FormatPostgreSQLType(typ)), nil
		},
		FormatComplexPlugins: []spanvalue.FormatComplexFunc{
			rejectUnsupportedPostgreSQLLiteralType,
		},
		FormatNullable: formatNullableValuePostgresqlLiteral,
	}
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
