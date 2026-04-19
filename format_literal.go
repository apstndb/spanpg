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

var postgreSQLLiteralFormatConfig = PostgreSQLLiteralFormatConfig()

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
			if len(elemStrings) == 0 {
				return fmt.Sprintf("CAST(ARRAY[] AS %s)", FormatPostgreSQLType(typ)), nil
			}
			return fmt.Sprintf("ARRAY[%s]", strings.Join(elemStrings, ", ")), nil
		},
		FormatStruct: spanvalue.FormatStruct{
			FormatStructField: spanvalue.FormatSimpleStructField,
			FormatStructParen: func(_ *sppb.Type, _ bool, fieldStrings []string) (string, error) {
				return fmt.Sprintf("ROW(%s)", strings.Join(fieldStrings, ", ")), nil
			},
		},
		FormatComplexPlugins: []spanvalue.FormatComplexFunc{
			rejectUnsupportedPostgreSQLLiteralType,
		},
		FormatNullable: formatNullableValuePostgreSQLLiteral,
	}
}

// FormatRowPostgreSQLLiteral formats a row using PostgreSQLLiteralFormatConfig.
func FormatRowPostgreSQLLiteral(value *spanner.Row) ([]string, error) {
	return postgreSQLLiteralFormatConfig.FormatRow(value)
}

// FormatColumnPostgreSQLLiteral formats a top-level column using PostgreSQLLiteralFormatConfig.
func FormatColumnPostgreSQLLiteral(value spanner.GenericColumnValue) (string, error) {
	return postgreSQLLiteralFormatConfig.FormatToplevelColumn(value)
}

func unsupportedPostgreSQLType(typ *sppb.Type) bool {
	if typ == nil {
		return false
	}
	switch typ.GetCode() {
	case sppb.TypeCode_PROTO, sppb.TypeCode_ENUM, sppb.TypeCode_STRUCT:
		return true
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

func postgreSQLStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func postgreSQLCast(expr string, typ string) string {
	return fmt.Sprintf("CAST(%s AS %s)", expr, typ)
}

func postgreSQLFloatLiteral(v float64, bits int) string {
	typ := "float8"
	if bits == 32 {
		typ = "float4"
	}
	switch {
	case math.IsNaN(v):
		return postgreSQLCast(postgreSQLStringLiteral("NaN"), typ)
	case math.IsInf(v, 1):
		return postgreSQLCast(postgreSQLStringLiteral("Infinity"), typ)
	case math.IsInf(v, -1):
		return postgreSQLCast(postgreSQLStringLiteral("-Infinity"), typ)
	default:
		return postgreSQLCast(strconv.FormatFloat(v, 'g', -1, bits), typ)
	}
}

func formatNullableValuePostgreSQLLiteral(value spanvalue.NullableValue) (string, error) {
	switch v := value.(type) {
	case spanner.NullString:
		return postgreSQLStringLiteral(v.StringVal), nil
	case spanner.NullBool:
		return strconv.FormatBool(v.Bool), nil
	case spanvalue.NullBytes:
		return postgreSQLCast(postgreSQLStringLiteral(`\x`+hex.EncodeToString(v)), "bytea"), nil
	case spanner.NullFloat32:
		return postgreSQLFloatLiteral(float64(v.Float32), 32), nil
	case spanner.NullFloat64:
		return postgreSQLFloatLiteral(v.Float64, 64), nil
	case spanner.NullInt64:
		return strconv.FormatInt(v.Int64, 10), nil
	case spanner.NullNumeric:
		return postgreSQLCast(postgreSQLStringLiteral(spanner.NumericString(&v.Numeric)), "numeric"), nil
	case spanner.PGNumeric:
		return postgreSQLCast(postgreSQLStringLiteral(v.Numeric), "numeric"), nil
	case spanner.NullTime:
		return postgreSQLCast(postgreSQLStringLiteral(v.Time.UTC().Format(time.RFC3339Nano)), "timestamptz"), nil
	case spanner.NullDate:
		return postgreSQLCast(postgreSQLStringLiteral(v.Date.String()), "date"), nil
	case spanner.NullJSON:
		return postgreSQLCast(postgreSQLStringLiteral(v.String()), "json"), nil
	case spanner.PGJsonB:
		b, err := json.Marshal(v.Value)
		if err != nil {
			return "", err
		}
		return postgreSQLCast(postgreSQLStringLiteral(string(b)), "jsonb"), nil
	case spanner.NullInterval:
		return postgreSQLCast(postgreSQLStringLiteral(v.String()), "interval"), nil
	case spanner.NullUUID:
		return postgreSQLCast(postgreSQLStringLiteral(v.String()), "uuid"), nil
	default:
		return "", fmt.Errorf("%w: %T", spanvalue.ErrUnknownType, v)
	}
}
