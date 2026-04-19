package spanpg_test

import (
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanpg"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/uuid"
)

func mustGCV(t *testing.T, v spanner.GenericColumnValue, err error) spanner.GenericColumnValue {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func mustRow(t *testing.T, row *spanner.Row, err error) *spanner.Row {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestFormatColumnPostgreSQLLiteral(t *testing.T) {
	t.Parallel()

	pgJSONBGCV, pgJSONBErr := gcvctor.PGJSONBValue(map[string]any{"msg": "foo"})
	pgJSONBValue := mustGCV(t, pgJSONBGCV, pgJSONBErr)
	int64ArrayGCV, int64ArrayErr := gcvctor.ArrayValue(gcvctor.Int64Value(1), gcvctor.Int64Value(2))
	int64ArrayValue := mustGCV(t, int64ArrayGCV, int64ArrayErr)
	nullInt64ArrayGCV, nullInt64ArrayErr := gcvctor.ArrayValue(gcvctor.NullFromCode(sppb.TypeCode_INT64), gcvctor.NullFromCode(sppb.TypeCode_INT64))
	nullInt64ArrayValue := mustGCV(t, nullInt64ArrayGCV, nullInt64ArrayErr)

	tests := []struct {
		name  string
		value spanner.GenericColumnValue
		want  string
	}{
		{
			name:  "string",
			value: gcvctor.StringValue("that's it"),
			want:  `'that''s it'`,
		},
		{
			name:  "null string",
			value: gcvctor.NullFromCode(sppb.TypeCode_STRING),
			want:  `NULL`,
		},
		{
			name:  "null int64",
			value: gcvctor.NullFromCode(sppb.TypeCode_INT64),
			want:  `NULL`,
		},
		{
			name:  "int64",
			value: gcvctor.Int64Value(123),
			want:  `123`,
		},
		{
			name:  "bytes",
			value: gcvctor.BytesValue([]byte("abc")),
			want:  `CAST('\x616263' AS bytea)`,
		},
		{
			name:  "date",
			value: gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15}),
			want:  `CAST('2024-01-15' AS date)`,
		},
		{
			name:  "timestamp",
			value: gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 0, time.FixedZone("+09", 9*60*60))),
			want:  `CAST('2024-01-15T03:00:00Z' AS timestamptz)`,
		},
		{
			name:  "numeric",
			value: gcvctor.NumericValue(big.NewRat(123456, 100)),
			want:  `CAST('1234.560000000' AS numeric)`,
		},
		{
			name:  "pg jsonb",
			value: pgJSONBValue,
			want:  `CAST('{"msg":"foo"}' AS jsonb)`,
		},
		{
			name: "interval",
			value: gcvctor.IntervalValue(spanner.Interval{
				Months: 13,
				Days:   1,
				Nanos:  big.NewInt((3600 + 60 + 1) * 1000 * 1000 * 1000),
			}),
			want: `CAST('P1Y1M1DT1H1M1S' AS interval)`,
		},
		{
			name:  "uuid",
			value: gcvctor.UUIDValue(uuid.MustParse("858ebda5-f6df-4f5d-9151-aa98796053c4")),
			want:  `CAST('858ebda5-f6df-4f5d-9151-aa98796053c4' AS uuid)`,
		},
		{
			name:  "empty int64 array",
			value: gcvctor.EmptyArrayOf(typector.CodeToSimpleType(sppb.TypeCode_INT64)),
			want:  `CAST(ARRAY[] AS bigint[])`,
		},
		{
			name:  "int64 array",
			value: int64ArrayValue,
			want:  `CAST(ARRAY[1, 2] AS bigint[])`,
		},
		{
			name:  "all null int64 array",
			value: nullInt64ArrayValue,
			want:  `CAST(ARRAY[NULL, NULL] AS bigint[])`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := spanpg.FormatColumnPostgreSQLLiteral(tt.value)
			if err != nil {
				t.Fatalf("FormatColumnPostgreSQLLiteral() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("FormatColumnPostgreSQLLiteral() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatColumnPostgreSQLLiteralFloats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value spanner.GenericColumnValue
		want  string
	}{
		{"float32 finite", gcvctor.Float32Value(1.5), `CAST(1.5 AS float4)`},
		{"float32 NaN", gcvctor.Float32Value(float32(math.NaN())), `CAST('NaN' AS float4)`},
		{"float32 Infinity", gcvctor.Float32Value(float32(math.Inf(1))), `CAST('Infinity' AS float4)`},
		{"float64 finite", gcvctor.Float64Value(2.5), `CAST(2.5 AS float8)`},
		{"float64 -Infinity", gcvctor.Float64Value(math.Inf(-1)), `CAST('-Infinity' AS float8)`},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := spanpg.FormatColumnPostgreSQLLiteral(tt.value)
			if err != nil {
				t.Fatalf("FormatColumnPostgreSQLLiteral() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("FormatColumnPostgreSQLLiteral() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFormatColumnPostgreSQLLiteralUnsupportedTypes(t *testing.T) {
	t.Parallel()

	structGCV, structErr := gcvctor.StructValueOf(
		[]string{"a", "b"},
		[]spanner.GenericColumnValue{
			gcvctor.Int64Value(1),
			gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15}),
		},
	)
	structValue := mustGCV(t, structGCV, structErr)
	nestedStructGCV, nestedStructErr := gcvctor.StructValueOf(
		[]string{"a"},
		[]spanner.GenericColumnValue{gcvctor.Int64Value(1)},
	)
	arrayOfStructGCV, arrayOfStructErr := gcvctor.ArrayValueOf(
		typector.NameCodeToStructType("a", sppb.TypeCode_INT64),
		mustGCV(t, nestedStructGCV, nestedStructErr),
	)
	arrayOfStructValue := mustGCV(t, arrayOfStructGCV, arrayOfStructErr)
	jsonGCV, jsonErr := gcvctor.JSONValue(map[string]any{"msg": "foo"})
	jsonValue := mustGCV(t, jsonGCV, jsonErr)

	tests := []struct {
		name  string
		value spanner.GenericColumnValue
	}{
		{
			name:  "struct",
			value: structValue,
		},
		{
			name:  "proto",
			value: gcvctor.ProtoValue("examples.spanner.music.SingerInfo", []byte("abc")),
		},
		{
			name:  "enum",
			value: gcvctor.EnumValue("examples.spanner.music.Genre", 1),
		},
		{
			name:  "plain json",
			value: jsonValue,
		},
		{
			name:  "array of struct",
			value: arrayOfStructValue,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := spanpg.FormatColumnPostgreSQLLiteral(tt.value)
			if !errors.Is(err, spanpg.ErrUnsupportedPostgreSQLType) {
				t.Fatalf("FormatColumnPostgreSQLLiteral() error = %v, want ErrUnsupportedPostgreSQLType", err)
			}
		})
	}
}

func TestFormatRowPostgreSQLLiteral(t *testing.T) {
	t.Parallel()

	rowValue, rowErr := spanner.NewRow(
		[]string{"name", "active"},
		[]interface{}{"that's it", true},
	)
	row := mustRow(t, rowValue, rowErr)

	got, err := spanpg.FormatRowPostgreSQLLiteral(row)
	if err != nil {
		t.Fatalf("FormatRowPostgreSQLLiteral() error = %v", err)
	}
	want := []string{`'that''s it'`, `true`}
	if len(got) != len(want) {
		t.Fatalf("FormatRowPostgreSQLLiteral() len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("FormatRowPostgreSQLLiteral()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
