package spanpg_test

import (
	"math/big"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spanpg"
)

func TestFormatPostgreSQLType(t *testing.T) {
	t.Parallel()
	st := typector.MustNameCodeSlicesToStructType([]string{"a", "b"}, []sppb.TypeCode{
		sppb.TypeCode_INT64, sppb.TypeCode_STRING,
	})
	for _, tt := range []struct {
		desc string
		typ  *sppb.Type
		want string
	}{
		{"nil", nil, ""},
		{"bool", typector.Bool(), "bool"},
		{"int64", typector.Int64(), "bigint"},
		{"pg oid", typector.PGOID(), "oid"},
		{"float32", typector.Float32(), "float4"},
		{"float64", typector.Float64(), "float8"},
		{"string", typector.String(), "text"},
		{"bytes", typector.Bytes(), "bytea"},
		{"timestamp", typector.Timestamp(), "timestamptz"},
		{"date", typector.Date(), "date"},
		{"numeric", typector.Numeric(), "numeric"},
		{"pg numeric", typector.PGNumeric(), "numeric"},
		{"json without PG_JSONB", typector.JSON(), "json"},
		{"json with PG_JSONB", typector.PGJSONB(), "jsonb"},
		{"interval", typector.Interval(), "interval"},
		{"uuid", typector.UUID(), "uuid"},
		{"proto", typector.FQNToProtoType("google.spanner.v1.TypeProto"), "proto"},
		{"enum", typector.FQNToEnumType("my.Enum"), "enum"},
		{"unspecified", &sppb.Type{}, "unknown"},
		{"array bigint", typector.ElemCodeToArrayType(sppb.TypeCode_INT64), "bigint[]"},
		{"nested array", typector.ElemTypeToArrayType(typector.ElemCodeToArrayType(sppb.TypeCode_STRING)), "text[][]"},
		{"array jsonb", typector.ElemTypeToArrayType(typector.PGJSONB()), "jsonb[]"},
		{"array json", typector.ElemTypeToArrayType(typector.JSON()), "json[]"},
		{"struct named", st, "STRUCT<a bigint, b text>"},
		{"struct unnamed", typector.StructTypeFieldsToStructType([]*sppb.StructType_Field{
			typector.CodeToUnnamedStructTypeField(sppb.TypeCode_STRING),
			typector.CodeToUnnamedStructTypeField(sppb.TypeCode_BOOL),
		}), "STRUCT<text, bool>"},
		{"empty struct", typector.StructTypeFieldsToStructType(nil), "STRUCT<>"},
		{"struct of array", typector.NameTypeToStructType("xs", typector.ElemCodeToArrayType(sppb.TypeCode_INT64)),
			"STRUCT<xs bigint[]>"},
		{"nested struct", typector.NameTypeToStructType("inner", typector.MustNameCodeSlicesToStructType(
			[]string{"x", "y"}, []sppb.TypeCode{sppb.TypeCode_INT64, sppb.TypeCode_STRING})),
			"STRUCT<inner STRUCT<x bigint, y text>>"},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()
			got := spanpg.FormatPostgreSQLType(tt.typ)
			if got != tt.want {
				t.Fatalf("FormatPostgreSQLType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPostgreSQLCatalogTypeName(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		desc string
		typ  *sppb.Type
		want string
		ok   bool
	}{
		{"PG numeric", typector.PGNumeric(), "numeric", true},
		{"PG jsonb", typector.PGJSONB(), "jsonb", true},
		{"PG oid", typector.PGOID(), "oid", true},
		{"plain int64", typector.CodeToSimpleType(sppb.TypeCode_INT64), "", false},
		{"nil", nil, "", false},
		{"array", typector.ElemTypeToArrayType(typector.PGNumeric()), "", false},
	} {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()
			got, ok := spanpg.PostgreSQLCatalogTypeName(tt.typ)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("PostgreSQLCatalogTypeName() = (%q, %v), want (%q, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestStatementParamKey_and_PostgreSQLPlaceholder(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		n       int
		wantKey string
		wantPh  string
		ok      bool
	}{
		{1, "p1", "$1", true},
		{2, "p2", "$2", true},
		{0, "", "", false},
		{-1, "", "", false},
	} {
		k, okK := spanpg.StatementParamKey(tt.n)
		p, okP := spanpg.PostgreSQLPlaceholder(tt.n)
		if okK != tt.ok || okP != tt.ok {
			t.Fatalf("n=%d: okKey=%v okPh=%v want ok=%v", tt.n, okK, okP, tt.ok)
		}
		if tt.ok && (k != tt.wantKey || p != tt.wantPh) {
			t.Fatalf("n=%d: key=%q ph=%q want key=%q ph=%q", tt.n, k, p, tt.wantKey, tt.wantPh)
		}
	}
}

func TestFormatColumnSimple_PGNumeric(t *testing.T) {
	t.Parallel()
	rat := big.NewRat(314, 100)
	gcv := gcvctor.PGNumericValue(rat)
	want, err := spanpg.FormatColumnSimple(gcvctor.NumericValue(rat))
	if err != nil {
		t.Fatalf("want: %v", err)
	}
	got, err := spanpg.FormatColumnSimple(gcv)
	if err != nil {
		t.Fatalf("FormatColumnSimple: %v", err)
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("FormatColumnSimple (-want +got):\n%s", diff)
	}
}
