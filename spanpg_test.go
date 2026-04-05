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
