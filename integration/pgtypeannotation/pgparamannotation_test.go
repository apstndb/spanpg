// Probes for issue #3 phase 3: does the POSTGRESQL dialect REQUIRE the
// PG_NUMERIC / PG_JSONB TypeAnnotation on query parameters, or is the
// annotation merely the canonical form the client sends?
//
// The existing TypeAnnotation tests bind spanner.NullNumeric / spanner.NullJSON
// and let the client encode; these probes bind spanner.GenericColumnValue
// params directly, which pins the exact sppb.Type on the wire (Statement
// Params accept GenericColumnValue as-is).
package pgtypeannotation_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"google.golang.org/grpc/status"
)

// queryGCVParam executes `SELECT $1` binding param as-is and returns the
// metadata type of the result column, or the query error.
func queryGCVParam(ctx context.Context, t *testing.T, client *spanner.Client, param any) (*sppb.Type, error) {
	t.Helper()
	stmt := spanner.Statement{
		SQL:    `SELECT $1 AS out_col`,
		Params: map[string]any{"p1": param},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()
	if _, err := iter.Next(); err != nil {
		return nil, err
	}
	if iter.Metadata == nil || iter.Metadata.RowType == nil || len(iter.Metadata.RowType.GetFields()) != 1 {
		t.Fatal("expected single-column ResultSetMetadata.RowType after first Next")
	}
	return iter.Metadata.RowType.GetFields()[0].GetType(), nil
}

// TestPGDialect_GCVParamAnnotationRequirement binds GenericColumnValue params
// whose Type differs only in TypeAnnotation and records which the backend
// accepts. The findings drive spanpg.EncodeOptions' godoc claims.
//
// Probe result (emulator 1.5.54): the annotation is REQUIRED, not just the
// canonical form the client sends. Un-annotated params are rejected with
// codes.Unimplemented:
//
//	code:NUMERIC            -> Unimplemented "Unsupported GoogleSQL Type: NUMERIC"
//	code:JSON               -> Unimplemented "Unsupported GoogleSQL Type: JSON"
//	code:NUMERIC + PG_NUMERIC -> accepted; result column echoes the annotation
//	code:JSON    + PG_JSONB   -> accepted; result column echoes the annotation
func TestPGDialect_GCVParamAnnotationRequirement(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	client := setupPGEnv(ctx, t).client

	tests := []struct {
		name  string
		param spanner.GenericColumnValue
		// wantAccepted pins the probe result (emulator); a flip on another
		// backend is a finding worth re-recording, not a silent pass.
		wantAccepted   bool
		wantAnnotation sppb.TypeAnnotationCode
	}{
		{
			name:           "NUMERIC without annotation",
			param:          gcvctor.StringBasedValueOf(typector.Numeric(), "3.14"),
			wantAccepted:   false,
			wantAnnotation: sppb.TypeAnnotationCode_TYPE_ANNOTATION_CODE_UNSPECIFIED,
		},
		{
			name:           "NUMERIC with PG_NUMERIC annotation",
			param:          gcvctor.StringBasedValueOf(typector.PGNumeric(), "3.14"),
			wantAccepted:   true,
			wantAnnotation: sppb.TypeAnnotationCode_PG_NUMERIC,
		},
		{
			name:           "JSON without annotation",
			param:          gcvctor.StringBasedValueOf(typector.JSON(), `{"k": 1}`),
			wantAccepted:   false,
			wantAnnotation: sppb.TypeAnnotationCode_TYPE_ANNOTATION_CODE_UNSPECIFIED,
		},
		{
			name:           "JSON with PG_JSONB annotation",
			param:          gcvctor.StringBasedValueOf(typector.PGJSONB(), `{"k": 1}`),
			wantAccepted:   true,
			wantAnnotation: sppb.TypeAnnotationCode_PG_JSONB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, err := queryGCVParam(ctx, t, client, tt.param)
			if err != nil {
				if tt.wantAccepted {
					t.Fatalf("param %v rejected: %v", tt.param.Type, err)
				}
				s, _ := status.FromError(err)
				t.Logf("probe: param %v rejected as expected: code=%v msg=%q", tt.param.Type, s.Code(), s.Message())
				return
			}
			if !tt.wantAccepted {
				t.Fatalf("param %v unexpectedly accepted; result type=%v (update EncodeOptions godoc)", tt.param.Type, typ)
			}
			t.Logf("probe: param %v accepted; result column type=%v", tt.param.Type, typ)
			if got := typ.GetTypeAnnotation(); got != tt.wantAnnotation {
				t.Errorf("result TypeAnnotation: got %v want %v", got, tt.wantAnnotation)
			}
		})
	}
}
