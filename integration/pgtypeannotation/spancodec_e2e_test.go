// End-to-end probes for issue #3 phase 3: struct rows flow through
// spancodec.MutationColumnsAndValues -> spanpg.InsertStatement with values
// encoded via spancodec.ValueOf(v, spanpg.EncodeOptions()...), execute as DML
// on the POSTGRESQL-dialect emulator, read back via spancodec.ToStruct (no
// DecodeOptions — decode needs no PG adaptation, pinned below), and compare.
package pgtypeannotation_test

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spancodec"
	"github.com/apstndb/spanpg"
	"github.com/apstndb/spantype/typector"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
)

// codecRow exercises every Go type spanpg.EncodeOptions registers plus the
// pass-through PG wrappers and an ARRAY of a registered type.
type codecRow struct {
	ID     int64               `spanner:"id"`
	Rat    big.Rat             `spanner:"rat"`
	RatPtr *big.Rat            `spanner:"rat_ptr"`
	NN     spanner.NullNumeric `spanner:"nn"`
	NJ     spanner.NullJSON    `spanner:"nj"`
	PGN    spanner.PGNumeric   `spanner:"pgn"`
	PGJ    spanner.PGJsonB     `spanner:"pgj"`
	Rats   []big.Rat           `spanner:"rats"`
}

// ratComparer compares big.Rat by value (Cmp), not by internal representation.
var ratComparer = cmp.Comparer(func(a, b big.Rat) bool { return a.Cmp(&b) == 0 })

func createCodecTable(ctx context.Context, t *testing.T, env *pgEnv) string {
	t.Helper()
	table := fmt.Sprintf("codec_e2e_%d", time.Now().UnixNano())
	ddl := fmt.Sprintf(`CREATE TABLE %s (
		id bigint NOT NULL,
		rat numeric,
		rat_ptr numeric,
		nn numeric,
		nj jsonb,
		pgn numeric,
		pgj jsonb,
		rats numeric[],
		PRIMARY KEY (id)
	)`, table)
	op, err := env.dbAdmin.UpdateDatabaseDdl(ctx, &adminpb.UpdateDatabaseDdlRequest{
		Database:   env.dbPath,
		Statements: []string{ddl},
	})
	if err != nil {
		t.Fatalf("UpdateDatabaseDdl: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("UpdateDatabaseDdl Wait: %v", err)
	}
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer dropCancel()
		if op, err := env.dbAdmin.UpdateDatabaseDdl(dropCtx, &adminpb.UpdateDatabaseDdlRequest{
			Database:   env.dbPath,
			Statements: []string{"DROP TABLE " + table},
		}); err == nil {
			_ = op.Wait(dropCtx)
		}
	})
	return table
}

func execDML(ctx context.Context, client *spanner.Client, stmt spanner.Statement) error {
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, stmt)
		return err
	})
	return err
}

func TestPGDialect_SpancodecInsertReadBack(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	env := setupPGEnv(ctx, t)
	client := env.client
	table := createCodecTable(ctx, t, env)

	in := codecRow{
		ID:     1,
		Rat:    *big.NewRat(314, 100),
		RatPtr: nil, // typed NULL PG_NUMERIC via EncodeOptions
		NN:     spanner.NullNumeric{Numeric: *big.NewRat(-1, 8), Valid: true},
		NJ:     spanner.NullJSON{Value: map[string]any{"k": 1.0}, Valid: true},
		PGN:    spanner.PGNumeric{Numeric: "NaN", Valid: true}, // pass-through wrapper; NaN exercises the PG-only value space
		PGJ:    spanner.PGJsonB{Value: []any{1.0, "s"}, Valid: true},
		Rats:   []big.Rat{*big.NewRat(1, 2), *big.NewRat(3, 1)},
	}

	cols, vals, err := spancodec.MutationColumnsAndValues(in)
	if err != nil {
		t.Fatalf("MutationColumnsAndValues: %v", err)
	}

	t.Run("EncodeOptions-encoded GCV params accepted", func(t *testing.T) {
		encoded := make([]any, len(vals))
		for i, v := range vals {
			gcv, err := spancodec.ValueOf(v, spanpg.EncodeOptions()...)
			if err != nil {
				t.Fatalf("ValueOf(%s): %v", cols[i], err)
			}
			encoded[i] = gcv
		}
		stmt, err := spanpg.InsertStatement(table, cols, encoded)
		if err != nil {
			t.Fatalf("InsertStatement: %v", err)
		}
		if err := execDML(ctx, client, stmt); err != nil {
			t.Fatalf("insert with EncodeOptions-encoded params: %v", err)
		}
	})

	// Counter-probe: the same row bound as plain Go values, letting the client
	// encode. The client mirrors GoogleSQL forms for big.Rat / NullNumeric /
	// NullJSON, and the backend rejects the un-annotated types (emulator
	// 1.5.54: Unimplemented "Unsupported GoogleSQL Type: NUMERIC"), which is
	// exactly why EncodeOptions exists.
	t.Run("plain Go value params rejected", func(t *testing.T) {
		plain := make([]any, len(vals))
		copy(plain, vals)
		stmt, err := spanpg.InsertStatement(table, cols, plain)
		if err != nil {
			t.Fatalf("InsertStatement: %v", err)
		}
		stmt.Params["p1"] = int64(2) // distinct primary key in case a backend accepts
		err = execDML(ctx, client, stmt)
		if err == nil {
			t.Fatal("plain Go value params unexpectedly accepted; re-record the probe and revisit EncodeOptions godoc")
		}
		s, _ := status.FromError(err)
		switch s.Code() {
		case codes.Unimplemented, codes.InvalidArgument, codes.FailedPrecondition:
			t.Logf("probe: plain Go value params rejected as expected: code=%v msg=%q", s.Code(), s.Message())
		default:
			t.Fatalf("unexpected rejection: %v", err)
		}
	})

	t.Run("ToStruct read-back without DecodeOptions", func(t *testing.T) {
		iter := client.Single().Query(ctx, spanner.NewStatement(fmt.Sprintf(
			"SELECT %s FROM %s WHERE id = 1", strings.Join(cols, ", "), table)))
		defer iter.Stop()
		row, err := iter.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		var got codecRow
		if err := spancodec.ToStruct(row, &got); err != nil {
			t.Fatalf("ToStruct: %v", err)
		}
		if _, err := iter.Next(); err != iterator.Done {
			t.Fatalf("expected single row, got err=%v", err)
		}
		if diff := cmp.Diff(in, got, ratComparer, cmpopts.EquateEmpty()); diff != "" {
			t.Errorf("round trip (-want +got):\n%s", diff)
		}
	})
}

// TestPGDialect_RowTypeForWithEncodeOptions pins that static inference
// (spancodec.RowTypeFor with spanpg.EncodeOptions) yields PG-annotated field
// types for struct fields of the registered Go types, matching what the
// backend reports for the corresponding columns.
func TestPGDialect_RowTypeForWithEncodeOptions(t *testing.T) {
	t.Parallel()

	got, err := spancodec.RowTypeFor[codecRow](spanpg.EncodeOptions()...)
	if err != nil {
		t.Fatalf("RowTypeFor: %v", err)
	}
	want := &sppb.StructType{Fields: typector.MustNameTypeSlicesToStructTypeFields(
		[]string{"id", "rat", "rat_ptr", "nn", "nj", "pgn", "pgj", "rats"},
		[]*sppb.Type{
			typector.Int64(),
			typector.PGNumeric(),
			typector.PGNumeric(),
			typector.PGNumeric(),
			typector.PGJSONB(),
			typector.PGNumeric(),
			typector.PGJSONB(),
			typector.ElemTypeToArrayType(typector.PGNumeric()),
		},
	)}
	if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
		t.Errorf("RowTypeFor[codecRow] (-want +got):\n%s", diff)
	}
}

// TestPGDialect_DecodeNeedsNoAdaptation pins the task-3 decision: spanpg ships
// no DecodeOptions because the client (and so spancodec.Decode/ToStruct)
// checks only the TypeCode for big.Rat / NullNumeric / NullJSON destinations,
// decoding server-returned PG_NUMERIC / PG_JSONB GCVs natively. The remaining
// hazard is the value space, not the type: PG_NUMERIC "NaN" cannot decode into
// any big.Rat-based destination; spanner.PGNumeric is the NaN-safe target.
func TestPGDialect_DecodeNeedsNoAdaptation(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	client := setupPGEnv(ctx, t).client

	t.Run("PG_NUMERIC into big.Rat", func(t *testing.T) {
		gcv := selectOneGCV(ctx, t, client, "SELECT CAST('3.14' AS numeric)")
		if gcv.Type.GetTypeAnnotation() != sppb.TypeAnnotationCode_PG_NUMERIC {
			t.Fatalf("server column type = %v, want PG_NUMERIC annotation", gcv.Type)
		}
		var rat big.Rat
		if err := spancodec.Decode(gcv, &rat); err != nil {
			t.Fatalf("Decode into big.Rat: %v", err)
		}
		if rat.Cmp(big.NewRat(314, 100)) != 0 {
			t.Errorf("decoded %v, want 3.14", &rat)
		}
		var nn spanner.NullNumeric
		if err := spancodec.Decode(gcv, &nn); err != nil {
			t.Fatalf("Decode into NullNumeric: %v", err)
		}
		if !nn.Valid || nn.Numeric.Cmp(big.NewRat(314, 100)) != 0 {
			t.Errorf("decoded %v, want valid 3.14", nn)
		}
	})

	t.Run("PG_JSONB into NullJSON", func(t *testing.T) {
		gcv := selectOneGCV(ctx, t, client, `SELECT CAST('{"k": 1}' AS jsonb)`)
		if gcv.Type.GetTypeAnnotation() != sppb.TypeAnnotationCode_PG_JSONB {
			t.Fatalf("server column type = %v, want PG_JSONB annotation", gcv.Type)
		}
		var nj spanner.NullJSON
		if err := spancodec.Decode(gcv, &nj); err != nil {
			t.Fatalf("Decode into NullJSON: %v", err)
		}
		if !nj.Valid {
			t.Fatal("expected valid NullJSON")
		}
		if diff := cmp.Diff(map[string]any{"k": float64(1)}, nj.Value); diff != "" {
			t.Errorf("NullJSON.Value (-want +got):\n%s", diff)
		}
	})

	t.Run("PG_NUMERIC NaN cannot decode into big.Rat", func(t *testing.T) {
		gcv := selectOneGCV(ctx, t, client, "SELECT CAST('NaN' AS numeric)")
		var rat big.Rat
		err := spancodec.Decode(gcv, &rat)
		if err == nil {
			t.Fatal("Decode of NaN into big.Rat unexpectedly succeeded")
		}
		t.Logf("probe: NaN into big.Rat fails as documented: %v", err)
		var pgn spanner.PGNumeric
		if err := spancodec.Decode(gcv, &pgn); err != nil {
			t.Fatalf("Decode NaN into PGNumeric: %v", err)
		}
		want := spanner.PGNumeric{Numeric: "NaN", Valid: true}
		if diff := cmp.Diff(want, pgn); diff != "" {
			t.Errorf("PGNumeric (-want +got):\n%s", diff)
		}
	})
}
