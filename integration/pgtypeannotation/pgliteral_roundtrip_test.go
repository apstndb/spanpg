// Literal round-trip harness for spanpg.PostgreSQLLiteralFormatConfig (issue #3, phase 2).
//
// Every literal form the formatter emits is pinned by execution, not inspection:
// build a GCV with gcvctor, format it with FormatColumnPostgreSQLLiteral, execute
// `SELECT <literal>` against a POSTGRESQL-dialect Spanner database, read column 0
// back as a GCV, and compare Type+Value with the expected GCV. The default expected
// value is the input itself; per-case `want` overrides document server-side
// canonicalization (e.g. jsonb text normalization).
package pgtypeannotation_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	adminpb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spanpg"
	"github.com/apstndb/spantype/typector"
	"github.com/apstndb/spanvalue/gcvctor"
	"github.com/apstndb/spanvalue/writer"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/testing/protocmp"
)

// selectOneGCV executes sql in a single-use read-only transaction and returns
// the single row's column 0 as a GenericColumnValue.
func selectOneGCV(ctx context.Context, t *testing.T, client *spanner.Client, sql string) spanner.GenericColumnValue {
	t.Helper()
	iter := client.Single().Query(ctx, spanner.NewStatement(sql))
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		t.Fatalf("query %q: %v", sql, err)
	}
	var gcv spanner.GenericColumnValue
	if err := row.Column(0, &gcv); err != nil {
		t.Fatalf("Column(0) for %q: %v", sql, err)
	}
	if _, err := iter.Next(); err != iterator.Done {
		t.Fatalf("expected single row for %q, got err=%v", sql, err)
	}
	return gcv
}

func pgNumericGCV(wire string) spanner.GenericColumnValue {
	return gcvctor.StringBasedValueOf(typector.PGNumeric(), wire)
}

func pgJSONBWireGCV(wire string) spanner.GenericColumnValue {
	return gcvctor.StringBasedValueOf(typector.PGJSONB(), wire)
}

func gcvPtr(v spanner.GenericColumnValue) *spanner.GenericColumnValue { return &v }

func TestPostgreSQLLiteralRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	env := setupPGEnv(ctx, t)
	client := env.client

	tests := []struct {
		name  string
		input spanner.GenericColumnValue
		// want overrides the expected GCV when the server canonicalizes the value;
		// nil means the round trip must return the input unchanged.
		want *spanner.GenericColumnValue
		// wantLiteral, when non-empty, additionally pins the exact literal text.
		wantLiteral string
		// skip documents probe results that make the case non-executable.
		skip string
	}{
		// bool
		{name: "bool true", input: gcvctor.BoolValue(true)},
		{name: "bool false", input: gcvctor.BoolValue(false)},

		// bigint
		{name: "bigint zero", input: gcvctor.Int64Value(0)},
		{name: "bigint min", input: gcvctor.Int64Value(math.MinInt64)},
		{name: "bigint max", input: gcvctor.Int64Value(math.MaxInt64)},

		// float8
		{name: "float8 1.5", input: gcvctor.Float64Value(1.5)},
		{name: "float8 integral", input: gcvctor.Float64Value(1)},
		{name: "float8 1e300", input: gcvctor.Float64Value(1e300)},
		{name: "float8 smallest subnormal", input: gcvctor.Float64Value(math.SmallestNonzeroFloat64)},
		{name: "float8 NaN", input: gcvctor.Float64Value(math.NaN())},
		{name: "float8 +Inf", input: gcvctor.Float64Value(math.Inf(1))},
		{name: "float8 -Inf", input: gcvctor.Float64Value(math.Inf(-1))},

		// float4
		{name: "float4 1.5", input: gcvctor.Float32Value(1.5)},
		{name: "float4 NaN", input: gcvctor.Float32Value(float32(math.NaN()))},

		// numeric (PGNumeric wire strings)
		{name: "numeric 3.14", input: pgNumericGCV("3.14")},
		{name: "numeric tiny negative", input: pgNumericGCV("-0.000000001")},
		{name: "numeric 30 digits", input: pgNumericGCV("123456789012345678901234567890")},
		{name: "numeric NaN", input: pgNumericGCV("NaN")},

		// text — probes standard_conforming_strings: backslashes must stay literal in '...'
		{name: "text empty", input: gcvctor.StringValue("")},
		{name: "text single quote", input: gcvctor.StringValue("it's")},
		{name: "text backslash", input: gcvctor.StringValue(`a\b`)},
		{name: "text trailing backslash", input: gcvctor.StringValue(`tail\`)},
		{name: "text newline", input: gcvctor.StringValue("line1\nline2")},
		{name: "text unicode", input: gcvctor.StringValue("日本語🙂")},

		// bytea
		{name: "bytea empty", input: gcvctor.BytesValue([]byte{})},
		{name: "bytea binary", input: gcvctor.BytesValue([]byte{0x00, 0xff, 0x27})},

		// timestamptz
		{
			// Probe: PG-dialect timestamptz parses literals with microsecond precision;
			// sub-microsecond digits are rounded (123456789ns -> 123457µs).
			name:  "timestamptz nanosecond fraction rounds to microseconds",
			input: gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 123456789, time.UTC)),
			want:  gcvPtr(gcvctor.MustTimestampStringValue("2024-01-15T12:00:00.123457Z")),
		},
		{
			name:  "timestamptz trailing-zero fraction",
			input: gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 500000000, time.UTC)),
		},
		{
			// Non-UTC input: gcvctor + the formatter normalize to UTC; pin the wire literal.
			name:        "timestamptz zoned input",
			input:       gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 0, time.FixedZone("+09", 9*60*60))),
			wantLiteral: `CAST('2024-01-15T03:00:00Z' AS timestamptz)`,
		},
		{name: "timestamptz min year", input: gcvctor.TimestampValue(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC))},
		{name: "timestamptz max year microsecond", input: gcvctor.TimestampValue(time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC))},
		{
			name:  "timestamptz max year nanosecond",
			input: gcvctor.TimestampValue(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)),
			// Probe (emulator 1.5.54): microsecond rounding pushes the maximum Spanner
			// timestamp out of range, so the literal is rejected:
			//   spanner: code = "InvalidArgument", desc = "Timestamp is out of supported range"
			// The wire value itself is valid (GoogleSQL-dialect Spanner can return it);
			// only the PG-dialect literal parse fails. Documented in
			// PostgreSQLLiteralFormatConfig's godoc.
			skip: `PG-dialect timestamptz literal parse rounds to microseconds; 9999-12-31T23:59:59.999999999Z rounds out of range (InvalidArgument "Timestamp is out of supported range")`,
		},

		// date
		{name: "date", input: gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15})},
		{name: "date min", input: gcvctor.DateValue(civil.Date{Year: 1, Month: 1, Day: 1})},

		// interval — the formatter embeds the INTERVAL wire string as-is
		// (server-canonical form round-trips byte-identically).
		{name: "interval", input: gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P1Y2M3DT4H5M6.5S")},
		{name: "interval negative parts", input: gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P-1Y-2M3DT-3H-55M-6.5S")},
		{
			// MustIntervalStringValue canonicalizes through spanner.Interval.String(), which
			// pads fractional seconds to 3-digit groups ("6.5S" -> "6.500S"). Probe: the
			// padded literal parses fine; the server trims trailing fraction zeros on return.
			name:  "interval client-padded fraction",
			input: gcvctor.MustIntervalStringValue("P1Y2M3DT4H5M6.5S"),
			want:  gcvPtr(gcvctor.StringBasedValueFromCode(sppb.TypeCode_INTERVAL, "P1Y2M3DT4H5M6.5S")),
		},

		// uuid
		{name: "uuid", input: gcvctor.UUIDValue(uuid.MustParse("858ebda5-f6df-4f5d-9151-aa98796053c4"))},

		// jsonb (PG_JSONB GCVs). Probe: the server normalizes jsonb text on return
		// (space after ':' and ',', unicode escapes decoded), so compact Go-marshaled
		// inputs come back in canonical form; want overrides pin that canonicalization.
		{
			name:  "jsonb object",
			input: gcvctor.MustPGJSONBValue(map[string]any{"a": 1.0, "b": "s"}),
			want:  gcvPtr(pgJSONBWireGCV(`{"a": 1, "b": "s"}`)),
		},
		{
			name:  "jsonb nested array",
			input: gcvctor.MustPGJSONBValue([]any{1.0, []any{2.0, 3.0}, map[string]any{"k": nil}}),
			want:  gcvPtr(pgJSONBWireGCV(`[1, [2, 3], {"k": null}]`)),
		},
		{name: "jsonb string value", input: gcvctor.MustPGJSONBValue("foo")},
		{
			// Go's json.Marshal HTML-escapes <, >, and & as Unicode escape sequences
			// (u003c, u003e, u0026 with a backslash) in the input wire; PG jsonb
			// canonicalization decodes the escapes back to raw characters.
			// Non-ASCII (including the emoji) passes through unescaped in both directions.
			name:  "jsonb unicode and html chars",
			input: gcvctor.MustPGJSONBValue(map[string]any{"s": `<a>&"日本語🙂`}),
			want:  gcvPtr(pgJSONBWireGCV(`{"s": "<a>&\"日本語🙂"}`)),
		},
		{
			// Already in canonical form, so this pins byte-identical round-trip: the
			// formatter embeds the jsonb wire text as-is (no float64 re-marshaling, which
			// would corrupt the integer to 12345678901234567000).
			name:  "jsonb large integer",
			input: pgJSONBWireGCV(`{"n": 12345678901234567890}`),
		},

		// arrays
		{
			name: "bigint array with NULL element",
			input: gcvctor.MustArrayValueOf(typector.Int64(),
				gcvctor.Int64Value(1), gcvctor.NullOf(typector.Int64()), gcvctor.Int64Value(3)),
		},
		{name: "empty text array", input: gcvctor.EmptyArrayOf(typector.String())},
		{
			name: "float8 array with NaN",
			input: gcvctor.MustArrayValueOf(typector.Float64(),
				gcvctor.Float64Value(1.5), gcvctor.Float64Value(math.NaN())),
		},
		{
			// Element canonicalization applies inside arrays too ({"a":1} -> {"a": 1}).
			name: "jsonb array",
			input: gcvctor.MustArrayValueOf(typector.PGJSONB(),
				gcvctor.MustPGJSONBValue(map[string]any{"a": 1.0}), gcvctor.NullOf(typector.PGJSONB())),
			want: gcvPtr(gcvctor.MustArrayValueOf(typector.PGJSONB(),
				pgJSONBWireGCV(`{"a": 1}`), gcvctor.NullOf(typector.PGJSONB()))),
		},
		{
			name: "bytea array",
			input: gcvctor.MustArrayValueOf(typector.Bytes(),
				gcvctor.BytesValue([]byte{0x00, 0xff}), gcvctor.BytesValue([]byte("abc"))),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skip != "" {
				t.Skip(tt.skip)
			}
			lit, err := spanpg.FormatColumnPostgreSQLLiteral(tt.input)
			if err != nil {
				t.Fatalf("FormatColumnPostgreSQLLiteral() error = %v", err)
			}
			if tt.wantLiteral != "" && lit != tt.wantLiteral {
				t.Errorf("literal = %q, want %q", lit, tt.wantLiteral)
			}
			got := selectOneGCV(ctx, t, client, "SELECT "+lit)
			want := tt.input
			if tt.want != nil {
				want = *tt.want
			}
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Errorf("round trip of %q (-want +got):\n%s", lit, diff)
			}
		})
	}
}

// TestPostgreSQLLiteralNullHandling pins the NULL rendering contract:
// scalar NULL and NULL arrays render as the bare keyword NULL (untyped,
// INSERT-context-friendly), and the same literal is valid in a typed context
// (CAST(NULL AS <pgtype>) round-trips to the typed NULL GCV). A separate probe
// records what a context-free `SELECT NULL` does on Spanner PG.
func TestPostgreSQLLiteralNullHandling(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	env := setupPGEnv(ctx, t)
	client := env.client

	// Probe result (emulator 1.5.54): SELECT NULL is accepted and the column
	// metadata reports code:STRING — Spanner PG resolves the untyped NULL to text,
	// matching PostgreSQL's "unknown"-to-text resolution. The probe stays
	// observational (log, not assert) because real Cloud Spanner may differ.
	t.Run("bare SELECT NULL probe", func(t *testing.T) {
		iter := client.Single().Query(ctx, spanner.NewStatement("SELECT NULL"))
		defer iter.Stop()
		row, err := iter.Next()
		if err != nil {
			t.Logf("probe: SELECT NULL rejected: %v", err)
			return
		}
		var gcv spanner.GenericColumnValue
		if err := row.Column(0, &gcv); err != nil {
			t.Fatalf("Column(0): %v", err)
		}
		t.Logf("probe: SELECT NULL accepted; metadata type = %v", gcv.Type)
	})

	nullTypes := []*sppb.Type{
		typector.Bool(),
		typector.Int64(),
		typector.Float64(),
		typector.Float32(),
		typector.PGNumeric(),
		typector.String(),
		typector.Bytes(),
		typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP),
		typector.Date(),
		typector.PGJSONB(),
		typector.Interval(),
		typector.CodeToSimpleType(sppb.TypeCode_UUID),
		typector.ElemTypeToArrayType(typector.Int64()),
		typector.ElemTypeToArrayType(typector.String()),
		typector.ElemTypeToArrayType(typector.PGJSONB()),
	}
	for _, typ := range nullTypes {
		spelling := spanpg.FormatPostgreSQLType(typ)
		t.Run("typed context "+spelling, func(t *testing.T) {
			input := gcvctor.NullOf(typ)
			lit, err := spanpg.FormatColumnPostgreSQLLiteral(input)
			if err != nil {
				t.Fatalf("FormatColumnPostgreSQLLiteral() error = %v", err)
			}
			if lit != "NULL" {
				t.Fatalf("NULL literal = %q, want bare NULL", lit)
			}
			sql := fmt.Sprintf("SELECT CAST(%s AS %s)", lit, spelling)
			got := selectOneGCV(ctx, t, client, sql)
			if diff := cmp.Diff(input, got, protocmp.Transform()); diff != "" {
				t.Errorf("round trip of %q (-want +got):\n%s", sql, diff)
			}
		})
	}
}

// TestPostgreSQLSQLInsertEndToEnd proves the documented composition for PG SQL INSERT
// export (apstndb/spanvalue#126): writer.NewSQLInsertWriter with
// WithSQLDialect(POSTGRESQL) for identifier quoting plus
// WithFormatter(spanpg.PostgreSQLLiteralFormatConfig()) for PG value literals.
// Generated statements are executed as DML against a PG-dialect table and the
// rows are read back and compared as GCVs.
func TestPostgreSQLSQLInsertEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	env := setupPGEnv(ctx, t)
	client := env.client

	// Note: table names with the prefix "pg_" are rejected by Spanner PG
	// (InvalidArgument: 'pg_' is not supported as a prefix for a table name).
	table := fmt.Sprintf("literal_e2e_%d", time.Now().UnixNano())
	ddl := fmt.Sprintf(`CREATE TABLE %s (
		id bigint NOT NULL,
		b bool,
		f8 double precision,
		f4 real,
		n numeric,
		s text,
		byt bytea,
		ts timestamptz,
		d date,
		j jsonb,
		u uuid,
		arr bigint[],
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

	cols := []string{"id", "b", "f8", "f4", "n", "s", "byt", "ts", "d", "j", "u", "arr"}
	rows := [][]spanner.GenericColumnValue{
		{
			gcvctor.Int64Value(1),
			gcvctor.BoolValue(true),
			gcvctor.Float64Value(1.5),
			gcvctor.Float32Value(2.5),
			pgNumericGCV("3.14"),
			gcvctor.StringValue("it's a \\ test\n"),
			gcvctor.BytesValue([]byte{0x00, 0xff, 0x27}),
			gcvctor.TimestampValue(time.Date(2024, 1, 15, 12, 0, 0, 123456789, time.UTC)),
			gcvctor.DateValue(civil.Date{Year: 2024, Month: 1, Day: 15}),
			gcvctor.MustPGJSONBValue(map[string]any{"k": 1.0}),
			gcvctor.UUIDValue(uuid.MustParse("858ebda5-f6df-4f5d-9151-aa98796053c4")),
			gcvctor.MustArrayValueOf(typector.Int64(),
				gcvctor.Int64Value(1), gcvctor.NullOf(typector.Int64()), gcvctor.Int64Value(3)),
		},
		{
			// Bare NULL in every nullable column: pins that the formatter's untyped NULL
			// is valid in INSERT context where the column fixes the type.
			gcvctor.Int64Value(2),
			gcvctor.NullOf(typector.Bool()),
			gcvctor.NullOf(typector.Float64()),
			gcvctor.NullOf(typector.Float32()),
			gcvctor.NullOf(typector.PGNumeric()),
			gcvctor.NullOf(typector.String()),
			gcvctor.NullOf(typector.Bytes()),
			gcvctor.NullOf(typector.CodeToSimpleType(sppb.TypeCode_TIMESTAMP)),
			gcvctor.NullOf(typector.Date()),
			gcvctor.NullOf(typector.PGJSONB()),
			gcvctor.NullOf(typector.CodeToSimpleType(sppb.TypeCode_UUID)),
			gcvctor.NullOf(typector.ElemTypeToArrayType(typector.Int64())),
		},
	}

	var buf bytes.Buffer
	w, err := writer.NewSQLInsertWriter(&buf, table,
		writer.WithSQLDialect(adminpb.DatabaseDialect_POSTGRESQL),
		writer.WithFormatter(spanpg.PostgreSQLLiteralFormatConfig()),
	)
	if err != nil {
		t.Fatalf("NewSQLInsertWriter: %v", err)
	}
	if err := w.PrepareColumnNames(cols); err != nil {
		t.Fatalf("PrepareColumnNames: %v", err)
	}
	for i, row := range rows {
		if err := w.WriteGCVs(row); err != nil {
			t.Fatalf("WriteGCVs(row %d): %v", i, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	t.Logf("generated SQL:\n%s", buf.String())

	var stmts []string
	for _, s := range strings.Split(buf.String(), ";\n") {
		if s = strings.TrimSpace(s); s != "" {
			stmts = append(stmts, s)
		}
	}
	if len(stmts) != len(rows) {
		t.Fatalf("statements: got %d, want %d", len(stmts), len(rows))
	}
	if _, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		for _, s := range stmts {
			if _, err := txn.Update(ctx, spanner.NewStatement(s)); err != nil {
				return fmt.Errorf("DML %q: %w", s, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("ReadWriteTransaction: %v", err)
	}

	// Server-side canonicalization overrides for read-back comparison
	// (both pinned by TestPostgreSQLLiteralRoundTrip).
	wantRows := [][]spanner.GenericColumnValue{rows[0], rows[1]}
	wantRows[0] = append([]spanner.GenericColumnValue(nil), rows[0]...)
	wantRows[0][7] = gcvctor.MustTimestampStringValue("2024-01-15T12:00:00.123457Z") // timestamptz literals parse at microsecond precision
	wantRows[0][9] = pgJSONBWireGCV(`{"k": 1}`)                                      // jsonb canonical text form adds a space after ':'

	iter := client.Single().Query(ctx, spanner.NewStatement(
		fmt.Sprintf("SELECT %s FROM %s ORDER BY id", strings.Join(cols, ", "), table)))
	defer iter.Stop()
	var gotRows [][]spanner.GenericColumnValue
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("read back: %v", err)
		}
		got := make([]spanner.GenericColumnValue, row.Size())
		for i := range got {
			if err := row.Column(i, &got[i]); err != nil {
				t.Fatalf("Column(%d): %v", i, err)
			}
		}
		gotRows = append(gotRows, got)
	}
	if diff := cmp.Diff(wantRows, gotRows, protocmp.Transform()); diff != "" {
		t.Errorf("read back rows (-want +got):\n%s", diff)
	}
}
