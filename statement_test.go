package spanpg_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/apstndb/spanpg"
)

func TestPositionalParams(t *testing.T) {
	t.Parallel()

	got, err := spanpg.PositionalParams([]any{int64(1), "two", nil})
	if err != nil {
		t.Fatalf("PositionalParams: %v", err)
	}
	want := map[string]any{"p1": int64(1), "p2": "two", "p3": nil}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("PositionalParams (-want +got):\n%s", diff)
	}

	empty, err := spanpg.PositionalParams(nil)
	if err != nil {
		t.Fatalf("PositionalParams(nil): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("PositionalParams(nil) = %v, want empty map", empty)
	}
}

func TestInsertStatement(t *testing.T) {
	t.Parallel()

	t.Run("empty table name", func(t *testing.T) {
		t.Parallel()
		if _, err := spanpg.InsertStatement("  ", []string{"id"}, []any{int64(1)}); err == nil {
			t.Error("want error for empty table name, got nil")
		}
	})

	t.Run("empty column name", func(t *testing.T) {
		t.Parallel()
		if _, err := spanpg.InsertStatement("t", []string{"id", " "}, []any{int64(1), "x"}); err == nil {
			t.Error("want error for empty column name, got nil")
		}
	})

	t.Run("quotes identifiers and pairs placeholders with params", func(t *testing.T) {
		t.Parallel()
		stmt, err := spanpg.InsertStatement(`my"table`, []string{"id", `na"me`}, []any{int64(1), "x"})
		if err != nil {
			t.Fatalf("InsertStatement: %v", err)
		}
		wantSQL := `INSERT INTO "my""table" ("id", "na""me") VALUES ($1, $2)`
		if stmt.SQL != wantSQL {
			t.Errorf("SQL = %q, want %q", stmt.SQL, wantSQL)
		}
		wantParams := map[string]any{"p1": int64(1), "p2": "x"}
		if diff := cmp.Diff(wantParams, stmt.Params); diff != "" {
			t.Errorf("Params (-want +got):\n%s", diff)
		}
	})

	t.Run("length mismatch", func(t *testing.T) {
		t.Parallel()
		if _, err := spanpg.InsertStatement("t", []string{"a", "b"}, []any{1}); err == nil {
			t.Error("expected error for 2 columns / 1 value")
		}
	})

	t.Run("empty columns", func(t *testing.T) {
		t.Parallel()
		if _, err := spanpg.InsertStatement("t", nil, nil); err == nil {
			t.Error("expected error for empty column list")
		}
	})
}
