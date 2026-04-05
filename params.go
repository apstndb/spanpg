package spanpg

import "fmt"

// StatementParamKey returns the map key used in [cloud.google.com/go/spanner.Statement.Params]
// for the PostgreSQL-style placeholder $n where n is 1-based.
// For example, placeholder $1 uses Params key "p1", $2 uses "p2".
//
// This matches the cloud.google.com/go/spanner client convention used with PostgreSQL dialect
// SQL (placeholders $1, $2, …). See integration coverage in
// https://github.com/apstndb/spanvalue/pull/45.
func StatementParamKey(n int) (key string, ok bool) {
	if n < 1 {
		return "", false
	}
	return fmt.Sprintf("p%d", n), true
}

// PostgreSQLPlaceholder returns the SQL text for the n-th bind placeholder in PostgreSQL
// dialect ($n, 1-based), e.g. 1 → "$1".
func PostgreSQLPlaceholder(n int) (sql string, ok bool) {
	if n < 1 {
		return "", false
	}
	return fmt.Sprintf("$%d", n), true
}
