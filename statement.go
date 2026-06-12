package spanpg

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/apstndb/spanvalue"
)

// PositionalParams maps ordered values to the p1..pn keys the PostgreSQL
// interface pairs with $1..$n placeholders (see [StatementParamKey]). values
// may be plain Go values (the client encodes them) or
// [cloud.google.com/go/spanner.GenericColumnValue] (binding an exact wire
// Type, e.g. from [github.com/apstndb/spancodec.ValueOf] with
// [EncodeOptions]).
func PositionalParams(values []any) (map[string]any, error) {
	params := make(map[string]any, len(values))
	for i, v := range values {
		key, ok := StatementParamKey(i + 1)
		if !ok {
			return nil, fmt.Errorf("spanpg: no statement parameter key for position %d", i+1)
		}
		params[key] = v
	}
	return params, nil
}

// InsertStatement builds INSERT INTO <table> (<cols>) VALUES ($1..$n) with
// [PositionalParams], quoting the table and column identifiers with
// [github.com/apstndb/spanvalue.QuoteIdentifier] PostgreSQL rules. It returns
// an error when len(columns) != len(values) or when columns is empty.
//
// values follow the [PositionalParams] contract: plain Go values or
// GenericColumnValue. Generic INSERT fragment helpers belong to spanvalue
// (apstndb/spanvalue#79); this binds the PostgreSQL-specific $n / p-n pairing.
func InsertStatement(table string, columns []string, values []any) (spanner.Statement, error) {
	if len(columns) != len(values) {
		return spanner.Statement{}, fmt.Errorf("spanpg: %d columns but %d values", len(columns), len(values))
	}
	if len(columns) == 0 {
		return spanner.Statement{}, fmt.Errorf("spanpg: empty column list")
	}

	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = spanvalue.QuoteIdentifier(databasepb.DatabaseDialect_POSTGRESQL, c)
	}
	placeholders := make([]string, len(values))
	for i := range values {
		ph, ok := PostgreSQLPlaceholder(i + 1)
		if !ok {
			return spanner.Statement{}, fmt.Errorf("spanpg: no placeholder for position %d", i+1)
		}
		placeholders[i] = ph
	}
	params, err := PositionalParams(values)
	if err != nil {
		return spanner.Statement{}, err
	}

	return spanner.Statement{
		SQL: fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			spanvalue.QuoteIdentifier(databasepb.DatabaseDialect_POSTGRESQL, table),
			strings.Join(quotedCols, ", "),
			strings.Join(placeholders, ", ")),
		Params: params,
	}, nil
}
