package spanpg

import (
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/apstndb/spantype"
)

// FormatPostgreSQLType renders a [sppb.Type] using PostgreSQL-dialect
// spellings.
//
// Deprecated: the spelling table moved to
// [github.com/apstndb/spantype.FormatTypePostgreSQL] (spantype v0.3.12+) so
// type rendering is available without depending on spanpg; this alias
// delegates there and will be removed in the next breaking release. The
// spellings remain pinned against PostgreSQL-dialect Spanner databases by
// this repository's integration/pgtypeannotation probes.
func FormatPostgreSQLType(typ *sppb.Type) string {
	return spantype.FormatTypePostgreSQL(typ)
}
