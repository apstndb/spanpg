package spanpg

import (
	"cloud.google.com/go/spanner"

	"github.com/apstndb/spanvalue"
)

// FormatColumnSimple formats a [spanner.GenericColumnValue] using
// [github.com/apstndb/spanvalue.SimpleFormatConfig] (human-readable scalar and container output).
func FormatColumnSimple(gcv spanner.GenericColumnValue) (string, error) {
	return spanvalue.SimpleFormatConfig().FormatToplevelColumn(gcv)
}
