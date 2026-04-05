package pgtypeannotation_test

import (
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanpg"
)

func TestParentSpanpgModuleLinked(t *testing.T) {
	t.Parallel()
	// Ensures go.mod replace (../..) resolves the repo-root spanpg module for this nested module.
	_, ok := spanpg.PostgreSQLCatalogTypeName((*sppb.Type)(nil))
	if ok {
		t.Fatal("expected ok false for nil type")
	}
}
