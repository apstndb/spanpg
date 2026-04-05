package pgtypeannotation_test

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"

	"github.com/apstndb/spanpg"
)

func TestParentSpanpgModuleLinked(t *testing.T) {
	t.Parallel()
	_, ok := spanpg.PostgreSQLCatalogTypeName((*sppb.Type)(nil))
	if ok {
		t.Fatal("expected ok false for nil type")
	}
}

func TestGoModReplaceParentSpanpg(t *testing.T) {
	t.Parallel()
	// Ensures this nested module resolves github.com/apstndb/spanpg via go.mod "replace ../.."
	// (same intent as checking ReadBuildInfo replace; go list -m is authoritative for module graph).
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	dir := filepath.Dir(file)
	cmd := exec.Command("go", "list", "-m", "-json", "github.com/apstndb/spanpg")
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			t.Fatalf("go list: %v\n%s", err, ee.Stderr)
		}
		t.Fatalf("go list: %v", err)
	}
	var mod struct {
		Path    string `json:"Path"`
		Replace *struct {
			Path string `json:"Path"`
		} `json:"Replace"`
	}
	if err := json.Unmarshal(out, &mod); err != nil {
		t.Fatalf("json: %v", err)
	}
	if mod.Path != "github.com/apstndb/spanpg" {
		t.Fatalf("unexpected module path: %q", mod.Path)
	}
	if mod.Replace == nil || mod.Replace.Path == "" {
		t.Fatalf("expected go.mod replace for github.com/apstndb/spanpg, got %+v", mod.Replace)
	}
}
