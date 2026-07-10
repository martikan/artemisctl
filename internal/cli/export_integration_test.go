// internal/cli/export_integration_test.go
package cli

import (
	"os"
	"path/filepath"
	"testing"
)

func TestExportCommandCreatesStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	// Reuse broker package's container helper via a fresh broker for seeding.
	props := startArtemisForCLI(t)
	seedQueue(t, props, "orders", []string{"a", "b"})

	out := filepath.Join(t.TempDir(), "dump.artx")
	root := NewRootCmd()
	root.SetArgs([]string{
		"export", "--out", out,
		"--url", props.URL, "-u", props.Username, "-p", props.Password,
		"--drain-timeout", "3s",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("export: %v", err)
	}
	fi, err := os.Stat(out)
	if err != nil || fi.Size() <= 5 {
		t.Fatalf("store not written: %v size=%v", err, fi)
	}
}
