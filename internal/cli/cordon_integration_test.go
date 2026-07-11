// internal/cli/cordon_integration_test.go
package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestCordonUncordonAgainstBroker drives the cordon/uncordon commands end-to-end
// against the shared broker: the happy cordon→restore cycle plus the two
// no-state-file uncordon branches (error without --force-remove, wildcard removal
// with it). This covers the RunE success bodies the unit tests stop short of.
func TestCordonUncordonAgainstBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemisForCLI(t)

	run := func(t *testing.T, args ...string) (string, error) {
		t.Helper()
		root := NewRootCmd()
		var out bytes.Buffer
		root.SetOut(&out)
		root.SetErr(&out)
		conn := []string{"--url", props.URL, "-u", props.Username, "-p", props.Password}
		root.SetArgs(append(args, conn...))
		err := root.Execute()
		return out.String(), err
	}

	state := filepath.Join(t.TempDir(), "cordon.json")

	t.Run("cordon", func(t *testing.T) {
		out, err := run(t, "cordon", "--yes", "--state-file", state)
		if err != nil {
			t.Fatalf("cordon: %v", err)
		}
		if !strings.Contains(out, "broker cordoned") {
			t.Fatalf("cordon output unexpected: %q", out)
		}
		s, err := readCordonState(state)
		if err != nil {
			t.Fatalf("state file not written: %v", err)
		}
		if s.SavedSettings == "" {
			t.Fatal("state file recorded empty saved settings")
		}
	})

	t.Run("uncordon restores and removes state", func(t *testing.T) {
		out, err := run(t, "uncordon", "--state-file", state)
		if err != nil {
			t.Fatalf("uncordon: %v", err)
		}
		if !strings.Contains(out, "settings restored") {
			t.Fatalf("uncordon output unexpected: %q", out)
		}
		if _, statErr := os.Stat(state); !os.IsNotExist(statErr) {
			t.Fatal("uncordon should remove the state file after restoring")
		}
	})

	t.Run("uncordon without state file errors", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "none.json")
		_, err := run(t, "uncordon", "--state-file", missing)
		if err == nil || !strings.Contains(err.Error(), "no state file") {
			t.Fatalf("want no-state-file error, got %v", err)
		}
	})

	t.Run("uncordon --force-remove without state file", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "none.json")
		out, err := run(t, "uncordon", "--force-remove", "--state-file", missing)
		if err != nil {
			t.Fatalf("uncordon --force-remove: %v", err)
		}
		if !strings.Contains(out, "wildcard settings entry removed") {
			t.Fatalf("force-remove output unexpected: %q", out)
		}
	})
}
