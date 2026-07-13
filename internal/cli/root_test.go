package cli

import (
	"bytes"
	"testing"
	"time"
)

func TestRootCmdHasConnectionFlags(t *testing.T) {
	cmd := NewRootCmd()
	for _, name := range []string{"url", "username", "password"} {
		if cmd.PersistentFlags().Lookup(name) == nil {
			t.Fatalf("missing persistent flag %q", name)
		}
	}
}

// TestRootCmdHasTimeoutFlag is the I2 regression: a --timeout persistent flag
// must exist and default to 30s so an unresponsive broker fails the Connect
// fast instead of hanging on context.Background forever.
func TestRootCmdHasTimeoutFlag(t *testing.T) {
	cmd := NewRootCmd()
	f := cmd.PersistentFlags().Lookup("timeout")
	if f == nil {
		t.Fatal("missing persistent flag \"timeout\"")
	}
	if f.DefValue != "30s" {
		t.Fatalf("timeout default = %q, want 30s", f.DefValue)
	}
	d, err := cmd.PersistentFlags().GetDuration("timeout")
	if err != nil {
		t.Fatalf("GetDuration: %v", err)
	}
	if d != 30*time.Second {
		t.Fatalf("timeout parsed = %v, want 30s", d)
	}
}

func TestRootCmdHelpRuns(t *testing.T) {
	cmd := NewRootCmd()
	cmd.SetArgs([]string{"--help"})
	cmd.SetOut(&bytes.Buffer{})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("help failed: %v", err)
	}
}
