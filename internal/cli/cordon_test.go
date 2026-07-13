package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// runCmdStdin is runCmd with a fixed stdin, so prompts (confirm) can be driven.
func runCmdStdin(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetIn(strings.NewReader(stdin))
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

func TestCordonStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	want := cordonState{
		BrokerURL:     "broker:61616",
		SavedSettings: `{"maxSizeBytes":123}`,
		CordonedAt:    time.Now().Truncate(time.Second),
	}
	if err := writeCordonState(path, want); err != nil {
		t.Fatalf("writeCordonState: %v", err)
	}
	got, err := readCordonState(path)
	if err != nil {
		t.Fatalf("readCordonState: %v", err)
	}
	if got.BrokerURL != want.BrokerURL || got.SavedSettings != want.SavedSettings || !got.CordonedAt.Equal(want.CordonedAt) {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, want)
	}
}

func TestReadCordonStateMissingFile(t *testing.T) {
	_, err := readCordonState(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err == nil {
		t.Fatal("reading a missing state file should error")
	}
}

func TestConfirm(t *testing.T) {
	cases := map[string]bool{
		"y\n":   true,
		"yes\n": true,
		"YES\n": true,
		"n\n":   false,
		"no\n":  false,
		"":      false, // EOF -> false
	}
	for in, want := range cases {
		cmd := &cobra.Command{}
		cmd.SetIn(strings.NewReader(in))
		var out bytes.Buffer
		cmd.SetOut(&out)
		if got := confirm(cmd, "continue? "); got != want {
			t.Errorf("confirm(%q) = %v, want %v", in, got, want)
		}
		if !strings.Contains(out.String(), "continue?") {
			t.Errorf("confirm(%q) did not write the prompt", in)
		}
	}
}

// TestCordonStateFileExists: cordon refuses to run when the state file is
// already present (broker may already be cordoned), before touching the broker.
func TestCordonStateFileExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := runCmdStdin(t, "", "cordon", "--state-file", path)
	if err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("want already-exists error, got %v", err)
	}
}

// TestCordonAbort: without --yes and a "no" answer, cordon aborts before
// connecting and leaves no state file behind.
func TestCordonAbort(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	out, err := runCmdStdin(t, "n\n", "cordon", "--state-file", path)
	if err != nil {
		t.Fatalf("abort should not error: %v", err)
	}
	if !strings.Contains(out, "aborted") {
		t.Fatalf("output = %q, want abort notice", out)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("aborted cordon must not write a state file")
	}
}

// TestCordonConnectFailure: with --yes the prompt is skipped and cordon fails at
// connect against an unreachable broker, writing no state file.
func TestCordonConnectFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	args := append([]string{"cordon", "--yes", "--state-file", path}, unreachable...)
	_, err := runCmdStdin(t, "", args...)
	if err == nil {
		t.Fatal("cordon against unreachable broker: want error, got nil")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatal("failed cordon must not write a state file")
	}
}

// TestUncordonConnectFailure: uncordon connects before reading state, so an
// unreachable broker surfaces a connect error.
func TestUncordonConnectFailure(t *testing.T) {
	args := append([]string{"uncordon"}, unreachable...)
	_, err := runCmdStdin(t, "", args...)
	if err == nil {
		t.Fatal("uncordon against unreachable broker: want error, got nil")
	}
}
