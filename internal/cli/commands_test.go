package cli

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

// runCmd executes the root command with args, capturing combined output.
func runCmd(t *testing.T, args ...string) (string, error) {
	t.Helper()
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)
	err := root.Execute()
	return out.String(), err
}

// unreachable points at a port nothing listens on, so broker.Connect fails
// fast with connection-refused; it lets the tests drive each command's RunE up
// to (and through) the Connect error path without a live broker. The short
// --timeout bounds the dial in case the OS is slow to refuse.
var unreachable = []string{"--url", "127.0.0.1:1", "--timeout", "3s"}

func TestSubcommandsRegistered(t *testing.T) {
	root := NewRootCmd()
	want := []string{"status", "health", "browse", "produce", "export", "redeliver"}
	have := map[string]bool{}
	for _, c := range root.Commands() {
		have[c.Name()] = true
	}
	for _, w := range want {
		if !have[w] {
			t.Errorf("subcommand %q not registered", w)
		}
	}
}

func TestRequiredFlagsEnforced(t *testing.T) {
	cases := []struct {
		name string
		args []string
		flag string
	}{
		{"browse", []string{"browse"}, "queue"},
		{"produce", []string{"produce"}, "queue"},
		{"redeliver", []string{"redeliver"}, "in"},
		{"export", []string{"export"}, "out"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runCmd(t, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.flag) {
				t.Fatalf("want required-flag error mentioning %q, got %v", tc.flag, err)
			}
		})
	}
}

func TestParseProperties(t *testing.T) {
	got, err := parseProperties(nil)
	if err != nil || got != nil {
		t.Fatalf("nil input: got %v, %v; want nil, nil", got, err)
	}

	got, err = parseProperties([]string{"region=eu", "tier=gold"})
	if err != nil {
		t.Fatalf("valid: %v", err)
	}
	if got["region"] != "eu" || got["tier"] != "gold" {
		t.Fatalf("parsed = %v", got)
	}

	for _, bad := range []string{"novalue", "=orphan"} {
		if _, err := parseProperties([]string{bad}); err == nil {
			t.Fatalf("parseProperties(%q) should error", bad)
		}
	}
}

// TestProduceNoMessages: --count 0 generates nothing and short-circuits before
// any broker connection, so it must succeed and say so.
func TestProduceNoMessages(t *testing.T) {
	out, err := runCmd(t, "produce", "--queue", "q", "--count", "0")
	if err != nil {
		t.Fatalf("produce --count 0: %v", err)
	}
	if !strings.Contains(out, "no messages to send") {
		t.Fatalf("output = %q, want no-messages notice", out)
	}
}

func TestProduceInvalidProperty(t *testing.T) {
	_, err := runCmd(t, "produce", "--queue", "q", "--property", "novalue")
	if err == nil || !strings.Contains(err.Error(), "--property") {
		t.Fatalf("want --property error, got %v", err)
	}
}

func TestProduceMissingFile(t *testing.T) {
	_, err := runCmd(t, "produce", "--queue", "q", "--file", "/no/such/file.json")
	if err == nil || !strings.Contains(err.Error(), "read message file") {
		t.Fatalf("want read-file error, got %v", err)
	}
}

// TestConnectFailurePaths drives each connect-requiring command against an
// unreachable broker; every one must surface a non-nil error rather than hang
// or panic.
func TestConnectFailurePaths(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "store.artx")
	cases := map[string][]string{
		"status":    {"status"},
		"health":    {"health"},
		"browse":    {"browse", "--queue", "q"},
		"produce":   {"produce", "--queue", "q", "--count", "1"},
		"export":    {"export", "--out", tmp},
		"redeliver": {"redeliver", "--in", tmp},
	}
	for name, base := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := runCmd(t, append(base, unreachable...)...)
			if err == nil {
				t.Fatalf("%s against unreachable broker: want error, got nil", name)
			}
		})
	}
}

func TestResolvePassword(t *testing.T) {
	t.Setenv("ARTEMIS_PASSWORD", "")
	if got := resolvePassword("flagpw"); got != "flagpw" {
		t.Fatalf("empty env: got %q, want flagpw", got)
	}
	t.Setenv("ARTEMIS_PASSWORD", "envpw")
	if got := resolvePassword("flagpw"); got != "envpw" {
		t.Fatalf("env set: got %q, want envpw", got)
	}
}

func TestConnectCtxHonorsTimeout(t *testing.T) {
	cmd := &cobra.Command{}
	cmd.Flags().Duration("timeout", 0, "")

	// timeout > 0 -> deadline is set.
	_ = cmd.Flags().Set("timeout", "5s")
	ctx, cancel := connectCtx(cmd, context.Background())
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("timeout=5s: expected a deadline")
	}

	// timeout <= 0 -> cancelable, but no deadline.
	_ = cmd.Flags().Set("timeout", "0s")
	ctx2, cancel2 := connectCtx(cmd, context.Background())
	defer cancel2()
	if _, ok := ctx2.Deadline(); ok {
		t.Fatal("timeout=0: expected no deadline")
	}
}

func TestConnPropsReadsFlags(t *testing.T) {
	root := NewRootCmd()
	root.SetArgs([]string{"status", "--url", "h:1", "-u", "bob", "-p", "secret"})
	// Parse flags without executing RunE by resolving the target command.
	target, _, err := root.Find([]string{"status", "--url", "h:1", "-u", "bob", "-p", "secret"})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.ParseFlags([]string{"--url", "h:1", "-u", "bob", "-p", "secret"}); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ARTEMIS_PASSWORD", "")
	p := connProps(target)
	if p.URL != "h:1" || p.Username != "bob" || p.Password != "secret" {
		t.Fatalf("connProps = %+v", p)
	}
}

func TestSignalCtxCancelable(t *testing.T) {
	ctx, stop := signalCtx()
	defer stop()
	select {
	case <-ctx.Done():
		t.Fatal("signalCtx already canceled")
	case <-time.After(10 * time.Millisecond):
	}
}
