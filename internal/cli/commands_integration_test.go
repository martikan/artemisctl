// internal/cli/commands_integration_test.go
package cli

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"
)

// TestCommandsAgainstBroker exercises every connect-requiring command's success
// path against one shared broker container: status, health, browse, then an
// export→redeliver round-trip. Bundling them into a single container boot keeps
// the integration suite fast while covering the RunE bodies the unit tests stop
// short of (they only reach the Connect error).
func TestCommandsAgainstBroker(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemisForCLI(t)
	seedQueue(t, props, "orders", []string{"alpha", "bravo", "charlie"})

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

	t.Run("status", func(t *testing.T) {
		out, err := run(t, "status")
		if err != nil {
			t.Fatalf("status: %v", err)
		}
		if !strings.Contains(out, "orders") {
			t.Fatalf("status output missing queue: %q", out)
		}
	})

	t.Run("health", func(t *testing.T) {
		out, err := run(t, "health")
		if err != nil {
			t.Fatalf("health: %v", err)
		}
		if !strings.Contains(out, "verdict:") {
			t.Fatalf("health output missing verdict: %q", out)
		}
	})

	t.Run("browse", func(t *testing.T) {
		out, err := run(t, "browse", "--queue", "orders", "--limit", "10")
		if err != nil {
			t.Fatalf("browse: %v", err)
		}
		for _, want := range []string{"alpha", "bravo", "charlie"} {
			if !strings.Contains(out, want) {
				t.Fatalf("browse output missing %q: %q", want, out)
			}
		}

		// The single-message path: pull an ID from the list output's first data
		// row and fetch its full body via --message.
		id := firstMessageID(t, out)
		body, err := run(t, "browse", "--queue", "orders", "--message", id)
		if err != nil {
			t.Fatalf("browse --message: %v", err)
		}
		if !strings.Contains(body, "Body:") {
			t.Fatalf("browse --message output missing body: %q", body)
		}
	})

	// 100+ messages so the export→redeliver round-trip drains enough records to
	// fire redeliver's every-100 progress callback. Tagged with a property so the
	// browse --message path below exercises the property-printing block.
	t.Run("produce", func(t *testing.T) {
		out, err := run(t, "produce", "--queue", "generated", "--count", "100", "--size", "64",
			"--property", "batch=x")
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		if !strings.Contains(out, "produced 100 messages") {
			t.Fatalf("produce output unexpected: %q", out)
		}
	})

	t.Run("browse message with properties", func(t *testing.T) {
		list, err := run(t, "browse", "--queue", "generated", "--limit", "1")
		if err != nil {
			t.Fatalf("browse generated: %v", err)
		}
		id := firstMessageID(t, list)
		body, err := run(t, "browse", "--queue", "generated", "--message", id)
		if err != nil {
			t.Fatalf("browse --message: %v", err)
		}
		if !strings.Contains(body, "Properties:") || !strings.Contains(body, "batch = x") {
			t.Fatalf("browse --message output missing properties: %q", body)
		}
	})

	// Round-trip: export drains orders to a store file, redeliver replays it.
	store := filepath.Join(t.TempDir(), "dump.artx")
	t.Run("export", func(t *testing.T) {
		out, err := run(t, "export", "--out", store, "--drain-timeout", "3s")
		if err != nil {
			t.Fatalf("export: %v", err)
		}
		if !strings.Contains(out, "exported") {
			t.Fatalf("export output unexpected: %q", out)
		}
	})

	t.Run("redeliver", func(t *testing.T) {
		out, err := run(t, "redeliver", "--in", store)
		if err != nil {
			t.Fatalf("redeliver: %v", err)
		}
		if !strings.Contains(out, "redelivered") {
			t.Fatalf("redeliver output unexpected: %q", out)
		}
	})
}

// firstMessageID extracts the ID from the first data row of `browse` list
// output (tab/space-aligned: ID SIZE TIMESTAMP PREVIEW, with a header row).
func firstMessageID(t *testing.T, listOutput string) string {
	t.Helper()
	for _, line := range strings.Split(listOutput, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] == "ID" {
			continue // blank or header row
		}
		return fields[0]
	}
	t.Fatalf("no message ID found in browse output: %q", listOutput)
	return ""
}
