// internal/broker/redeliver_integration_test.go
package broker

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/martikan/artemisctl/internal/store"
)

func TestRedeliverRoundTripAndDedup(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	if err := sendTestMessages(ctx, c, "orders", []string{"m1", "m2", "m3"}); err != nil {
		t.Fatalf("send test messages: %v", err)
	}

	// Drain to a store file.
	path := filepath.Join(t.TempDir(), "dump.artx")
	w, err := store.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DrainQueue(ctx, "orders", w, 3*time.Second, 10); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	// Redeliver back.
	n, _, err := c.Redeliver(ctx, path, RedeliverOpts{}, nil)
	if err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	if n != 3 {
		t.Fatalf("want 3 redelivered, got %d", n)
	}

	// Redeliver AGAIN from offset 0 (simulate lost checkpoint) -> dedup should drop repeats.
	if err := store.SaveCheckpoint(path, 0); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Redeliver(ctx, path, RedeliverOpts{}, nil); err != nil {
		t.Fatal(err)
	}

	// Queue should hold exactly 3 (dedup prevented 6).
	stats, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int64 = -1
	for _, s := range stats {
		if s.Name == "orders" {
			count = s.MessageCount
		}
	}
	if count != 3 {
		t.Fatalf("dedup failed: orders has %d messages, want 3", count)
	}
}

// TestRedeliverGracefulCancel models the I3 SIGINT path with a context cancel
// (real signals are flaky): cancelling mid-replay must return promptly with a
// context error, no panic, and a durable checkpoint at the last successfully
// sent record so the same command resumes exactly.
func TestRedeliverGracefulCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	if err := sendTestMessages(ctx, c, "cancelq", []string{"c1", "c2", "c3", "c4", "c5"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	path := filepath.Join(t.TempDir(), "dump.artx")
	w, err := store.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DrainQueue(ctx, "cancelq", w, 3*time.Second, 10); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	// Cancel the replay after the first record is sent + checkpointed. The
	// next Send observes the canceled context and Redeliver returns gracefully.
	replayCtx, replayCancel := context.WithCancel(ctx)
	defer replayCancel()
	n, _, err := c.Redeliver(replayCtx, path, RedeliverOpts{}, func(sent int) {
		if sent == 1 {
			replayCancel()
		}
	})
	if err == nil {
		t.Fatalf("want a context error after cancel, got nil (n=%d)", n)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got: %v", err)
	}
	if n < 1 || n >= 5 {
		t.Fatalf("want a partial redeliver (1..4), got %d", n)
	}
	// Checkpoint must be durable and point past the delivered records.
	off, err := store.LoadCheckpoint(path)
	if err != nil {
		t.Fatalf("load checkpoint: %v", err)
	}
	if off <= 0 {
		t.Fatalf("checkpoint not saved on graceful cancel: offset=%d", off)
	}
}
