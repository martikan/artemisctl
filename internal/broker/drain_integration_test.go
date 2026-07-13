package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/martikan/artemisctl/internal/store"
)

// sliceSink collects records in memory for assertions.
type sliceSink struct{ recs []store.Record }

func (s *sliceSink) Append(r store.Record) error { s.recs = append(s.recs, r); return nil }
func (s *sliceSink) Sync() error                 { return nil }

func TestDrainQueueRemovesMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	if err := sendTestMessages(ctx, c, "orders", []string{"m1", "m2", "m3"}); err != nil {
		t.Fatalf("send test messages: %v", err)
	}

	sink := &sliceSink{}
	n, err := c.DrainQueue(ctx, "orders", sink, 3*time.Second, 10)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 3 || len(sink.recs) != 3 {
		t.Fatalf("want 3 drained, got %d/%d", n, len(sink.recs))
	}
	// Second drain finds nothing.
	n2, err := c.DrainQueue(ctx, "orders", &sliceSink{}, 2*time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Fatalf("queue not emptied, second drain got %d", n2)
	}
}

// TestDrainAllEnumeratesAndDrainsEveryQueue seeds two queues and confirms
// DrainAll walks every queue, reports each via the onQueue callback, and returns
// the combined total.
func TestDrainAllEnumeratesAndDrainsEveryQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	if err := sendTestMessages(ctx, c, "orders", []string{"a", "b"}); err != nil {
		t.Fatalf("seed orders: %v", err)
	}
	if err := sendTestMessages(ctx, c, "payments", []string{"c"}); err != nil {
		t.Fatalf("seed payments: %v", err)
	}

	sink := &sliceSink{}
	perQueue := map[string]int{}
	total, err := c.DrainAll(ctx, sink, 3*time.Second, 10, func(name string, n int) {
		perQueue[name] = n
	})
	if err != nil {
		t.Fatalf("drain all: %v", err)
	}
	if total != 3 || len(sink.recs) != 3 {
		t.Fatalf("total drained = %d (sink %d), want 3", total, len(sink.recs))
	}
	if perQueue["orders"] != 2 || perQueue["payments"] != 1 {
		t.Fatalf("per-queue callback = %v, want orders:2 payments:1", perQueue)
	}
}

// TestDrainQueueRejectsExpiredCallerContext is a regression test for the
// idle-timeout vs caller-cancellation confusion: DrainQueue must not treat
// an already-expired (or canceled) OUTER ctx as "queue idle, drained". Before
// the fix, a context.DeadlineExceeded surfacing from Receive was always read
// as "queue empty" and swallowed into a (n, nil) success, even when it was
// really the caller's own deadline that had expired — silently reporting a
// drain as complete while messages could remain on the broker.
func TestDrainQueueRejectsExpiredCallerContext(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	const queue = "deadline-check"
	if err := sendTestMessages(ctx, c, queue, []string{"d1", "d2", "d3"}); err != nil {
		t.Fatalf("send test messages: %v", err)
	}

	// A parent context whose deadline has ALREADY passed, used as the OUTER
	// ctx for DrainQueue. A large idle window ensures that, if this were
	// mistaken for a normal idle timeout, the bug would be masked.
	expiredCtx, expiredCancel := context.WithDeadline(ctx, time.Now().Add(-1*time.Second))
	defer expiredCancel()

	sink := &sliceSink{}
	n, err := c.DrainQueue(expiredCtx, queue, sink, 3*time.Second, 10)
	if err == nil {
		t.Fatalf("want non-nil error for an already-expired caller context, got (n=%d, nil) claiming success", n)
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("want a context deadline/cancel error, got: %v", err)
	}

	// Prove nothing was falsely marked as drained: the messages must still
	// be sitting on the broker, retrievable via a normal, live context.
	n2, err := c.DrainQueue(ctx, queue, &sliceSink{}, 3*time.Second, 10)
	if err != nil {
		t.Fatalf("follow-up drain: %v", err)
	}
	if n2 != 3 {
		t.Fatalf("want all 3 messages still present after the false-timeout was rejected, got %d", n2)
	}
}
