package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
)

// scheduleAt marks every message for delivery at t. Artemis honours the AMQP
// x-opt-delivery-time annotation for scheduled delivery.
func scheduleAt(msgs []*amqp.Message, at time.Time) {
	for _, m := range msgs {
		m.Annotations = amqp.Annotations{"x-opt-delivery-time": at.UnixMilli()}
	}
}

// TestDrainQueueScheduledRemainderIsReported pins the reported bug: a queue
// whose depth is mostly messages the broker will not deliver yet must NOT look
// like a completed drain. Before the fix, DrainQueue took the deliverable
// messages, saw the broker go quiet, and returned nil -- so a DLQ reporting
// 210K exported ~50K and still exited 0.
func TestDrainQueueScheduledRemainderIsReported(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	queue := benchQueue("sched-remainder")
	defer func() { _, _ = c.PurgeQueue(context.Background(), queue) }()

	if _, err := c.Produce(ctx, queue, GenerateMessages(400, 256, nil), 0, 4, nil); err != nil {
		t.Fatalf("produce deliverable: %v", err)
	}
	later := GenerateMessages(600, 256, nil)
	scheduleAt(later, time.Now().Add(time.Hour))
	if _, err := c.Produce(ctx, queue, later, 0, 4, nil); err != nil {
		t.Fatalf("produce scheduled: %v", err)
	}

	sink := &sliceSink{}
	n, err := c.DrainQueue(ctx, queue, sink, 3*time.Second, 100)

	var pde *PartialDrainError
	if !errors.As(err, &pde) {
		t.Fatalf("drain returned n=%d err=%v, want *PartialDrainError", n, err)
	}
	if n != 400 || len(sink.recs) != 400 {
		t.Fatalf("drained %d (sink %d), want the 400 deliverable messages", n, len(sink.recs))
	}
	if pde.Stat.ScheduledCount != 600 {
		t.Fatalf("reported ScheduledCount = %d, want 600", pde.Stat.ScheduledCount)
	}
	if pde.Stat.MessageCount != 600 {
		t.Fatalf("reported MessageCount = %d, want 600 left on broker", pde.Stat.MessageCount)
	}
	t.Logf("drain correctly refused to claim success: %v", err)
}

// TestDrainQueuePausedIsReported covers the other way a queue can be quiet but
// non-empty: a paused queue dispatches nothing at all, so every message stays.
func TestDrainQueuePausedIsReported(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	queue := benchQueue("paused-remainder")
	if _, err := c.Produce(ctx, queue, GenerateMessages(25, 256, nil), 0, 1, nil); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if _, err := c.callManagement(ctx, "queue."+queue, "pause", "[]"); err != nil {
		t.Fatalf("pause queue: %v", err)
	}
	defer func() {
		_, _ = c.callManagement(context.Background(), "queue."+queue, "resume", "[]")
		_, _ = c.PurgeQueue(context.Background(), queue)
	}()

	n, err := c.DrainQueue(ctx, queue, &sliceSink{}, 2*time.Second, 100)

	var pde *PartialDrainError
	if !errors.As(err, &pde) {
		t.Fatalf("drain returned n=%d err=%v, want *PartialDrainError", n, err)
	}
	if !pde.Stat.Paused {
		t.Fatalf("reported Stat.Paused = false, want true; stat=%+v", pde.Stat)
	}
	if pde.Stat.MessageCount != 25 {
		t.Fatalf("reported MessageCount = %d, want 25", pde.Stat.MessageCount)
	}
}

// TestDrainQueueFullyDrainsWhenTrulyEmpty guards the other direction: the new
// broker-verified completion check must not turn an ordinary complete drain
// into an error.
func TestDrainQueueFullyDrainsWhenTrulyEmpty(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	queue := benchQueue("fully-drains")
	if _, err := c.Produce(ctx, queue, GenerateMessages(750, 256, nil), 0, 4, nil); err != nil {
		t.Fatalf("produce: %v", err)
	}

	sink := &sliceSink{}
	n, err := c.DrainQueue(ctx, queue, sink, 2*time.Second, 100)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if n != 750 || len(sink.recs) != 750 {
		t.Fatalf("drained %d (sink %d), want 750", n, len(sink.recs))
	}
	stat, err := c.QueueStatByName(ctx, queue)
	if err != nil && !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("stat: %v", err)
	}
	if err == nil && stat.MessageCount != 0 {
		t.Fatalf("broker still holds %d messages after a nil-error drain", stat.MessageCount)
	}
}

// TestQueueStatByNameReportsCounters pins the decoding of the counters the
// drain's completion check relies on, against a real broker reply.
func TestQueueStatByNameReportsCounters(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	queue := benchQueue("stat-counters")
	defer func() { _, _ = c.PurgeQueue(context.Background(), queue) }()

	if _, err := c.Produce(ctx, queue, GenerateMessages(10, 128, nil), 0, 1, nil); err != nil {
		t.Fatalf("produce now: %v", err)
	}
	later := GenerateMessages(15, 128, nil)
	scheduleAt(later, time.Now().Add(time.Hour))
	if _, err := c.Produce(ctx, queue, later, 0, 1, nil); err != nil {
		t.Fatalf("produce scheduled: %v", err)
	}

	stat, err := c.QueueStatByName(ctx, queue)
	if err != nil {
		t.Fatalf("QueueStatByName: %v", err)
	}
	if stat.Name != queue || stat.MessageCount != 25 || stat.ScheduledCount != 15 {
		t.Fatalf("stat = %+v, want name=%s messageCount=25 scheduledCount=15", stat, queue)
	}
	if stat.Paused {
		t.Fatalf("stat.Paused = true on a live queue: %+v", stat)
	}
	if got := stat.DeliverableNow(); got != 10 {
		t.Fatalf("DeliverableNow() = %d, want 10", got)
	}
}

// TestQueueStatByNameMissingQueue pins the not-found path, which DrainQueue
// treats as "the queue was auto-deleted once emptied", not as a failure.
func TestQueueStatByNameMissingQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	if _, err := c.QueueStatByName(ctx, "no-such-queue-9f3a"); !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("err = %v, want ErrQueueNotFound", err)
	}
}

// TestPurgeQueueRemovesScheduled pins that purge clears what a drain cannot,
// which is what resetBroker relies on to give each test a clean slate.
func TestPurgeQueueRemovesScheduled(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	queue := benchQueue("purge-scheduled")
	msgs := GenerateMessages(30, 128, nil)
	scheduleAt(msgs, time.Now().Add(time.Hour))
	if _, err := c.Produce(ctx, queue, msgs, 0, 1, nil); err != nil {
		t.Fatalf("produce scheduled: %v", err)
	}

	removed, err := c.PurgeQueue(ctx, queue)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if removed != 30 {
		t.Fatalf("purge removed %d, want 30", removed)
	}
	stat, err := c.QueueStatByName(ctx, queue)
	if err != nil && !errors.Is(err, ErrQueueNotFound) {
		t.Fatalf("stat: %v", err)
	}
	if err == nil && stat.MessageCount != 0 {
		t.Fatalf("queue still holds %d after purge", stat.MessageCount)
	}
}
