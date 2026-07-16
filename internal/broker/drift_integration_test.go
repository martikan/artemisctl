package broker

import (
	"context"
	"testing"
	"time"
)

// TestCheckDriftOnHealthyQueue pins the direction that matters most: a drift
// check must NOT cry drift on a queue whose counter is fine. A detector that
// reports drift on healthy queues is worse than none, because it trains the
// operator to ignore the one queue that has actually drifted.
func TestCheckDriftOnHealthyQueue(t *testing.T) {
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

	queue := benchQueue("drift-healthy")
	defer func() { _, _ = c.PurgeQueue(context.Background(), queue) }()

	if _, err := c.Produce(ctx, queue, GenerateMessages(200, 128, nil), 0, 4, nil); err != nil {
		t.Fatalf("produce: %v", err)
	}

	r, err := c.CheckDrift(ctx, queue)
	if err != nil {
		t.Fatalf("CheckDrift: %v", err)
	}
	if r.Verdict() != DriftNone {
		t.Fatalf("verdict = %v on a healthy queue, want DriftNone; report=%+v", r.Verdict(), r)
	}
	if r.Counted != 200 || r.CounterBefore != 200 {
		t.Fatalf("report = %+v, want counter and scan both 200", r)
	}
	if r.Missing() != 0 {
		t.Fatalf("Missing() = %d on a healthy queue, want 0", r.Missing())
	}
}

// TestCheckDriftCountsMessagesNoConsumerCanTake guards against a false positive
// that would otherwise be easy to ship: scheduled messages are real messages on
// the queue that no consumer can receive yet. The scan counts them and so does
// the counter, so this is NOT drift -- only DeliverableNow() should exclude them.
func TestCheckDriftCountsMessagesNoConsumerCanTake(t *testing.T) {
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

	queue := benchQueue("drift-scheduled")
	defer func() { _, _ = c.PurgeQueue(context.Background(), queue) }()

	msgs := GenerateMessages(40, 128, nil)
	scheduleAt(msgs, time.Now().Add(time.Hour))
	if _, err := c.Produce(ctx, queue, msgs, 0, 1, nil); err != nil {
		t.Fatalf("produce scheduled: %v", err)
	}

	r, err := c.CheckDrift(ctx, queue)
	if err != nil {
		t.Fatalf("CheckDrift: %v", err)
	}
	if r.Verdict() != DriftNone {
		t.Fatalf("verdict = %v on a wholly scheduled queue, want DriftNone; report=%+v", r.Verdict(), r)
	}
	if r.Counted != 40 {
		t.Fatalf("scan counted %d, want 40: scheduled messages are still on the queue", r.Counted)
	}
}

// TestCheckDriftAllSkipsInternalQueues pins that a drift sweep covers the user's
// queues and does not fail on the broker's own.
func TestCheckDriftAllSkipsInternalQueues(t *testing.T) {
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

	queue := benchQueue("drift-sweep")
	defer func() { _, _ = c.PurgeQueue(context.Background(), queue) }()
	if _, err := c.Produce(ctx, queue, GenerateMessages(10, 128, nil), 0, 1, nil); err != nil {
		t.Fatalf("produce: %v", err)
	}

	reports, err := c.CheckDriftAll(ctx)
	if err != nil {
		t.Fatalf("CheckDriftAll: %v", err)
	}
	var found bool
	for _, r := range reports {
		if r.Queue == queue {
			found = true
			if r.Verdict() != DriftNone {
				t.Fatalf("verdict = %v for %s, want DriftNone; report=%+v", r.Verdict(), queue, r)
			}
		}
	}
	if !found {
		t.Fatalf("sweep of %d queue(s) did not include %s", len(reports), queue)
	}
}
