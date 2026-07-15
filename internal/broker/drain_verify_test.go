package broker

import (
	"errors"
	"strings"
	"testing"
)

func TestDeliverableNow(t *testing.T) {
	tests := []struct {
		name string
		stat QueueStat
		want int64
	}{
		{"empty queue", QueueStat{}, 0},
		{"all deliverable", QueueStat{MessageCount: 10}, 10},
		{"scheduled held back", QueueStat{MessageCount: 10, ScheduledCount: 6}, 4},
		{"in flight to another consumer", QueueStat{MessageCount: 10, DeliveringCount: 3, ConsumerCount: 1}, 7},
		// No consumer holds these: the broker is returning them to the queue.
		{"in flight with no consumer is settlement lag", QueueStat{MessageCount: 10, DeliveringCount: 10}, 10},
		{"scheduled and delivering", QueueStat{MessageCount: 10, ScheduledCount: 6, DeliveringCount: 3, ConsumerCount: 1}, 1},
		{"wholly scheduled", QueueStat{MessageCount: 10, ScheduledCount: 10}, 0},
		{"paused ignores counters", QueueStat{MessageCount: 10, Paused: true}, 0},
		// The counters are sampled independently and can overlap in flight.
		{"overlapping counters never go negative", QueueStat{MessageCount: 5, ScheduledCount: 4, DeliveringCount: 4, ConsumerCount: 1}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.stat.DeliverableNow(); got != tc.want {
				t.Fatalf("DeliverableNow() = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestDrainOutcome(t *testing.T) {
	tests := []struct {
		name      string
		stat      QueueStat
		fruitless int
		want      outcome
	}{
		{
			name: "empty queue is complete",
			stat: QueueStat{MessageCount: 0},
			want: drainComplete,
		},
		{
			name:      "empty queue is complete even after fruitless passes",
			stat:      QueueStat{MessageCount: 0},
			fruitless: maxFruitlessPasses,
			want:      drainComplete,
		},
		{
			name: "deliverable remainder after progress means the gap was a stall",
			stat: QueueStat{MessageCount: 500},
			want: drainRetry,
		},
		{
			name:      "one fruitless pass is tolerated: acks may still be settling",
			stat:      QueueStat{MessageCount: 500},
			fruitless: 1,
			want:      drainRetry,
		},
		{
			name:      "a queue that keeps promising messages it never delivers gives up",
			stat:      QueueStat{MessageCount: 500},
			fruitless: maxFruitlessPasses,
			want:      drainStuck,
		},
		{
			name: "in-flight remainder with no consumer is retried, not reported stuck",
			stat: QueueStat{MessageCount: 50, DeliveringCount: 50},
			want: drainRetry,
		},
		{
			name: "scheduled remainder is stuck, retrying cannot help",
			stat: QueueStat{MessageCount: 600, ScheduledCount: 600},
			want: drainStuck,
		},
		{
			name: "remainder in flight to another consumer is stuck",
			stat: QueueStat{MessageCount: 50, DeliveringCount: 50, ConsumerCount: 2},
			want: drainStuck,
		},
		{
			name: "paused queue is stuck",
			stat: QueueStat{MessageCount: 50, Paused: true},
			want: drainStuck,
		},
		{
			name: "partly scheduled remainder still has deliverable messages to take",
			stat: QueueStat{MessageCount: 600, ScheduledCount: 400},
			want: drainRetry,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := drainOutcome(tc.stat, tc.fruitless); got != tc.want {
				t.Fatalf("drainOutcome() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPartialDrainErrorMessage(t *testing.T) {
	tests := []struct {
		name  string
		err   *PartialDrainError
		wants []string
	}{
		{
			name: "scheduled",
			err: &PartialDrainError{
				Queue:   "DLQ",
				Drained: 50701,
				Stat:    QueueStat{Name: "DLQ", MessageCount: 160000, ScheduledCount: 160000},
			},
			wants: []string{"incomplete drain of DLQ", "50701", "160000", "scheduled for later"},
		},
		{
			name: "in flight to another consumer",
			err: &PartialDrainError{
				Queue:   "orders",
				Drained: 5,
				Stat:    QueueStat{Name: "orders", MessageCount: 20, DeliveringCount: 20, ConsumerCount: 3},
			},
			wants: []string{"incomplete drain of orders", "in flight to 3 other consumer(s)"},
		},
		{
			name: "paused",
			err: &PartialDrainError{
				Queue:   "orders",
				Drained: 0,
				Stat:    QueueStat{Name: "orders", MessageCount: 7, Paused: true},
			},
			wants: []string{"queue is paused"},
		},
		{
			name: "deliverable but the broker went quiet: no counter explains it",
			err: &PartialDrainError{
				Queue:   "orders",
				Drained: 3,
				Stat:    QueueStat{Name: "orders", MessageCount: 9},
			},
			wants: []string{"the broker stopped delivering them"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.err.Error()
			for _, want := range tc.wants {
				if !strings.Contains(got, want) {
					t.Fatalf("Error() = %q, want it to mention %q", got, want)
				}
			}
		})
	}
}

// A PartialDrainError must survive errors.Join + errors.As, which is how
// DrainAll reports several bad queues at once and how export detects one.
func TestPartialDrainErrorIsUnwrappable(t *testing.T) {
	pde := &PartialDrainError{Queue: "DLQ", Drained: 1, Stat: QueueStat{MessageCount: 2}}
	joined := errors.Join(errors.New("other queue failed"), pde)

	var got *PartialDrainError
	if !errors.As(joined, &got) {
		t.Fatal("errors.As did not find *PartialDrainError in a joined error")
	}
	if got.Queue != "DLQ" {
		t.Fatalf("unwrapped Queue = %q, want DLQ", got.Queue)
	}
}
