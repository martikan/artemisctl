package broker

import (
	"errors"
	"fmt"
	"testing"
)

func TestDriftReportVerdict(t *testing.T) {
	tests := []struct {
		name        string
		report      DriftReport
		want        DriftVerdict
		wantMissing int64
	}{
		{
			name:   "counter agrees with the scan",
			report: DriftReport{CounterBefore: 500, Counted: 500, CounterAfter: 500},
			want:   DriftNone,
		},
		{
			name:   "empty queue agrees trivially",
			report: DriftReport{},
			want:   DriftNone,
		},
		{
			// The incident: the counter advertises a backlog no scan can find.
			name:        "stable counter promising messages a scan cannot find",
			report:      DriftReport{CounterBefore: 158782, Counted: 0, CounterAfter: 158782},
			want:        DriftConfirmed,
			wantMissing: 158782,
		},
		{
			name:        "partial drift still reports the gap",
			report:      DriftReport{CounterBefore: 1000, Counted: 400, CounterAfter: 1000},
			want:        DriftConfirmed,
			wantMissing: 600,
		},
		{
			// A live queue moves between the samples, so the counter and the scan
			// disagree for an ordinary reason. Reporting drift here would light up
			// every healthy queue that has a consumer attached.
			name:   "queue draining under us is inconclusive, not drift",
			report: DriftReport{CounterBefore: 1000, Counted: 940, CounterAfter: 880},
			want:   DriftInconclusive,
		},
		{
			name:   "queue filling under us is inconclusive",
			report: DriftReport{CounterBefore: 100, Counted: 130, CounterAfter: 160},
			want:   DriftInconclusive,
		},
		{
			// The counter under-reporting does not strand messages: the scan can
			// still find them and a drain can still take them.
			name:        "scan finding more than the counter claims reports no missing messages",
			report:      DriftReport{CounterBefore: 10, Counted: 25, CounterAfter: 10},
			want:        DriftConfirmed,
			wantMissing: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.report.Verdict(); got != tc.want {
				t.Fatalf("Verdict() = %v, want %v", got, tc.want)
			}
			if got := tc.report.Missing(); got != tc.wantMissing {
				t.Fatalf("Missing() = %d, want %d", got, tc.wantMissing)
			}
		})
	}
}

func TestDriftVerdictString(t *testing.T) {
	tests := []struct {
		verdict DriftVerdict
		want    string
	}{
		{DriftNone, "ok"},
		{DriftConfirmed, "DRIFT"},
		{DriftInconclusive, "inconclusive"},
	}
	for _, tc := range tests {
		if got := tc.verdict.String(); got != tc.want {
			t.Fatalf("String() = %q, want %q", got, tc.want)
		}
	}
}

// A queue auto-deleted mid-sweep must not fail a whole drift check, and the two
// calls a check makes report it in two different ways.
func TestIsQueueGone(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"QueueStatByName not-found", fmt.Errorf("%w: orders", ErrQueueNotFound), true},
		{"wrapped not-found", fmt.Errorf("check drift: %w", fmt.Errorf("%w: orders", ErrQueueNotFound)), true},
		{
			name: "broker rejection for a missing management resource",
			err:  errors.New(`broker rejected queue.orders.countMessages: Cannot find resource with name queue.orders`),
			want: true,
		},
		{"an unrelated failure is not a missing queue", errors.New("connection reset by peer"), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isQueueGone(tc.err); got != tc.want {
				t.Fatalf("isQueueGone(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}
