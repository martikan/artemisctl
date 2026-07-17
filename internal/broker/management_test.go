package broker

import (
	"encoding/json"
	"testing"

	"github.com/Azure/go-amqp"
)

func TestParseQueueStatsReply(t *testing.T) {
	t.Run("double-encoded JSON with quoted messageCount", func(t *testing.T) {
		inner := `{"data":[{"name":"orders","messageCount":"7"},{"name":"activemq.notifications","messageCount":"1"}]}`
		outerBytes, err := json.Marshal([]string{inner})
		if err != nil {
			t.Fatalf("marshal outer: %v", err)
		}
		msg := &amqp.Message{Value: string(outerBytes)}

		got, err := parseQueueStatsReply(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("expected exactly one queue stat, got %+v", got)
		}
		want := QueueStat{Name: "orders", MessageCount: 7}
		if got[0] != want {
			t.Fatalf("got %+v want %+v", got[0], want)
		}
	})

	t.Run("empty data set returns nil without error", func(t *testing.T) {
		inner := `{"data":[]}`
		outerBytes, err := json.Marshal([]string{inner})
		if err != nil {
			t.Fatalf("marshal outer: %v", err)
		}
		msg := &amqp.Message{Value: string(outerBytes)}

		got, err := parseQueueStatsReply(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected no queue stats, got %+v", got)
		}
	})

	t.Run("empty outer array returns nil without error", func(t *testing.T) {
		msg := &amqp.Message{Value: `[]`}
		got, err := parseQueueStatsReply(msg)
		if err != nil || got != nil {
			t.Fatalf("got %+v, %v; want nil, nil", got, err)
		}
	})

	t.Run("non-string reply value errors", func(t *testing.T) {
		if _, err := parseQueueStatsReply(&amqp.Message{Value: 123}); err == nil {
			t.Fatal("want error for non-string management reply")
		}
	})

	t.Run("malformed outer JSON errors", func(t *testing.T) {
		if _, err := parseQueueStatsReply(&amqp.Message{Value: `not-json`}); err == nil {
			t.Fatal("want error for malformed outer array")
		}
	})

	t.Run("malformed paged JSON errors", func(t *testing.T) {
		outer, _ := json.Marshal([]string{`{"data": not-json}`})
		if _, err := parseQueueStatsReply(&amqp.Message{Value: string(outer)}); err == nil {
			t.Fatal("want error for malformed paged response")
		}
	})
}

func TestFilterInternalQueues(t *testing.T) {
	in := []QueueStat{
		{Name: "orders", MessageCount: 5},
		{Name: "activemq.notifications", MessageCount: 1},
		{Name: "$sys.foo", MessageCount: 1},
		// The dynamic reply queue callManagement opens: the broker flags it
		// temporary, which is what we filter on.
		{Name: "123e4567-e89b-12d3-a456-426614174000", MessageCount: 1, Temporary: true},
		{Name: "internal.bookkeeping", MessageCount: 1, Internal: true},
		// A real user queue that is 36 characters long must survive: an export
		// that silently skips a queue loses every message on it.
		{Name: "billing_credit_approved_debit_rows_q", MessageCount: 7},
		{Name: "payments", MessageCount: 2},
	}
	got := filterInternalQueues(in)
	want := []string{"orders", "billing_credit_approved_debit_rows_q", "payments"}
	if len(got) != len(want) {
		t.Fatalf("filterInternalQueues returned %d queues, want %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		if got[i].Name != w {
			t.Fatalf("filterInternalQueues()[%d] = %q, want %q", i, got[i].Name, w)
		}
	}
}
