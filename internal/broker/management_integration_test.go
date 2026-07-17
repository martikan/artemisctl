package broker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"

	"github.com/martikan/artemisctl/internal/brokertest"
	"github.com/martikan/artemisctl/internal/store"
)

// sendTestMessages sends the given bodies to the named queue over the
// client's existing AMQP session, forcing Artemis to auto-create the
// queue and populate its message count.
func sendTestMessages(ctx context.Context, c *Client, queue string, bodies []string) error {
	// TargetCapabilities: []string{"queue"} tells Artemis to route this as an
	// anycast queue rather than the default multicast address, so the queue
	// is auto-created and picks up a message count even with no consumer
	// attached.
	sender, err := c.Session().NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
	if err != nil {
		return err
	}
	defer sender.Close(context.Background())
	for i, body := range bodies {
		msg := amqp.NewMessage([]byte(body))
		// Real producers (notably JMS) always stamp an AMQP message-id; browse
		// reports it as BrowsedMessage.ID and BrowseMessage matches on it, so
		// give each test message a unique one instead of leaving it nil.
		msg.Properties = &amqp.MessageProperties{MessageID: fmt.Sprintf("%s-%d-%s", queue, i, body)}
		if err := sender.Send(ctx, msg, nil); err != nil {
			return err
		}
	}
	return nil
}

// startArtemis returns connection props for the shared integration broker,
// resetting it to a clean slate first. Takes testing.TB so both tests and
// benchmarks can use it.
func startArtemis(t testing.TB) ConnectionProps {
	t.Helper()
	sc := brokertest.Shared(t)
	props := ConnectionProps{URL: sc.URL, Username: sc.Username, Password: sc.Password}
	resetBroker(t, props)
	return props
}

// discardSink drops every drained record; used only to purge queues.
type discardSink struct{}

func (discardSink) Append(store.Record) error { return nil }
func (discardSink) Sync() error               { return nil }

// resetBroker returns the shared, reused broker to a clean slate so each test
// starts fresh despite dirty-context reuse: it lifts any wildcard cordon a
// crashed cordon test may have left (which would otherwise reject the next
// test's producers), then drains every user queue empty.
func resetBroker(t testing.TB, props ConnectionProps) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("reset connect: %v", err)
	}
	defer c.Close(ctx)
	// Lift any leftover cordon by overwriting the wildcard with permissive
	// settings. removeAddressSettings("#") does not reliably clear it on 2.42.
	_ = c.Uncordon(ctx, brokertest.PermissiveWildcardSettings)
	qs, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatalf("reset list queues: %v", err)
	}
	for _, q := range qs {
		// Purge rather than drain: removeAllMessages also clears messages a
		// consumer cannot receive (scheduled, held for redelivery), which a
		// drain leaves behind -- and which DrainQueue now correctly reports as
		// an incomplete drain, so a single leftover scheduled message from an
		// earlier test would fail every later one.
		if _, err := c.PurgeQueue(ctx, q.Name); err != nil {
			t.Fatalf("reset purge %s: %v", q.Name, err)
		}
	}
}

func TestListQueuesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	if err := sendTestMessages(ctx, c, "orders", []string{"order-1", "order-2"}); err != nil {
		t.Fatalf("send test messages: %v", err)
	}

	stats, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatalf("list queues: %v", err)
	}

	var found *QueueStat
	for i := range stats {
		s := stats[i]
		if strings.HasPrefix(s.Name, "activemq.") || strings.HasPrefix(s.Name, "$") {
			t.Fatalf("filter did not hold against live broker, got internal queue: %+v", s)
		}
		if s.Name == "orders" {
			found = &s
		}
	}
	if found == nil {
		t.Fatalf("expected queue %q in results, got %+v", "orders", stats)
	}
	if found.MessageCount < 2 {
		t.Fatalf("expected MessageCount >= 2 for %q, got %d", "orders", found.MessageCount)
	}
}
