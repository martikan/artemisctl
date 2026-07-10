package broker

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/testcontainers/testcontainers-go/modules/artemis"
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

// startArtemis boots a broker container and returns connection props + cleanup.
// Takes testing.TB so both tests and benchmarks can use it.
func startArtemis(t testing.TB) ConnectionProps {
	t.Helper()
	ctx := context.Background()
	ctr, err := artemis.Run(ctx, "apache/activemq-artemis:2.31.2")
	if err != nil {
		t.Fatalf("start artemis: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })
	host, err := ctr.BrokerEndpoint(ctx)
	if err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	return ConnectionProps{URL: host, Username: ctr.User(), Password: ctr.Password()}
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
