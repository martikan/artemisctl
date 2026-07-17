// internal/cli/testhelpers_test.go
package cli

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/broker"
	"github.com/martikan/artemisctl/internal/brokertest"
)

// startArtemisForCLI returns connection props for the shared integration broker,
// resetting it to a clean slate first.
func startArtemisForCLI(t *testing.T) broker.ConnectionProps {
	t.Helper()
	sc := brokertest.Shared(t)
	props := broker.ConnectionProps{URL: sc.URL, Username: sc.Username, Password: sc.Password}
	resetBrokerForCLI(t, props)
	return props
}

// resetBrokerForCLI empties the shared broker before a CLI integration test,
// mirroring the broker package's resetBroker over the exported client API.
func resetBrokerForCLI(t *testing.T, props broker.ConnectionProps) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := broker.Connect(ctx, props)
	if err != nil {
		t.Fatalf("reset connect: %v", err)
	}
	defer c.Close(ctx)
	// Lift any leftover cordon (see resetBroker in the broker package).
	_ = c.Uncordon(ctx, brokertest.PermissiveWildcardSettings)
	qs, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatalf("reset list queues: %v", err)
	}
	for _, q := range qs {
		// Destroy user queues outright rather than draining or merely purging
		// them, matching the broker package's resetBroker. A drain pays a
		// per-queue idle-timeout even on an empty queue; a purge is instant but
		// leaves the queue in place, so it still accumulates on the shared,
		// never-terminated broker -- and every later `export`/DrainAll then
		// visits each leftover queue and pays ITS drain-timeout, which is what
		// actually dominated the CLI suite's runtime. Destroying keeps the
		// broker's queue list minimal so DrainAll stays cheap; auto-create
		// settings recreate a queue on the next send. DLQ/ExpiryQueue are broker
		// infrastructure (targets of the DLA/expiry address settings), so they
		// are only emptied, never destroyed.
		if q.Name == "DLQ" || q.Name == "ExpiryQueue" {
			if _, err := c.PurgeQueue(ctx, q.Name); err != nil {
				t.Fatalf("reset purge %s: %v", q.Name, err)
			}
			continue
		}
		if err := c.DestroyQueue(ctx, q.Name); err != nil {
			t.Fatalf("reset destroy %s: %v", q.Name, err)
		}
	}
}

// seedQueue sends bodies to queue over a standalone AMQP connection.
// TargetCapabilities: []string{"queue"} tells Artemis to route this as an
// anycast queue rather than the default multicast address, so the queue is
// auto-created and actually holds the messages for export to drain.
func seedQueue(t *testing.T, props broker.ConnectionProps, queue string, bodies []string) {
	t.Helper()
	ctx := context.Background()
	conn, err := amqp.Dial(ctx, "amqp://"+props.Username+":"+props.Password+"@"+props.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	sender, err := sess.NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close(ctx)
	for i, b := range bodies {
		msg := amqp.NewMessage([]byte(b))
		// Real producers stamp an AMQP message-id; browse reports it as the ID
		// column and BrowseMessage matches on it, so give each a unique one.
		msg.Properties = &amqp.MessageProperties{MessageID: fmt.Sprintf("%s-%d-%s", queue, i, b)}
		if err := sender.Send(ctx, msg, nil); err != nil {
			t.Fatal(err)
		}
	}
}
