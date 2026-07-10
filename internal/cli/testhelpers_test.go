// internal/cli/testhelpers_test.go
package cli

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/broker"
	"github.com/testcontainers/testcontainers-go/modules/artemis"
)

// startArtemisForCLI boots a broker container and returns connection props.
// It is a cli-package-local copy of the broker package's startArtemis helper
// (test-only symbols cannot be shared across packages).
func startArtemisForCLI(t *testing.T) broker.ConnectionProps {
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
	return broker.ConnectionProps{URL: host, Username: ctr.User(), Password: ctr.Password()}
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
