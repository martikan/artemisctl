package broker

import (
	"context"
	"errors"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startArtemisJSON boots a broker recent enough to expose the two-arg JSON
// addAddressSettings(String,String) overload over AMQP (the pinned 2.31.2 used
// elsewhere does NOT). It drives the container directly rather than via the
// artemis module because: (1) newer images must use the NIO journal to boot
// under rootless podman (default AIO fails libaio io_getevents), and (2) the
// module's wait strategy greps "Server is now live", which 2.40 renamed.
func startArtemisJSON(t testing.TB) ConnectionProps {
	t.Helper()
	ctx := context.Background()
	req := tc.ContainerRequest{
		Image:        "apache/activemq-artemis:2.40.0-alpine",
		ExposedPorts: []string{"61616/tcp"},
		Env: map[string]string{
			"ARTEMIS_USER":     "artemis",
			"ARTEMIS_PASSWORD": "artemis",
			"EXTRA_ARGS":       "--nio --relax-jolokia",
		},
		WaitingFor: wait.ForListeningPort("61616/tcp").WithStartupTimeout(120 * time.Second),
	}
	ctr, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start artemis (JSON overload image): %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })
	host, err := ctr.Host(ctx)
	if err != nil {
		t.Fatalf("host: %v", err)
	}
	port, err := ctr.MappedPort(ctx, "61616/tcp")
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return ConnectionProps{URL: host + ":" + port.Port(), Username: "artemis", Password: "artemis"}
}

func TestCordonBlocksAndUncordonRestores(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemisJSON(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	// Before cordon: a producer can send.
	warmCtx, warmCancel := context.WithTimeout(ctx, 8*time.Second)
	if err := sendTestMessages(warmCtx, c, "cordon.before", []string{"ok"}); err != nil {
		t.Fatalf("baseline send should succeed: %v", err)
	}
	warmCancel()

	// Cordon.
	saved, err := c.Cordon(ctx)
	if err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if saved == "" {
		t.Fatal("cordon returned empty saved settings")
	}

	// Under cordon: producing enough to cross the block threshold fails.
	blockCtx, blockCancel := context.WithTimeout(ctx, 6*time.Second)
	blockErr := sendTestMessages(blockCtx, c, "cordon.blocked",
		[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
	blockCancel()
	if blockErr == nil {
		t.Fatal("expected producer to be blocked while cordoned")
	}

	// Uncordon restores the saved settings; producing works again.
	if err := c.Uncordon(ctx, saved); err != nil {
		t.Fatalf("uncordon: %v", err)
	}
	afterCtx, afterCancel := context.WithTimeout(ctx, 8*time.Second)
	defer afterCancel()
	if err := sendTestMessages(afterCtx, c, "cordon.after", []string{"back"}); err != nil {
		t.Fatalf("send after uncordon should succeed: %v", err)
	}
}

func TestUncordonRemoveLiftsCordon(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemisJSON(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	if _, err := c.Cordon(ctx); err != nil {
		t.Fatalf("cordon: %v", err)
	}
	if err := c.UncordonRemove(ctx); err != nil {
		t.Fatalf("uncordon-remove: %v", err)
	}
	afterCtx, afterCancel := context.WithTimeout(ctx, 8*time.Second)
	defer afterCancel()
	if err := sendTestMessages(afterCtx, c, "cordon.removed", []string{"back"}); err != nil {
		t.Fatalf("send after uncordon-remove should succeed: %v", err)
	}
}

// TestCordonOldBrokerRejected documents that the pinned 2.31.2 broker cannot
// apply the setting over AMQP and that Cordon surfaces ErrBrokerTooOld.
func TestCordonOldBrokerRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t) // 2.31.2
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	_, err = c.Cordon(ctx)
	if !errors.Is(err, ErrBrokerTooOld) {
		t.Fatalf("expected ErrBrokerTooOld on 2.31.2, got %v", err)
	}
}
