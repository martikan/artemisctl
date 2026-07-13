// internal/broker/redeliver_core_integration_test.go
package broker

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/journal"
	"github.com/martikan/artemisctl/internal/store"
)

// TestRedeliverCoreConvertsAndLands writes a KindCore store record (a decoded
// Core TEXT message), redelivers it, and confirms the converted AMQP message
// actually lands on the queue with the right body and application property —
// the end-to-end proof that Core messages salvaged offline can be replayed to
// a live broker via Core->AMQP conversion.
func TestRedeliverCoreConvertsAndLands(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	const queue = "coreq"
	payload := (&journal.CorePayload{
		MessageID:  1,
		Address:    queue,
		Type:       journal.CoreTypeText,
		Durable:    true,
		Priority:   4,
		Properties: map[string]any{"region": "eu"},
		Body:       coreSimpleString("core-hello"),
	}).Encode()

	path := filepath.Join(t.TempDir(), "core.artx")
	w, err := store.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	rec := store.Record{
		UUID:        store.DedupIDCore(payload, queue),
		Queue:       queue,
		DrainedAt:   time.Now().UnixNano(),
		Kind:        store.KindCore,
		CorePayload: payload,
	}
	if err := w.Append(rec); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	n, coreSkipped, err := c.Redeliver(ctx, path, RedeliverOpts{}, nil)
	if err != nil {
		t.Fatalf("redeliver: %v", err)
	}
	if n != 1 || coreSkipped != 0 {
		t.Fatalf("redeliver core: sent=%d skipped=%d, want 1/0", n, coreSkipped)
	}

	recv, err := c.Session().NewReceiver(ctx, queue, &amqp.ReceiverOptions{SourceCapabilities: []string{"queue"}})
	if err != nil {
		t.Fatalf("open receiver: %v", err)
	}
	defer recv.Close(context.Background())
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	defer rcancel()
	msg, err := recv.Receive(rctx, nil)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	_ = recv.AcceptMessage(ctx, msg)

	if got, ok := msg.Value.(string); !ok || got != "core-hello" {
		t.Errorf("body = %#v, want AmqpValue \"core-hello\"", msg.Value)
	}
	if got := msg.ApplicationProperties["region"]; got != "eu" {
		t.Errorf("app property region = %v, want eu", got)
	}
}
