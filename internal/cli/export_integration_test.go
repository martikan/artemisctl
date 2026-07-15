// internal/cli/export_integration_test.go
package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/broker"
)

func TestExportCommandCreatesStore(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	// Reuse broker package's container helper via a fresh broker for seeding.
	props := startArtemisForCLI(t)
	seedQueue(t, props, "orders", []string{"a", "b"})

	out := filepath.Join(t.TempDir(), "dump.artx")
	root := NewRootCmd()
	root.SetArgs([]string{
		"export", "--out", out,
		"--url", props.URL, "-u", props.Username, "-p", props.Password,
		"--drain-timeout", "3s",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("export: %v", err)
	}
	fi, err := os.Stat(out)
	if err != nil || fi.Size() <= 5 {
		t.Fatalf("store not written: %v size=%v", err, fi)
	}
}

// TestExportFailsOnPartialDrain pins the export contract that the reported bug
// broke: when the broker still holds messages the drain could not take, export
// must NOT report success. Previously it printed "exported N messages" and
// exited 0 while most of a 210K-deep DLQ stayed on the broker, which is how a
// partial store gets mistaken for a full one.
//
// The undrainable remainder here is a scheduled message: counted in the
// queue's depth, but not deliverable to any consumer until its delivery time.
func TestExportFailsOnPartialDrain(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemisForCLI(t)
	queue := fmt.Sprintf("export.partial.%d", time.Now().UnixNano())

	seedQueue(t, props, queue, []string{"take-me", "and-me"})
	seedScheduled(t, props, queue, 3, time.Now().Add(time.Hour))

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := broker.Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(context.Background())
	defer func() { _, _ = c.PurgeQueue(context.Background(), queue) }()

	out := filepath.Join(t.TempDir(), "partial.artx")
	var stdout bytes.Buffer
	root := NewRootCmd()
	root.SetOut(&stdout)
	root.SetArgs([]string{
		"export", "--out", out,
		"--url", props.URL, "-u", props.Username, "-p", props.Password,
		"--drain-timeout", "2s",
	})

	err = root.Execute()
	if err == nil {
		t.Fatalf("export reported success on a partial drain; stdout:\n%s", stdout.String())
	}
	var pde *broker.PartialDrainError
	if !errors.As(err, &pde) {
		t.Fatalf("export err = %v, want *PartialDrainError", err)
	}
	if got := stdout.String(); !strings.Contains(got, "INCOMPLETE") {
		t.Fatalf("export stdout must flag the partial store; got:\n%s", got)
	}
	if strings.Contains(stdout.String(), "exported ") {
		t.Fatalf("export printed a success headline on a partial drain:\n%s", stdout.String())
	}
	// The messages it DID drain must still be durably saved: a failed export is
	// not a reason to throw away recovered messages.
	fi, statErr := os.Stat(out)
	if statErr != nil || fi.Size() <= 5 {
		t.Fatalf("partial store not written: %v size=%v", statErr, fi)
	}
}

// seedScheduled puts n messages on queue that the broker will hold until at,
// so they count towards the queue's depth but cannot be drained.
func seedScheduled(t *testing.T, props broker.ConnectionProps, queue string, n int, at time.Time) {
	t.Helper()
	ctx := context.Background()
	conn, err := amqp.Dial(ctx, "amqp://"+props.URL, &amqp.ConnOptions{
		SASLType: amqp.SASLTypePlain(props.Username, props.Password),
	})
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
	for i := 0; i < n; i++ {
		msg := amqp.NewMessage([]byte(fmt.Sprintf("scheduled-%d", i)))
		msg.Properties = &amqp.MessageProperties{MessageID: fmt.Sprintf("sched-%d-%d", time.Now().UnixNano(), i)}
		msg.Annotations = amqp.Annotations{"x-opt-delivery-time": at.UnixMilli()}
		if err := sender.Send(ctx, msg, nil); err != nil {
			t.Fatalf("send scheduled: %v", err)
		}
	}
}
