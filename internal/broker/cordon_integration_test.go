package broker

import (
	"context"
	"testing"
	"time"
)

func TestCordonBlocksAndUncordonRestores(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
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
	props := startArtemis(t)
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

	// Confirm the cordon is actually in force before lifting it: many sends to
	// one address must fail. Without this, UncordonRemove could be a no-op and
	// the test below would still pass on the first-message-accepted loophole.
	blockCtx, blockCancel := context.WithTimeout(ctx, 6*time.Second)
	blockErr := sendTestMessages(blockCtx, c, "cordon.remove.blocked",
		[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"})
	blockCancel()
	if blockErr == nil {
		t.Fatal("expected producer to be blocked while cordoned")
	}

	if err := c.UncordonRemove(ctx); err != nil {
		t.Fatalf("uncordon-remove: %v", err)
	}

	// After the lift the same volume of sends must all succeed. A single send
	// would pass even under the FAIL policy (the first message to an empty
	// address is accepted), so send many to prove the cordon is truly gone.
	afterCtx, afterCancel := context.WithTimeout(ctx, 10*time.Second)
	defer afterCancel()
	if err := sendTestMessages(afterCtx, c, "cordon.removed",
		[]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}); err != nil {
		t.Fatalf("send after uncordon-remove should succeed: %v", err)
	}
}
