package broker

import (
	"context"
	"strings"
	"testing"
	"time"
)

// browseWithRetry wraps BrowseQueue with the caller-side retry the browse
// contract asks for: peeking-and-releasing a batch churns Artemis's internal
// state, and verifyMetaUnchanged can transiently report the queue "changed
// during browse" even with no concurrent consumer. That error is documented as
// retryable; large batches (see TestProduceParallelIntegration) trip it often
// enough that tests must retry rather than fail on the first transient.
func browseWithRetry(ctx context.Context, t *testing.T, c *Client, queue string, limit, offset int) []BrowsedMessage {
	t.Helper()
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		browsed, err := c.BrowseQueue(ctx, queue, limit, offset)
		if err == nil {
			return browsed
		}
		if !strings.Contains(err.Error(), "changed during browse") {
			t.Fatalf("browse: %v", err)
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("browse retry: %v", ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
	t.Fatalf("browse still transiently changing after retries: %v", lastErr)
	return nil
}

func TestBrowseIsNonDestructive(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	if err := sendTestMessages(ctx, c, "orders", []string{"m1", "m2", "m3"}); err != nil {
		t.Fatalf("send test messages: %v", err)
	}

	msgs, err := c.BrowseQueue(ctx, "orders", 10, 0)
	if err != nil {
		t.Fatalf("browse: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("want 3 browsed, got %d", len(msgs))
	}

	// The zip-by-position must actually pair the right body with the right
	// metadata: previews must come back in the exact order sent, and every
	// message must have picked up a distinct, non-empty broker messageID.
	wantPreviews := []string{"m1", "m2", "m3"}
	seenIDs := make(map[string]bool, len(msgs))
	for i, m := range msgs {
		if m.Preview != wantPreviews[i] {
			t.Fatalf("msgs[%d].Preview = %q, want %q (zip-by-position mismatch)", i, m.Preview, wantPreviews[i])
		}
		if m.ID == "" {
			t.Fatalf("msgs[%d].ID is empty", i)
		}
		if seenIDs[m.ID] {
			t.Fatalf("msgs[%d].ID %q is a duplicate", i, m.ID)
		}
		seenIDs[m.ID] = true
	}
	m2ID := msgs[1].ID

	// Non-destructive: count unchanged.
	time.Sleep(500 * time.Millisecond)
	stats, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stats {
		if s.Name == "orders" && s.MessageCount != 3 {
			t.Fatalf("browse consumed messages: orders=%d want 3", s.MessageCount)
		}
	}

	// Paging: limit=2, offset=1 must return exactly m2,m3 and must not
	// consume anything (the offset path releases everything it walks past,
	// including messages before the offset).
	paged, err := c.BrowseQueue(ctx, "orders", 2, 1)
	if err != nil {
		t.Fatalf("browse paged: %v", err)
	}
	if len(paged) != 2 {
		t.Fatalf("want 2 paged messages, got %d", len(paged))
	}
	wantPaged := []string{"m2", "m3"}
	for i, m := range paged {
		if m.Preview != wantPaged[i] {
			t.Fatalf("paged[%d].Preview = %q, want %q", i, m.Preview, wantPaged[i])
		}
	}
	stats, err = c.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range stats {
		if s.Name == "orders" && s.MessageCount != 3 {
			t.Fatalf("paged browse consumed messages: orders=%d want 3", s.MessageCount)
		}
	}

	// Drill-down: BrowseMessage for the m2 message's ID must return its body.
	full, err := c.BrowseMessage(ctx, "orders", m2ID)
	if err != nil {
		t.Fatalf("browse message %s: %v", m2ID, err)
	}
	if got := string(full.GetData()); got != "m2" {
		t.Fatalf("BrowseMessage(%s) body = %q, want %q", m2ID, got, "m2")
	}

	// A messageID that isn't on the queue must return a not-found error.
	if _, err := c.BrowseMessage(ctx, "orders", "999999999"); err == nil {
		t.Fatal("BrowseMessage for a missing ID: want error, got nil")
	}

	// An offset past the end of the queue returns nothing without error.
	beyond, err := c.BrowseQueue(ctx, "orders", 10, 100)
	if err != nil {
		t.Fatalf("browse beyond end: %v", err)
	}
	if len(beyond) != 0 {
		t.Fatalf("offset past end: want 0 messages, got %d", len(beyond))
	}
}
