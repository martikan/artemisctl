package broker

import (
	"context"
	"testing"
	"time"
)

func TestCheckHealthIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	h, err := c.CheckHealth(ctx)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if h.Verdict != OK {
		t.Fatalf("fresh broker should be OK, got %s (disk %.1f mem %.1f)", h.Verdict, h.DiskUsagePct, h.MemoryUsagePct)
	}
	// I1: a fresh broker with a near-empty disk is well below max-disk-usage,
	// so the disk-full producer-block indicator must be false.
	if h.Blocking {
		t.Fatalf("fresh broker should not report producer blocking (disk %.1f%%)", h.DiskUsagePct)
	}
}
