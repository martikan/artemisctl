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
	// DiskUsagePct reflects the real filesystem holding the broker's data dir,
	// which on a dev host is a shared partition this test does not control. A
	// busy disk (>=70%) legitimately yields DEGRADED on an otherwise-fresh
	// broker, so we do NOT assert Verdict == OK. What must always hold for a
	// fresh broker is that it is not in the CRITICAL band (disk/mem >90% or
	// producers blocked).
	if h.Verdict == Critical {
		t.Fatalf("fresh broker should not be CRITICAL, got %s (disk %.1f mem %.1f blocking %v)", h.Verdict, h.DiskUsagePct, h.MemoryUsagePct, h.Blocking)
	}
	// I1: a fresh broker with a near-empty disk is well below max-disk-usage,
	// so the disk-full producer-block indicator must be false.
	if h.Blocking {
		t.Fatalf("fresh broker should not report producer blocking (disk %.1f%%)", h.DiskUsagePct)
	}
}
