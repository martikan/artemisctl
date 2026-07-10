package broker

import (
	"context"
	"encoding/json"
	"fmt"
)

// Verdict is the single-word health rating derived from a broker's disk,
// memory, and producer-blocking state. See classify for the thresholds.
type Verdict string

const (
	OK       Verdict = "OK"       // all usage below 70%, not blocking
	Degraded Verdict = "DEGRADED" // some usage in 70–90%, still serving
	Critical Verdict = "CRITICAL" // usage above 90% or producers are blocked
)

// Health is a point-in-time snapshot of a broker's resource pressure. Disk and
// memory are percentages (0–100); Blocking is true when the broker is refusing
// producers because of disk-full protection; Verdict is the classify result.
type Health struct {
	DiskUsagePct   float64
	MemoryUsagePct float64
	Blocking       bool
	Verdict        Verdict
}

// classify maps raw usage percentages and the blocking flag to a Verdict:
// Critical if blocking or either usage exceeds 90%, Degraded if either reaches
// 70%, otherwise OK.
func classify(diskPct, memPct float64, blocking bool) Verdict {
	if blocking || diskPct > 90 || memPct > 90 {
		return Critical
	}
	if diskPct >= 70 || memPct >= 70 {
		return Degraded
	}
	return OK
}

// scalarReply parses Artemis's array-wrapped scalar management reply into a float64.
//
// Confirmed against apache/activemq-artemis:2.31.2 in Task 8's spike
// (internal/broker/probe_test.go, run and deleted after verification):
//   - broker.getDiskStoreUsage returns reply.Value as the string "[0.0]" — a
//     JSON-array-wrapped float64 fraction in 0..1 (fresh broker: 0.0).
//   - broker.getAddressMemoryUsagePercentage returns reply.Value as the
//     string "[0]" — a JSON-array-wrapped number already in 0..100 percent
//     (fresh broker: 0).
//
// Both op names and shapes matched the brief's candidates exactly; no
// adjustment to the operation names or parsing was required.
func scalarReply(reply interface{}) (float64, error) {
	s, ok := reply.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected scalar reply type %T", reply)
	}
	var arr []float64
	if err := json.Unmarshal([]byte(s), &arr); err != nil {
		return 0, fmt.Errorf("parse scalar reply %q: %w", s, err)
	}
	if len(arr) == 0 {
		return 0, fmt.Errorf("empty scalar reply")
	}
	return arr[0], nil
}

// CheckHealth queries the broker's disk-store usage, address-memory usage, and
// producer-blocking state over the management address and returns a Health
// snapshot with the classified Verdict. Blocking is inferred from the broker's
// own max-disk-usage threshold (see the disk-full logic below), so it fires
// under a custom lower limit too.
func (c *Client) CheckHealth(ctx context.Context) (Health, error) {
	diskReply, err := c.callManagement(ctx, "broker", "getDiskStoreUsage", "[]")
	if err != nil {
		return Health{}, err
	}
	diskFrac, err := scalarReply(diskReply.Value)
	if err != nil {
		return Health{}, err
	}
	memReply, err := c.callManagement(ctx, "broker", "getAddressMemoryUsagePercentage", "[]")
	if err != nil {
		return Health{}, err
	}
	memPct, err := scalarReply(memReply.Value)
	if err != nil {
		return Health{}, err
	}
	diskPct := diskFrac * 100 // getDiskStoreUsage returns a 0..1 fraction

	// I1 — real producer-blocking (disk-full block).
	//
	// Artemis's disk-full protection blocks ALL producers once disk store
	// usage crosses the configured max-disk-usage. That is genuine producer
	// blocking, not an arbitrary threshold. The dedicated broker.isDiskFull
	// operation does NOT exist on apache/activemq-artemis:2.31.2 (probed in
	// the I1 spike: reply "AMQ229069: no operation isDiskFull/0"), so we read
	// the configured threshold and compare it against current usage.
	//
	// Confirmed op + reply shape (I1 spike, run and deleted):
	//   broker.getMaxDiskUsage -> reply.Value string "[90]" — a
	//   JSON-array-wrapped percent (0..100); the block threshold in percent.
	// (broker.getDiskStoreUsage already gives current usage as a 0..1
	// fraction, above.) Blocking is true when disk usage has reached the
	// broker's own block threshold, so it tracks the real config rather than
	// the hardcoded classify() cutoffs — e.g. it fires at a custom
	// max-disk-usage of 70 that the >90 rule would miss.
	maxDiskReply, err := c.callManagement(ctx, "broker", "getMaxDiskUsage", "[]")
	if err != nil {
		return Health{}, err
	}
	maxDiskPct, err := scalarReply(maxDiskReply.Value)
	if err != nil {
		return Health{}, err
	}
	blocking := maxDiskPct > 0 && diskPct >= maxDiskPct

	h := Health{DiskUsagePct: diskPct, MemoryUsagePct: memPct, Blocking: blocking}
	h.Verdict = classify(h.DiskUsagePct, h.MemoryUsagePct, h.Blocking)
	return h, nil
}
