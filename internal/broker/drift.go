package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// DriftVerdict is what a drift check concluded about one queue.
type DriftVerdict int

const (
	// DriftNone: the scan and the counter agree. The depth is real.
	DriftNone DriftVerdict = iota
	// DriftConfirmed: the counter is stable and still disagrees with a scan of
	// the queue. The counter is advertising messages that are not there.
	DriftConfirmed
	// DriftInconclusive: the queue's depth moved while it was being checked, so
	// the counter and the scan were sampled against different queues and their
	// disagreement proves nothing.
	DriftInconclusive
)

func (v DriftVerdict) String() string {
	switch v {
	case DriftConfirmed:
		return "DRIFT"
	case DriftInconclusive:
		return "inconclusive"
	default:
		return "ok"
	}
}

// DriftReport is one queue's drift check: the counter read on either side of a
// scan, and what the scan found in between.
//
// The counter is read twice on purpose. A drift check compares two numbers
// sampled at different instants, so on a queue with live producers or consumers
// they disagree simply because the queue moved -- which would report drift on
// every healthy busy queue. Bracketing the scan with two counter reads
// distinguishes the two: only a counter that did not move while the scan ran
// can be meaningfully compared against it.
type DriftReport struct {
	Queue string

	// CounterBefore and CounterAfter are QueueStat.MessageCount either side of
	// the scan; Counted is what the scan found.
	CounterBefore int64
	Counted       int64
	CounterAfter  int64
}

// Verdict classifies the report. Missing is the magnitude of a confirmed drift:
// how many messages the counter claims that do not exist.
func (r DriftReport) Verdict() DriftVerdict {
	if r.CounterBefore != r.CounterAfter {
		return DriftInconclusive
	}
	if r.CounterBefore == r.Counted {
		return DriftNone
	}
	return DriftConfirmed
}

// Missing is how many messages the counter over-reports. It is only meaningful
// for a DriftConfirmed report; it is 0 otherwise, including when a scan finds
// MORE than the counter claims (an under-reporting counter is a different bug
// and does not strand messages, so it is not what this reports).
func (r DriftReport) Missing() int64 {
	if r.Verdict() != DriftConfirmed {
		return 0
	}
	if n := r.CounterBefore - r.Counted; n > 0 {
		return n
	}
	return 0
}

// CheckDrift reports whether a queue's message counter agrees with a scan of
// the queue itself.
//
// This detects the failure that makes a queue undrainable: for a paged queue
// the broker derives messageCount from page-counter journal records, and when
// those drift the queue advertises a depth whose messages do not exist. It
// reports a large backlog, refuses to deliver any of it, and no export can ever
// empty it -- the only repair is a broker restart, which rebuilds the counter
// from the journal.
//
// It is deliberately not free: the scan walks the queue, so this costs far more
// than reading the counter and is meant to be run when a queue looks wrong, not
// on every status.
func (c *Client) CheckDrift(ctx context.Context, name string) (DriftReport, error) {
	before, err := c.QueueStatByName(ctx, name)
	if err != nil {
		return DriftReport{}, err
	}
	counted, err := c.CountMessages(ctx, name)
	if err != nil {
		return DriftReport{}, err
	}
	after, err := c.QueueStatByName(ctx, name)
	if err != nil {
		return DriftReport{}, err
	}
	return DriftReport{
		Queue:         name,
		CounterBefore: before.MessageCount,
		Counted:       counted,
		CounterAfter:  after.MessageCount,
	}, nil
}

// isQueueGone reports whether err means "that queue no longer exists".
//
// The two calls a drift check makes report it differently: listQueues simply
// does not return the queue, which QueueStatByName turns into ErrQueueNotFound,
// while a management op against a queue that is gone is REJECTED by the broker
// with "Cannot find resource with name queue.X" -- an error with no structure to
// match on, hence the string. Matching it is worth it: an auto-delete race is
// routine and must not fail a whole drift sweep.
func isQueueGone(err error) bool {
	if errors.Is(err, ErrQueueNotFound) {
		return true
	}
	return strings.Contains(err.Error(), "Cannot find resource")
}

// CheckDriftAll runs CheckDrift over every user queue.
//
// A queue that vanishes mid-check (auto-deleted once emptied) is skipped rather
// than failing the run: it holds nothing, so it cannot be hiding a drifted
// backlog.
func (c *Client) CheckDriftAll(ctx context.Context) ([]DriftReport, error) {
	queues, err := c.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	reports := make([]DriftReport, 0, len(queues))
	for _, q := range queues {
		r, err := c.CheckDrift(ctx, q.Name)
		if err != nil {
			if isQueueGone(err) {
				continue
			}
			return nil, fmt.Errorf("check drift on %s: %w", q.Name, err)
		}
		reports = append(reports, r)
	}
	return reports, nil
}
