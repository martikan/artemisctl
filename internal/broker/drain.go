package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/store"
)

// RecordSink is the persistence target for drained messages. It is
// satisfied by *store.Writer.
type RecordSink interface {
	Append(store.Record) error
	Sync() error
}

// PartialDrainError reports that a drain finished with messages still on the
// broker. It carries the broker's own counters at the moment the drain gave
// up, so the caller can see WHY the rest did not come: scheduled for later,
// in flight to another consumer, or a paused queue.
//
// This exists because an idle receive is not proof of an empty queue, and an
// export that quietly returns a partial file is worse than one that fails.
type PartialDrainError struct {
	Queue   string
	Drained int
	Stat    QueueStat

	// Scanned reports whether a countMessages scan ran; Counted is what it
	// found. Stat.MessageCount is only a counter, so when a scan finds 0 while
	// the counter reports a backlog, the counter has drifted and the
	// "remaining" messages do not exist -- no drain can ever retrieve them, and
	// saying so is the difference between a broker bug and a tool bug.
	//
	// Scanned gates Counted so that the zero value means "no scan ran" rather
	// than the far more alarming "a scan found nothing".
	Scanned bool
	Counted int64
}


// outcome is what the broker's counters say about a finished drain pass.
type outcome int

const (
	// drainComplete: the broker holds nothing more.
	drainComplete outcome = iota
	// drainRetry: messages remain and the broker would hand them over, so the
	// quiet gap was a stall (GC, depaging, slow network) rather than an end.
	drainRetry
	// drainStuck: messages remain that the broker will not deliver to us now.
	drainStuck
)

// maxFruitlessPasses bounds how many times in a row DrainQueue will re-attempt
// a queue that the broker says still holds deliverable messages but that hands
// over nothing. One fruitless pass is expected and harmless (the broker can
// still be settling the previous pass's acks, or briefly stalled), but a queue
// that keeps promising messages it never delivers must fail rather than spin.
// Each pass already costs a full idle timeout, so this stays cheap.
const maxFruitlessPasses = 3

func (e *PartialDrainError) Error() string {
	reasons := make([]string, 0, 3)
	if e.Stat.ScheduledCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d scheduled for later delivery", e.Stat.ScheduledCount))
	}
	if e.Stat.DeliveringCount > 0 && e.Stat.ConsumerCount > 0 {
		reasons = append(reasons, fmt.Sprintf("%d in flight to %d other consumer(s)",
			e.Stat.DeliveringCount, e.Stat.ConsumerCount))
	}
	if e.Stat.Paused {
		reasons = append(reasons, "queue is paused")
	}
	// With no counter to explain the remainder, the broker is telling us these
	// messages ARE deliverable yet handing over none of them -- a broker-side
	// stall, not a queue holding messages back. Say exactly that: "the broker
	// stopped delivering them" reads like a tool giving up, and leaves the
	// operator with nowhere to look.
	why := fmt.Sprintf("all %d are deliverable now, but the broker handed over none across %d retries"+
		"; this is a broker-side stall, not a queue holding them back -- check the broker log and its disk",
		e.Stat.DeliverableNow(), maxFruitlessPasses)
	if e.Scanned && e.Counted == 0 && e.Stat.MessageCount > 0 {
		// The counter advertises a backlog that a scan of the queue cannot
		// find. Nothing is recoverable here and no retry will help: say so
		// plainly rather than let it read as an export that failed.
		why = fmt.Sprintf("messageCount reports %d but a countMessages scan finds 0"+
			": the queue's message counter has drifted and those messages do not exist"+
			" -- nothing is left to export; restart the broker to rebuild the counter from its journal",
			e.Stat.MessageCount)
	} else if e.Scanned && e.Counted > 0 && len(reasons) == 0 {
		why = fmt.Sprintf("a scan confirms %d real message(s) and all are deliverable now,"+
			" yet the broker handed over none across %d retries"+
			": this is a broker-side stall -- check the broker log and its disk",
			e.Counted, maxFruitlessPasses)
	}
	if len(reasons) > 0 {
		why = strings.Join(reasons, ", ")
	}
	return fmt.Sprintf("incomplete drain of %s: took %d message(s), but %d remain on the broker (%s)",
		e.Queue, e.Drained, e.Stat.MessageCount, why)
}

// DrainQueue consumes every message on queue, persisting each batch to sink
// (Append + Sync/fsync) before acking the broker, so a crash mid-drain never
// loses a message: it is either still on the broker (unacked) or durably on
// disk (or both, which is safe to re-drain/replay).
//
// Completion is decided by the broker, not by silence. A gap in delivery only
// ends a drain *pass*; DrainQueue then asks the broker for the queue's real
// depth and acts on it:
//
//   - depth 0: the queue is genuinely empty, the drain is complete.
//   - messages remain and are deliverable: the gap was a stall (broker GC,
//     depaging, a slow network), so it drains again.
//   - messages remain but the broker is holding them back (scheduled, in
//     flight to another consumer, paused queue): no amount of waiting will
//     produce them, so it returns *PartialDrainError with the counters.
//
// The earlier behaviour — treat any idle gap as "queue drained" and return nil
// — silently under-drained: a queue reporting 210K messages could export ~50K
// and still exit 0, because most of the depth was never deliverable to us.
func (c *Client) DrainQueue(ctx context.Context, queue string, sink RecordSink, idle time.Duration, batch int) (int, error) {
	total := 0
	fruitless := 0
	for {
		n, err := c.drainPass(ctx, queue, sink, idle, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n > 0 {
			fruitless = 0
		} else {
			fruitless++
		}

		stat, err := c.QueueStatByName(ctx, queue)
		if err != nil {
			if errors.Is(err, ErrQueueNotFound) {
				// Auto-delete removed the queue once we emptied it; nothing
				// is left behind, so the drain is complete.
				return total, nil
			}
			return total, fmt.Errorf("verify drain of %s: %w", queue, err)
		}
		switch drainOutcome(stat, fruitless) {
		case drainComplete:
			return total, nil
		case drainRetry:
			continue
		default:
			// Only now, on the failure path, pay for a scan: countMessages
			// walks the queue, so it is far more expensive than the counter
			// read above and is worth it exactly once, to say WHY.
			pde := &PartialDrainError{Queue: queue, Drained: total, Stat: stat}
			if n, cErr := c.CountMessages(ctx, queue); cErr == nil {
				pde.Scanned, pde.Counted = true, n
			}
			return total, pde
		}
	}
}


// drainOutcome decides what a pass means, given the broker's counters
// afterwards and how many consecutive passes have now come back empty
// (fruitless == 0 means the last pass took messages). It is the whole of the
// "is this drain actually finished?" judgement, kept pure so every case is
// testable without a broker.
func drainOutcome(stat QueueStat, fruitless int) outcome {
	if stat.MessageCount == 0 {
		return drainComplete
	}
	// Retrying is only worth it if the broker would actually hand the
	// remainder over -- otherwise we would spin forever on messages we cannot
	// have -- and only while passes are still producing something.
	if stat.DeliverableNow() > 0 && fruitless < maxFruitlessPasses {
		return drainRetry
	}
	return drainStuck
}

// drainPass consumes messages from queue until the broker goes quiet for idle,
// returning how many it took. A quiet broker ends the pass; it does NOT mean
// the queue is empty -- only DrainQueue's check against the broker's counters
// can establish that.
//
// Idle detection: each Receive uses a per-call context with timeout idle;
// a context.DeadlineExceeded from Receive ends the pass, but only when the
// outer ctx is still live. If the caller's own ctx has expired/been canceled,
// that deadline propagates to the child context too, so DeadlineExceeded alone
// can't distinguish "broker quiet" from "caller ran out of time"; we
// disambiguate against ctx.Err() below so a caller-timeout is reported as an
// error instead of a false "fully drained".
func (c *Client) drainPass(ctx context.Context, queue string, sink RecordSink, idle time.Duration, batch int) (int, error) {
	if batch <= 0 {
		// Credit: int32(batch) with batch<=0 grants zero link credit, which
		// silently receives nothing (or reports (0,nil) on a non-empty
		// queue) instead of failing loudly, so guard it here.
		batch = 1
	}
	recv, err := c.sess.NewReceiver(ctx, queue, &amqp.ReceiverOptions{Credit: int32(batch)})
	if err != nil {
		return 0, fmt.Errorf("open receiver for %s: %w", queue, err)
	}
	defer recv.Close(context.Background())

	total := 0
	pending := make([]*amqp.Message, 0, batch)

	// flush persists all currently-pending messages (Append already done
	// per-message as they arrive) via Sync, then acks each one. Only after
	// Sync succeeds do we ack, preserving persist-before-ack.
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := sink.Sync(); err != nil {
			return fmt.Errorf("sync store: %w", err)
		}
		for _, m := range pending {
			if err := recv.AcceptMessage(ctx, m); err != nil {
				return fmt.Errorf("ack message: %w", err)
			}
		}
		pending = pending[:0]
		return nil
	}

	for {
		rctx, cancel := context.WithTimeout(ctx, idle)
		msg, err := recv.Receive(rctx, nil)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if ctx.Err() != nil {
					// The outer/caller context is itself done (its own
					// deadline expired, or explicit cancel() was called) —
					// its DeadlineExceeded/Canceled propagated into rctx,
					// so this is a caller-timeout, not an idle queue.
					// Returning (total, nil) here would be a false "drain
					// complete" while messages remain on the broker, so
					// surface it as an error instead.
					return total, ctx.Err()
				}
				break // broker quiet => end of pass (NOT proof of an empty queue)
			}
			// Messages already Appended-but-not-yet-Acked when this error
			// aborts the drain stay safely on the broker (no loss), but may
			// be re-Appended as duplicate records on the next drain attempt
			// — acceptable under the at-least-once invariant, since
			// redelivery is expected to be deduped.
			return total, fmt.Errorf("receive from %s: %w", queue, err)
		}

		raw, err := msg.MarshalBinary()
		if err != nil {
			return total, fmt.Errorf("marshal message: %w", err)
		}
		rec := store.Record{Queue: queue, DrainedAt: time.Now().UnixNano(), AMQP: raw}
		// Deterministic dedup id: derive the record UUID from a stable content
		// hash rather than a random uuid. On redeliver this UUID becomes the
		// broker's _AMQ_DUPL_ID, so the same broker message re-drained after a
		// crash (fsync'd to the store but never acked, then re-drained) yields
		// the SAME id and the broker collapses the pair to a single delivery.
		//
		// The hash is computed by store.DedupID over a normalized projection of
		// the message that EXCLUDES the transport fields Artemis mutates on
		// redelivery (Header.DeliveryCount, Header.FirstAcquirer, and the
		// delivery-annotations section) — NOT over rec.AMQP. rec.AMQP keeps the
		// full-fidelity bytes for a perfect replay; hashing them directly would
		// break dedup because a redelivered message carries a bumped
		// delivery-count that changes the marshaled bytes (empirically verified
		// on Artemis 2.31.2: delivery-count 0->1 on a failed/redelivered
		// delivery), yielding a different _AMQ_DUPL_ID and defeating the very
		// crash-dedup this WAL exists to provide.
		//
		// Tradeoff (accepted): two messages that are identical apart from those
		// excluded transport fields hash equal, so the broker drops one as a
		// duplicate.
		rec.UUID = store.DedupID(msg, queue)
		// Persist BEFORE ack: append the record now; fsync+ack happens at
		// batch flush, so the message is durable on disk before the broker
		// is told it can be discarded.
		if err := sink.Append(rec); err != nil {
			return total, fmt.Errorf("append record: %w", err)
		}
		pending = append(pending, msg)
		total++

		if len(pending) >= batch {
			if err := flush(); err != nil {
				return total, err
			}
		}
	}
	if err := flush(); err != nil {
		return total, err
	}
	return total, nil
}

// DrainAll enumerates every queue via ListQueues and drains each one in
// turn, invoking onQueue (if non-nil) after each queue completes with the
// queue's name and the number of messages drained from it.
//
// A queue that cannot be fully drained does not abort the run: whatever the
// other queues hold is still worth exporting, so DrainAll records the
// *PartialDrainError, moves on, and returns every one of them joined together
// at the end. The messages it did take are already persisted, and the returned
// count reflects them, but the error means the export is NOT complete.
// Any other error (a broken connection, a failing sink) aborts immediately.
func (c *Client) DrainAll(ctx context.Context, sink RecordSink, idle time.Duration, batch int, onQueue func(name string, n int)) (int, error) {
	queues, err := c.ListQueues(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	var partial []error
	for _, q := range queues {
		n, err := c.DrainQueue(ctx, q.Name, sink, idle, batch)
		total += n
		if err != nil {
			var pde *PartialDrainError
			if !errors.As(err, &pde) {
				return total, err
			}
			partial = append(partial, err)
		}
		if onQueue != nil {
			onQueue(q.Name, n)
		}
	}
	return total, errors.Join(partial...)
}
