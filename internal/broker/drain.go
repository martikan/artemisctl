package broker

import (
	"context"
	"errors"
	"fmt"
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

// DrainQueue consumes every message currently on queue, persisting each
// batch to sink (Append + Sync/fsync) before acking the broker, so a crash
// mid-drain never loses a message: it is either still on the broker
// (unacked) or durably on disk (or both, which is safe to re-drain/replay).
//
// Idle detection: each Receive uses a per-call context with timeout idle;
// a context.DeadlineExceeded from Receive is treated as "queue is empty",
// not an error, and ends the drain — but only when the outer ctx is still
// live. If the caller's own ctx has expired/been canceled, that deadline
// propagates to the child context too, so DeadlineExceeded alone can't
// distinguish "queue idle" from "caller ran out of time"; we disambiguate
// against ctx.Err() below so a caller-timeout is reported as an error
// instead of a false "fully drained".
func (c *Client) DrainQueue(ctx context.Context, queue string, sink RecordSink, idle time.Duration, batch int) (int, error) {
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
				break // queue idle => drained
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
func (c *Client) DrainAll(ctx context.Context, sink RecordSink, idle time.Duration, batch int, onQueue func(name string, n int)) (int, error) {
	queues, err := c.ListQueues(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, q := range queues {
		n, err := c.DrainQueue(ctx, q.Name, sink, idle, batch)
		if err != nil {
			return total, err
		}
		if onQueue != nil {
			onQueue(q.Name, n)
		}
		total += n
	}
	return total, nil
}
