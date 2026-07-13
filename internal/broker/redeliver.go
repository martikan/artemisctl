package broker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/store"
)

// RedeliverOpts controls how Redeliver replays a store file back to the
// broker. QueueOverride, when set, redirects every record to that queue
// instead of its originally-drained queue.
type RedeliverOpts struct {
	QueueOverride string
}

// Redeliver replays the records in the store file at storePath back onto
// the broker, resuming from the last saved checkpoint. Each record is sent
// to its original queue (or opts.QueueOverride, if set) with its record
// UUID set as the AMQP application property "_AMQ_DUPL_ID" so Artemis's
// duplicate-detection can drop repeats if the same record is redelivered
// more than once (e.g. after a lost/rolled-back checkpoint). The checkpoint
// is only advanced after a send succeeds, so a crash mid-replay leaves the
// checkpoint pointing at the last durably-redelivered record, not beyond
// it.
func (c *Client) Redeliver(ctx context.Context, storePath string, opts RedeliverOpts, onProgress func(sent int)) (sent, coreSkipped int, err error) {
	startOffset, err := store.LoadCheckpoint(storePath)
	if err != nil {
		return 0, 0, fmt.Errorf("load checkpoint: %w", err)
	}
	r, err := store.OpenReader(storePath)
	if err != nil {
		return 0, 0, err
	}
	defer r.Close()
	if err := r.SeekTo(startOffset); err != nil {
		return 0, 0, err
	}

	senders := map[string]*amqp.Sender{}
	defer func() {
		for _, s := range senders {
			_ = s.Close(context.Background())
		}
	}()
	senderFor := func(queue string) (*amqp.Sender, error) {
		if s, ok := senders[queue]; ok {
			return s, nil
		}
		// TargetCapabilities: []string{"queue"} tells Artemis to route this
		// as an anycast queue rather than the default multicast address;
		// without it the message is routed multicast and never lands in
		// the anycast queue, so redelivery would silently fail to restore
		// messages.
		s, err := c.sess.NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
		if err != nil {
			return nil, fmt.Errorf("sender for %s: %w", queue, err)
		}
		senders[queue] = s
		return s, nil
	}

	for {
		// Graceful cancellation (e.g. SIGINT): stop between records and
		// return a clean context error. The last successful send has already
		// advanced the durable checkpoint, so re-running resumes exactly.
		if cerr := ctx.Err(); cerr != nil {
			return sent, coreSkipped, cerr
		}
		rec, afterOffset, nerr := r.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			return sent, coreSkipped, nerr // ErrCorrupt: stop, good prefix already delivered
		}

		var msg amqp.Message
		switch rec.Kind {
		case store.KindCore:
			// Core records have no AMQP wire form: convert on send. A record
			// that fails to convert is skipped (with the checkpoint advanced so
			// it isn't retried forever) rather than aborting the whole replay --
			// the faithful Core record stays in the store for a later attempt.
			converted, cerr := coreToAMQP(rec.CorePayload)
			if cerr != nil {
				coreSkipped++
				if serr := store.SaveCheckpoint(storePath, afterOffset); serr != nil {
					return sent, coreSkipped, fmt.Errorf("save checkpoint: %w", serr)
				}
				continue
			}
			msg = *converted
		default:
			if uerr := msg.UnmarshalBinary(rec.AMQP); uerr != nil {
				return sent, coreSkipped, fmt.Errorf("unmarshal record: %w", uerr)
			}
		}
		if msg.ApplicationProperties == nil {
			msg.ApplicationProperties = map[string]interface{}{}
		}
		msg.ApplicationProperties["_AMQ_DUPL_ID"] = hex.EncodeToString(rec.UUID[:])

		queue := rec.Queue
		if opts.QueueOverride != "" {
			queue = opts.QueueOverride
		}
		sender, serr := senderFor(queue)
		if serr != nil {
			return sent, coreSkipped, serr
		}
		if serr := sender.Send(ctx, &msg, nil); serr != nil {
			return sent, coreSkipped, fmt.Errorf("send to %s: %w", queue, serr)
		}
		if serr := store.SaveCheckpoint(storePath, afterOffset); serr != nil {
			return sent, coreSkipped, fmt.Errorf("save checkpoint: %w", serr)
		}
		sent++
		if onProgress != nil {
			onProgress(sent)
		}
	}
	return sent, coreSkipped, nil
}
