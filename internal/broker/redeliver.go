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
func (c *Client) Redeliver(ctx context.Context, storePath string, opts RedeliverOpts, onProgress func(sent int)) (int, error) {
	startOffset, err := store.LoadCheckpoint(storePath)
	if err != nil {
		return 0, fmt.Errorf("load checkpoint: %w", err)
	}
	r, err := store.OpenReader(storePath)
	if err != nil {
		return 0, err
	}
	defer r.Close()
	if err := r.SeekTo(startOffset); err != nil {
		return 0, err
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

	sent := 0
	for {
		// Graceful cancellation (e.g. SIGINT): stop between records and
		// return a clean context error. The last successful send has already
		// advanced the durable checkpoint, so re-running resumes exactly.
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		rec, afterOffset, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return sent, err // ErrCorrupt: stop, good prefix already delivered
		}

		var msg amqp.Message
		if err := msg.UnmarshalBinary(rec.AMQP); err != nil {
			return sent, fmt.Errorf("unmarshal record: %w", err)
		}
		if msg.ApplicationProperties == nil {
			msg.ApplicationProperties = map[string]interface{}{}
		}
		msg.ApplicationProperties["_AMQ_DUPL_ID"] = hex.EncodeToString(rec.UUID[:])

		queue := rec.Queue
		if opts.QueueOverride != "" {
			queue = opts.QueueOverride
		}
		sender, err := senderFor(queue)
		if err != nil {
			return sent, err
		}
		if err := sender.Send(ctx, &msg, nil); err != nil {
			return sent, fmt.Errorf("send to %s: %w", queue, err)
		}
		if err := store.SaveCheckpoint(storePath, afterOffset); err != nil {
			return sent, fmt.Errorf("save checkpoint: %w", err)
		}
		sent++
		if onProgress != nil {
			onProgress(sent)
		}
	}
	return sent, nil
}
