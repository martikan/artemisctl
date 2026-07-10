// The produce command injects messages onto a queue for testing and load
// generation. Two sources feed it, both of which resolve to a slice of
// *amqp.Message before anything is sent:
//
//   - GenerateMessages builds synthetic messages of a fixed body size.
//   - ParseArtemisMessages reads a JSON array in the Artemis web-console
//     layout (the same shape listMessagesAsJSON emits, with typed property
//     buckets like StringProperties/IntProperties), so a message copied out
//     of the console can be replayed verbatim.
//
// Produce then sends the slice to a single queue with an optional
// messages-per-second throttle, honoring context cancellation (SIGINT) so a
// long run stops cleanly between messages.

package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/google/uuid"
)

// defaultPriority is the AMQP/Artemis default message priority (0-9) used when
// a message does not specify one.
const defaultPriority uint8 = 4

// bodyAlphabet fills the random tail of a generated message body.
const bodyAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateMessages builds count synthetic durable messages, each with a body
// of exactly size bytes: a "msg-<n>-" prefix followed by random alphanumeric
// padding (truncated to size if the prefix alone already exceeds it). Each
// message is stamped with a unique AMQP message-id ("gen-<n>-<uuid>") so browse
// reports it in the ID column and BrowseMessage can drill into it -- Artemis
// does not synthesize an AMQP message-id for a send that omits one. The
// supplied application properties are copied onto every message.
func GenerateMessages(count, size int, props map[string]interface{}) []*amqp.Message {
	if count < 0 {
		count = 0
	}
	if size < 0 {
		size = 0
	}
	msgs := make([]*amqp.Message, 0, count)
	for i := 0; i < count; i++ {
		body := make([]byte, size)
		n := copy(body, fmt.Sprintf("msg-%d-", i))
		for j := n; j < size; j++ {
			body[j] = bodyAlphabet[rand.Intn(len(bodyAlphabet))]
		}
		msg := amqp.NewMessage(body)
		msg.Header = &amqp.MessageHeader{Durable: true, Priority: defaultPriority}
		msg.Properties = &amqp.MessageProperties{MessageID: fmt.Sprintf("gen-%d-%s", i, uuid.NewString())}
		if len(props) > 0 {
			msg.ApplicationProperties = copyProps(props)
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// artemisMessage mirrors the fields of the Artemis web-console / management
// listMessagesAsJSON message layout that are meaningful when re-producing a
// message. messageID/timestamp/userID are intentionally omitted: the broker
// assigns those on send, so echoing them back would be meaningless. The typed
// property buckets each map a property name to a value of the corresponding
// AMQP type.
type artemisMessage struct {
	Durable    *bool  `json:"durable"`
	Priority   *uint8 `json:"priority"`
	Expiration int64  `json:"expiration"`
	Text       string `json:"text"`

	StringProperties  map[string]string  `json:"StringProperties"`
	BooleanProperties map[string]bool    `json:"BooleanProperties"`
	ByteProperties    map[string]int8    `json:"ByteProperties"`
	ShortProperties   map[string]int16   `json:"ShortProperties"`
	IntProperties     map[string]int32   `json:"IntProperties"`
	LongProperties    map[string]int64   `json:"LongProperties"`
	FloatProperties   map[string]float32 `json:"FloatProperties"`
	DoubleProperties  map[string]float64 `json:"DoubleProperties"`
}

// ParseArtemisMessages decodes a JSON array of Artemis-console-shaped messages
// into amqp.Messages ready to send. The message body is taken from the "text"
// field; "durable" defaults to true when absent, "priority" to 4; a non-zero
// "expiration" (unix millis) becomes the AMQP absolute expiry time. Every typed
// property bucket is flattened into the message's application properties, and
// the supplied extra properties are merged on top (overriding any file property
// of the same name). The "address"/"type" fields are ignored — the caller's
// target queue is authoritative and the body is always sent as a data section.
func ParseArtemisMessages(data []byte, extra map[string]interface{}) ([]*amqp.Message, error) {
	var raw []artemisMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse message file: %w", err)
	}
	msgs := make([]*amqp.Message, 0, len(raw))
	for _, am := range raw {
		msg := amqp.NewMessage([]byte(am.Text))

		durable := true
		if am.Durable != nil {
			durable = *am.Durable
		}
		priority := defaultPriority
		if am.Priority != nil {
			priority = *am.Priority
		}
		msg.Header = &amqp.MessageHeader{Durable: durable, Priority: priority}

		if am.Expiration > 0 {
			expiry := time.UnixMilli(am.Expiration)
			msg.Properties = &amqp.MessageProperties{AbsoluteExpiryTime: &expiry}
		}

		props := map[string]interface{}{}
		for k, v := range am.StringProperties {
			props[k] = v
		}
		for k, v := range am.BooleanProperties {
			props[k] = v
		}
		for k, v := range am.ByteProperties {
			props[k] = v
		}
		for k, v := range am.ShortProperties {
			props[k] = v
		}
		for k, v := range am.IntProperties {
			props[k] = v
		}
		for k, v := range am.LongProperties {
			props[k] = v
		}
		for k, v := range am.FloatProperties {
			props[k] = v
		}
		for k, v := range am.DoubleProperties {
			props[k] = v
		}
		for k, v := range extra {
			props[k] = v
		}
		if len(props) > 0 {
			msg.ApplicationProperties = props
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// copyProps returns a shallow copy of src so callers that mutate one message's
// properties do not affect others sharing the same source map.
func copyProps(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// Produce sends msgs to queue, optionally throttled to ratePerSec messages
// per second in total (ratePerSec <= 0 sends as fast as possible). workers is
// the number of parallel senders: 1 (or less) sends sequentially over a
// single sender, preserving order; more than 1 opens one sender per worker,
// each on its own AMQP session, so settlement round-trips overlap — this
// raises throughput but does NOT preserve delivery order. It stops early and
// returns the count sent so far, together with ctx.Err(), if the context is
// canceled (e.g. SIGINT). onProgress, when non-nil, is called after each
// successful send with the running total; with workers > 1 it may be called
// from multiple goroutines concurrently.
func (c *Client) Produce(ctx context.Context, queue string, msgs []*amqp.Message, ratePerSec, workers int, onProgress func(sent int)) (int, error) {
	if workers <= 1 {
		return c.produceSequential(ctx, queue, msgs, ratePerSec, onProgress)
	}
	return c.produceParallel(ctx, queue, msgs, ratePerSec, workers, onProgress)
}

// produceSequential is the ordered single-sender path.
func (c *Client) produceSequential(ctx context.Context, queue string, msgs []*amqp.Message, ratePerSec int, onProgress func(sent int)) (int, error) {
	// TargetCapabilities: []string{"queue"} routes as an anycast queue rather
	// than the default multicast address; without it messages never land in
	// the queue (see redeliver.go / sendTestMessages for the same requirement).
	sender, err := c.sess.NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
	if err != nil {
		return 0, fmt.Errorf("open sender for %s: %w", queue, err)
	}
	defer sender.Close(context.Background())

	var interval time.Duration
	if ratePerSec > 0 {
		interval = time.Second / time.Duration(ratePerSec)
	}

	sent := 0
	for i, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		if interval > 0 && i > 0 {
			select {
			case <-ctx.Done():
				return sent, ctx.Err()
			case <-time.After(interval):
			}
		}
		if err := sender.Send(ctx, msg, nil); err != nil {
			return sent, fmt.Errorf("send to %s: %w", queue, err)
		}
		sent++
		if onProgress != nil {
			onProgress(sent)
		}
	}
	return sent, nil
}

// produceParallel fans msgs out to workers senders, each on its own session
// so the per-link transfer serialization in go-amqp doesn't gate throughput —
// each worker's settlement wait overlaps the others'. A single shared ticker
// keeps ratePerSec global across workers. The first send error (or ctx
// cancellation) cancels the remaining workers; the returned count is the
// number of messages actually settled.
func (c *Client) produceParallel(ctx context.Context, queue string, msgs []*amqp.Message, ratePerSec, workers int, onProgress func(sent int)) (int, error) {
	if workers > len(msgs) {
		workers = len(msgs)
	}

	senders := make([]*amqp.Sender, 0, workers)
	defer func() {
		for _, s := range senders {
			_ = s.Close(context.Background())
		}
	}()
	for i := 0; i < workers; i++ {
		sess, err := c.conn.NewSession(ctx, nil)
		if err != nil {
			return 0, fmt.Errorf("open session %d: %w", i, err)
		}
		// Anycast routing, same requirement as the sequential path.
		s, err := sess.NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
		if err != nil {
			return 0, fmt.Errorf("open sender %d for %s: %w", i, queue, err)
		}
		senders = append(senders, s)
	}

	// Global throttle: one ticker shared by all workers, so ratePerSec is the
	// total rate, not per-worker. Receiving from the shared channel naturally
	// distributes send slots across workers.
	var tick <-chan time.Time
	if ratePerSec > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(ratePerSec))
		defer ticker.Stop()
		tick = ticker.C
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	work := make(chan *amqp.Message)
	go func() {
		defer close(work)
		for _, m := range msgs {
			select {
			case work <- m:
			case <-ctx.Done():
				return
			}
		}
	}()

	var (
		sent     atomic.Int64
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	fail := func(err error) {
		errOnce.Do(func() { firstErr = err })
		cancel()
	}

	for _, sender := range senders {
		wg.Add(1)
		go func(sender *amqp.Sender) {
			defer wg.Done()
			for msg := range work {
				if tick != nil {
					select {
					case <-tick:
					case <-ctx.Done():
						fail(ctx.Err())
						return
					}
				}
				if err := sender.Send(ctx, msg, nil); err != nil {
					if ctxErr := ctx.Err(); ctxErr != nil {
						fail(ctxErr)
					} else {
						fail(fmt.Errorf("send to %s: %w", queue, err))
					}
					return
				}
				n := sent.Add(1)
				if onProgress != nil {
					onProgress(int(n))
				}
			}
		}(sender)
	}
	wg.Wait()

	if firstErr == nil {
		// Workers exited via a closed work channel, but the feeder also closes
		// it on cancellation — surface the cancellation if that's what happened.
		firstErr = ctx.Err()
	}
	return int(sent.Load()), firstErr
}
