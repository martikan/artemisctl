// Browse mechanism confirmed in Task 10 Step 1 (spike against
// apache/activemq-artemis:2.31.2 in a throwaway internal/broker/browse_spike_test.go,
// run and deleted after verification):
//
//   - Brief's Mechanism A candidate, `queue.<name> browse []`: DOES succeed, but its
//     CompositeData[] reply comes back as raw Java Object Serialization (base64,
//     starting with the "rO0AB..." stream magic) wrapped in a JSON string -- not
//     plain JSON fields. Parsing it would require a Java ObjectInputStream-
//     compatible decoder in Go. Rejected as impractical/fragile.
//     `address.<name> browse []` doesn't exist at all: broker replies
//     `AMQ229069: no operation browse/0`.
//   - Brief's Mechanism B candidate, a receiver with
//     ReceiverOptions{SourceCapabilities: []string{"COPY"}}: attaches fine, but
//     Artemis does not honor "COPY" as browse-only -- explicitly ACCEPTing all
//     messages received over such a receiver still removed them from the queue
//     (count went 3 -> 0). go-amqp v1.5.1 also doesn't expose AMQP's real
//     `distribution-mode` field on the public ReceiverOptions (it exists only on
//     an unexported internal frames.Source), so a spec-correct COPY link can't be
//     requested through the public API at all. Rejected: not actually non-destructive.
//
// What actually works, confirmed live:
//   - A PLAIN AMQP receiver (no special source capabilities) that calls Receive
//     and then explicitly ReleaseMessage (instead of AcceptMessage) puts the
//     message straight back on the queue: repeated probes confirmed the queue's
//     message count is unchanged after receiving+releasing every message on it.
//     The receiver delivers messages in the queue's real browse order and each
//     *amqp.Message carries everything the summary needs (AMQP message-id,
//     creation time, body), so this receive-then-release peek is the single
//     source of truth for BrowseQueue/BrowseMessage.
//
// Why NOT zip against `queue.<name> listMessagesAsJSON [""]` (an earlier design):
//   - listMessagesAsJSON returns clean JSON metadata (double-encoded like
//     ListQueues' reply) and never spends link credit, but two live assumptions
//     it was trusted for are FALSE on a real broker:
//       1. Its array order is NOT ascending-messageID / arrival order. On a real
//          ExpiryQueue it came back non-monotonic (e.g. [610,609,604,606,605,602]),
//          and it does NOT match the order a plain receiver delivers in.
//       2. Peeking (receive+release) reorders the order listMessagesAsJSON then
//          reports -- the same messages come back in a new, stable order. So a
//          re-read-and-compare guard can never confirm a stable prefix for n>1.
//     Because the broker's internal messageID is not surfaced on the received
//     AMQP message (msg.Properties.MessageID is the AMQP message-id, delivery
//     annotations are empty), there is no key to join listMessagesAsJSON metadata
//     to a received body except position -- and position is exactly what (1)/(2)
//     make unsound. So we do not zip: listMessagesAsJSON is used ONLY for a cheap
//     message count (how many to peek), never for per-message identity or order.
//
// Implementation: BrowseQueue counts the queue via listMessagesAsJSON, opens a
// plain receiver bounded to just enough credit to cover [0, offset+limit),
// receiving-then-releasing each message in order, and builds one BrowsedMessage
// per peeked message (AMQP message-id, creation time, body size/preview). Every
// peeked message -- including ones before offset -- is released, so nothing is
// ever consumed. BrowseMessage peeks the same way and returns the raw
// *amqp.Message whose AMQP message-id matches the requested id.
//
// Known caveat -- browsing twice in quick succession: releasing a message
// increments its delivery count and Artemis reschedules it under a brief
// redelivery-delay. While that delay is in effect the messages are invisible
// BOTH to a fresh receiver AND to listMessagesAsJSON (getMessageCount still
// counts them, but listMessagesAsJSON does not), so a browse issued within ~1-2s
// of a previous one can report the queue as momentarily empty. It refills on its
// own once the delay elapses. This is inherent to any receive-then-release peek
// (the only non-destructive body-reading mechanism go-amqp exposes) and is not a
// data loss -- nothing is ever consumed.

package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Azure/go-amqp"
)

// previewMaxLen bounds how much of a message body is copied into
// BrowsedMessage.Preview for the summary table.
const previewMaxLen = 80

// peekTimeout bounds each individual Receive call in peekBodies; a queue
// that has fewer messages than requested must not hang forever.
const peekTimeout = 3 * time.Second

// BrowsedMessage is a one-line summary of a queued message returned by
// BrowseQueue: its AMQP message-id, body size in bytes, enqueue timestamp
// (unix millis, from the message's creation-time), and a short printable
// preview of the body.
type BrowsedMessage struct {
	ID        string
	Size      int
	Timestamp int64
	Preview   string
}

// queueMessageMeta mirrors one entry of Artemis's listMessagesAsJSON reply.
// Only the count of entries is used (see queueCount); the per-message fields
// are deliberately NOT trusted for identity or order (see the package comment).
type queueMessageMeta struct {
	MessageID int64 `json:"messageID"`
}

// queueCount returns how many messages queue currently holds, via the
// queue.<name>.listMessagesAsJSON management operation. Its reply is
// double-encoded (an outer []string of length 1 wrapping the real JSON array),
// the same shape parseQueueStatsReply handles for listQueues. Only the length
// of the array is used -- to bound how many messages BrowseQueue/BrowseMessage
// need to peek -- never its order or contents.
func (c *Client) queueCount(ctx context.Context, queue string) (int, error) {
	reply, err := c.callManagement(ctx, "queue."+queue, "listMessagesAsJSON", `[""]`)
	if err != nil {
		return 0, err
	}
	s, ok := reply.Value.(string)
	if !ok {
		return 0, fmt.Errorf("unexpected listMessagesAsJSON reply type %T", reply.Value)
	}
	var outer []string
	if err := json.Unmarshal([]byte(s), &outer); err != nil {
		return 0, fmt.Errorf("parse outer array: %w", err)
	}
	if len(outer) == 0 {
		return 0, nil
	}
	var meta []queueMessageMeta
	if err := json.Unmarshal([]byte(outer[0]), &meta); err != nil {
		return 0, fmt.Errorf("parse message metadata: %w", err)
	}
	return len(meta), nil
}

// amqpMessageID renders a received message's AMQP message-id as a string. This
// is the stable per-message identity BrowseQueue reports and BrowseMessage
// matches on (the broker's internal messageID is not surfaced over AMQP). It
// returns "" when the message carries no message-id.
func amqpMessageID(m *amqp.Message) string {
	if m.Properties == nil || m.Properties.MessageID == nil {
		return ""
	}
	if s, ok := m.Properties.MessageID.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", m.Properties.MessageID)
}

// browsedFromMessage builds the one-line summary for a single peeked message.
func browsedFromMessage(m *amqp.Message) BrowsedMessage {
	data := m.GetData()
	preview := string(data)
	if len(preview) > previewMaxLen {
		preview = preview[:previewMaxLen]
	}
	var ts int64
	if m.Properties != nil && m.Properties.CreationTime != nil {
		ts = m.Properties.CreationTime.UnixMilli()
	}
	return BrowsedMessage{
		ID:        amqpMessageID(m),
		Size:      len(data),
		Timestamp: ts,
		Preview:   preview,
	}
}

// peekBodies receives and immediately releases up to n messages from queue,
// in browse order, returning each raw message it saw. Every message is
// released (never accepted), so nothing is removed from the queue. If the
// queue holds fewer than n messages, a per-Receive timeout is treated as
// "nothing left to peek" and the shorter slice is returned without error.
func (c *Client) peekBodies(ctx context.Context, queue string, n int) ([]*amqp.Message, error) {
	if n <= 0 {
		return nil, nil
	}
	// Credit: -1 puts the receiver in manual-credit mode. With the default
	// auto-credit mode, go-amqp automatically re-issues credit as messages
	// are settled, so releasing a message here would prompt the broker to
	// immediately redeliver it (or a later one) back to this same receiver
	// -- beyond the n messages we asked for, and sometimes racing our
	// deferred Close badly enough to abort the whole connection (observed
	// live: an unsolicited redelivery arriving just as we detach). Issuing
	// exactly n credits up front and never renewing them keeps this
	// receiver bounded to precisely the n messages we intend to peek.
	recv, err := c.sess.NewReceiver(ctx, queue, &amqp.ReceiverOptions{Credit: -1})
	if err != nil {
		return nil, fmt.Errorf("open peek receiver for %s: %w", queue, err)
	}
	defer recv.Close(context.Background())
	if err := recv.IssueCredit(uint32(n)); err != nil {
		return nil, fmt.Errorf("issue peek credit for %s: %w", queue, err)
	}

	msgs := make([]*amqp.Message, 0, n)
	for i := 0; i < n; i++ {
		rctx, cancel := context.WithTimeout(ctx, peekTimeout)
		msg, err := recv.Receive(rctx, nil)
		cancel()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				break // queue has fewer messages than requested
			}
			return msgs, fmt.Errorf("peek receive from %s: %w", queue, err)
		}
		if err := recv.ReleaseMessage(ctx, msg); err != nil {
			return msgs, fmt.Errorf("release peeked message: %w", err)
		}
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

// BrowseQueue non-destructively lists up to limit messages starting at
// offset, in queue browse order. limit<=0 means "no limit" (return everything
// from offset onward).
func (c *Client) BrowseQueue(ctx context.Context, queue string, limit, offset int) ([]BrowsedMessage, error) {
	if offset < 0 {
		offset = 0
	}
	count, err := c.queueCount(ctx, queue)
	if err != nil {
		return nil, err
	}
	if offset >= count {
		return nil, nil
	}
	end := count
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}

	// Peek from the head of the queue up to end; AMQP gives no way to start
	// receiving mid-queue, so we must walk (and release) everything before
	// offset too. Each returned message is its own source of truth -- no zip
	// against listMessagesAsJSON -- so order and identity are always consistent.
	bodies, err := c.peekBodies(ctx, queue, end)
	if err != nil {
		return nil, err
	}

	out := make([]BrowsedMessage, 0, end-offset)
	for i := offset; i < len(bodies); i++ {
		out = append(out, browsedFromMessage(bodies[i]))
	}
	return out, nil
}

// BrowseMessage non-destructively fetches the full raw message whose AMQP
// message-id (as reported in BrowsedMessage.ID) matches id on queue.
func (c *Client) BrowseMessage(ctx context.Context, queue, id string) (*amqp.Message, error) {
	count, err := c.queueCount(ctx, queue)
	if err != nil {
		return nil, err
	}
	bodies, err := c.peekBodies(ctx, queue, count)
	if err != nil {
		return nil, err
	}
	for _, m := range bodies {
		if amqpMessageID(m) == id {
			return m, nil
		}
	}
	return nil, fmt.Errorf("message %s not found in %s", id, queue)
}
