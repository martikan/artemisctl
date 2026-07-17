package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/go-amqp"
)

// QueueStat is one queue's depth and delivery state, as reported by the
// broker's listQueues management operation. Every counter arrives as a JSON
// string and is decoded into its Go type.
//
// MessageCount is the total depth, and it is NOT the number of messages a
// consumer can receive right now: it also counts messages the broker will not
// hand out yet. ScheduledCount (scheduled delivery / redelivery delay, not due
// yet) and DeliveringCount (already dispatched to some consumer, awaiting
// settlement) are both included in it, and a Paused queue dispatches nothing at
// all regardless of the counters. DeliverableNow is the subset a drain can
// actually expect to receive.
type QueueStat struct {
	Name            string `json:"name"`
	MessageCount    int64  `json:"messageCount,string"`
	ScheduledCount  int64  `json:"scheduledCount,string"`
	DeliveringCount int64  `json:"deliveringCount,string"`
	ConsumerCount   int64  `json:"consumerCount,string"`
	Paused          bool   `json:"paused,string"`

	// Temporary and Internal are the broker's own classification of the queue.
	// They are how ListQueues recognises queues that are not the user's data
	// (dynamic reply queues, internal bookkeeping) without having to guess from
	// the name.
	Temporary bool `json:"temporary,string"`
	Internal  bool `json:"internalQueue,string"`
}

// DeliverableNow reports how many of the queue's messages the broker would
// hand to a consumer right now: the depth minus the messages it is holding
// back. A paused queue delivers nothing, so it always reports 0.
//
// DeliveringCount only counts against the total while some consumer is
// attached to hold those messages. With no consumers, an in-flight count is
// just settlement lag -- typically our own just-closed receiver, whose unacked
// messages the broker is in the middle of returning to the queue -- and those
// messages ARE coming back, so they must not be mistaken for messages we can
// never have.
func (q QueueStat) DeliverableNow() int64 {
	if q.Paused {
		return 0
	}
	n := q.MessageCount - q.ScheduledCount
	if q.ConsumerCount > 0 {
		n -= q.DeliveringCount
	}
	if n < 0 {
		// The counters are sampled independently and can overlap in flight;
		// never report a negative backlog.
		return 0
	}
	return n
}

// callManagement performs a request/reply against activemq.management.
func (c *Client) callManagement(ctx context.Context, resource, operation, body string) (*amqp.Message, error) {
	recv, err := c.sess.NewReceiver(ctx, "", &amqp.ReceiverOptions{DynamicAddress: true})
	if err != nil {
		return nil, fmt.Errorf("create reply receiver: %w", err)
	}
	defer recv.Close(context.Background())
	replyTo := recv.Address()

	sender, err := c.sess.NewSender(ctx, "activemq.management", nil)
	if err != nil {
		return nil, fmt.Errorf("create management sender: %w", err)
	}
	defer sender.Close(context.Background())

	msg := &amqp.Message{
		Value:      body,
		Properties: &amqp.MessageProperties{ReplyTo: &replyTo},
		ApplicationProperties: map[string]interface{}{
			"_AMQ_ResourceName":  resource,
			"_AMQ_OperationName": operation,
		},
	}
	if err := sender.Send(ctx, msg, nil); err != nil {
		return nil, fmt.Errorf("send management request: %w", err)
	}
	reply, err := recv.Receive(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("receive management reply: %w", err)
	}
	_ = recv.AcceptMessage(ctx, reply)
	if ok, present := reply.ApplicationProperties["_AMQ_OperationSucceeded"].(bool); present && !ok {
		return nil, fmt.Errorf("broker rejected %s.%s: %v", resource, operation, reply.Value)
	}
	return reply, nil
}

// ListQueues returns the broker's user queues with their message counts,
// sorted by depth descending, with internal and temporary queues filtered out.
// It asks the broker for up to 1000 queues in a single management call.
func (c *Client) ListQueues(ctx context.Context) ([]QueueStat, error) {
	filter := `{"field":"","operation":"","value":"","sortField":"messageCount","sortOrder":"desc"}`
	body := fmt.Sprintf(`[%q, 1, 1000]`, filter)
	reply, err := c.callManagement(ctx, "broker", "listQueues", body)
	if err != nil {
		return nil, err
	}
	return parseQueueStatsReply(reply)
}

// ErrQueueNotFound is returned by QueueStatByName when the broker does not
// list the named queue, which normally means it has been auto-deleted.
var ErrQueueNotFound = fmt.Errorf("queue not found on broker")

// QueueStatByName returns the broker's current stats for a single queue. It
// asks the broker to filter by exact name rather than listing every queue, so
// it stays cheap enough to call repeatedly during a drain. Unlike ListQueues it
// does not filter internal queues: the caller already named the queue it wants.
func (c *Client) QueueStatByName(ctx context.Context, name string) (QueueStat, error) {
	filter, err := json.Marshal(map[string]string{
		"field": "name", "operation": "EQUALS", "value": name,
		"sortField": "messageCount", "sortOrder": "desc",
	})
	if err != nil {
		return QueueStat{}, fmt.Errorf("marshal queue filter: %w", err)
	}
	body, err := json.Marshal([]interface{}{string(filter), 1, 1})
	if err != nil {
		return QueueStat{}, fmt.Errorf("marshal listQueues args: %w", err)
	}
	reply, err := c.callManagement(ctx, "broker", "listQueues", string(body))
	if err != nil {
		return QueueStat{}, err
	}
	stats, err := parseQueueStatsRaw(reply)
	if err != nil {
		return QueueStat{}, err
	}
	for _, q := range stats {
		if q.Name == name {
			return q, nil
		}
	}
	return QueueStat{}, fmt.Errorf("%w: %s", ErrQueueNotFound, name)
}

// PurgeQueue removes every message from a queue, including messages a consumer
// cannot receive (scheduled, or held for redelivery). It returns the number of
// messages removed. This is destructive and does not persist anything: it is
// for resetting a queue, not for exporting it.
func (c *Client) PurgeQueue(ctx context.Context, name string) (int64, error) {
	reply, err := c.callManagement(ctx, "queue."+name, "removeAllMessages", "[]")
	if err != nil {
		return 0, fmt.Errorf("purge %s: %w", name, err)
	}
	return parseCountReply(reply), nil
}

// CountMessages returns how many messages a scan of the queue actually finds.
//
// This is NOT the same as QueueStat.MessageCount. MessageCount is a counter the
// broker maintains (for a paged queue, derived from page-counter journal
// records); countMessages walks the queue itself. When the two disagree, the
// counter has drifted and reports messages that do not exist -- a queue that
// advertises a large depth, refuses to deliver anything, and can never be
// drained. Comparing the two is the only way to tell that apart from a broker
// that genuinely holds messages but has stalled.
func (c *Client) CountMessages(ctx context.Context, name string) (int64, error) {
	reply, err := c.callManagement(ctx, "queue."+name, "countMessages", "[]")
	if err != nil {
		return 0, fmt.Errorf("count messages on %s: %w", name, err)
	}
	return parseCountReply(reply), nil
}

// parseCountReply decodes the count-shaped management reply the broker returns
// for countMessages and removeAllMessages: a JSON array holding a single number,
// encoded as a string ("[12]").
func parseCountReply(reply *amqp.Message) int64 {
	raw, ok := reply.Value.(string)
	if !ok {
		return 0
	}
	var outer []int64
	if err := json.Unmarshal([]byte(raw), &outer); err != nil || len(outer) == 0 {
		return 0
	}
	return outer[0]
}

func parseQueueStatsReply(reply *amqp.Message) ([]QueueStat, error) {
	stats, err := parseQueueStatsRaw(reply)
	if err != nil {
		return nil, err
	}
	return filterInternalQueues(stats), nil
}

// parseQueueStatsRaw decodes a listQueues reply without filtering internal
// queues, so a caller asking for one queue by name gets it back whatever it is
// called.
func parseQueueStatsRaw(reply *amqp.Message) ([]QueueStat, error) {
	strVal, ok := reply.Value.(string)
	if !ok {
		return nil, fmt.Errorf("unexpected management response format")
	}
	var outer []string
	if err := json.Unmarshal([]byte(strVal), &outer); err != nil {
		return nil, fmt.Errorf("parse outer array: %w", err)
	}
	if len(outer) == 0 {
		return nil, nil
	}
	var paged struct {
		Data []QueueStat `json:"data"`
	}
	if err := json.Unmarshal([]byte(outer[0]), &paged); err != nil {
		return nil, fmt.Errorf("parse paged response: %w", err)
	}
	return paged.Data, nil
}

// filterInternalQueues drops the queues an export has no business draining:
// the broker's own internal and temporary queues (including the dynamic reply
// queue callManagement opens for every management call).
//
// It trusts the broker's temporary/internalQueue flags rather than the shape of
// the name. An earlier version also skipped every 36-character name to catch
// UUID-named reply queues, which silently excluded any real user queue whose
// name happened to be 36 characters long -- an export that quietly skips a
// queue is exactly the kind of silent data loss this tool exists to prevent.
func filterInternalQueues(in []QueueStat) []QueueStat {
	var out []QueueStat
	for _, q := range in {
		if q.Temporary || q.Internal {
			continue
		}
		if strings.HasPrefix(q.Name, "activemq.") || strings.HasPrefix(q.Name, "$") {
			continue
		}
		out = append(out, q)
	}
	return out
}
