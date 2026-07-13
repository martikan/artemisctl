package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Azure/go-amqp"
)

// QueueStat is one queue's name and current message depth, as reported by the
// broker's listQueues management operation. MessageCount arrives as a
// JSON string and is decoded into an int64.
type QueueStat struct {
	Name         string `json:"name"`
	MessageCount int64  `json:"messageCount,string"`
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

func parseQueueStatsReply(reply *amqp.Message) ([]QueueStat, error) {
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
	return filterInternalQueues(paged.Data), nil
}

func filterInternalQueues(in []QueueStat) []QueueStat {
	var out []QueueStat
	for _, q := range in {
		if strings.HasPrefix(q.Name, "activemq.") || strings.HasPrefix(q.Name, "$") || len(q.Name) == 36 {
			continue
		}
		out = append(out, q)
	}
	return out
}
