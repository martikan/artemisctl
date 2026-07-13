// Package broker is artemisctl's AMQP 1.0 client for Apache ActiveMQ Artemis.
// It wraps a single broker connection and exposes the operations the CLI is
// built from: querying queues and health via the activemq.management address
// (management.go, health.go), non-destructively browsing a queue (browse.go),
// draining every queue into a local store and replaying it (drain.go,
// redeliver.go), and producing messages for testing or load (produce.go).
//
// Every operation is a method on *Client, which owns one *amqp.Conn and a
// default *amqp.Session. Operations that need parallelism or isolation open
// their own additional sessions on that same connection (see produce.go).
package broker

import (
	"context"
	"fmt"

	"github.com/Azure/go-amqp"
)

// ConnectionProps holds the coordinates needed to reach a broker. Username and
// Password may be empty for an anonymous connection.
type ConnectionProps struct {
	URL      string
	Username string
	Password string
}

// Client is a live connection to one broker. It carries a single AMQP
// connection and a default session shared by the short read-only operations;
// callers that need concurrency open extra sessions on conn themselves. A
// Client is created with Connect and must be released with Close.
type Client struct {
	conn *amqp.Conn
	sess *amqp.Session
}

// Connect dials the broker described by p and opens the default session,
// bounded by ctx. On any failure it cleans up a half-open connection and
// returns a wrapped error. The returned Client must be closed with Close.
func Connect(ctx context.Context, p ConnectionProps) (*Client, error) {
	conn, err := amqp.Dial(ctx, fmt.Sprintf("amqp://%s", buildConnectionURL(p)), nil)
	if err != nil {
		return nil, fmt.Errorf("dial broker: %w", err)
	}
	sess, err := conn.NewSession(ctx, nil)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open session: %w", err)
	}
	return &Client{conn: conn, sess: sess}, nil
}

// Session exposes the Client's default AMQP session so callers in this package
// can open their own links on it.
func (c *Client) Session() *amqp.Session { return c.sess }

// Close tears down the default session and the underlying connection. It is
// safe to call on a partially-initialized Client and returns the connection
// close error, if any. The ctx bounds only the session close.
func (c *Client) Close(ctx context.Context) error {
	if c.sess != nil {
		_ = c.sess.Close(ctx)
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func buildConnectionURL(p ConnectionProps) string {
	if p.Username != "" && p.Password != "" {
		return fmt.Sprintf("%s:%s@%s", p.Username, p.Password, p.URL)
	}
	return p.URL
}
