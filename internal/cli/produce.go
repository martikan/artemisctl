package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newProduceCmd() *cobra.Command {
	var queue, file string
	var count, size, rate, workers int
	var properties []string

	cmd := &cobra.Command{
		Use:   "produce",
		Short: "Send messages to a queue (generated test data or from a JSON file)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if workers < 1 {
				return fmt.Errorf("--workers must be >= 1, got %d", workers)
			}
			props, err := parseProperties(properties)
			if err != nil {
				return err
			}

			var msgs []*amqp.Message
			if file != "" {
				data, err := os.ReadFile(file)
				if err != nil {
					return fmt.Errorf("read message file: %w", err)
				}
				msgs, err = broker.ParseArtemisMessages(data, props)
				if err != nil {
					return err
				}
			} else {
				msgs = broker.GenerateMessages(count, size, props)
			}
			if len(msgs) == 0 {
				fmt.Fprintln(cmd.OutOrStdout(), "no messages to send")
				return nil
			}

			// Bound Connect by --timeout; the send loop runs under a separate
			// SIGINT-cancelable context so Ctrl-C stops cleanly between messages.
			connCtx, connCancel := connectCtx(cmd, context.Background())
			defer connCancel()
			c, err := broker.Connect(connCtx, connProps(cmd))
			if err != nil {
				return err
			}
			defer c.Close(context.Background())

			ctx, stop := signalCtx()
			defer stop()

			sent, err := c.Produce(ctx, queue, msgs, rate, workers, nil)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					fmt.Fprintf(cmd.OutOrStdout(), "interrupted after %d of %d messages\n", sent, len(msgs))
					return err
				}
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "produced %d messages to %s\n", sent, queue)
			return nil
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "target queue (required)")
	cmd.Flags().StringVar(&file, "file", "", "JSON file of Artemis-console-shaped messages to send (array)")
	cmd.Flags().IntVar(&count, "count", 1, "number of generated messages (ignored with --file)")
	cmd.Flags().IntVar(&size, "size", 256, "generated message body size in bytes (ignored with --file)")
	cmd.Flags().IntVar(&rate, "rate", 0, "max messages per second in total (0 = unlimited)")
	cmd.Flags().IntVar(&workers, "workers", 1, "parallel sender sessions (1 = ordered, sequential)")
	cmd.Flags().StringArrayVar(&properties, "property", nil, "application property k=v set on every message (repeatable)")
	_ = cmd.MarkFlagRequired("queue")
	return cmd
}

// parseProperties turns repeated "k=v" flag values into an application-property
// map. Values are always strings; the key is everything before the first "=".
func parseProperties(pairs []string) (map[string]interface{}, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	props := make(map[string]interface{}, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid --property %q, expected k=v", p)
		}
		props[k] = v
	}
	return props, nil
}
