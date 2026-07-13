package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newRedeliverCmd() *cobra.Command {
	var in, queueOverride string
	var force bool

	cmd := &cobra.Command{
		Use:   "redeliver",
		Short: "Replay a store file back to the broker (health-gated, resumable, dedup)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Bound Connect + the pre-flight health check by --timeout so an
			// unresponsive broker fails fast.
			connCtx, connCancel := connectCtx(cmd, context.Background())
			defer connCancel()
			c, err := broker.Connect(connCtx, connProps(cmd))
			if err != nil {
				return err
			}
			defer c.Close(context.Background())

			h, err := c.CheckHealth(connCtx)
			if err != nil {
				return err
			}
			if h.Verdict == broker.Critical && !force {
				return fmt.Errorf("broker health CRITICAL (disk %.1f%%, mem %.1f%%); refuse to redeliver (use --force)",
					h.DiskUsagePct, h.MemoryUsagePct)
			}

			// The replay loop runs under a separate long-lived context that is
			// NOT capped by --timeout (a large replay may exceed it) but IS
			// cancelable via SIGINT. Per-record checkpointing means a clean
			// cancellation leaves an exact resume point.
			ctx, stop := signalCtx()
			defer stop()

			n, coreSkipped, err := c.Redeliver(ctx, in, broker.RedeliverOpts{QueueOverride: queueOverride},
				func(sent int) {
					if sent%100 == 0 {
						fmt.Fprintf(cmd.OutOrStdout(), "redelivered %d...\n", sent)
					}
				})
			if err != nil {
				if errors.Is(err, context.Canceled) {
					fmt.Fprintf(cmd.OutOrStdout(), "interrupted after %d messages, progress saved (resume with the same command)\n", n)
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "redelivered %d messages before stopping: %v\n", n, err)
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "redelivered %d messages from %s\n", n, in)
			if coreSkipped > 0 {
				fmt.Fprintf(cmd.OutOrStdout(), "warning: %d Core-protocol record(s) could not be converted to AMQP and were skipped (left in the store)\n", coreSkipped)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&in, "in", "", "input store file (required)")
	cmd.Flags().StringVar(&queueOverride, "queue", "", "redirect all messages to this queue")
	cmd.Flags().BoolVar(&force, "force", false, "redeliver even if broker health is CRITICAL")
	_ = cmd.MarkFlagRequired("in")
	return cmd
}
