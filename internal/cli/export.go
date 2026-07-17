package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/martikan/artemisctl/internal/store"
	"github.com/spf13/cobra"
)

func newExportCmd() *cobra.Command {
	var out string
	var drainTimeout time.Duration
	var batch int

	cmd := &cobra.Command{
		Use:   "export",
		Short: "Drain every queue (destructive) into a local store file",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// Bound Connect by --timeout so an unresponsive broker fails fast.
			connCtx, connCancel := connectCtx(cmd, context.Background())
			defer connCancel()
			c, err := broker.Connect(connCtx, connProps(cmd))
			if err != nil {
				return err
			}
			// The drain itself runs under a separate long-lived context that
			// is NOT capped by --timeout (a large drain may exceed it) but IS
			// cancelable via SIGINT for a graceful stop.
			ctx, stop := signalCtx()
			defer stop()
			defer c.Close(context.Background())

			w, err := store.NewWriter(out)
			if err != nil {
				return err
			}
			defer w.Close()
			// This is a fresh store (NewWriter refuses a non-empty existing
			// file). Remove any stale sidecar checkpoint left by a prior
			// redeliver of a same-named store; otherwise a later `redeliver`
			// would seek to a bogus offset and silently skip records.
			if err := os.Remove(out + ".ckpt"); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove stale checkpoint: %w", err)
			}

			total, drainErr := c.DrainAll(ctx, w, drainTimeout, batch, func(q string, n int) {
				fmt.Fprintf(cmd.OutOrStdout(), "drained %d from %s\n", n, q)
			})
			// Whatever was drained is worth keeping even when the drain did not
			// complete, so fsync before reporting either way.
			syncErr := w.Sync()
			if drainErr == nil && syncErr != nil {
				return syncErr
			}
			if drainErr != nil {
				// SIGINT: drained records are already fsync'd; report a clean
				// interruption rather than a crash. Re-running export needs a
				// fresh --out path (an existing non-empty store is refused).
				if errors.Is(drainErr, context.Canceled) {
					fmt.Fprintf(cmd.OutOrStdout(), "interrupted after %d messages, progress saved to %s\n", total, out)
					return drainErr
				}
				var pde *broker.PartialDrainError
				if errors.As(drainErr, &pde) {
					// The store holds real, replayable messages -- it is just
					// not the whole queue. Say so on stdout so the count is not
					// mistaken for a complete export, and still fail: exporting
					// a subset while exiting 0 is how messages get lost.
					fmt.Fprintf(cmd.OutOrStdout(), "INCOMPLETE: saved %d messages to %s, but the broker still holds messages\n", total, out)
				}
				return drainErr
			}
			fmt.Fprintf(cmd.OutOrStdout(), "exported %d messages to %s\n", total, out)
			return nil
		},
	}
	cmd.Flags().StringVar(&out, "out", "", "output store file with .art extension (required)")
	cmd.Flags().DurationVar(&drainTimeout, "drain-timeout", 5*time.Second, "idle time before a queue is considered empty")
	cmd.Flags().IntVar(&batch, "batch", 100, "persist/ack batch size")
	_ = cmd.MarkFlagRequired("out")
	return cmd
}
