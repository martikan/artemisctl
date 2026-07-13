package cli

import (
	"context"
	"fmt"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Report resource usage and a redelivery-readiness verdict",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := connectCtx(cmd, context.Background())
			defer cancel()
			c, err := broker.Connect(ctx, connProps(cmd))
			if err != nil {
				return err
			}
			defer c.Close(ctx)
			h, err := c.CheckHealth(ctx)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "disk:     %.1f%%\n", h.DiskUsagePct)
			fmt.Fprintf(out, "memory:   %.1f%%\n", h.MemoryUsagePct)
			fmt.Fprintf(out, "blocking: %v\n", h.Blocking)
			fmt.Fprintf(out, "verdict:  %s\n", h.Verdict)
			switch h.Verdict {
			case broker.Critical:
				cmd.SilenceErrors = true
				return fmt.Errorf("broker CRITICAL")
			case broker.Degraded:
				// exit 0 but visible; keep simple
			}
			return nil
		},
	}
}
