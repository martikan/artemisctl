package cli

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "List queues and message counts (descending)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := connectCtx(cmd, context.Background())
			defer cancel()
			c, err := broker.Connect(ctx, connProps(cmd))
			if err != nil {
				return err
			}
			defer c.Close(ctx)
			stats, err := c.ListQueues(ctx)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "QUEUE NAME\tMESSAGE COUNT")
			for _, s := range stats {
				fmt.Fprintf(w, "%s\t%d\n", s.Name, s.MessageCount)
			}
			return w.Flush()
		},
	}
}
