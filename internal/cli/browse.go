package cli

import (
	"context"
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newBrowseCmd() *cobra.Command {
	var queue, message string
	var limit, offset int

	cmd := &cobra.Command{
		Use:   "browse",
		Short: "Non-destructively peek at messages in a queue",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := connectCtx(cmd, context.Background())
			defer cancel()
			c, err := broker.Connect(ctx, connProps(cmd))
			if err != nil {
				return err
			}
			defer c.Close(ctx)

			if message != "" {
				m, err := c.BrowseMessage(ctx, queue, message)
				if err != nil {
					return err
				}
				out := cmd.OutOrStdout()
				fmt.Fprintf(out, "Message %s on %s\n", message, queue)
				fmt.Fprintf(out, "Body: %s\n", string(m.GetData()))
				if len(m.ApplicationProperties) > 0 {
					fmt.Fprintln(out, "Properties:")
					keys := make([]string, 0, len(m.ApplicationProperties))
					for k := range m.ApplicationProperties {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					for _, k := range keys {
						fmt.Fprintf(out, "  %s = %v\n", k, m.ApplicationProperties[k])
					}
				}
				return nil
			}

			msgs, err := c.BrowseQueue(ctx, queue, limit, offset)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
			fmt.Fprintln(w, "ID\tSIZE\tTIMESTAMP\tPREVIEW")
			for _, m := range msgs {
				fmt.Fprintf(w, "%s\t%d\t%d\t%s\n", m.ID, m.Size, m.Timestamp, m.Preview)
			}
			return w.Flush()
		},
	}
	cmd.Flags().StringVar(&queue, "queue", "", "queue to browse (required)")
	cmd.Flags().StringVar(&message, "message", "", "show full body for a single message ID")
	cmd.Flags().IntVar(&limit, "limit", 20, "max messages to list")
	cmd.Flags().IntVar(&offset, "offset", 0, "skip the first N messages")
	_ = cmd.MarkFlagRequired("queue")
	return cmd
}
