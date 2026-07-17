package cli

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
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
			if verify, _ := cmd.Flags().GetBool("verify"); verify {
				return runStatusVerify(cmd, c)
			}
			stats, err := c.ListQueues(ctx)
			if err != nil {
				return err
			}
			w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
			// MESSAGE COUNT alone cannot explain why an export leaves messages
			// behind: the depth also counts messages the broker will not hand to
			// a consumer. DELIVERABLE is the subset a drain can actually take,
			// and the remaining columns say where the difference went.
			fmt.Fprintln(w, "QUEUE NAME\tMESSAGE COUNT\tDELIVERABLE\tSCHEDULED\tDELIVERING\tCONSUMERS\tPAUSED")
			for _, s := range stats {
				fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\t%d\t%t\n",
					s.Name, s.MessageCount, s.DeliverableNow(),
					s.ScheduledCount, s.DeliveringCount, s.ConsumerCount, s.Paused)
			}
			return w.Flush()
		},
	}
	cmd.Flags().Bool("verify", false,
		"scan every queue and report drift detection (slow: walks each queue)")
	return cmd
}

// runStatusVerify prints the drift check: the counter against a scan of each
// queue. It exits non-zero on confirmed drift, because a drifted counter means
// the broker is advertising messages that do not exist and an export of that
// queue can never complete -- a condition a script should be able to catch.
func runStatusVerify(cmd *cobra.Command, c *broker.Client) error {
	// The scan walks every queue, which on a deep backlog takes far longer than
	// the --timeout meant for connecting, so this runs under a signal context
	// like the other long operations rather than being cut off mid-sweep.
	ctx, stop := signalCtx()
	defer stop()

	reports, err := c.CheckDriftAll(ctx)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 3, ' ', 0)
	// COUNTER is what the broker advertises; SCANNED is what walking the queue
	// actually finds. MISSING is the gap: messages the counter promises that are
	// not there.
	fmt.Fprintln(w, "QUEUE NAME\tCOUNTER\tSCANNED\tMISSING\tVERDICT")
	drifted := 0
	for _, r := range reports {
		if r.Verdict() == broker.DriftConfirmed {
			drifted++
		}
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%s\n",
			r.Queue, r.CounterBefore, r.Counted, r.Missing(), r.Verdict())
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if drifted > 0 {
		fmt.Fprintf(cmd.OutOrStdout(),
			"\n%d queue(s) have a drifted message counter: they advertise messages that do not exist,\n"+
				"so no export can ever empty them. Restart the broker to rebuild the counters from its journal.\n",
			drifted)
		return fmt.Errorf("%d queue(s) with a drifted message counter", drifted)
	}
	return nil
}
