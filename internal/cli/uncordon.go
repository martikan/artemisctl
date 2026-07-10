package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

func newUncordonCmd() *cobra.Command {
	var stateFile string
	var forceRemove bool

	cmd := &cobra.Command{
		Use:   "uncordon",
		Short: "Lift a cordon, restoring the pre-cordon settings",
		Long: "uncordon reverses cordon by restoring the settings saved in the state " +
			"file. With --force-remove (or when no state file exists) it instead " +
			"removes the wildcard settings entry, reverting to the broker's defaults.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := connectCtx(cmd, context.Background())
			defer cancel()
			c, err := broker.Connect(ctx, connProps(cmd))
			if err != nil {
				return err
			}
			defer c.Close(context.Background())

			state, readErr := readCordonState(stateFile)
			if readErr != nil || forceRemove {
				if readErr != nil && !forceRemove {
					if os.IsNotExist(readErr) {
						return fmt.Errorf("no state file at %s; re-run with --force-remove to clear the wildcard settings entry", stateFile)
					}
					return fmt.Errorf("read state file %s: %w", stateFile, readErr)
				}
				// --force-remove path: no saved state to restore.
				if err := c.UncordonRemove(ctx); err != nil {
					return err
				}
				fmt.Fprintln(cmd.OutOrStdout(), "cordon lifted (wildcard settings entry removed)")
				return nil
			}

			if err := c.Uncordon(ctx, state.SavedSettings); err != nil {
				return err
			}
			if err := os.Remove(stateFile); err != nil && !os.IsNotExist(err) {
				fmt.Fprintf(cmd.OutOrStdout(), "warning: cordon lifted but could not remove state file %s: %v\n", stateFile, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "cordon lifted; settings restored from %s\n", stateFile)
			return nil
		},
	}
	cmd.Flags().StringVar(&stateFile, "state-file", defaultCordonState, "path to the pre-cordon settings written by cordon")
	cmd.Flags().BoolVar(&forceRemove, "force-remove", false, "remove the wildcard settings entry instead of restoring saved state")
	return cmd
}
