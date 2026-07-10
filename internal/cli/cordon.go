package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

// defaultCordonState is where cordon stashes the pre-cordon settings so a later
// uncordon (a separate process) can restore them.
const defaultCordonState = "artemisctl-cordon.json"

// cordonState is the sidecar written by cordon and consumed by uncordon. It
// persists the pre-cordon wildcard address-settings so the block can be lifted
// faithfully from a different invocation.
type cordonState struct {
	BrokerURL     string    `json:"brokerURL"`
	SavedSettings string    `json:"savedSettings"`
	CordonedAt    time.Time `json:"cordonedAt"`
}

func writeCordonState(path string, s cordonState) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func readCordonState(path string) (cordonState, error) {
	var s cordonState
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	err = json.Unmarshal(b, &s)
	return s, err
}

func newCordonCmd() *cobra.Command {
	var stateFile string
	var yes bool

	cmd := &cobra.Command{
		Use:   "cordon",
		Short: "Block producers broker-wide (address-full BLOCK) before an export",
		Long: "cordon applies an address-full BLOCK policy to every address so no new " +
			"messages can be produced, while consumers and export keep working. The " +
			"pre-cordon settings are saved to a state file for uncordon to restore. " +
			"Requires a broker new enough to accept addAddressSettings over AMQP (2.33+).",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if _, err := os.Stat(stateFile); err == nil {
				return fmt.Errorf("state file %s already exists; broker may already be cordoned (uncordon first, or remove the file)", stateFile)
			}
			if !yes && !confirm(cmd, "This blocks ALL producers on the broker. Continue? [y/N] ") {
				fmt.Fprintln(cmd.OutOrStdout(), "aborted")
				return nil
			}

			ctx, cancel := connectCtx(cmd, context.Background())
			defer cancel()
			props := connProps(cmd)
			c, err := broker.Connect(ctx, props)
			if err != nil {
				return err
			}
			defer c.Close(context.Background())

			saved, err := c.Cordon(ctx)
			if err != nil {
				if errors.Is(err, broker.ErrBrokerTooOld) {
					return fmt.Errorf("%w — cordon over AMQP is unavailable on this broker", err)
				}
				return err
			}
			if err := writeCordonState(stateFile, cordonState{
				BrokerURL:     props.URL,
				SavedSettings: saved,
				CordonedAt:    time.Now(),
			}); err != nil {
				return fmt.Errorf("cordon applied but failed to write state file %s: %w", stateFile, err)
			}
			fmt.Fprintf(cmd.OutOrStdout(), "broker cordoned; producers blocked. state saved to %s\n", stateFile)
			fmt.Fprintln(cmd.OutOrStdout(), "run 'artemisctl uncordon' to lift the block")
			return nil
		},
	}
	cmd.Flags().StringVar(&stateFile, "state-file", defaultCordonState, "path to persist pre-cordon settings for uncordon")
	cmd.Flags().BoolVar(&yes, "yes", false, "skip the confirmation prompt")
	return cmd
}

// confirm reads a yes/no answer from the command's input stream.
func confirm(cmd *cobra.Command, prompt string) bool {
	fmt.Fprint(cmd.OutOrStdout(), prompt)
	r := bufio.NewReader(cmd.InOrStdin())
	line, err := r.ReadString('\n')
	if err != nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(line)) {
	case "y", "yes":
		return true
	default:
		return false
	}
}
