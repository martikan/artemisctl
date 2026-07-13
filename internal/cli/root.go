// Package cli wires the artemisctl command tree. It builds the cobra commands
// (status, health, browse, produce, export, redeliver), reads the shared
// connection flags, and translates each command into calls on the broker
// package. The helpers here — connProps, connectCtx, signalCtx — centralize
// how every command connects and how it scopes cancellation: a --timeout only
// bounds the initial connect, while long-running drains and replays run under
// a SIGINT-cancelable context instead.
package cli

import (
	"context"
	"os"
	"os/signal"
	"time"

	"github.com/martikan/artemisctl/internal/broker"
	"github.com/spf13/cobra"
)

// NewRootCmd builds the root artemisctl command with its persistent connection
// flags and every subcommand attached, ready to Execute.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "artemisctl",
		Short:         "Manage and recover Apache ActiveMQ Artemis brokers",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.String("url", "127.0.0.1:61616", "AMQP 1.0 broker address host:port")
	pf.StringP("username", "u", "artemis", "broker username")
	pf.StringP("password", "p", "artemis", "broker password (prefer ARTEMIS_PASSWORD env)")
	pf.Duration("timeout", 30*time.Second, "max time to establish the broker connection before failing fast")
	root.AddCommand(newStatusCmd())
	root.AddCommand(newExportCmd())
	root.AddCommand(newHealthCmd())
	root.AddCommand(newRedeliverCmd())
	root.AddCommand(newBrowseCmd())
	root.AddCommand(newProduceCmd())
	root.AddCommand(newCordonCmd())
	root.AddCommand(newUncordonCmd())
	root.AddCommand(newSalvageCmd())
	return root
}

// resolvePassword returns ARTEMIS_PASSWORD when set, else the flag value.
func resolvePassword(flagVal string) string {
	if env := os.Getenv("ARTEMIS_PASSWORD"); env != "" {
		return env
	}
	return flagVal
}

// connProps reads the persistent flags + ARTEMIS_PASSWORD into ConnectionProps.
func connProps(cmd *cobra.Command) broker.ConnectionProps {
	url, _ := cmd.Flags().GetString("url")
	user, _ := cmd.Flags().GetString("username")
	pass, _ := cmd.Flags().GetString("password")
	return broker.ConnectionProps{URL: url, Username: user, Password: resolvePassword(pass)}
}

// connectCtx derives a context bounded by the --timeout flag. It always bounds
// broker.Connect so an unresponsive broker fails fast instead of hanging
// forever; for the short read-only commands (status/health/browse) the same
// context bounds the whole operation, since those complete quickly. The caller
// must defer the returned cancel. It intentionally does NOT bound long-running
// work (export drain / redeliver replay), which may legitimately exceed the
// timeout — those pass a separate unbounded/signal context to the drain/replay
// loop.
func connectCtx(cmd *cobra.Command, parent context.Context) (context.Context, context.CancelFunc) {
	d, _ := cmd.Flags().GetDuration("timeout")
	if d <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, d)
}

// signalCtx returns a context that is canceled on SIGINT, for the long-running
// drain/replay loops so Ctrl-C returns gracefully with a durable checkpoint
// rather than aborting an in-flight send. The caller must defer the returned
// stop.
func signalCtx() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}
