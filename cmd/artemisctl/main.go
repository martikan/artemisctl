// Command artemisctl is a command-line tool for managing and recovering Apache
// ActiveMQ Artemis brokers over AMQP 1.0: check status and health, browse
// queues non-destructively, drain a dying broker into a local store and replay
// it, and produce messages for testing. It is a thin entry point; the command
// tree lives in internal/cli and the broker operations in internal/broker.
package main

import (
	"fmt"
	"os"

	"github.com/martikan/artemisctl/internal/cli"
)

// Version is stamped at release time via -ldflags "-X main.Version=...".
// It defaults to "dev" for local/source builds.
var Version = "dev"

func main() {
	root := cli.NewRootCmd()
	root.Version = Version
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
