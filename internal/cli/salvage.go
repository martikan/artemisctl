package cli

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/martikan/artemisctl/internal/journal"
	"github.com/martikan/artemisctl/internal/store"
	"github.com/spf13/cobra"
)

// newSalvageCmd builds `artemisctl salvage`: the offline counterpart to
// `export` that reads a *stopped* broker's data directory straight off disk
// (bindings + message journal + large-messages + paging, via
// internal/journal's Salvage pipeline) and writes every recoverable message
// to a local .artx store. Unlike every other command in this tree it never
// dials a broker, so it deliberately ignores the persistent --url/--username/
// --password/--timeout connection flags -- that is called out explicitly in
// the Long help below so `--help` output doesn't mislead an operator into
// thinking those flags matter here.
func newSalvageCmd() *cobra.Command {
	var dataDir, bindingsDir, journalDir, largeDir, pagingDir, out string
	var force, allowSkips bool

	cmd := &cobra.Command{
		Use:   "salvage",
		Short: "Recover messages from a stopped broker's data directory into a local store (offline)",
		Long: "salvage reads a broker data directory directly off disk -- the bindings and message " +
			"journals, large-message bodies, and paging state -- and writes every recoverable " +
			"message to a local .artx store for a later `redeliver`, without needing a running " +
			"broker. It IGNORES the persistent --url/--username/--password/--timeout connection " +
			"flags entirely: this command never dials the broker. It refuses to run against a data " +
			"directory whose broker process is still alive (server.lock held); pass --force to " +
			"override that guard, but export from a live broker is the safer path when one is " +
			"available. Core-protocol messages are decoded and exported alongside AMQP messages " +
			"(redeliver converts them back to AMQP on send). The summary printed at the end " +
			"itemizes both skipped records (unsupported data) and diagnostics (corruption incidents and other " +
			"notes); by default the command exits non-zero whenever either skips or corruption " +
			"diagnostics are present, so a script can't miss them -- pass --allow-skips to accept " +
			"them and exit 0 anyway.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if dataDir == "" && (bindingsDir == "" || journalDir == "") {
				return errors.New("salvage: --data is required unless both --bindings and --journal are set")
			}
			bindings := resolveSalvageSubDir(bindingsDir, dataDir, "bindings")
			journalD := resolveSalvageSubDir(journalDir, dataDir, "journal")
			largeMessages := resolveSalvageSubDir(largeDir, dataDir, "large-messages")
			paging := resolveSalvageSubDir(pagingDir, dataDir, "paging")

			if err := requireExistingDir(bindings); err != nil {
				return fmt.Errorf("salvage: bindings dir: %w", err)
			}
			if err := requireExistingDir(journalD); err != nil {
				return fmt.Errorf("salvage: journal dir: %w", err)
			}

			// Pre-check the FINAL --out path before doing any work, mirroring
			// store.NewWriter's own refusal semantics: re-running salvage into
			// an already-populated store must never silently clobber it. This
			// check has to happen here (not just rely on NewWriter on the
			// .partial path below) because the .partial file is a different
			// path than --out, so NewWriter alone would never see --out at all.
			if fi, err := os.Stat(out); err == nil && fi.Size() > 0 {
				return fmt.Errorf("store %s already exists and is non-empty; choose a new path to avoid overwriting drained data", out)
			} else if err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("salvage: stat %s: %w", out, err)
			}

			if !force {
				if err := checkLiveBroker(journalD, dataDir); err != nil {
					return err
				}
			}

			partial := out + ".partial"
			// A killed/crashed prior run can leave a non-empty .partial
			// behind; store.NewWriter refuses to reuse a non-empty file (its
			// own guard exists to protect a REAL store, e.g. --out itself,
			// against accidental truncation -- see the --out check above).
			// .partial is never a source of truth here -- it only ever holds
			// bytes from a run that did not reach the final rename to --out
			// -- so it is always safe to discard before starting this run's
			// write. Ignore a not-exist error; anything else surfaces below
			// when NewWriter itself fails to open the path.
			if err := os.Remove(partial); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("salvage: remove stale partial store %s: %w", partial, err)
			}
			w, err := store.NewWriter(partial)
			if err != nil {
				return fmt.Errorf("salvage: open partial store %s: %w", partial, err)
			}

			// maxAMQPLen tracks, per destination queue, the largest single
			// record's encoded AMQP byte length seen during this run. Summary
			// (Task 10's frozen interface) only carries a single global
			// LargestBytes -- it has no per-queue breakdown, and store.Record
			// itself carries no "this came from a large message" flag either,
			// so there is no way to ask the journal package which queue the
			// large message landed on. Tracking the max length here, at the
			// CLI layer, recovers that attribution for realistic data without
			// touching Task 10's interfaces: a large message's encoded length
			// dwarfs every ordinary message's, so the queue with the biggest
			// single record is -- in practice -- the queue that received the
			// large message. See the print-format decision note in the task
			// report for the full reasoning.
			maxAMQPLen := make(map[string]int)
			summary, salvageErr := journal.Salvage(journal.Options{
				Bindings:      bindings,
				Journal:       journalD,
				LargeMessages: largeMessages,
				Paging:        paging,
			}, func(r store.Record) error {
				n := len(r.AMQP)
				if r.Kind == store.KindCore {
					n = len(r.CorePayload)
				}
				if n > maxAMQPLen[r.Queue] {
					maxAMQPLen[r.Queue] = n
				}
				return w.Append(r)
			})
			if salvageErr != nil {
				_ = w.Close()
				_ = os.Remove(partial)
				return salvageErr
			}

			total := summary.Total()
			if total == 0 {
				// spec: never leave an empty store behind.
				_ = w.Close()
				_ = os.Remove(partial)
				printSalvageSummary(cmd.OutOrStdout(), summary, out, 0, "")
				// The skips/corruption gate applies here too (finding I1): a
				// journal that salvages nothing but still has skips or
				// corruption diagnostics (e.g. a data dir holding only
				// Core-protocol messages, or large messages whose bodies are
				// all missing) is the worst case for a scripted recovery --
				// exiting 0 would make that silently indistinguishable from
				// "genuinely nothing here to salvage".
				if (summary.HasSkips() || summary.HasCorruption()) && !allowSkips {
					return errors.New("skips or corruption present — failing (pass --allow-skips to override)")
				}
				return nil
			}

			if err := w.Sync(); err != nil {
				_ = w.Close()
				return fmt.Errorf("salvage: sync partial store: %w", err)
			}
			if err := w.Close(); err != nil {
				return fmt.Errorf("salvage: close partial store: %w", err)
			}
			if err := os.Rename(partial, out); err != nil {
				return fmt.Errorf("salvage: rename %s to %s: %w", partial, out, err)
			}

			largestQueue := ""
			if summary.Large > 0 {
				names := make([]string, 0, len(maxAMQPLen))
				for q := range maxAMQPLen {
					names = append(names, q)
				}
				sort.Strings(names)
				best := -1
				for _, q := range names {
					if maxAMQPLen[q] > best {
						best = maxAMQPLen[q]
						largestQueue = q
					}
				}
			}
			printSalvageSummary(cmd.OutOrStdout(), summary, out, total, largestQueue)

			if (summary.HasSkips() || summary.HasCorruption()) && !allowSkips {
				return errors.New("skips or corruption present — failing (pass --allow-skips to override)")
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&dataDir, "data", "", "broker data directory (parent of bindings/journal/large-messages/paging); optional if --bindings and --journal are both given")
	cmd.Flags().StringVar(&bindingsDir, "bindings", "", "bindings journal dir (default <data>/bindings)")
	cmd.Flags().StringVar(&journalDir, "journal", "", "message journal dir (default <data>/journal)")
	cmd.Flags().StringVar(&largeDir, "large-messages", "", "large-messages dir (default <data>/large-messages)")
	cmd.Flags().StringVar(&pagingDir, "paging", "", "paging dir (default <data>/paging)")
	cmd.Flags().StringVar(&out, "out", "", "output store file (required)")
	cmd.Flags().BoolVar(&force, "force", false, "skip the live-broker (server.lock) guard")
	cmd.Flags().BoolVar(&allowSkips, "allow-skips", false, "exit 0 even though some messages were skipped or corruption diagnostics were reported (unsupported/corrupt data)")
	_ = cmd.MarkFlagRequired("out")

	return cmd
}

// resolveSalvageSubDir returns explicit if set, else dataDir/sub.
func resolveSalvageSubDir(explicit, dataDir, sub string) string {
	if explicit != "" {
		return explicit
	}
	return filepath.Join(dataDir, sub)
}

// requireExistingDir returns an error if dir does not exist, is unreadable,
// or is not a directory -- the CLI's own fast-fail check ahead of
// journal.Salvage's identical (but differently worded) internal check, so a
// missing --bindings/--journal dir is reported before any other validation
// or work (live-broker probe, partial-store creation) happens.
func requireExistingDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s: not a directory", dir)
	}
	return nil
}

// checkLiveBroker guards against running salvage against a data directory
// whose broker process is still alive. Artemis's JournalStorageManager holds
// an exclusive flock on server.lock for as long as the broker is up.
//
// journalDir is the message-journal sub-dir (e.g. <data>/journal); dataDir
// is the broker data directory that is its parent (e.g. <instance>/data) --
// dataDir may be "" when the caller supplied --bindings/--journal directly
// without --data. The instance root is dataDir's own parent (e.g.
// <instance>), since the standard broker layout is
// <instance>/data/{bindings,journal,...} alongside <instance>/server.lock.
//
// Three candidate locations are probed, in order, and the first one that
// exists on disk is used:
//  1. <journalDir>/server.lock -- fixture-verified real location for the
//     artemis-2.42-data fixture this package tests against.
//  2. <dataDir>/server.lock (only if dataDir != "") -- kept for backward
//     compatibility; harmless to probe even though no observed layout uses
//     it, since a probe-only check on a path that never exists is a no-op.
//  3. filepath.Dir(dataDir)/server.lock, i.e. the instance root (only if
//     dataDir != "") -- the layout the task brief calls out explicitly.
//
// If none of the candidates exist, there is nothing to check against and
// salvage proceeds -- this offline tool never creates the lock file itself,
// only probes an existing one.
func checkLiveBroker(journalDir, dataDir string) error {
	candidates := []string{filepath.Join(journalDir, "server.lock")}
	if dataDir != "" {
		candidates = append(candidates,
			filepath.Join(dataDir, "server.lock"),
			filepath.Join(filepath.Dir(dataDir), "server.lock"),
		)
	}
	var lockPath string
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && !fi.IsDir() {
			lockPath = c
			break
		}
	}
	if lockPath == "" {
		return nil
	}

	f, err := os.OpenFile(lockPath, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("salvage: open %s for live-broker check: %w", lockPath, err)
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return errors.New("broker appears to be running (server.lock held) — use `export` for a live broker, or --force")
		}
		return fmt.Errorf("salvage: flock %s: %w", lockPath, err)
	}
	// The probe only ever wants to know whether the lock is free; release it
	// immediately rather than holding it for the run.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}

// printSalvageSummary writes the operator-facing report: the headline count
// (and destination, unless nothing was recovered), one aligned line per
// destination queue, and -- if any messages were unrecoverable -- a
// "skipped:" section listing why, followed by -- if anything else worth
// flagging happened -- a "diagnostics:" section (corruption incidents and
// benign notes alike; see journal.Summary.Diags). largestQueue, when
// non-empty, names the queue whose line gets the "(largest N)" annotation
// (see the caller's maxAMQPLen comment for how that queue is chosen).
func printSalvageSummary(w io.Writer, summary journal.Summary, out string, total int, largestQueue string) {
	if total == 0 {
		fmt.Fprintln(w, "salvaged 0 messages (nothing recoverable; no store written)")
	} else {
		fmt.Fprintf(w, "salvaged %d messages to %s\n", total, out)
		if summary.Core > 0 {
			fmt.Fprintf(w, "  (%d Core-protocol messages decoded; redeliver converts them to AMQP)\n", summary.Core)
		}
	}

	names := make([]string, 0, len(summary.PerQueue))
	maxNameLen := 0
	for name := range summary.PerQueue {
		names = append(names, name)
		if len(name) > maxNameLen {
			maxNameLen = len(name)
		}
	}
	sort.Strings(names)
	width := maxNameLen + 2
	for _, name := range names {
		line := fmt.Sprintf("  %-*s%d", width, name, summary.PerQueue[name])
		if name == largestQueue {
			line += fmt.Sprintf("  (largest %s)", formatKiB(summary.LargestBytes))
		}
		fmt.Fprintln(w, line)
	}

	if summary.HasSkips() {
		fmt.Fprintln(w, "skipped:")
		for _, s := range summary.Skips {
			fmt.Fprintf(w, "  %s\n", s)
		}
	}

	if len(summary.Diags) > 0 {
		fmt.Fprintln(w, "diagnostics:")
		for _, d := range summary.Diags {
			fmt.Fprintf(w, "  %s\n", d)
		}
	}
}

// formatKiB renders a byte count as "N.N KiB", one decimal place.
func formatKiB(n int64) string {
	return fmt.Sprintf("%.1f KiB", float64(n)/1024)
}
