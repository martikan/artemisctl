package cli

import (
	"archive/tar"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/martikan/artemisctl/internal/store"
)

// salvageFixtureDir extracts internal/journal's committed
// artemis-2.42-data.tar.gz fixture into t.TempDir() and returns the
// extracted data/ path (containing bindings/journal/large-messages/paging).
// internal/journal's own fixtureDir helper (testdata_test.go) is unexported
// to that package, so this is a small local copy of the same extraction
// logic per the task brief.
func salvageFixtureDir(t *testing.T) string {
	t.Helper()
	tarball := filepath.Join("..", "journal", "testdata", "artemis-2.42-data.tar.gz")
	f, err := os.Open(tarball)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("fixture %s missing; run `make fixtures` to harvest it", tarball)
		}
		t.Fatalf("open fixture: %v", err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gunzip fixture: %v", err)
	}
	defer gz.Close()

	dst := t.TempDir()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read fixture tar: %v", err)
		}
		name := filepath.Clean(hdr.Name)
		if strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
			t.Fatalf("fixture tar has unsafe path %q", hdr.Name)
		}
		path := filepath.Join(dst, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("extract dir %s: %v", name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("extract parent of %s: %v", name, err)
			}
			out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("extract file %s: %v", name, err)
			}
			if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // trusted committed fixture
				out.Close()
				t.Fatalf("extract file %s: %v", name, err)
			}
			if err := out.Close(); err != nil {
				t.Fatalf("close extracted %s: %v", name, err)
			}
		default:
			t.Fatalf("fixture tar has unexpected entry type %d for %q", hdr.Typeflag, hdr.Name)
		}
	}
	return filepath.Join(dst, "data")
}

// writeEmptyJournalFile writes a syntactically valid but empty journal file
// (just the 16-byte header: int formatVersion=2, int userVersion=0, long
// fileID=1 -- format_notes.md section 1) at dir/activemq-data-1.amq, so
// ReadJournalDir accepts the file and yields zero records.
func writeEmptyJournalFile(t *testing.T, dir string) {
	t.Helper()
	var hdr [16]byte
	binary.BigEndian.PutUint32(hdr[0:4], 2)  // formatVersion
	binary.BigEndian.PutUint32(hdr[4:8], 0)  // userVersion
	binary.BigEndian.PutUint64(hdr[8:16], 1) // fileID
	path := filepath.Join(dir, "activemq-data-1.amq")
	if err := os.WriteFile(path, hdr[:], 0o644); err != nil {
		t.Fatalf("write empty journal file: %v", err)
	}
}

// runSalvage runs `artemisctl salvage <args...>` via cobra's ExecuteC and
// returns captured combined stdout/stderr and the RunE error.
func runSalvage(t *testing.T, args ...string) (string, error) {
	t.Helper()
	return runCmdStdin(t, "", append([]string{"salvage"}, args...)...)
}

// writeCorePagedEntry writes a single-entry page file at
// <pagingDir>/<some-addr-dir>/1.page containing one Core-persister page
// entry: an 8-byte transactionID (value irrelevant -- read and discarded),
// a 1-byte largeMessageType of NONE (0), and a 1-byte persister id of Core
// (1) -- format_notes.md section 8's outer '{' size '}' framing wraps a body
// paging.go's decodePagedMessage classifies as persisterDecodeCore, which
// ReadPaging counts as a Skip (PagingDiag.CoreSkipped) without ever
// producing an exportable PagedMessage.
//
// This exists to build a zero-exported-records-but-with-a-skip fixture
// (finding I1's gating test) cheaply: a page file's framing is a simple
// size-prefixed block, unlike the message/bindings journal's much heavier
// framing (16-byte header, fileID echo, check-size), so it is far less work
// to hand-encode from scratch than an equivalent Core-skipped message-journal
// record would be.
func writeCorePagedEntry(t *testing.T, pagingDir string) {
	t.Helper()
	addrDir := filepath.Join(pagingDir, "salvage.corepaged")
	if err := os.MkdirAll(addrDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var body [10]byte
	binary.BigEndian.PutUint64(body[0:8], 0) // transactionID: irrelevant, read and discarded
	body[8] = 0                              // largeMessageType = NONE
	body[9] = 1                              // persister id = Core (1) -> persisterDecodeCore -> CoreSkipped

	frame := make([]byte, 0, 1+4+len(body)+1)
	frame = append(frame, '{')
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(body)))
	frame = append(frame, size[:]...)
	frame = append(frame, body[:]...)
	frame = append(frame, '}')

	if err := os.WriteFile(filepath.Join(addrDir, "1.page"), frame, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestSalvageFixtureSuccess(t *testing.T) {
	dir := salvageFixtureDir(t)
	out := filepath.Join(t.TempDir(), "rescue.artx")

	stdout, err := runSalvage(t, "--data", dir, "--out", out)
	if err != nil {
		t.Fatalf("salvage: %v\noutput:\n%s", err, stdout)
	}

	// Full-block equality, not per-line Contains soup: this pins line ORDER
	// (alphabetical by queue name), the exact column alignment (width =
	// longest queue name "salvage.scheduled" + 2), the "(largest N)"
	// annotation landing on the one queue attributed the large message, and
	// that no "skipped:" section is emitted for this zero-skip fixture.
	// Golden value captured from a verified-correct run of this exact
	// fixture (artemis-2.42-data.tar.gz, 512 total records across 5 queues)
	// and re-verified by `go test -run TestSalvageFixtureSuccess -v`.
	wantStdout := "salvaged 512 messages to " + out + "\n" +
		"  salvage.large      1  (largest 300.1 KiB)\n" +
		"  salvage.paged      500\n" +
		"  salvage.plain      5\n" +
		"  salvage.props      5\n" +
		"  salvage.scheduled  1\n"
	if stdout != wantStdout {
		t.Errorf("stdout mismatch:\n got:  %q\n want: %q", stdout, wantStdout)
	}

	fi, statErr := os.Stat(out)
	if statErr != nil {
		t.Fatalf("stat --out: %v", statErr)
	}
	if fi.Size() == 0 {
		t.Fatal("--out is empty")
	}
	if _, statErr := os.Stat(out + ".partial"); !os.IsNotExist(statErr) {
		t.Errorf(".partial file left behind: %v", statErr)
	}

	rd, err := store.OpenReader(out)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rd.Close()
	total := 0
	for {
		_, _, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		total++
	}
	if total != 512 {
		t.Errorf("store has %d records, want 512", total)
	}
}

func TestSalvageBindingsAndJournalWithoutData(t *testing.T) {
	dir := salvageFixtureDir(t)
	out := filepath.Join(t.TempDir(), "rescue.artx")

	stdout, err := runSalvage(t,
		"--bindings", filepath.Join(dir, "bindings"),
		"--journal", filepath.Join(dir, "journal"),
		"--large-messages", filepath.Join(dir, "large-messages"),
		"--paging", filepath.Join(dir, "paging"),
		"--out", out,
	)
	if err != nil {
		t.Fatalf("salvage: %v\noutput:\n%s", err, stdout)
	}
	if !strings.Contains(stdout, "salvaged 512 messages to "+out) {
		t.Errorf("stdout missing headline; got:\n%s", stdout)
	}
}

func TestSalvageMissingDataFlagCombo(t *testing.T) {
	out := filepath.Join(t.TempDir(), "rescue.artx")

	// Neither --data nor the full --bindings+--journal pair: rejected before
	// touching the filesystem.
	_, err := runSalvage(t, "--journal", "/nonexistent/journal", "--out", out)
	if err == nil {
		t.Fatal("want error when --data is absent and --bindings is not set")
	}
	if !strings.Contains(err.Error(), "--data is required") {
		t.Errorf("error = %v, want mention of --data requirement", err)
	}
}

func TestSalvageZeroRecords(t *testing.T) {
	dataDir := t.TempDir()
	bindings := filepath.Join(dataDir, "bindings")
	journalDir := filepath.Join(dataDir, "journal")
	if err := os.MkdirAll(bindings, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeEmptyJournalFile(t, journalDir)

	out := filepath.Join(t.TempDir(), "rescue.artx")
	stdout, err := runSalvage(t, "--data", dataDir, "--out", out)
	if err != nil {
		t.Fatalf("salvage: %v\noutput:\n%s", err, stdout)
	}
	if !strings.Contains(stdout, "salvaged 0 messages") {
		t.Errorf("stdout missing zero-record headline; got:\n%s", stdout)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatalf("--out must not exist after a zero-record run, stat err = %v", statErr)
	}
	if _, statErr := os.Stat(out + ".partial"); !os.IsNotExist(statErr) {
		t.Fatalf(".partial must not be left behind, stat err = %v", statErr)
	}
}

// TestSalvageZeroRecordsWithSkipsGated pins finding I1: a run that salvages
// zero records must still be gated on skips (and corruption) exactly like a
// non-empty run -- exiting 0 on "nothing recovered, but something was lost"
// is the worst case for a scripted recovery, since it is silently
// indistinguishable from "genuinely nothing here to salvage". The journal
// and bindings dirs are empty (zero exported records); the paging dir holds
// one Core-protocol page entry (writeCorePagedEntry), which is a Skip that
// never contributes to PerQueue.
func TestSalvageZeroRecordsWithSkipsGated(t *testing.T) {
	dataDir := t.TempDir()
	bindings := filepath.Join(dataDir, "bindings")
	journalDir := filepath.Join(dataDir, "journal")
	pagingDir := filepath.Join(dataDir, "paging")
	if err := os.MkdirAll(bindings, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(pagingDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeEmptyJournalFile(t, journalDir)
	writeCorePagedEntry(t, pagingDir)

	t.Run("without allow-skips fails", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "rescue.artx")
		stdout, err := runSalvage(t, "--data", dataDir, "--out", out)
		if err == nil {
			t.Fatalf("want error for a zero-record run with skips present; output:\n%s", stdout)
		}
		if !strings.Contains(err.Error(), "skips or corruption present") {
			t.Errorf("error = %v, want it to mention skips/corruption", err)
		}
		if !strings.Contains(stdout, "salvaged 0 messages") {
			t.Errorf("stdout missing zero-record headline; got:\n%s", stdout)
		}
		if !strings.Contains(stdout, "skipped:") {
			t.Errorf("stdout missing skipped section; got:\n%s", stdout)
		}
		if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
			t.Errorf("--out must not be created for a zero-record run, stat err = %v", statErr)
		}
		if _, statErr := os.Stat(out + ".partial"); !os.IsNotExist(statErr) {
			t.Errorf(".partial must not be left behind, stat err = %v", statErr)
		}
	})

	t.Run("with allow-skips succeeds", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "rescue.artx")
		stdout, err := runSalvage(t, "--data", dataDir, "--out", out, "--allow-skips")
		if err != nil {
			t.Fatalf("salvage with --allow-skips: %v\noutput:\n%s", err, stdout)
		}
		if !strings.Contains(stdout, "salvaged 0 messages") {
			t.Errorf("stdout missing zero-record headline; got:\n%s", stdout)
		}
		if !strings.Contains(stdout, "skipped:") {
			t.Errorf("stdout missing skipped section; got:\n%s", stdout)
		}
		if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
			t.Errorf("--out must still never be created for a zero-record run, even with --allow-skips, stat err = %v", statErr)
		}
	})
}

func TestSalvageMissingJournalDir(t *testing.T) {
	dataDir := t.TempDir()
	bindings := filepath.Join(dataDir, "bindings")
	if err := os.MkdirAll(bindings, 0o755); err != nil {
		t.Fatal(err)
	}
	// journal/ deliberately not created.

	out := filepath.Join(t.TempDir(), "rescue.artx")
	_, err := runSalvage(t, "--data", dataDir, "--out", out)
	if err == nil {
		t.Fatal("want error for missing journal dir")
	}
	if !strings.Contains(err.Error(), "journal") {
		t.Errorf("error = %v, want it to mention the journal dir", err)
	}
	if _, statErr := os.Stat(out); !os.IsNotExist(statErr) {
		t.Fatal("--out must not be created when validation fails")
	}
}

// TestSalvageSkipsGating exercises both sides of the --allow-skips gate using
// a deliberately-missing --large-messages dir: the fixture's one large
// message then becomes an unrecoverable skip (its body file is unreachable),
// while everything else still salvages fine.
func TestSalvageSkipsGating(t *testing.T) {
	dir := salvageFixtureDir(t)
	missingLarge := filepath.Join(t.TempDir(), "no-such-large-messages-dir")

	t.Run("without allow-skips fails", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "rescue.artx")
		stdout, err := runSalvage(t,
			"--bindings", filepath.Join(dir, "bindings"),
			"--journal", filepath.Join(dir, "journal"),
			"--large-messages", missingLarge,
			"--paging", filepath.Join(dir, "paging"),
			"--out", out,
		)
		if err == nil {
			t.Fatalf("want error when skips are present without --allow-skips; output:\n%s", stdout)
		}
		if !strings.Contains(err.Error(), "skips or corruption present") {
			t.Errorf("error = %v, want it to mention skips", err)
		}
		if !strings.Contains(stdout, "skipped:") {
			t.Errorf("stdout missing skipped section; got:\n%s", stdout)
		}
		// A partial (but real) result is still saved -- a recovery tool must
		// not throw away what it *did* manage to salvage.
		if fi, statErr := os.Stat(out); statErr != nil || fi.Size() == 0 {
			t.Errorf("--out should still be written despite the skip-gated exit: stat err=%v", statErr)
		}
	})

	t.Run("with allow-skips succeeds", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "rescue.artx")
		stdout, err := runSalvage(t,
			"--bindings", filepath.Join(dir, "bindings"),
			"--journal", filepath.Join(dir, "journal"),
			"--large-messages", missingLarge,
			"--paging", filepath.Join(dir, "paging"),
			"--out", out,
			"--allow-skips",
		)
		if err != nil {
			t.Fatalf("salvage with --allow-skips: %v\noutput:\n%s", err, stdout)
		}
		if !strings.Contains(stdout, "skipped:") {
			t.Errorf("stdout missing skipped section; got:\n%s", stdout)
		}
	})
}

// TestSalvageCorruptJournalGatesExitAndPrintsDiagnostics pins finding C1
// end-to-end through the actual CLI binary: a corrupted message-journal
// record must show up as a "diagnostics:" section in stdout (not silently
// swallowed) and must gate the exit code exactly like a skip, honoring
// --allow-skips the same way.
//
// The corrupted byte is the last byte of a record's trailing check-size int
// (the same technique internal/journal's
// TestReadJournalDirCorruptRecordResyncs uses against this exact fixture
// file) at a fixture-verified, deterministic offset: the fixture tarball is
// an immutable committed file, so activemq-data-1.amq's second record (an
// UPDATE_RECORD/ADD_REF for message id 38) always starts at byte 273 and
// ends at byte 304 in a fresh extraction -- offset 303 is the last byte of
// its check-size. This offset is pinned as a literal (rather than located
// dynamically via internal/journal's unexported scanCleanSpans/parseRecord,
// which this package cannot import) because it is deterministic against the
// committed fixture; internal/journal/file_test.go independently re-derives
// and asserts the same offset every run, so a fixture change that moved it
// would already fail there first.
func TestSalvageCorruptJournalGatesExitAndPrintsDiagnostics(t *testing.T) {
	dir := salvageFixtureDir(t)
	journalFile := filepath.Join(dir, "journal", "activemq-data-1.amq")

	data, err := os.ReadFile(journalFile)
	if err != nil {
		t.Fatalf("read journal file: %v", err)
	}
	const corruptOffset = 303 // fixture-verified: last byte of record[1]'s trailing check-size int
	data[corruptOffset] ^= 0xFF
	if err := os.WriteFile(journalFile, data, 0o644); err != nil {
		t.Fatalf("write corrupted journal file: %v", err)
	}

	t.Run("without allow-skips fails and reports", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "rescue.artx")
		stdout, err := runSalvage(t, "--data", dir, "--out", out)
		if err == nil {
			t.Fatalf("want error for a corrupt journal record without --allow-skips; output:\n%s", stdout)
		}
		if !strings.Contains(err.Error(), "skips or corruption present") {
			t.Errorf("error = %v, want it to mention skips/corruption", err)
		}
		if !strings.Contains(stdout, "diagnostics:") {
			t.Errorf("stdout missing diagnostics section; got:\n%s", stdout)
		}
		if !strings.Contains(stdout, "check-size mismatch") {
			t.Errorf("stdout diagnostics missing the check-size-mismatch incident; got:\n%s", stdout)
		}
		// A partial (but real) result is still saved, same contract as a
		// skip-gated exit.
		if fi, statErr := os.Stat(out); statErr != nil || fi.Size() == 0 {
			t.Errorf("--out should still be written despite the corruption-gated exit: stat err=%v", statErr)
		}
	})

	t.Run("with allow-skips succeeds and still reports", func(t *testing.T) {
		out := filepath.Join(t.TempDir(), "rescue.artx")
		stdout, err := runSalvage(t, "--data", dir, "--out", out, "--allow-skips")
		if err != nil {
			t.Fatalf("salvage with --allow-skips: %v\noutput:\n%s", err, stdout)
		}
		if !strings.Contains(stdout, "diagnostics:") {
			t.Errorf("stdout missing diagnostics section; got:\n%s", stdout)
		}
	})
}

func TestSalvageOutRefusedWhenExisting(t *testing.T) {
	dir := salvageFixtureDir(t)
	out := filepath.Join(t.TempDir(), "rescue.artx")
	if err := os.WriteFile(out, []byte("not empty"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := runSalvage(t, "--data", dir, "--out", out)
	if err == nil {
		t.Fatal("want error when --out already exists and is non-empty")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error = %v, want already-exists message", err)
	}
	if _, statErr := os.Stat(out + ".partial"); !os.IsNotExist(statErr) {
		t.Fatal("must not start writing a .partial file when --out is refused up front")
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "not empty" {
		t.Fatal("existing --out content must be left untouched")
	}
}

// TestSalvageStalePartialIsConsumedNotRefused pins finding I3: a killed
// prior run can leave a non-empty <out>.partial behind (store.NewWriter
// itself never got the chance to finish writing it, let alone rename it to
// --out). Before this fix, store.NewWriter's own non-empty-file refusal
// (there to protect a REAL store, i.e. --out, from accidental truncation)
// fired against that leftover .partial too, permanently blocking every
// re-run with a misleading "avoid overwriting drained data" error --
// misleading because .partial is never a source of truth to begin with. The
// fix removes any stale .partial immediately before opening it for this
// run's write, leaving the separate, real --out check (a different code
// path, still exercised by TestSalvageOutRefusedWhenExisting) untouched.
func TestSalvageStalePartialIsConsumedNotRefused(t *testing.T) {
	dir := salvageFixtureDir(t)
	out := filepath.Join(t.TempDir(), "rescue.artx")

	// Simulate a killed prior run: a non-empty .partial with garbage content,
	// no matching --out.
	if err := os.WriteFile(out+".partial", []byte("leftover from a killed run"), 0o600); err != nil {
		t.Fatal(err)
	}

	stdout, err := runSalvage(t, "--data", dir, "--out", out)
	if err != nil {
		t.Fatalf("salvage: %v\noutput:\n%s", err, stdout)
	}
	if !strings.Contains(stdout, "salvaged 512 messages to "+out) {
		t.Errorf("stdout missing success headline; got:\n%s", stdout)
	}

	fi, statErr := os.Stat(out)
	if statErr != nil || fi.Size() == 0 {
		t.Fatalf("--out should be written normally, stat err=%v", statErr)
	}
	if _, statErr := os.Stat(out + ".partial"); !os.IsNotExist(statErr) {
		t.Errorf(".partial should be consumed (renamed away) by a successful run, stat err = %v", statErr)
	}

	rd, err := store.OpenReader(out)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rd.Close()
	total := 0
	for {
		_, _, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		total++
	}
	if total != 512 {
		t.Errorf("store has %d records, want 512 (the stale .partial's garbage content must not have survived into --out)", total)
	}
}

func TestSalvageOutRequired(t *testing.T) {
	dir := salvageFixtureDir(t)
	_, err := runSalvage(t, "--data", dir)
	if err == nil {
		t.Fatal("want error when --out is not given")
	}
}

// holdFlock opens path and takes a non-blocking exclusive flock on it,
// returning a cleanup func that unlocks and closes it. flock locks attach to
// the *open file description*, not the process or the path, so a second,
// independent os.OpenFile+syscall.Flock in this same process -- exactly
// what checkLiveBroker does -- gets EWOULDBLOCK against this one. No helper
// process is required to exercise the "lock held" path.
func holdFlock(t *testing.T, path string) func() {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s to hold flock: %v", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		t.Fatalf("acquire test flock on %s: %v", path, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}
}

// TestSalvageCheckLiveBrokerLockHeld covers the primary, fixture-verified location
// (<journalDir>/server.lock): with the lock held by (this same process, a
// second independent fd) checkLiveBroker must refuse.
func TestSalvageCheckLiveBrokerLockHeld(t *testing.T) {
	journalDir := t.TempDir()
	lockPath := filepath.Join(journalDir, "server.lock")
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	release := holdFlock(t, lockPath)
	defer release()

	err := checkLiveBroker(journalDir, "")
	if err == nil {
		t.Fatal("want error when server.lock is held")
	}
	if !strings.Contains(err.Error(), "broker appears to be running") {
		t.Errorf("error = %v, want the live-broker message", err)
	}
}

// TestSalvageCheckLiveBrokerLockFileExistsButUnheld exercises the direct-call path
// explicitly: a server.lock file is present but nobody holds it, so the
// guard must pass (the fixture success tests exercise this implicitly --
// their journal dirs carry no server.lock at all -- this pins the case
// where the file exists but is free).
func TestSalvageCheckLiveBrokerLockFileExistsButUnheld(t *testing.T) {
	journalDir := t.TempDir()
	lockPath := filepath.Join(journalDir, "server.lock")
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkLiveBroker(journalDir, ""); err != nil {
		t.Fatalf("checkLiveBroker = %v, want nil (lock file present but unheld)", err)
	}
}

// TestSalvageCheckLiveBrokerNoLockAnywhere covers the case where none of the
// candidate paths exist: nothing to check against, so salvage proceeds.
func TestSalvageCheckLiveBrokerNoLockAnywhere(t *testing.T) {
	dataDir := t.TempDir()
	journalDir := filepath.Join(dataDir, "journal")
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := checkLiveBroker(journalDir, dataDir); err != nil {
		t.Fatalf("checkLiveBroker = %v, want nil (no server.lock anywhere)", err)
	}
}

// TestSalvageCheckLiveBrokerInstanceRootFallback pins finding-1's fix: the instance
// root fallback (filepath.Dir(dataDir)/server.lock) is only reachable when
// dataDir is passed in, which it wasn't before this fix. A standard broker
// layout is <instance>/data/journal alongside <instance>/server.lock.
func TestSalvageCheckLiveBrokerInstanceRootFallback(t *testing.T) {
	instanceRoot := t.TempDir()
	dataDir := filepath.Join(instanceRoot, "data")
	journalDir := filepath.Join(dataDir, "journal")
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(instanceRoot, "server.lock")
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	release := holdFlock(t, lockPath)
	defer release()

	err := checkLiveBroker(journalDir, dataDir)
	if err == nil {
		t.Fatal("want error when the instance-root server.lock is held")
	}
	if !strings.Contains(err.Error(), "broker appears to be running") {
		t.Errorf("error = %v, want the live-broker message", err)
	}
}

// TestSalvageForceBypassesLiveBrokerGuard drives the guard through the full
// `salvage` command: without --force a held journal-dir server.lock must
// refuse the run; with --force the command must proceed past the guard (it
// may still fail later for unrelated reasons against this minimal fixture,
// but that failure must not be the live-broker error).
func TestSalvageForceBypassesLiveBrokerGuard(t *testing.T) {
	dataDir := t.TempDir()
	bindings := filepath.Join(dataDir, "bindings")
	journalDir := filepath.Join(dataDir, "journal")
	if err := os.MkdirAll(bindings, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(journalDir, 0o755); err != nil {
		t.Fatal(err)
	}
	writeEmptyJournalFile(t, journalDir)

	lockPath := filepath.Join(journalDir, "server.lock")
	if err := os.WriteFile(lockPath, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	release := holdFlock(t, lockPath)
	defer release()

	out := filepath.Join(t.TempDir(), "rescue.artx")
	if _, err := runSalvage(t, "--data", dataDir, "--out", out); err == nil ||
		!strings.Contains(err.Error(), "broker appears to be running") {
		t.Fatalf("want live-broker guard error without --force, got: %v", err)
	}

	out2 := filepath.Join(t.TempDir(), "rescue.artx")
	_, err := runSalvage(t, "--data", dataDir, "--out", out2, "--force")
	if err != nil && strings.Contains(err.Error(), "broker appears to be running") {
		t.Fatalf("--force should bypass the live-broker guard, got: %v", err)
	}
}
