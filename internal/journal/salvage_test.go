package journal

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/store"
)

// fixtureSalvageOptions returns Options pointing at fixtureDir(t)'s four
// sub-dirs.
func fixtureSalvageOptions(t *testing.T, dir string) Options {
	t.Helper()
	return Options{
		Bindings:      filepath.Join(dir, "bindings"),
		Journal:       filepath.Join(dir, "journal"),
		LargeMessages: filepath.Join(dir, "large-messages"),
		Paging:        filepath.Join(dir, "paging"),
	}
}

// --- golden fixture test (brief Step 1, assertions a-f) ---

func TestSalvageFixtureGolden(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)

	var records []store.Record
	summary, err := Salvage(opts, func(r store.Record) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}

	man := loadManifest(t)

	// (a) per-queue counts exactly match the manifest.
	for q, entries := range man.Queues {
		if got, want := summary.PerQueue[q], len(entries); got != want {
			t.Errorf("PerQueue[%s] = %d, want %d", q, got, want)
		}
	}
	for q := range summary.PerQueue {
		if _, ok := man.Queues[q]; !ok {
			t.Errorf("PerQueue has unexpected queue %s = %d", q, summary.PerQueue[q])
		}
	}
	if summary.Total() != 5+5+1+1+500 {
		t.Errorf("Total() = %d, want %d", summary.Total(), 5+5+1+1+500)
	}

	// (b) every record's AMQP unmarshals and its body sha256 is in the
	// manifest for that queue.
	hashesByQueue := make(map[string]map[string]bool, len(man.Queues))
	for q, entries := range man.Queues {
		set := make(map[string]bool, len(entries))
		for _, e := range entries {
			set[e.BodySha256] = true
		}
		hashesByQueue[q] = set
	}

	var scheduledRecord *store.Record
	for i := range records {
		r := &records[i]
		var am amqp.Message
		if err := am.UnmarshalBinary(r.AMQP); err != nil {
			t.Fatalf("unmarshal record AMQP (queue %s): %v", r.Queue, err)
		}
		body := am.GetData()
		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])
		set, ok := hashesByQueue[r.Queue]
		if !ok || !set[hash] {
			t.Errorf("record on queue %s has body sha256 %s not present in manifest", r.Queue, hash)
		}
		if r.Queue == "salvage.scheduled" {
			if scheduledRecord != nil {
				t.Fatalf("more than one salvage.scheduled record")
			}
			scheduledRecord = r
		}
	}

	// (c) UUIDs are unique across the run.
	seen := make(map[[16]byte]string, len(records))
	for _, r := range records {
		if prevQueue, ok := seen[r.UUID]; ok {
			t.Errorf("duplicate UUID %x: queues %s and %s", r.UUID, prevQueue, r.Queue)
		}
		seen[r.UUID] = r.Queue
	}

	// (d) scheduled record carries the annotation.
	if scheduledRecord == nil {
		t.Fatal("no salvage.scheduled record emitted")
	}
	var schedMsg amqp.Message
	if err := schedMsg.UnmarshalBinary(scheduledRecord.AMQP); err != nil {
		t.Fatalf("unmarshal scheduled record: %v", err)
	}
	gotMs, ok := schedMsg.Annotations["x-opt-delivery-time"]
	if !ok {
		t.Fatal("scheduled record missing x-opt-delivery-time annotation")
	}
	wantMs := man.Queues["salvage.scheduled"][0].ScheduledAtMs
	if fmt.Sprint(gotMs) != fmt.Sprint(wantMs) {
		t.Errorf("x-opt-delivery-time = %v, want %v", gotMs, wantMs)
	}

	// (e) Summary.HasSkips() == false.
	if summary.HasSkips() {
		t.Errorf("HasSkips() = true, want false; skips: %v", summary.Skips)
	}

	// (e1) Summary.Large == 1 (records from large-message-sourced messages).
	if got, want := summary.Large, 1; got != want {
		t.Errorf("Large = %d, want %d", got, want)
	}

	// (e2) Summary.Paged == 456 (records read from page files).
	if got, want := summary.Paged, 456; got != want {
		t.Errorf("Paged = %d, want %d", got, want)
	}

	// (f) piping the emitted records through a real store.NewWriter +
	// store.OpenReader round-trips byte-identically.
	storePath := filepath.Join(t.TempDir(), "salvage.artx")
	w, err := store.NewWriter(storePath)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, r := range records {
		if err := w.Append(r); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	rd, err := store.OpenReader(storePath)
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	defer rd.Close()

	var roundTripped []store.Record
	for {
		rec, _, err := rd.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		roundTripped = append(roundTripped, rec)
	}
	if len(roundTripped) != len(records) {
		t.Fatalf("round-tripped %d records, want %d", len(roundTripped), len(records))
	}
	for i := range records {
		got, want := roundTripped[i], records[i]
		if got.UUID != want.UUID || got.Queue != want.Queue || got.DrainedAt != want.DrainedAt || !bytes.Equal(got.AMQP, want.AMQP) {
			t.Fatalf("record %d round-trip mismatch:\n got  %+v\n want %+v", i, got, want)
		}
	}
}

// --- corrupt journal record (finding C1: corruption must be reported and
// gate the exit code, not silently dropped) ---

// TestSalvageCorruptJournalRecordReportedAsCorruption pins finding C1 at the
// journal-package level: a corrupted message-journal record must not be a
// silent data loss. Reviewer's proof was exactly this -- flipping one
// check-size byte in the fixture journal dropped a message with zero output
// about the corruption and exit 0. This test reuses file_test.go's own
// technique (scanCleanSpans + flipping the last byte of a record's trailing
// check-size int, TestReadJournalDirCorruptRecordResyncs) but drives it
// through the full Salvage pipeline rather than just ReadJournalDir, and
// asserts the corruption surfaces in Summary.Diags as a corruption-class
// (HasCorruption) entry.
func TestSalvageCorruptJournalRecordReportedAsCorruption(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)

	// Corrupt a fresh copy of just the message-journal sub-dir; Bindings/
	// LargeMessages/Paging stay pointed at the original, untouched fixture
	// (Salvage only reads them).
	corruptedJournalDir := t.TempDir()
	entries, err := os.ReadDir(opts.Journal)
	if err != nil {
		t.Fatalf("read journal dir: %v", err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(opts.Journal, e.Name()))
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if err := os.WriteFile(filepath.Join(corruptedJournalDir, e.Name()), data, 0o644); err != nil {
			t.Fatalf("write %s: %v", e.Name(), err)
		}
	}

	targetPath := filepath.Join(corruptedJournalDir, "activemq-data-1.amq")
	data, err := os.ReadFile(targetPath)
	if err != nil {
		t.Fatalf("read copied journal file: %v", err)
	}
	spans := scanCleanSpans(t, data)
	if len(spans) < 2 {
		t.Fatalf("need >= 2 records to corrupt one and keep survivors, got %d", len(spans))
	}
	target := spans[1]
	corruptOffset := target.end - 1 // last byte of the trailing check-size int
	data[corruptOffset] ^= 0xFF
	if err := os.WriteFile(targetPath, data, 0o644); err != nil {
		t.Fatalf("write corrupted journal file: %v", err)
	}

	opts.Journal = corruptedJournalDir

	summary, err := Salvage(opts, func(store.Record) error { return nil })
	if err != nil {
		t.Fatalf("Salvage: %v (a corrupted record must resync past, not abort the run)", err)
	}

	// The corruption must not be silently lost: it must show up as a Diags
	// entry classified corruption-class (gates exit like a skip).
	if !summary.HasCorruption() {
		t.Errorf("HasCorruption() = false, want true; diags: %v", summary.Diags)
	}
	foundDiag := false
	for _, d := range summary.Diags {
		if strings.Contains(d, "message journal") && strings.Contains(d, "check-size mismatch") {
			foundDiag = true
		}
	}
	if !foundDiag {
		t.Errorf("Diags missing a check-size-mismatch entry for the corrupted record: %v", summary.Diags)
	}

	// The corrupted record is the fixture's message-id-38 ADD_REF: its only
	// surviving queue ref, so losing it drops message 38 entirely (zero
	// remaining refs, see message.go's DecodeMessages/decodeRefs). Verified
	// empirically against this exact fixture: 511, one fewer than the
	// clean-fixture baseline of 512 (TestSalvageFixtureGolden). Asserted as
	// an exact count, not just "<= 512", since the whole point of this test
	// is that the loss is real and precisely accounted for, not merely
	// "not worse than before".
	if got, want := summary.Total(), 511; got != want {
		t.Errorf("Total() = %d, want %d (clean-fixture baseline 512 minus the one message whose only surviving ref was corrupted)", got, want)
	}
}

// --- fatal dir errors (spec §5: missing bindings/journal is fatal) ---

func TestSalvageMissingBindingsDirFatal(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)
	opts.Bindings = filepath.Join(dir, "does-not-exist")

	_, err := Salvage(opts, func(store.Record) error { return nil })
	if err == nil {
		t.Fatal("want error for missing bindings dir, got nil")
	}
}

func TestSalvageMissingJournalDirFatal(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)
	opts.Journal = filepath.Join(dir, "does-not-exist")

	_, err := Salvage(opts, func(store.Record) error { return nil })
	if err == nil {
		t.Fatal("want error for missing journal dir, got nil")
	}
}

// --- non-fatal missing large-messages/paging dirs (spec §5: fine, warn only
// if referenced) ---

func TestSalvageMissingLargeMessagesDirWarnsWhenReferenced(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)
	opts.LargeMessages = filepath.Join(dir, "does-not-exist")

	summary, err := Salvage(opts, func(store.Record) error { return nil })
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}
	foundWarn := false
	for _, d := range summary.Diags {
		if strings.Contains(d, "large-messages") {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("Diags missing large-messages-dir-missing warning: %v", summary.Diags)
	}
	// The fixture's one large message can no longer be attached, so it must
	// show up as a skip (missing-file), and HasSkips() must be true.
	if !summary.HasSkips() {
		t.Errorf("HasSkips() = false, want true (large message file unreachable): skips=%v", summary.Skips)
	}
}

func TestSalvageMissingPagingDirWarnsWhenReferenced(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)
	opts.Paging = filepath.Join(dir, "does-not-exist")

	summary, err := Salvage(opts, func(store.Record) error { return nil })
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}
	foundWarn := false
	for _, d := range summary.Diags {
		if strings.Contains(d, "paging") {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Errorf("Diags missing paging-dir-missing warning: %v", summary.Diags)
	}
	// 456 of the 500 salvage.paged messages live only in page files; with
	// the paging dir gone, salvage.paged should be short by exactly that
	// many (only the 44 journal-resident ones survive).
	if got, want := summary.PerQueue["salvage.paged"], 44; got != want {
		t.Errorf("PerQueue[salvage.paged] = %d, want %d", got, want)
	}
}

// --- emit errors abort ---

func TestSalvageEmitErrorAborts(t *testing.T) {
	dir := fixtureDir(t)
	opts := fixtureSalvageOptions(t, dir)

	wantErr := fmt.Errorf("sink closed")
	n := 0
	_, err := Salvage(opts, func(store.Record) error {
		n++
		return wantErr
	})
	if err == nil {
		t.Fatal("want error from aborted emit, got nil")
	}
	if n != 1 {
		t.Errorf("emit called %d times, want exactly 1 (abort on first error)", n)
	}
}

// --- Summary helper methods ---

func TestSummaryTotalAndHasSkips(t *testing.T) {
	s := Summary{PerQueue: map[string]int{"a": 2, "b": 3}}
	if s.Total() != 5 {
		t.Errorf("Total() = %d, want 5", s.Total())
	}
	if s.HasSkips() {
		t.Error("HasSkips() = true, want false")
	}
	s.Skips = append(s.Skips, "something skipped: 1")
	if !s.HasSkips() {
		t.Error("HasSkips() = false, want true")
	}
}
