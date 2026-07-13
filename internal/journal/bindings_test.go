package journal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadQueueBindings(t *testing.T) {
	dir := fixtureDir(t)
	bindingsDir := filepath.Join(dir, "bindings")

	got, diags, err := ReadQueueBindings(bindingsDir)
	if err != nil {
		t.Fatalf("ReadQueueBindings: %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("want 0 diags over the clean fixture, got %d: %+v", len(diags), diags)
	}

	names := make(map[string]bool, len(got))
	for _, name := range got {
		names[name] = true
	}

	want := []string{
		"salvage.plain",
		"salvage.props",
		"salvage.scheduled",
		"salvage.large",
		"salvage.paged",
		"salvage.acked",
	}
	for _, q := range want {
		if !names[q] {
			t.Errorf("want queue %q present in bindings, got %v", q, got)
		}
	}
}

func TestReadQueueBindingsIgnoresNonQueueBindingRecords(t *testing.T) {
	dir := fixtureDir(t)
	bindingsDir := filepath.Join(dir, "bindings")

	got, _, err := ReadQueueBindings(bindingsDir)
	if err != nil {
		t.Fatalf("ReadQueueBindings: %v", err)
	}

	// Every value must be a plausible queue name, never empty -- a
	// non-QUEUE_BINDING_RECORD accidentally decoded as one would typically
	// produce an empty or garbage name.
	for id, name := range got {
		if name == "" {
			t.Errorf("queue id %d decoded to empty name", id)
		}
	}
}

// TestReadQueueBindingsCorruptBindingBodySkipsOneAndSurvives pins finding I2:
// a single binding record whose body fails to decode (e.g. a garbled
// length-prefix byte) must not abort ReadQueueBindings for the whole
// bindings journal -- only that one binding is skipped (reported as a
// corruption-class FileDiag), and every other binding still resolves
// normally. Reviewer's proof used the exact same technique: garbling one
// queueName length-prefix byte on binding id 3 killed the entire salvage run
// before this fix ("decode queue binding id 3: ... truncated at offset 4").
func TestReadQueueBindingsCorruptBindingBodySkipsOneAndSurvives(t *testing.T) {
	dir := fixtureDir(t)
	srcPath := filepath.Join(dir, "bindings", "activemq-bindings-1.bindings")
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read fixture file: %v", err)
	}

	// Locate QUEUE_BINDING_RECORD id=3's body and garble its first byte --
	// the high byte of the queueName SimpleString's 4-byte big-endian
	// byte-length prefix (format_notes.md section 6) -- without touching the
	// record's framing/checkSize (only body content changes, not any
	// length field the framing layer itself checks). This makes
	// decodeQueueBinding's SimpleString length read implausible (negative or
	// wildly oversized), failing deep inside the body rather than at the
	// framing layer -- exactly the class of damage this fix targets.
	const bindingID3RecordStart = 117 // fixture-verified offset of id=3's ADD_RECORD_TX (activemq-bindings-1.bindings)
	const fixedPrefixLen = 27         // type(1)+fileIDEcho(4)+compactCount(1)+txID(8)+recordID(8)+variableSize(4)+userType(1), precedes the body (id=3 is journaled inside a transaction, hence the extra txID field vs. a plain ADD_RECORD)
	corruptOffset := bindingID3RecordStart + fixedPrefixLen

	corrupted := append([]byte(nil), data...)
	corrupted[corruptOffset] ^= 0xFF

	dstDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dstDir, "activemq-bindings-1.bindings"), corrupted, 0o644); err != nil {
		t.Fatalf("write corrupted bindings file: %v", err)
	}
	// Copy the fixture's second bindings file unmodified so its bindings
	// still participate normally alongside the corrupted file's survivors.
	if data2, err := os.ReadFile(filepath.Join(dir, "bindings", "activemq-bindings-2.bindings")); err == nil {
		if err := os.WriteFile(filepath.Join(dstDir, "activemq-bindings-2.bindings"), data2, 0o644); err != nil {
			t.Fatalf("copy activemq-bindings-2.bindings: %v", err)
		}
	}

	got, diags, err := ReadQueueBindings(dstDir)
	if err != nil {
		t.Fatalf("ReadQueueBindings: %v (want the run to survive one garbled binding, not abort)", err)
	}

	if name, stillPresent := got[3]; stillPresent {
		t.Errorf("binding id 3 should have been skipped after body corruption, got %q", name)
	}
	// Every other binding in the fixture (fixture-verified ids from
	// TestZZ-equivalent manual inspection of activemq-bindings-1.bindings)
	// must survive untouched.
	for _, id := range []int64{7, 13, 35, 45, 55, 61, 67, 82} {
		if _, ok := got[id]; !ok {
			t.Errorf("binding id %d missing after unrelated corruption; got %v", id, got)
		}
	}

	foundDiag := false
	for _, d := range diags {
		if strings.Contains(d.Reason, "decode queue binding id 3") {
			foundDiag = true
			if !d.Corrupt {
				t.Errorf("diag for binding id 3 has Corrupt=false, want true (structural bindings-record damage): %+v", d)
			}
		}
	}
	if !foundDiag {
		t.Errorf("diags missing entry for binding id 3: %+v", diags)
	}
}
