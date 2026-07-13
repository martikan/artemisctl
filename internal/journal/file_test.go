package journal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadJournalDirMessageJournal(t *testing.T) {
	dir := fixtureDir(t)
	journalDir := filepath.Join(dir, "journal")

	var records []RawRecord
	diags, err := ReadJournalDir(journalDir, "activemq-data", "amq", func(r RawRecord) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("want 0 diags over the clean fixture, got %d: %+v", len(diags), diags)
	}
	if len(records) == 0 {
		t.Fatalf("want > 0 records, got 0")
	}

	var sawAddMessageProtocol, sawAcknowledgeRef bool
	for _, r := range records {
		switch r.UserType {
		case AddMessageProtocol:
			sawAddMessageProtocol = true
		case AcknowledgeRef:
			sawAcknowledgeRef = true
		}
	}
	if !sawAddMessageProtocol {
		t.Errorf("want at least one record with UserType == AddMessageProtocol")
	}
	if !sawAcknowledgeRef {
		t.Errorf("want at least one record with UserType == AcknowledgeRef")
	}
}

func TestReadJournalDirBindingsJournal(t *testing.T) {
	dir := fixtureDir(t)
	bindingsDir := filepath.Join(dir, "bindings")

	var records []RawRecord
	diags, err := ReadJournalDir(bindingsDir, "activemq-bindings", "bindings", func(r RawRecord) error {
		records = append(records, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("want 0 diags over the clean fixture, got %d: %+v", len(diags), diags)
	}

	var queueBindings int
	for _, r := range records {
		if r.UserType == QueueBindingRecord {
			queueBindings++
		}
	}
	if queueBindings == 0 {
		t.Errorf("want >= 1 QueueBindingRecord, got 0")
	}
}

// recordSpan is a [start, end) byte range of one cleanly-parsed record,
// gathered by re-walking a file with the package's own framing logic. Tests
// use this instead of hand-decoded fixture hex offsets so corruption
// injection doesn't depend on manually tracking real broker output byte-for-
// byte.
type recordSpan struct{ start, end int }

func scanCleanSpans(t *testing.T, data []byte) []recordSpan {
	t.Helper()
	hdr := newReader(data[:sizeHeader])
	hdr.i32() // formatVersion
	hdr.i32() // userVersion
	fileID := hdr.i64()
	if hdr.err() != nil {
		t.Fatalf("scanCleanSpans: reading header: %v", hdr.err())
	}
	fileIDEcho := int32(fileID)

	var spans []recordSpan
	pos := sizeHeader
	for pos < len(data) {
		typeByte := data[pos]
		if typeByte < EventRecord || typeByte > RollbackRecord {
			pos++
			continue
		}
		_, consumed, reason, _ := parseRecord(data, pos, fileIDEcho, typeByte)
		if reason != "" {
			t.Fatalf("scanCleanSpans: fixture file did not parse cleanly at offset %d: %s", pos, reason)
		}
		spans = append(spans, recordSpan{start: pos, end: pos + consumed})
		pos += consumed
	}
	return spans
}

func copyFile(t *testing.T, dstDir, name string, data []byte) string {
	t.Helper()
	dst := filepath.Join(dstDir, name)
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
	return dst
}

func TestReadJournalDirCorruptRecordResyncs(t *testing.T) {
	dir := fixtureDir(t)
	srcPath := filepath.Join(dir, "journal", "activemq-data-1.amq")
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read fixture file: %v", err)
	}

	spans := scanCleanSpans(t, data)
	if len(spans) < 2 {
		t.Fatalf("need >= 2 records in activemq-data-1.amq to exercise resync-with-survivors, got %d", len(spans))
	}
	target := spans[1] // leave spans[0] as a pre-corruption survivor

	corrupted := append([]byte(nil), data...)
	corruptOffset := target.end - 1 // last byte of the trailing check-size int
	corrupted[corruptOffset] ^= 0xFF

	dstDir := t.TempDir()
	dstPath := copyFile(t, dstDir, "activemq-data-1.amq", corrupted)

	var got []RawRecord
	diags, err := ReadJournalDir(dstDir, "activemq-data", "amq", func(r RawRecord) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(diags) != 1 {
		t.Fatalf("want exactly 1 diag for one corrupted record, got %d: %+v", len(diags), diags)
	}
	if diags[0].Path != dstPath {
		t.Errorf("diag path = %q, want %q", diags[0].Path, dstPath)
	}
	if diags[0].Offset != int64(target.start) {
		t.Errorf("diag offset = %d, want %d", diags[0].Offset, target.start)
	}
	if diags[0].Reason == "" {
		t.Errorf("diag reason is empty")
	}
	if len(got) != len(spans)-1 {
		t.Errorf("want %d surviving records (all but the corrupted one), got %d", len(spans)-1, len(got))
	}
	if len(got) == 0 {
		t.Fatalf("want records-before-corruption still emitted, got none")
	}
}

func TestReadJournalDirTruncatedRecordNoPanic(t *testing.T) {
	dir := fixtureDir(t)
	srcPath := filepath.Join(dir, "journal", "activemq-data-1.amq")
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read fixture file: %v", err)
	}

	spans := scanCleanSpans(t, data)
	if len(spans) < 2 {
		t.Fatalf("need >= 2 records in activemq-data-1.amq to exercise truncation-with-survivors, got %d", len(spans))
	}
	target := spans[1]
	mid := target.start + (target.end-target.start)/2
	if mid <= target.start || mid >= target.end {
		t.Fatalf("target record too small to truncate mid-record: %+v", target)
	}
	truncated := append([]byte(nil), data[:mid]...)

	dstDir := t.TempDir()
	dstPath := copyFile(t, dstDir, "activemq-data-1.amq", truncated)

	var got []RawRecord
	diags, err := ReadJournalDir(dstDir, "activemq-data", "amq", func(r RawRecord) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(diags) != 1 {
		t.Fatalf("want exactly 1 diag for the truncated record, got %d: %+v", len(diags), diags)
	}
	if diags[0].Path != dstPath {
		t.Errorf("diag path = %q, want %q", diags[0].Path, dstPath)
	}
	if diags[0].Offset != int64(target.start) {
		t.Errorf("diag offset = %d, want %d", diags[0].Offset, target.start)
	}
	if len(got) == 0 {
		t.Fatalf("want records-before-truncation still emitted, got none")
	}
}

func TestReadJournalDirWrongFormatVersionErrors(t *testing.T) {
	dir := fixtureDir(t)
	srcPath := filepath.Join(dir, "journal", "activemq-data-1.amq")
	data, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatalf("read fixture file: %v", err)
	}

	patched := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(patched[0:4], 99) // formatVersion := 99

	dstDir := t.TempDir()
	copyFile(t, dstDir, "activemq-data-1.amq", patched)

	_, err = ReadJournalDir(dstDir, "activemq-data", "amq", func(RawRecord) error { return nil })
	if err == nil {
		t.Fatalf("want an error for a mismatched format version, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "99") {
		t.Errorf("error %q does not mention the file's version (99)", msg)
	}
	if !strings.Contains(msg, "2") {
		t.Errorf("error %q does not mention the expected version (2)", msg)
	}
}
