// internal/store/reader_test.go
package store

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeRecords(t *testing.T, path string, recs []Record) {
	t.Helper()
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReaderRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	in := []Record{
		{UUID: [16]byte{1}, Queue: "orders", DrainedAt: 10, AMQP: []byte("a")},
		{UUID: [16]byte{2}, Queue: "payments", DrainedAt: 20, AMQP: []byte("bb")},
	}
	writeRecords(t, path, in)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var got []Record
	for {
		rec, _, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		got = append(got, rec)
	}
	if len(got) != 2 || got[0].Queue != "orders" || string(got[1].AMQP) != "bb" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestReaderDetectsTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	writeRecords(t, path, []Record{{UUID: [16]byte{1}, Queue: "orders", DrainedAt: 10, AMQP: []byte("hello")}})
	// Chop the last 3 bytes to simulate a crash mid-write.
	b, _ := os.ReadFile(path)
	_ = os.WriteFile(path, b[:len(b)-3], 0o600)

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, _, err = r.Next()
	if !errors.Is(err, ErrCorrupt) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected corruption/truncation error, got %v", err)
	}
}

// TestReaderRejectsBogusLengthPrefix guards against a single-byte corruption
// of the (uncovered by CRC) length prefix causing an oversized allocation
// (e.g. 0xFFFFFFFF ~ 4GB) instead of a graceful ErrCorrupt.
func TestReaderRejectsBogusLengthPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	writeRecords(t, path, []Record{{UUID: [16]byte{1}, Queue: "orders", DrainedAt: 10, AMQP: []byte("hello")}})

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The 4-byte length prefix of the first record starts right after the
	// 5-byte header (offset 5). Corrupt it to an implausibly large value.
	binary.BigEndian.PutUint32(b[5:9], 0xFFFFFFFF)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	done := make(chan struct{})
	var nextErr error
	go func() {
		_, _, nextErr = r.Next()
		close(done)
	}()

	select {
	case <-done:
		if !errors.Is(nextErr, ErrCorrupt) {
			t.Fatalf("expected ErrCorrupt, got %v", nextErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next() did not return promptly; likely attempted an oversized allocation")
	}
}

// TestReaderDetectsCRCMismatch flips a byte inside a record body while leaving
// the length prefix and stored CRC intact, so Next must reject it on the CRC
// check rather than decode a silently corrupted record.
func TestReaderDetectsCRCMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	writeRecords(t, path, []Record{{UUID: [16]byte{1}, Queue: "orders", DrainedAt: 10, AMQP: []byte("hello")}})

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Body begins after the 5-byte header + 4-byte length prefix (offset 9).
	// Flip a body byte; the trailing CRC still matches the original body.
	b[9] ^= 0xFF
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, _, err := r.Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("want ErrCorrupt on CRC mismatch, got %v", err)
	}
}

// TestSeekToErrorOnClosedFile exercises SeekTo's seek-error branch: seeking a
// closed file fails.
func TestSeekToErrorOnClosedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	writeRecords(t, path, []Record{{UUID: [16]byte{1}, Queue: "orders", DrainedAt: 10, AMQP: []byte("hello")}})
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.SeekTo(headerLen); err == nil {
		t.Fatal("SeekTo on a closed reader = nil, want seek error")
	}
}
