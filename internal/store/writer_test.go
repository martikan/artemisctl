// internal/store/writer_test.go
package store

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWriterWritesHeaderAndRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	rec := Record{UUID: [16]byte{1, 2, 3}, Queue: "orders", DrainedAt: 42, AMQP: []byte("hello")}
	if err := w.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	b, _ := os.ReadFile(path)
	if string(b[:4]) != Magic {
		t.Fatalf("missing magic, got %q", b[:4])
	}
	if b[4] != Version {
		t.Fatalf("bad version %d", b[4])
	}
	if len(b) <= 5 {
		t.Fatalf("record not written")
	}
}

func TestWriterEncodesRecordBytesExactly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}

	wantUUID := [16]byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	wantQueue := "orders"
	wantDrainedAt := int64(42)
	wantAMQP := []byte("hello")

	rec := Record{UUID: wantUUID, Queue: wantQueue, DrainedAt: wantDrainedAt, AMQP: wantAMQP}
	if err := w.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	if string(b[:4]) != Magic {
		t.Fatalf("bad magic: got %q", b[:4])
	}
	if b[4] != Version {
		t.Fatalf("bad version: got %d", b[4])
	}

	// v2 body leads with a 1-byte Kind, then the v1 layout.
	wantBodyLen := 1 + 16 + 8 + 2 + len(wantQueue) + 4 + len(wantAMQP)

	totalLen := binary.BigEndian.Uint32(b[5:9])
	if int(totalLen) != wantBodyLen {
		t.Fatalf("bad totalLen: got %d, want %d", totalLen, wantBodyLen)
	}

	fullBody := b[9 : 9+int(totalLen)]

	if fullBody[0] != KindAMQP {
		t.Fatalf("bad kind: got %d, want %d", fullBody[0], KindAMQP)
	}
	body := fullBody[1:]

	gotUUID := body[0:16]
	if !bytes.Equal(gotUUID, wantUUID[:]) {
		t.Fatalf("bad uuid: got %x, want %x", gotUUID, wantUUID)
	}

	gotDrainedAt := int64(binary.BigEndian.Uint64(body[16:24]))
	if gotDrainedAt != wantDrainedAt {
		t.Fatalf("bad drainedAt: got %d, want %d", gotDrainedAt, wantDrainedAt)
	}

	queueLen := binary.BigEndian.Uint16(body[24:26])
	if int(queueLen) != len(wantQueue) {
		t.Fatalf("bad queueLen: got %d, want %d", queueLen, len(wantQueue))
	}

	queueStart := 26
	queueEnd := queueStart + int(queueLen)
	gotQueue := string(body[queueStart:queueEnd])
	if gotQueue != wantQueue {
		t.Fatalf("bad queue: got %q, want %q", gotQueue, wantQueue)
	}

	amqpLenStart := queueEnd
	amqpLenEnd := amqpLenStart + 4
	amqpLen := binary.BigEndian.Uint32(body[amqpLenStart:amqpLenEnd])
	if int(amqpLen) != len(wantAMQP) {
		t.Fatalf("bad amqpLen: got %d, want %d", amqpLen, len(wantAMQP))
	}

	amqpStart := amqpLenEnd
	amqpEnd := amqpStart + int(amqpLen)
	gotAMQP := body[amqpStart:amqpEnd]
	if !bytes.Equal(gotAMQP, wantAMQP) {
		t.Fatalf("bad amqp: got %q, want %q", gotAMQP, wantAMQP)
	}

	crcStart := 9 + int(totalLen)
	crcEnd := crcStart + 4
	gotCRC := binary.BigEndian.Uint32(b[crcStart:crcEnd])
	wantCRC := crc32.ChecksumIEEE(fullBody)
	if gotCRC != wantCRC {
		t.Fatalf("bad crc: got %d, want %d", gotCRC, wantCRC)
	}

	wantFileSize := 5 + 4 + int(totalLen) + 4
	if len(b) != wantFileSize {
		t.Fatalf("bad file size: got %d, want %d", len(b), wantFileSize)
	}
}

// TestNewWriterRefusesNonEmptyExisting is the C2 regression: re-running
// `export --out X` after a crash must not truncate an already-populated store
// (permanent loss of drained-and-acked records). NewWriter must error and
// leave the file byte-for-byte intact.
func TestNewWriterRefusesNonEmptyExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "existing.artx")
	original := []byte("ARTX\x01already-drained-records-here")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("seed file: %v", err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat before: %v", err)
	}

	w, err := NewWriter(path)
	if err == nil {
		_ = w.Close()
		t.Fatal("want error opening a non-empty existing store, got nil")
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after: %v", err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("store was truncated: size %d -> %d", before.Size(), after.Size())
	}
	got, _ := os.ReadFile(path)
	if !bytes.Equal(got, original) {
		t.Fatalf("store contents modified: got %q want %q", got, original)
	}
}

// TestNewWriterAcceptsFreshAndZeroLength verifies NewWriter succeeds and writes
// the header for a missing path and for a pre-existing zero-length file (both
// safe to initialize).
func TestNewWriterAcceptsFreshAndZeroLength(t *testing.T) {
	t.Run("fresh path", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "fresh.artx")
		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("new writer on fresh path: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		b, _ := os.ReadFile(path)
		if len(b) != headerLen || string(b[:4]) != Magic || b[4] != Version {
			t.Fatalf("header not written correctly: %q", b)
		}
	})
	t.Run("zero-length existing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "empty.artx")
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			t.Fatalf("seed empty: %v", err)
		}
		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("new writer on zero-length file: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		b, _ := os.ReadFile(path)
		if len(b) != headerLen || string(b[:4]) != Magic || b[4] != Version {
			t.Fatalf("header not written correctly: %q", b)
		}
	})
}

func TestAppendRejectsOverlongQueueName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	defer w.Close()

	rec := Record{
		UUID:      [16]byte{1, 2, 3},
		Queue:     strings.Repeat("q", 65536),
		DrainedAt: 1,
		AMQP:      []byte("x"),
	}
	if err := w.Append(rec); err == nil {
		t.Fatalf("expected error for over-length queue name, got nil")
	}
}

// TestWriterCloseErrorOnClosedFile forces Close's flush-error branch by closing
// the underlying file out from under the Writer: the buffered header can then no
// longer be flushed. (TestSyncAfterCloseErrors already covers Sync's branch, but
// only after a clean Close, so Close's own error path is otherwise unexercised.)
func TestWriterCloseErrorOnClosedFile(t *testing.T) {
	w, err := NewWriter(filepath.Join(t.TempDir(), "s.artx"))
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := w.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err == nil {
		t.Error("Sync on a closed file = nil, want flush error")
	}
	if err := w.Close(); err == nil {
		t.Error("Close on a closed file = nil, want flush error")
	}
}
