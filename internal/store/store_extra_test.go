// internal/store/store_extra_test.go
package store

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestWriterOffsetAdvances(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := w.Offset(); got != headerLen {
		t.Fatalf("fresh Offset = %d, want %d", got, headerLen)
	}
	rec := Record{UUID: [16]byte{1}, Queue: "orders", DrainedAt: 1, AMQP: []byte("hi")}
	if err := w.Append(rec); err != nil {
		t.Fatal(err)
	}
	wantBody := 1 + 16 + 8 + 2 + len("orders") + 4 + len("hi") // +1 for the v2 Kind byte
	if got, want := w.Offset(), int64(headerLen)+int64(4+wantBody+4); got != want {
		t.Fatalf("Offset after append = %d, want %d", got, want)
	}
}

// TestReaderSeekToResumes writes two records, reads the first to learn the
// resume offset, then opens a fresh reader, SeekTo's that offset, and confirms
// it reads only the second record — the redelivery-resume path.
func TestReaderSeekToResumes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	writeRecords(t, path, []Record{
		{UUID: [16]byte{1}, Queue: "a", DrainedAt: 1, AMQP: []byte("first")},
		{UUID: [16]byte{2}, Queue: "b", DrainedAt: 2, AMQP: []byte("second")},
	})

	r1, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	_, afterFirst, err := r1.Next()
	if err != nil {
		t.Fatalf("read first: %v", err)
	}
	r1.Close()

	r2, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if err := r2.SeekTo(afterFirst); err != nil {
		t.Fatalf("seek: %v", err)
	}
	rec, _, err := r2.Next()
	if err != nil {
		t.Fatalf("read after seek: %v", err)
	}
	if string(rec.AMQP) != "second" {
		t.Fatalf("after seek got %q, want second", rec.AMQP)
	}
	if _, _, err := r2.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("want EOF after last record, got %v", err)
	}
}

// TestSeekToClampsBelowHeader: an offset inside the header is clamped up to the
// first record so a bogus checkpoint can't land mid-header.
func TestSeekToClampsBelowHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	writeRecords(t, path, []Record{{UUID: [16]byte{1}, Queue: "a", DrainedAt: 1, AMQP: []byte("x")}})
	r, err := OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if err := r.SeekTo(0); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if rec, _, err := r.Next(); err != nil || string(rec.AMQP) != "x" {
		t.Fatalf("clamped seek read = %+v, %v", rec, err)
	}
}

func TestOpenReaderErrors(t *testing.T) {
	dir := t.TempDir()

	if _, err := OpenReader(filepath.Join(dir, "missing.artx")); err == nil {
		t.Fatal("open missing file: want error")
	}

	short := filepath.Join(dir, "short.artx")
	if err := os.WriteFile(short, []byte("AR"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(short); err == nil {
		t.Fatal("open truncated header: want error")
	}

	badMagic := filepath.Join(dir, "badmagic.artx")
	if err := os.WriteFile(badMagic, []byte("XXXX\x01"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(badMagic); err == nil {
		t.Fatal("open bad magic: want error")
	}

	badVer := filepath.Join(dir, "badver.artx")
	if err := os.WriteFile(badVer, append([]byte(Magic), 0xFF), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenReader(badVer); err == nil {
		t.Fatal("open bad version: want error")
	}
}

func TestNewWriterStatError(t *testing.T) {
	// A path whose parent is a regular file (not a directory) makes Stat fail
	// with ENOTDIR — not IsNotExist — so NewWriter must surface it, not proceed.
	dir := t.TempDir()
	file := filepath.Join(dir, "afile")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWriter(filepath.Join(file, "under.artx")); err == nil {
		t.Fatal("want stat error for path under a regular file")
	}
}

func TestNewWriterOpenError(t *testing.T) {
	// Parent directory does not exist: Stat reports IsNotExist (allowed), then
	// OpenFile fails — the open-store error branch.
	sp := filepath.Join(t.TempDir(), "no-such-dir", "s.artx")
	if _, err := NewWriter(sp); err == nil {
		t.Fatal("want open error for a nonexistent parent directory")
	}
}

func TestSyncAfterCloseErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.artx")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// fsync on the already-closed file must error rather than silently succeed.
	if err := w.Sync(); err == nil {
		t.Fatal("want error syncing a closed writer")
	}
}

func TestLoadCheckpointReadError(t *testing.T) {
	// A checkpoint path that is a directory makes ReadFile fail with a non-
	// IsNotExist error, which LoadCheckpoint must propagate.
	sp := filepath.Join(t.TempDir(), "s.artx")
	if err := os.Mkdir(ckptPath(sp), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(sp); err == nil {
		t.Fatal("want read error when checkpoint path is a directory")
	}
}

func TestLoadCheckpointBadContent(t *testing.T) {
	sp := filepath.Join(t.TempDir(), "s.artx")
	if err := os.WriteFile(ckptPath(sp), []byte("not-a-number"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckpoint(sp); err == nil {
		t.Fatal("want parse error on non-numeric checkpoint")
	}
}

func TestSaveCheckpointErrorsOnBadDir(t *testing.T) {
	// A store path whose parent directory does not exist can't be written.
	sp := filepath.Join(t.TempDir(), "no-such-dir", "s.artx")
	if err := SaveCheckpoint(sp, 1); err == nil {
		t.Fatal("want error saving into a nonexistent directory")
	}
}

// frameRecord builds a store file whose single record has a valid length prefix
// and CRC but an arbitrary body, so Next passes CRC validation and exercises the
// decodeBody consistency checks.
func frameRecord(t *testing.T, body []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "framed.artx")
	buf := append([]byte(Magic), Version)
	var lenPfx [4]byte
	binary.BigEndian.PutUint32(lenPfx[:], uint32(len(body)))
	buf = append(buf, lenPfx[:]...)
	buf = append(buf, body...)
	var crc [4]byte
	binary.BigEndian.PutUint32(crc[:], crc32.ChecksumIEEE(body))
	buf = append(buf, crc[:]...)
	if err := os.WriteFile(path, buf, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDecodeBodyRejectsInconsistentRecords(t *testing.T) {
	cases := map[string][]byte{
		// < 16+8+2 bytes: too short to hold even the fixed header.
		"too short": make([]byte, 10),
		// 26 bytes with queue length 0xFFFF: the queue can't fit in the body.
		"queue overflow": func() []byte {
			b := make([]byte, 26)
			binary.BigEndian.PutUint16(b[24:26], 0xFFFF)
			return b
		}(),
		// qlen=0 but amqp length 5 with no trailing bytes: amqp can't fit.
		"amqp overflow": func() []byte {
			b := make([]byte, 30)
			binary.BigEndian.PutUint32(b[26:30], 5)
			return b
		}(),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := frameRecord(t, body)
			r, err := OpenReader(path)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			if _, _, err := r.Next(); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("want ErrCorrupt, got %v", err)
			}
		})
	}
}
