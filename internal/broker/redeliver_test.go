// internal/broker/redeliver_test.go
package broker

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/martikan/artemisctl/internal/store"
)

// TestRedeliverStopsOnCorruptRecord proves that Redeliver detects a corrupt
// first record and returns immediately, WITHOUT ever touching the broker
// session (senderFor / c.sess.NewSender is only reached after a successful
// r.Next()). This is exercised against a zero-value &Client{} whose sess
// field is nil: if Redeliver tried to dereference c.sess before returning
// the ErrCorrupt error, this test would panic instead of failing cleanly.
func TestRedeliverStopsOnCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dump.artx")

	w, err := store.NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	rec := store.Record{
		UUID:      [16]byte{1, 2, 3, 4},
		Queue:     "orders",
		DrainedAt: 1234,
		AMQP:      []byte("some amqp payload bytes"),
	}
	if err := w.Append(rec); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	corruptRecordBody(t, path)

	c := &Client{} // zero-value: sess is nil

	var n int
	var redelivErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Redeliver panicked: %v", r)
			}
		}()
		n, redelivErr = c.Redeliver(context.Background(), path, RedeliverOpts{}, nil)
	}()

	if n != 0 {
		t.Fatalf("want count 0, got %d", n)
	}
	if redelivErr == nil {
		t.Fatal("want non-nil error, got nil")
	}
	if !errors.Is(redelivErr, store.ErrCorrupt) {
		t.Fatalf("want errors.Is(err, store.ErrCorrupt), got: %v", redelivErr)
	}
}

// corruptRecordBody flips a byte inside the body of the first record in the
// store file at path, so its CRC no longer matches and store.OpenReader(...)
// .Next() returns store.ErrCorrupt on the very first read.
//
// Layout: 5-byte header (magic + version) | 4-byte body length | body | 4-byte
// crc32. We flip a byte a few bytes into the body (well past the UUID/offset
// fields that only affect metadata) so the CRC check fails deterministically.
func corruptRecordBody(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read store file: %v", err)
	}
	const headerLen = 5
	const lenPrefixLen = 4
	bodyStart := headerLen + lenPrefixLen
	if len(data) <= bodyStart+10 {
		t.Fatalf("store file too small to corrupt: %d bytes", len(data))
	}
	idx := bodyStart + 10 // a handful of bytes into the body
	data[idx] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("rewrite store file: %v", err)
	}
}
