package store

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestStoreCoreAndAMQPRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mix.artx")
	w, err := NewWriter(path)
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	amqpRec := Record{UUID: [16]byte{1}, Queue: "a", DrainedAt: 10, Kind: KindAMQP, AMQP: []byte("amqp-bytes")}
	coreRec := Record{UUID: [16]byte{2}, Queue: "b", DrainedAt: 20, Kind: KindCore, CorePayload: []byte("core-payload")}
	for _, r := range []Record{amqpRec, coreRec} {
		if err := w.Append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := w.Sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	_ = w.Close()

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer r.Close()

	got := readAll(t, r)
	if len(got) != 2 {
		t.Fatalf("want 2 records, got %d", len(got))
	}
	if got[0].Kind != KindAMQP || string(got[0].AMQP) != "amqp-bytes" || len(got[0].CorePayload) != 0 {
		t.Errorf("AMQP record round-trip mismatch: %+v", got[0])
	}
	if got[1].Kind != KindCore || string(got[1].CorePayload) != "core-payload" || len(got[1].AMQP) != 0 {
		t.Errorf("Core record round-trip mismatch: %+v", got[1])
	}
}

// TestStoreReadsVersion1 hand-writes a legacy v1 store (no per-record Kind
// byte) and confirms the reader still decodes it, defaulting to KindAMQP.
func TestStoreReadsVersion1(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.artx")

	var buf bytes.Buffer
	buf.WriteString(Magic)
	buf.WriteByte(Version1)

	// v1 body: UUID(16) + DrainedAt(8) + qlen(2)+queue + alen(4)+amqp
	uuid := [16]byte{9}
	queue := "legacy"
	amqp := []byte("v1-amqp")
	body := append([]byte(nil), uuid[:]...)
	body = binary.BigEndian.AppendUint64(body, 7)
	body = binary.BigEndian.AppendUint16(body, uint16(len(queue)))
	body = append(body, queue...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(amqp)))
	body = append(body, amqp...)

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	buf.Write(hdr[:])
	buf.Write(body)
	_ = binary.Write(&buf, binary.BigEndian, crc32.ChecksumIEEE(body))

	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatalf("write v1 store: %v", err)
	}

	r, err := OpenReader(path)
	if err != nil {
		t.Fatalf("open v1: %v", err)
	}
	defer r.Close()
	got := readAll(t, r)
	if len(got) != 1 {
		t.Fatalf("want 1 record, got %d", len(got))
	}
	if got[0].Kind != KindAMQP || got[0].Queue != "legacy" || string(got[0].AMQP) != "v1-amqp" {
		t.Errorf("v1 record mismatch: %+v", got[0])
	}
}

func readAll(t *testing.T, r *Reader) []Record {
	t.Helper()
	var out []Record
	for {
		rec, _, err := r.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		out = append(out, rec)
	}
	return out
}
