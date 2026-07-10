package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// ErrCorrupt wraps any record that fails to decode — a bad length prefix, a
// CRC mismatch, or a truncated tail. A caller that hits it has read every
// intact record up to that offset.
var ErrCorrupt = errors.New("store: corrupt or truncated record")

// Reader streams records out of a WAL store file in order. It validates the
// header on open and each record's CRC on read; call Next until io.EOF, or
// SeekTo an offset first to resume. A Reader is not safe for concurrent use.
type Reader struct {
	f      *os.File
	offset int64
	size   int64
}

// OpenReader opens path and verifies the store magic and version, returning a
// Reader positioned just after the header.
func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	hdr := make([]byte, headerLen)
	if _, err := io.ReadFull(f, hdr); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("read header: %w", err)
	}
	if string(hdr[:4]) != Magic || hdr[4] != Version {
		_ = f.Close()
		return nil, fmt.Errorf("bad store header (magic %q version %d)", hdr[:4], hdr[4])
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat store: %w", err)
	}
	return &Reader{f: f, offset: headerLen, size: fi.Size()}, nil
}

// SeekTo positions the reader at offset so the next Next reads the record
// there; it is used to resume a redelivery from a saved checkpoint. An offset
// below the header is clamped to the first record.
func (r *Reader) SeekTo(offset int64) error {
	if offset < headerLen {
		offset = headerLen
	}
	if _, err := r.f.Seek(offset, io.SeekStart); err != nil {
		return err
	}
	r.offset = offset
	return nil
}

// Next reads and returns the record at the current position, along with the
// offset just past it (the value to checkpoint after redelivering it). It
// returns io.EOF at a clean end of file, or an error wrapping ErrCorrupt if the
// record is truncated or its CRC fails.
func (r *Reader) Next() (Record, int64, error) {
	var lenBuf [4]byte
	n, err := io.ReadFull(r.f, lenBuf[:])
	if n == 0 && errors.Is(err, io.EOF) {
		return Record{}, r.offset, io.EOF
	}
	if err != nil {
		return Record{}, r.offset, fmt.Errorf("%w: length prefix: %w", ErrCorrupt, err)
	}
	bodyLen := binary.BigEndian.Uint32(lenBuf[:])
	maxBody := r.size - (r.offset + 4) - 4
	if maxBody < 0 || int64(bodyLen) > maxBody {
		return Record{}, r.offset, fmt.Errorf("%w: record length %d exceeds remaining file bytes", ErrCorrupt, bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r.f, body); err != nil {
		return Record{}, r.offset, fmt.Errorf("%w: body: %w", ErrCorrupt, err)
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(r.f, crcBuf[:]); err != nil {
		return Record{}, r.offset, fmt.Errorf("%w: crc: %w", ErrCorrupt, err)
	}
	if binary.BigEndian.Uint32(crcBuf[:]) != crc32.ChecksumIEEE(body) {
		return Record{}, r.offset, fmt.Errorf("%w: crc mismatch at offset %d", ErrCorrupt, r.offset)
	}

	rec, err := decodeBody(body)
	if err != nil {
		return Record{}, r.offset, fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	r.offset += int64(4 + bodyLen + 4)
	return rec, r.offset, nil
}

func decodeBody(body []byte) (Record, error) {
	if len(body) < 16+8+2 {
		return Record{}, errors.New("body too short")
	}
	var rec Record
	copy(rec.UUID[:], body[:16])
	p := 16
	rec.DrainedAt = int64(binary.BigEndian.Uint64(body[p : p+8]))
	p += 8
	qlen := int(binary.BigEndian.Uint16(body[p : p+2]))
	p += 2
	if p+qlen+4 > len(body) {
		return Record{}, errors.New("queue length overflow")
	}
	rec.Queue = string(body[p : p+qlen])
	p += qlen
	alen := int(binary.BigEndian.Uint32(body[p : p+4]))
	p += 4
	if p+alen != len(body) {
		return Record{}, errors.New("amqp length overflow")
	}
	rec.AMQP = append([]byte(nil), body[p:p+alen]...)
	return rec, nil
}

// Close closes the underlying store file.
func (r *Reader) Close() error { return r.f.Close() }
