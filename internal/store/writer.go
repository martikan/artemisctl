package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
)

// Writer appends records to a WAL store file. It buffers writes and tracks the
// current byte offset; call Sync to make appended records durable and Close
// when done. A Writer is not safe for concurrent use.
type Writer struct {
	f      *os.File
	buf    *bufio.Writer
	offset int64
}

// NewWriter opens path for appending and writes the file header if the file is
// new or empty. It refuses to open an existing non-empty store, so re-running a
// drain after a crash cannot silently overwrite already-drained records.
func NewWriter(path string) (*Writer, error) {
	// Refuse to overwrite an existing non-empty store. Re-running
	// `export --out X` after a crash is the natural recovery action; with
	// O_TRUNC that would permanently wipe already-drained-and-acked records
	// (data loss). A zero-length or missing file is safe to (re)initialize.
	if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
		return nil, fmt.Errorf("store %s already exists and is non-empty; choose a new path to avoid overwriting drained data", path)
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat store: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}
	w := &Writer{f: f, buf: bufio.NewWriter(f)}
	if _, err := w.buf.WriteString(Magic); err != nil {
		return nil, err
	}
	if err := w.buf.WriteByte(Version); err != nil {
		return nil, err
	}
	w.offset = headerLen
	return w, nil
}

// Append encodes r and writes it to the buffered stream: a length prefix, the
// record body (UUID, drain time, queue, AMQP bytes), and a CRC32 over the body.
// It does not flush; the record is not durable until Sync. It errors if the
// queue name or payload exceeds the format's length limits.
func (w *Writer) Append(r Record) error {
	if len(r.Queue) > math.MaxUint16 {
		return fmt.Errorf("store: queue name too long: %d bytes (max 65535)", len(r.Queue))
	}
	if len(r.AMQP) > math.MaxUint32 {
		return fmt.Errorf("store: amqp payload too long: %d bytes", len(r.AMQP))
	}

	body := make([]byte, 0, 16+8+2+len(r.Queue)+4+len(r.AMQP))
	body = append(body, r.UUID[:]...)
	body = binary.BigEndian.AppendUint64(body, uint64(r.DrainedAt))
	body = binary.BigEndian.AppendUint16(body, uint16(len(r.Queue)))
	body = append(body, r.Queue...)
	body = binary.BigEndian.AppendUint32(body, uint32(len(r.AMQP)))
	body = append(body, r.AMQP...)

	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	crc := crc32.ChecksumIEEE(body)

	if _, err := w.buf.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.buf.Write(body); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.BigEndian, crc); err != nil {
		return err
	}
	w.offset += int64(4 + len(body) + 4)
	return nil
}

// Sync flushes the buffer and fsyncs the file, making every appended record
// durable on disk. The drain loop calls this before acking messages on the
// broker so a crash never loses an acked message.
func (w *Writer) Sync() error {
	if err := w.buf.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Offset returns the byte offset just past the last appended record — the point
// a reader would resume from. It reflects buffered (not necessarily synced)
// writes.
func (w *Writer) Offset() int64 { return w.offset }

// Close flushes any buffered records and closes the file. It does not fsync;
// call Sync first if the final records must be durable.
func (w *Writer) Close() error {
	if err := w.buf.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}
