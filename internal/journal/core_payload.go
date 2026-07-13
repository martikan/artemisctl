package journal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Core payload store encoding.
//
// A decoded Core message (CorePayload) is serialized into an opaque byte blob
// that the .artx store carries verbatim (store.Record.CorePayload, Kind=Core)
// and broker/coreconvert reads back to build an amqp.Message for redelivery.
// The encoding is little-endian to match the store's own convention (the
// on-disk Artemis journal is big-endian; this is our format, not Artemis's).
//
// Property values preserve the Artemis TypedProperties value types (bool,
// byte, int16/32/64, float32/64, string, []byte) via a one-byte type tag so
// the AMQP conversion can map each to the right application-property type.

// core payload value type tags (independent of Artemis DataConstants ids).
const (
	cpvBool byte = iota
	cpvByte
	cpvInt16
	cpvInt32
	cpvInt64
	cpvFloat32
	cpvFloat64
	cpvString
	cpvBytes
	cpvNull
)

// coreWriter is a little-endian append buffer.
type coreWriter struct{ b []byte }

func (w *coreWriter) u8(v byte)   { w.b = append(w.b, v) }
func (w *coreWriter) u16(v int)   { w.b = binary.LittleEndian.AppendUint16(w.b, uint16(v)) }
func (w *coreWriter) u32(v int)   { w.b = binary.LittleEndian.AppendUint32(w.b, uint32(v)) }
func (w *coreWriter) i64(v int64) { w.b = binary.LittleEndian.AppendUint64(w.b, uint64(v)) }
func (w *coreWriter) str(s string) {
	w.u32(len(s))
	w.b = append(w.b, s...)
}
func (w *coreWriter) blob(p []byte) {
	w.u32(len(p))
	w.b = append(w.b, p...)
}

// Encode serializes p into the store's opaque Core payload blob.
func (p *CorePayload) Encode() []byte {
	w := &coreWriter{}
	w.i64(p.MessageID)
	w.str(p.Address)
	w.u8(byte(len(p.UserID)))
	w.b = append(w.b, p.UserID...)
	w.u8(p.Type)
	if p.Durable {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.i64(p.Expiration)
	w.i64(p.Timestamp)
	w.u8(p.Priority)
	if p.Large {
		w.u8(1)
	} else {
		w.u8(0)
	}
	encodeCoreProps(w, p.Properties)
	w.blob(p.Body)
	return w.b
}

func encodeCoreProps(w *coreWriter, props map[string]any) {
	w.u32(len(props))
	for k, v := range props {
		w.str(k)
		encodeCoreValue(w, v)
	}
}

func encodeCoreValue(w *coreWriter, v any) {
	switch val := v.(type) {
	case nil:
		w.u8(cpvNull)
	case bool:
		w.u8(cpvBool)
		if val {
			w.u8(1)
		} else {
			w.u8(0)
		}
	case byte: // uint8
		w.u8(cpvByte)
		w.u8(val)
	case int16:
		w.u8(cpvInt16)
		w.u16(int(uint16(val)))
	case int32:
		w.u8(cpvInt32)
		w.u32(int(uint32(val)))
	case int64:
		w.u8(cpvInt64)
		w.i64(val)
	case float32:
		w.u8(cpvFloat32)
		w.u32(int(math.Float32bits(val)))
	case float64:
		w.u8(cpvFloat64)
		w.i64(int64(math.Float64bits(val)))
	case string:
		w.u8(cpvString)
		w.str(val)
	case []byte:
		w.u8(cpvBytes)
		w.blob(val)
	default:
		// Unknown value type: store its string form (lossy but never fatal).
		w.u8(cpvString)
		w.str(fmt.Sprint(val))
	}
}

// coreReader is a bounds-checked little-endian cursor.
type coreReader struct {
	b   []byte
	off int
	err error
}

func (r *coreReader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if n < 0 || r.off+n > len(r.b) {
		r.err = errors.New("core payload: truncated")
		return false
	}
	return true
}
func (r *coreReader) u8() byte {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.off]
	r.off++
	return v
}
func (r *coreReader) u16() int {
	if !r.need(2) {
		return 0
	}
	v := binary.LittleEndian.Uint16(r.b[r.off:])
	r.off += 2
	return int(v)
}
func (r *coreReader) u32() int {
	if !r.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(r.b[r.off:])
	r.off += 4
	return int(v)
}
func (r *coreReader) i64() int64 {
	if !r.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b[r.off:])
	r.off += 8
	return int64(v)
}
func (r *coreReader) take(n int) []byte {
	if n < 0 || !r.need(n) {
		return nil
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return append([]byte(nil), v...)
}
func (r *coreReader) str() string {
	n := r.u32()
	return string(r.take(n))
}

// DecodeCorePayload parses a Core payload blob produced by CorePayload.Encode.
func DecodeCorePayload(b []byte) (*CorePayload, error) {
	r := &coreReader{b: b}
	p := &CorePayload{}
	p.MessageID = r.i64()
	p.Address = r.str()
	uidLen := int(r.u8())
	if uidLen > 0 {
		p.UserID = r.take(uidLen)
	}
	p.Type = r.u8()
	p.Durable = r.u8() != 0
	p.Expiration = r.i64()
	p.Timestamp = r.i64()
	p.Priority = r.u8()
	p.Large = r.u8() != 0
	p.Properties = decodeCoreProps(r)
	bodyLen := r.u32()
	p.Body = r.take(bodyLen)
	if r.err != nil {
		return nil, r.err
	}
	return p, nil
}

func decodeCoreProps(r *coreReader) map[string]any {
	n := r.u32()
	if r.err != nil || n == 0 {
		return nil
	}
	props := make(map[string]any, n)
	for i := 0; i < n; i++ {
		key := r.str()
		props[key] = decodeCoreValue(r)
		if r.err != nil {
			return props
		}
	}
	return props
}

// CoreTextBody decodes a Core TEXT message body, which is encoded as a
// nullableSimpleString (format_notes.md §6). Returns ("", false) if the body
// is not a well-formed SimpleString (caller should fall back to raw bytes).
func CoreTextBody(body []byte) (string, bool) {
	r := newReader(body)
	s, ok := r.nullableSimpleString()
	if r.err() != nil {
		return "", false
	}
	return s, ok
}

// CoreMapBody decodes a Core MAP message body, encoded as a TypedProperties
// block (format_notes.md §7). Returns (nil, false) on a malformed body.
func CoreMapBody(body []byte) (map[string]any, bool) {
	r := newReader(body)
	m := r.typedProperties()
	if r.err() != nil {
		return nil, false
	}
	return m, true
}

func decodeCoreValue(r *coreReader) any {
	switch r.u8() {
	case cpvNull:
		return nil
	case cpvBool:
		return r.u8() != 0
	case cpvByte:
		return r.u8()
	case cpvInt16:
		return int16(r.u16())
	case cpvInt32:
		return int32(r.u32())
	case cpvInt64:
		return r.i64()
	case cpvFloat32:
		return math.Float32frombits(uint32(r.u32()))
	case cpvFloat64:
		return math.Float64frombits(uint64(r.i64()))
	case cpvString:
		return r.str()
	case cpvBytes:
		n := r.u32()
		return r.take(n)
	default:
		r.err = errors.New("core payload: unknown value tag")
		return nil
	}
}
