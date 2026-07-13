package journal

import (
	"fmt"
	"math"
	"unicode/utf16"
)

// DataConstants type ids, per TypedProperties.encode / DataConstants (Artemis
// 2.42.0). See format_notes.md section 7.
const (
	dcNull    = 0
	dcNotNull = 1
	dcBoolean = 2
	dcByte    = 3
	dcBytes   = 4
	dcShort   = 5
	dcInt     = 6
	dcLong    = 7
	dcFloat   = 8
	dcDouble  = 9
	dcString  = 10
	dcChar    = 11
)

// reader is a bounds-checked big-endian cursor over a byte slice. All read
// methods are no-ops after the first error; check err() once at the end.
type reader struct {
	b   []byte
	off int
	e   error
}

// newReader wraps b in a reader positioned at offset 0.
func newReader(b []byte) *reader {
	return &reader{b: b}
}

// err returns the first error encountered by any read method, or nil.
func (r *reader) err() error {
	return r.e
}

// remaining returns the number of unread bytes. It is meaningless once err()
// is non-nil (the cursor stopped advancing at the failure point).
func (r *reader) remaining() int {
	return len(r.b) - r.off
}

// need reports whether n more bytes are available; on failure it sets a
// truncation error (if not already set) and returns false.
func (r *reader) need(n int) bool {
	if r.e != nil {
		return false
	}
	if n < 0 || r.off+n > len(r.b) {
		r.e = fmt.Errorf("journal: truncated at offset %d", r.off)
		return false
	}
	return true
}

func (r *reader) u8() byte {
	if !r.need(1) {
		return 0
	}
	v := r.b[r.off]
	r.off++
	return v
}

// bool decodes one byte; 0 = false, anything else (canonically 0xFF) = true.
func (r *reader) bool() bool {
	return r.u8() != 0
}

func (r *reader) i16() int16 {
	if !r.need(2) {
		return 0
	}
	v := int16(r.b[r.off])<<8 | int16(r.b[r.off+1])
	r.off += 2
	return v
}

func (r *reader) i32() int32 {
	if !r.need(4) {
		return 0
	}
	v := int32(r.b[r.off])<<24 | int32(r.b[r.off+1])<<16 | int32(r.b[r.off+2])<<8 | int32(r.b[r.off+3])
	r.off += 4
	return v
}

func (r *reader) i64() int64 {
	if !r.need(8) {
		return 0
	}
	var v int64
	for i := 0; i < 8; i++ {
		v = v<<8 | int64(r.b[r.off+i])
	}
	r.off += 8
	return v
}

func (r *reader) f32() float32 {
	bits := r.i32()
	if r.e != nil {
		return 0
	}
	return math.Float32frombits(uint32(bits))
}

func (r *reader) f64() float64 {
	bits := r.i64()
	if r.e != nil {
		return 0
	}
	return math.Float64frombits(uint64(bits))
}

// bytes returns a sub-slice (not a copy) of the next n bytes.
func (r *reader) bytes(n int) []byte {
	if !r.need(n) {
		return nil
	}
	v := r.b[r.off : r.off+n]
	r.off += n
	return v
}

// simpleString decodes a non-nullable SimpleString: a big-endian int
// byte-length followed by that many bytes of little-endian UTF-16 code-unit
// pairs (low byte first), per format_notes.md section 6.
func (r *reader) simpleString() string {
	n := r.i32()
	if r.e != nil {
		return ""
	}
	if n < 0 {
		r.e = fmt.Errorf("journal: negative SimpleString length at offset %d", r.off)
		return ""
	}
	data := r.bytes(int(n))
	if r.e != nil {
		return ""
	}
	if len(data)%2 != 0 {
		r.e = fmt.Errorf("journal: odd SimpleString byte length at offset %d", r.off)
		return ""
	}
	units := make([]uint16, len(data)/2)
	for i := range units {
		lo := data[2*i]
		hi := data[2*i+1]
		units[i] = uint16(lo) | uint16(hi)<<8
	}
	return string(utf16.Decode(units))
}

// nullableSimpleString decodes a flag byte (0 = NULL, else NOT_NULL) followed
// by a SimpleString body when present. It returns ("", false) when NULL.
func (r *reader) nullableSimpleString() (string, bool) {
	flag := r.u8()
	if r.e != nil {
		return "", false
	}
	if flag == 0 {
		return "", false
	}
	s := r.simpleString()
	if r.e != nil {
		return "", false
	}
	return s, true
}

// typedProperties decodes a TypedProperties block: a null-marker byte, then
// (if NOT_NULL) an int propertyCount and that many [key SimpleString][typed
// value] entries. It returns nil when the marker is NULL, per
// format_notes.md section 7.
func (r *reader) typedProperties() map[string]any {
	marker := r.u8()
	if r.e != nil {
		return nil
	}
	if marker == dcNull {
		return nil
	}

	count := r.i32()
	if r.e != nil {
		return nil
	}
	if count < 0 {
		r.e = fmt.Errorf("journal: negative TypedProperties count at offset %d", r.off)
		return nil
	}

	// Validate count won't cause OOM; minimum entry size is 5 bytes:
	// 4-byte SimpleString length prefix + 1 type byte.
	if count > int32(r.remaining()/5) {
		r.e = fmt.Errorf("journal: TypedProperties count too large at offset %d", r.off)
		return nil
	}

	props := make(map[string]any, count)
	for i := int32(0); i < count; i++ {
		key := r.simpleString()
		if r.e != nil {
			return nil
		}
		typ := r.u8()
		if r.e != nil {
			return nil
		}

		var val any
		switch typ {
		case dcNull:
			val = nil
		case dcBoolean:
			val = r.bool()
		case dcByte:
			val = r.u8()
		case dcBytes:
			n := r.i32()
			if r.e != nil {
				return nil
			}
			if n < 0 {
				r.e = fmt.Errorf("journal: negative BYTES length at offset %d", r.off)
				return nil
			}
			raw := r.bytes(int(n))
			if r.e != nil {
				return nil
			}
			cp := make([]byte, len(raw))
			copy(cp, raw)
			val = cp
		case dcShort:
			val = r.i16()
		case dcInt:
			val = r.i32()
		case dcLong:
			val = r.i64()
		case dcFloat:
			val = r.f32()
		case dcDouble:
			val = r.f64()
		case dcString:
			val = r.simpleString()
		case dcChar:
			val = r.i16()
		default:
			r.e = fmt.Errorf("journal: unknown TypedProperties type id %d at offset %d", typ, r.off)
			return nil
		}
		if r.e != nil {
			return nil
		}
		props[key] = val
	}
	return props
}
