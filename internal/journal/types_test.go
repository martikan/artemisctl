package journal

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// --- test helpers mirroring the verified on-disk layout (format_notes.md) ---

// writeSimpleStringBytes appends a raw (non-nullable) SimpleString: a
// big-endian int byte-length followed by little-endian UTF-16 char pairs.
func writeSimpleStringBytes(buf *bytes.Buffer, s string) {
	runes := []rune(s)
	data := make([]byte, 0, len(runes)*2)
	for _, r := range runes {
		c := uint16(r)
		data = append(data, byte(c&0xFF), byte(c>>8))
	}
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
	buf.Write(lenBuf[:])
	buf.Write(data)
}

// writeNullableSimpleStringBytes appends a nullableSimpleString: flag byte
// (0 = NULL, 1 = NOT_NULL) followed by the SimpleString body when present.
func writeNullableSimpleStringBytes(buf *bytes.Buffer, s string, present bool) {
	if !present {
		buf.WriteByte(0)
		return
	}
	buf.WriteByte(1)
	writeSimpleStringBytes(buf, s)
}

// writeTypedPropsHeader appends the TypedProperties null-marker + count.
func writeTypedPropsHeader(buf *bytes.Buffer, count int32) {
	if count < 0 {
		buf.WriteByte(0) // NULL marker -> empty/absent
		return
	}
	buf.WriteByte(1) // NOT_NULL marker
	var cntBuf [4]byte
	binary.BigEndian.PutUint32(cntBuf[:], uint32(count))
	buf.Write(cntBuf[:])
}

// writeTypedPropKey appends a TypedProperties key: raw SimpleString, no flag.
func writeTypedPropKey(buf *bytes.Buffer, key string) {
	writeSimpleStringBytes(buf, key)
}

func be16(v int16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], uint16(v))
	return b[:]
}

func be32(v int32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	return b[:]
}

func be64(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

// --- SimpleString / nullableSimpleString round trip ---

func TestReaderSimpleStringRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		s    string
	}{
		{"ascii", "hello"},
		{"empty", ""},
		{"nonascii", "héllo"},
		{"multibyte", "日本語"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			writeSimpleStringBytes(&buf, tc.s)
			r := newReader(buf.Bytes())
			got := r.simpleString()
			if err := r.err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.s {
				t.Fatalf("got %q, want %q", got, tc.s)
			}
			if r.remaining() != 0 {
				t.Fatalf("remaining = %d, want 0", r.remaining())
			}
		})
	}
}

func TestReaderNullableSimpleStringPresent(t *testing.T) {
	var buf bytes.Buffer
	writeNullableSimpleStringBytes(&buf, "hello", true)
	r := newReader(buf.Bytes())
	s, ok := r.nullableSimpleString()
	if err := r.err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected present=true")
	}
	if s != "hello" {
		t.Fatalf("got %q, want %q", s, "hello")
	}
}

func TestReaderNullableSimpleStringAbsent(t *testing.T) {
	var buf bytes.Buffer
	writeNullableSimpleStringBytes(&buf, "", false)
	r := newReader(buf.Bytes())
	s, ok := r.nullableSimpleString()
	if err := r.err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatalf("expected present=false")
	}
	if s != "" {
		t.Fatalf("got %q, want empty string", s)
	}
}

// --- TypedProperties ---

func TestReaderTypedPropertiesEmpty(t *testing.T) {
	var buf bytes.Buffer
	writeTypedPropsHeader(&buf, 0)
	r := newReader(buf.Bytes())
	props := r.typedProperties()
	if err := r.err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(props) != 0 {
		t.Fatalf("got %d props, want 0", len(props))
	}
}

func TestReaderTypedPropertiesNull(t *testing.T) {
	var buf bytes.Buffer
	writeTypedPropsHeader(&buf, -1) // NULL marker
	r := newReader(buf.Bytes())
	props := r.typedProperties()
	if err := r.err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if props != nil {
		t.Fatalf("got %#v, want nil", props)
	}
}

func TestReaderTypedPropertiesOneOfEach(t *testing.T) {
	var buf bytes.Buffer
	writeTypedPropsHeader(&buf, 11)

	// NULL
	writeTypedPropKey(&buf, "kNull")
	buf.WriteByte(0)

	// BOOLEAN true (encoded 0xFF per format_notes)
	writeTypedPropKey(&buf, "kBool")
	buf.WriteByte(2)
	buf.WriteByte(0xFF)

	// BYTE
	writeTypedPropKey(&buf, "kByte")
	buf.WriteByte(3)
	buf.WriteByte(0x7B)

	// BYTES
	writeTypedPropKey(&buf, "kBytes")
	buf.WriteByte(4)
	payload := []byte{0x01, 0x02, 0x03}
	buf.Write(be32(int32(len(payload))))
	buf.Write(payload)

	// SHORT
	writeTypedPropKey(&buf, "kShort")
	buf.WriteByte(5)
	buf.Write(be16(-1234))

	// INT
	writeTypedPropKey(&buf, "kInt")
	buf.WriteByte(6)
	buf.Write(be32(-123456))

	// LONG
	writeTypedPropKey(&buf, "kLong")
	buf.WriteByte(7)
	buf.Write(be64(-123456789012))

	// FLOAT
	writeTypedPropKey(&buf, "kFloat")
	buf.WriteByte(8)
	buf.Write(be32(int32(math.Float32bits(3.14))))

	// DOUBLE
	writeTypedPropKey(&buf, "kDouble")
	buf.WriteByte(9)
	buf.Write(be64(int64(math.Float64bits(2.71828))))

	// STRING
	writeTypedPropKey(&buf, "kString")
	buf.WriteByte(10)
	writeSimpleStringBytes(&buf, "héllo")

	// CHAR
	writeTypedPropKey(&buf, "kChar")
	buf.WriteByte(11)
	buf.Write(be16(int16('Z')))

	r := newReader(buf.Bytes())
	props := r.typedProperties()
	if err := r.err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.remaining() != 0 {
		t.Fatalf("remaining = %d, want 0", r.remaining())
	}
	if len(props) != 11 {
		t.Fatalf("got %d props, want 11: %#v", len(props), props)
	}

	if v, ok := props["kNull"]; !ok || v != nil {
		t.Errorf("kNull = %#v, want nil", v)
	}
	if v, ok := props["kBool"].(bool); !ok || v != true {
		t.Errorf("kBool = %#v, want true", props["kBool"])
	}
	if v, ok := props["kByte"].(byte); !ok || v != 0x7B {
		t.Errorf("kByte = %#v, want 0x7B", props["kByte"])
	}
	if v, ok := props["kBytes"].([]byte); !ok || !bytes.Equal(v, payload) {
		t.Errorf("kBytes = %#v, want %#v", props["kBytes"], payload)
	}
	if v, ok := props["kShort"].(int16); !ok || v != -1234 {
		t.Errorf("kShort = %#v, want -1234", props["kShort"])
	}
	if v, ok := props["kInt"].(int32); !ok || v != -123456 {
		t.Errorf("kInt = %#v, want -123456", props["kInt"])
	}
	if v, ok := props["kLong"].(int64); !ok || v != -123456789012 {
		t.Errorf("kLong = %#v, want -123456789012", props["kLong"])
	}
	if v, ok := props["kFloat"].(float32); !ok || v != float32(3.14) {
		t.Errorf("kFloat = %#v, want 3.14", props["kFloat"])
	}
	if v, ok := props["kDouble"].(float64); !ok || v != 2.71828 {
		t.Errorf("kDouble = %#v, want 2.71828", props["kDouble"])
	}
	if v, ok := props["kString"].(string); !ok || v != "héllo" {
		t.Errorf("kString = %#v, want héllo", props["kString"])
	}
	if v, ok := props["kChar"].(int16); !ok || v != int16('Z') {
		t.Errorf("kChar = %#v, want 'Z'", props["kChar"])
	}
}

// --- primitive reader round trips ---

func TestReaderPrimitives(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteByte(0x42) // u8
	buf.WriteByte(0xFF) // bool true
	buf.WriteByte(0x00) // bool false
	buf.Write(be16(-100))
	buf.Write(be32(-100000))
	buf.Write(be64(-100000000000))
	buf.Write(be32(int32(math.Float32bits(1.5))))
	buf.Write(be64(int64(math.Float64bits(2.5))))
	buf.Write([]byte{0xAA, 0xBB, 0xCC})

	r := newReader(buf.Bytes())
	if v := r.u8(); v != 0x42 {
		t.Errorf("u8 = %#x, want 0x42", v)
	}
	if v := r.bool(); v != true {
		t.Errorf("bool = %v, want true", v)
	}
	if v := r.bool(); v != false {
		t.Errorf("bool = %v, want false", v)
	}
	if v := r.i16(); v != -100 {
		t.Errorf("i16 = %d, want -100", v)
	}
	if v := r.i32(); v != -100000 {
		t.Errorf("i32 = %d, want -100000", v)
	}
	if v := r.i64(); v != -100000000000 {
		t.Errorf("i64 = %d, want -100000000000", v)
	}
	if v := r.f32(); v != 1.5 {
		t.Errorf("f32 = %v, want 1.5", v)
	}
	if v := r.f64(); v != 2.5 {
		t.Errorf("f64 = %v, want 2.5", v)
	}
	b := r.bytes(3)
	if !bytes.Equal(b, []byte{0xAA, 0xBB, 0xCC}) {
		t.Errorf("bytes = %#v, want AA BB CC", b)
	}
	if err := r.err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.remaining() != 0 {
		t.Fatalf("remaining = %d, want 0", r.remaining())
	}
}

// --- truncation / bounds checking ---

// TestReaderTruncatedSimpleString truncates a valid SimpleString buffer at
// every possible length and requires the reader to set an error without
// panicking, for every truncation point short of the full buffer.
func TestReaderTruncatedSimpleString(t *testing.T) {
	var full bytes.Buffer
	writeSimpleStringBytes(&full, "hello world")
	data := full.Bytes()

	for n := 0; n < len(data); n++ {
		t.Run("", func(t *testing.T) {
			trunc := data[:n]
			r := newReader(trunc)
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						t.Fatalf("panic at truncation %d: %v", n, rec)
					}
				}()
				_ = r.simpleString()
			}()
			if r.err() == nil {
				t.Fatalf("truncation at %d: expected error, got nil", n)
			}
		})
	}
}

// TestReaderTruncatedNullableSimpleString does the same for the present case
// (flag byte = 1) of nullableSimpleString.
func TestReaderTruncatedNullableSimpleString(t *testing.T) {
	var full bytes.Buffer
	writeNullableSimpleStringBytes(&full, "hello world", true)
	data := full.Bytes()

	for n := 0; n < len(data); n++ {
		t.Run("", func(t *testing.T) {
			trunc := data[:n]
			r := newReader(trunc)
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						t.Fatalf("panic at truncation %d: %v", n, rec)
					}
				}()
				_, _ = r.nullableSimpleString()
			}()
			if r.err() == nil {
				t.Fatalf("truncation at %d: expected error, got nil", n)
			}
		})
	}
}

// TestReaderTruncatedTypedProperties builds a one-of-each-type TypedProperties
// buffer and requires every truncation length 0..len-1 to produce an error,
// never a panic.
func TestReaderTruncatedTypedProperties(t *testing.T) {
	var full bytes.Buffer
	writeTypedPropsHeader(&full, 3)

	writeTypedPropKey(&full, "kBool")
	full.WriteByte(2)
	full.WriteByte(0xFF)

	writeTypedPropKey(&full, "kBytes")
	full.WriteByte(4)
	payload := []byte{0x01, 0x02, 0x03, 0x04}
	full.Write(be32(int32(len(payload))))
	full.Write(payload)

	writeTypedPropKey(&full, "kString")
	full.WriteByte(10)
	writeSimpleStringBytes(&full, "world")

	data := full.Bytes()

	for n := 0; n < len(data); n++ {
		t.Run("", func(t *testing.T) {
			trunc := data[:n]
			r := newReader(trunc)
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						t.Fatalf("panic at truncation %d: %v", n, rec)
					}
				}()
				_ = r.typedProperties()
			}()
			if r.err() == nil {
				t.Fatalf("truncation at %d: expected error, got nil", n)
			}
		})
	}
}

// TestReaderTypedPropertiesOOMRegression verifies that a malformed count
// (corrupted journal input: NOT_NULL marker + count = 0x7FFFFFFF) does not
// trigger fatal OOM allocation before buffer validation, but instead sets
// an error and returns nil.
func TestReaderTypedPropertiesOOMRegression(t *testing.T) {
	// Minimal corrupted input: NOT_NULL marker (1 byte) + count 0x7FFFFFFF (4 bytes).
	// This is only 5 bytes, far too small for 2147483647 entries.
	malformed := []byte{
		0x01,                   // NOT_NULL marker
		0x7F, 0xFF, 0xFF, 0xFF, // 0x7FFFFFFF in big-endian
	}
	r := newReader(malformed)
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("OOM crash (or panic) with malformed count: %v", rec)
			}
		}()
		props := r.typedProperties()
		if props != nil {
			t.Fatalf("expected nil, got %#v", props)
		}
	}()
	if r.err() == nil {
		t.Fatalf("expected error, got nil")
	}
}

// TestReaderErrIsSticky verifies that once an error occurs, subsequent reads
// are no-ops (return zero values) rather than panicking or advancing.
func TestReaderErrIsSticky(t *testing.T) {
	r := newReader([]byte{0x01})
	_ = r.i32() // underflow: only 1 byte available
	if r.err() == nil {
		t.Fatalf("expected error after underflow read")
	}
	// Further reads must not panic and must return zero values.
	if v := r.u8(); v != 0 {
		t.Errorf("u8 after error = %#x, want 0", v)
	}
	if v := r.i64(); v != 0 {
		t.Errorf("i64 after error = %d, want 0", v)
	}
	if v := r.simpleString(); v != "" {
		t.Errorf("simpleString after error = %q, want empty", v)
	}
	if b := r.bytes(5); b != nil {
		t.Errorf("bytes after error = %#v, want nil", b)
	}
}

func TestReaderBytesIsSubSlice(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	r := newReader(data)
	b := r.bytes(3)
	if &b[0] != &data[0] {
		t.Fatalf("bytes() did not return a sub-slice of the original backing array")
	}
}
