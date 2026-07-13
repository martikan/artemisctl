package journal

import (
	"bytes"
	"math"
	"reflect"
	"testing"
)

// TestCorePayloadRoundTripAllValueTypes exercises every typed property value
// (encodeCoreValue/decodeCoreValue) plus the non-empty UserID / Large / Durable
// Encode branches, so a Core message's normalized fields survive the store blob.
func TestCorePayloadRoundTripAllValueTypes(t *testing.T) {
	props := map[string]any{
		"n":   nil,
		"bt":  true,
		"bf":  false,
		"by":  byte(0x7B),
		"i16": int16(-1234),
		"i32": int32(-123456),
		"i64": int64(-123456789012),
		"f32": float32(3.14),
		"f64": float64(2.71828),
		"s":   "héllo",
		"raw": []byte{0x01, 0x02, 0x03},
		"unk": uint32(9), // unhandled type -> stored as its string form "9"
	}
	orig := &CorePayload{
		MessageID:  42,
		Address:    "salvage.core",
		UserID:     bytes.Repeat([]byte{0xAB}, 16),
		Type:       CoreTypeBytes,
		Durable:    true,
		Expiration: 1_700_000_000_123,
		Timestamp:  1_699_999_999_000,
		Priority:   9,
		Large:      true,
		Properties: props,
		Body:       []byte("body-bytes"),
	}
	got, err := DecodeCorePayload(orig.Encode())
	if err != nil {
		t.Fatalf("DecodeCorePayload: %v", err)
	}
	if got.MessageID != orig.MessageID || got.Address != orig.Address ||
		!bytes.Equal(got.UserID, orig.UserID) || got.Type != orig.Type ||
		!got.Durable || got.Expiration != orig.Expiration || got.Timestamp != orig.Timestamp ||
		got.Priority != orig.Priority || !got.Large || string(got.Body) != string(orig.Body) {
		t.Fatalf("scalar mismatch:\n got=%+v\norig=%+v", got, orig)
	}
	want := map[string]any{
		"n": nil, "bt": true, "bf": false, "by": byte(0x7B),
		"i16": int16(-1234), "i32": int32(-123456), "i64": int64(-123456789012),
		"f32": float32(3.14), "f64": float64(2.71828), "s": "héllo",
		"raw": []byte{0x01, 0x02, 0x03}, "unk": "9",
	}
	if !reflect.DeepEqual(got.Properties, want) {
		t.Errorf("props mismatch:\n got=%#v\nwant=%#v", got.Properties, want)
	}
}

// TestCorePayloadRoundTripMinimal covers the Durable=false / Large=false / empty
// UserID / nil-Properties Encode branches.
func TestCorePayloadRoundTripMinimal(t *testing.T) {
	orig := &CorePayload{MessageID: 1, Address: "q", Type: CoreTypeText, Body: []byte("x")}
	got, err := DecodeCorePayload(orig.Encode())
	if err != nil {
		t.Fatalf("DecodeCorePayload: %v", err)
	}
	if got.Durable || got.Large || len(got.UserID) != 0 || len(got.Properties) != 0 {
		t.Errorf("minimal mismatch: %+v", got)
	}
}

func TestDecodeCorePayloadTruncated(t *testing.T) {
	full := (&CorePayload{MessageID: 7, Address: "abc", Body: []byte("hello")}).Encode()
	if _, err := DecodeCorePayload(full[:len(full)-3]); err == nil {
		t.Fatalf("want truncation error, got nil")
	}
	if _, err := DecodeCorePayload(nil); err == nil {
		t.Fatalf("want error on empty blob, got nil")
	}
}

func TestCoreTextBody(t *testing.T) {
	var present bytes.Buffer
	writeNullableSimpleStringBytes(&present, "core-hello", true)
	if s, ok := CoreTextBody(present.Bytes()); !ok || s != "core-hello" {
		t.Errorf("present: got (%q,%v), want (core-hello,true)", s, ok)
	}

	var absent bytes.Buffer
	writeNullableSimpleStringBytes(&absent, "", false)
	if s, ok := CoreTextBody(absent.Bytes()); ok || s != "" {
		t.Errorf("absent: got (%q,%v), want (\"\",false)", s, ok)
	}

	if s, ok := CoreTextBody([]byte{0x01, 0xFF}); ok || s != "" {
		t.Errorf("malformed: got (%q,%v), want (\"\",false)", s, ok)
	}
}

func TestCoreMapBody(t *testing.T) {
	var buf bytes.Buffer
	writeTypedPropsHeader(&buf, 2)
	writeTypedPropKey(&buf, "kInt")
	buf.WriteByte(6)
	buf.Write(be32(99))
	writeTypedPropKey(&buf, "kString")
	buf.WriteByte(10)
	writeSimpleStringBytes(&buf, "v")

	m, ok := CoreMapBody(buf.Bytes())
	if !ok {
		t.Fatalf("CoreMapBody ok=false, want true")
	}
	if m["kInt"] != int32(99) || m["kString"] != "v" {
		t.Errorf("map = %#v, want {kInt:99, kString:v}", m)
	}

	// malformed: NOT_NULL marker then an impossibly large count.
	if _, ok := CoreMapBody([]byte{0x01, 0x7F, 0xFF, 0xFF, 0xFF}); ok {
		t.Errorf("malformed CoreMapBody ok=true, want false")
	}
}

// TestDecodeCoreValueUnknownTag hits decodeCoreValue's default (unknown tag)
// branch via a hand-built props blob DecodeCorePayload cannot otherwise produce.
func TestDecodeCoreValueUnknownTag(t *testing.T) {
	w := &coreWriter{}
	w.i64(1)   // MessageID
	w.str("q") // Address
	w.u8(0)    // UserID len
	w.u8(CoreTypeText)
	w.u8(0)     // Durable
	w.i64(0)    // Expiration
	w.i64(0)    // Timestamp
	w.u8(0)     // Priority
	w.u8(0)     // Large
	w.u32(1)    // one property
	w.str("k")  // key
	w.u8(0xEE)  // invalid value tag
	w.blob(nil) // body
	if _, err := DecodeCorePayload(w.b); err == nil {
		t.Fatalf("want unknown-value-tag error, got nil")
	}
}

func TestFloat32BitsRoundTrip(t *testing.T) {
	// guards the f32 encode/decode path independently of map iteration order.
	orig := &CorePayload{Type: CoreTypeBytes, Properties: map[string]any{"f": float32(math.MaxFloat32)}}
	got, _ := DecodeCorePayload(orig.Encode())
	if got.Properties["f"] != float32(math.MaxFloat32) {
		t.Errorf("f32 = %v, want MaxFloat32", got.Properties["f"])
	}
}
