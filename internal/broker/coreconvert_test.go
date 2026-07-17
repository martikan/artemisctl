package broker

import (
	"encoding/binary"
	"testing"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/journal"
)

// coreSimpleString builds a core nullableSimpleString (NOT_NULL flag, big-endian
// int byte-length, little-endian UTF-16 pairs) — the on-wire form of a Core
// TEXT message body.
func coreSimpleString(s string) []byte {
	units := []rune(s)
	data := make([]byte, 0, 1+4+len(units)*2)
	data = append(data, 1) // NOT_NULL
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(units)*2))
	data = append(data, lenBuf...)
	for _, r := range units {
		data = append(data, byte(r), byte(r>>8))
	}
	return data
}

func TestCoreToAMQPBytesMessage(t *testing.T) {
	p := &journal.CorePayload{
		MessageID:  5,
		Address:    "orders",
		Type:       journal.CoreTypeBytes,
		Durable:    true,
		Timestamp:  1_700_000_000_000,
		Priority:   4,
		Properties: map[string]any{"region": "eu", "__AMQ_CID": "internal"},
		Body:       []byte("hello"),
	}
	msg, err := coreToAMQP(p.Encode())
	if err != nil {
		t.Fatalf("coreToAMQP: %v", err)
	}
	if len(msg.Data) != 1 || string(msg.Data[0]) != "hello" {
		t.Errorf("Data = %v, want [hello]", msg.Data)
	}
	if msg.Value != nil {
		t.Errorf("Value = %v, want nil for a BYTES message", msg.Value)
	}
	if !msg.Header.Durable || msg.Header.Priority != 4 {
		t.Errorf("Header = %+v, want durable priority 4", msg.Header)
	}
	if msg.Properties.To == nil || *msg.Properties.To != "orders" {
		t.Errorf("To = %v, want orders", msg.Properties.To)
	}
	if got := msg.ApplicationProperties["region"]; got != "eu" {
		t.Errorf("app prop region = %v, want eu", got)
	}
	if _, ok := msg.ApplicationProperties["__AMQ_CID"]; ok {
		t.Errorf("internal __AMQ_CID leaked into application properties")
	}
	if got := msg.Annotations["x-opt-jms-msg-type"]; got != int8(jmsBytesMessageType) {
		t.Errorf("x-opt-jms-msg-type = %v, want %d", got, jmsBytesMessageType)
	}
}

func TestCoreToAMQPTextMessage(t *testing.T) {
	p := &journal.CorePayload{
		MessageID: 6,
		Address:   "news",
		Type:      journal.CoreTypeText,
		Body:      coreSimpleString("hi there"),
	}
	msg, err := coreToAMQP(p.Encode())
	if err != nil {
		t.Fatalf("coreToAMQP: %v", err)
	}
	if msg.Value != "hi there" {
		t.Errorf("Value = %v, want %q", msg.Value, "hi there")
	}
	if msg.Data != nil {
		t.Errorf("Data = %v, want nil for a TEXT message", msg.Data)
	}
}

func TestCoreToAMQPScheduled(t *testing.T) {
	p := &journal.CorePayload{
		Type:       journal.CoreTypeBytes,
		Body:       []byte("x"),
		Properties: map[string]any{scheduledMsProperty: int64(4_102_444_800_000)},
	}
	msg, err := coreToAMQP(p.Encode())
	if err != nil {
		t.Fatalf("coreToAMQP: %v", err)
	}
	if got := msg.Annotations["x-opt-delivery-time"]; got != int64(4_102_444_800_000) {
		t.Errorf("x-opt-delivery-time = %v, want scheduled ms", got)
	}
	if _, ok := msg.ApplicationProperties[scheduledMsProperty]; ok {
		t.Errorf("synthetic scheduling property leaked into application properties")
	}
}

// Message grouping (_AMQ_GROUP_ID / _AMQ_GROUP_SEQUENCE) is Artemis-internal in
// core form but maps onto standard AMQP message-properties; it must survive the
// Core->AMQP conversion rather than being stripped with the other _AMQ* keys.
func TestCoreToAMQPGroupProperties(t *testing.T) {
	p := &journal.CorePayload{
		Type: journal.CoreTypeBytes,
		Body: []byte("x"),
		Properties: map[string]any{
			coreGroupIDProperty:  "grp-core",
			coreGroupSeqProperty: int32(7),
			"region":             "eu",
		},
	}
	msg, err := coreToAMQP(p.Encode())
	if err != nil {
		t.Fatalf("coreToAMQP: %v", err)
	}
	if msg.Properties.GroupID == nil || *msg.Properties.GroupID != "grp-core" {
		t.Errorf("GroupID = %v, want grp-core", msg.Properties.GroupID)
	}
	if msg.Properties.GroupSequence == nil || *msg.Properties.GroupSequence != 7 {
		t.Errorf("GroupSequence = %v, want 7", msg.Properties.GroupSequence)
	}
	if _, ok := msg.ApplicationProperties[coreGroupIDProperty]; ok {
		t.Errorf("_AMQ_GROUP_ID leaked into application properties")
	}
	if _, ok := msg.ApplicationProperties[coreGroupSeqProperty]; ok {
		t.Errorf("_AMQ_GROUP_SEQUENCE leaked into application properties")
	}
	if got := msg.ApplicationProperties["region"]; got != "eu" {
		t.Errorf("app prop region = %v, want eu", got)
	}
}

func TestCoreToAMQPUnconvertible(t *testing.T) {
	if _, err := coreToAMQP([]byte{0x00, 0x01}); err == nil {
		t.Fatalf("want error for a truncated core payload, got nil")
	}
}

// coreRawSimpleString builds a raw (non-nullable) core SimpleString: big-endian
// int byte-length then little-endian UTF-16 pairs. Used for TypedProperties keys
// and string values, which carry no NULL flag byte.
func coreRawSimpleString(s string) []byte {
	units := []rune(s)
	out := make([]byte, 4, 4+len(units)*2)
	binary.BigEndian.PutUint32(out, uint32(len(units)*2))
	for _, r := range units {
		out = append(out, byte(r), byte(r>>8))
	}
	return out
}

// coreMapBody builds a single-string-entry Core MAP body (TypedProperties: a
// NOT_NULL marker, a big-endian entry count, then per entry a raw SimpleString
// key, a type byte, and the value). Type byte 10 is STRING, carried as a raw
// SimpleString.
func coreMapBody(key, val string) []byte {
	out := []byte{1}              // NOT_NULL marker
	out = append(out, 0, 0, 0, 1) // count = 1 (big-endian)
	out = append(out, coreRawSimpleString(key)...)
	out = append(out, 10) // STRING type byte
	out = append(out, coreRawSimpleString(val)...)
	return out
}

func TestSetCoreBody(t *testing.T) {
	tests := []struct {
		name    string
		typ     byte
		body    []byte
		wantVal any
		wantCT  string // expected ContentType, "" if none
	}{
		{"text malformed falls back to raw", journal.CoreTypeText, []byte{0x01, 0xFF}, nil, ""},
		{"map valid", journal.CoreTypeMap, coreMapBody("k", "v"), map[string]any{"k": "v"}, ""},
		{"map malformed falls back to raw", journal.CoreTypeMap, []byte{0x01, 0x7F, 0xFF, 0xFF, 0xFF}, nil, ""},
		{"object carries java content-type", journal.CoreTypeObject, []byte("ser"), nil, "application/x-java-serialized-object"},
		{"stream is raw data", journal.CoreTypeStream, []byte("raw"), nil, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &amqp.Message{Properties: &amqp.MessageProperties{}}
			setCoreBody(msg, &journal.CorePayload{Type: tt.typ, Body: tt.body})

			switch want := tt.wantVal.(type) {
			case map[string]any:
				got, ok := msg.Value.(map[string]any)
				if !ok || got["k"] != want["k"] {
					t.Fatalf("Value = %#v, want map %v", msg.Value, want)
				}
			case nil:
				if msg.Value != nil {
					t.Errorf("Value = %v, want nil (raw Data expected)", msg.Value)
				}
				if len(msg.Data) != 1 || string(msg.Data[0]) != string(tt.body) {
					t.Errorf("Data = %v, want [%s]", msg.Data, tt.body)
				}
			}

			if tt.wantCT == "" {
				if msg.Properties.ContentType != nil {
					t.Errorf("ContentType = %v, want nil", *msg.Properties.ContentType)
				}
			} else if msg.Properties.ContentType == nil || *msg.Properties.ContentType != tt.wantCT {
				t.Errorf("ContentType = %v, want %s", msg.Properties.ContentType, tt.wantCT)
			}
		})
	}
}

func TestJMSTypeFor(t *testing.T) {
	tests := []struct {
		coreType byte
		want     int
	}{
		{journal.CoreTypeText, jmsTextMessageType},
		{journal.CoreTypeBytes, jmsBytesMessageType},
		{journal.CoreTypeMap, jmsMapMessageType},
		{journal.CoreTypeObject, jmsObjectMessageType},
		{journal.CoreTypeStream, jmsStreamMessageType},
		{journal.CoreTypeDefault, jmsMessageType},
		{0xFF, jmsMessageType}, // unknown -> plain message
	}
	for _, tt := range tests {
		if got := jmsTypeFor(tt.coreType); got != tt.want {
			t.Errorf("jmsTypeFor(%d) = %d, want %d", tt.coreType, got, tt.want)
		}
	}
}

func TestAsInt64(t *testing.T) {
	tests := []struct {
		name   string
		in     any
		want   int64
		wantOK bool
	}{
		{"int64", int64(9), 9, true},
		{"int32", int32(8), 8, true},
		{"int16", int16(7), 7, true},
		{"int", int(6), 6, true},
		{"unsupported string", "5", 0, false},
		{"unsupported float", 4.0, 0, false},
	}
	for _, tt := range tests {
		got, ok := asInt64(tt.in)
		if got != tt.want || ok != tt.wantOK {
			t.Errorf("asInt64(%v) = (%d,%t), want (%d,%t)", tt.in, got, ok, tt.want, tt.wantOK)
		}
	}
}
