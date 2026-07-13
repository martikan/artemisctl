package broker

import (
	"encoding/binary"
	"testing"

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
