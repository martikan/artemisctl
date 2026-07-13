package journal

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf16"

	"github.com/Azure/go-amqp"
)

// --- synthetic byte-builders (mirror format_notes.md sections 5a/6/9's
// write-side layouts, the inverse of what message.go's readers expect) ---

func beI32(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func beI64(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// encodeSimpleString mirrors format_notes.md section 6: big-endian int
// byteLength, then little-endian UTF-16 code-unit pairs.
func encodeSimpleString(s string) []byte {
	units := utf16.Encode([]rune(s))
	data := make([]byte, len(units)*2)
	for i, u := range units {
		data[2*i] = byte(u)
		data[2*i+1] = byte(u >> 8)
	}
	return append(beI32(int32(len(data))), data...)
}

func encodeNullableSimpleString(s string, notNull bool) []byte {
	if !notNull {
		return []byte{0}
	}
	return append([]byte{1}, encodeSimpleString(s)...)
}

// buildAMQPStandardBody builds an ADD_MESSAGE_PROTOCOL body for persister
// id in {2 (V1), 3 (V2), 5 (V3)}: format_notes.md section 5a's fixed prefix
// (messageID, messageFormat, nullableSimpleString address, int amqpSize,
// amqp bytes). The V2/V3 tail (extra props / expiration) is intentionally
// omitted -- decodeAMQPStandardBody never reads past the amqp bytes.
func buildAMQPStandardBody(persisterID byte, amqpBytes []byte) []byte {
	body := []byte{persisterID}
	body = append(body, beI64(42)...) // messageID (persister-internal, unused by the reader)
	body = append(body, beI64(0)...)  // messageFormat
	body = append(body, encodeNullableSimpleString("test.address", true)...)
	body = append(body, beI32(int32(len(amqpBytes)))...)
	body = append(body, amqpBytes...)
	return body
}

func buildRefUpdate(msgID, queueID int64, userType byte) RawRecord {
	return RawRecord{Type: UpdateRecord, ID: msgID, UserType: userType, Body: beI64(queueID)}
}

func buildScheduledUpdate(msgID, queueID, ms int64) RawRecord {
	return RawRecord{Type: UpdateRecord, ID: msgID, UserType: SetScheduledDeliveryTime, Body: append(beI64(queueID), beI64(ms)...)}
}

// --- synthetic tests ---

func TestDecodeMessagesAMQPStandardPersisters(t *testing.T) {
	for _, persisterID := range []byte{persisterAMQPMessage, persisterAMQPMessageV2, persisterAMQPMessageV3} {
		t.Run(fmt.Sprintf("persister_%d", persisterID), func(t *testing.T) {
			amqpBytes := []byte("raw-amqp-bytes-verbatim")
			sv := Survivor{
				ID:       10,
				UserType: AddMessageProtocol,
				Body:     buildAMQPStandardBody(persisterID, amqpBytes),
				Updates:  []RawRecord{buildRefUpdate(10, 7, AddRef)},
			}

			messages, diag, err := DecodeMessages([]Survivor{sv})
			if err != nil {
				t.Fatalf("DecodeMessages: %v", err)
			}
			if len(messages) != 1 {
				t.Fatalf("want 1 message, got %d", len(messages))
			}
			m := messages[0]
			if m.ID != 10 {
				t.Errorf("ID = %d, want 10", m.ID)
			}
			if string(m.AMQP) != string(amqpBytes) {
				t.Errorf("AMQP = %q, want %q", m.AMQP, amqpBytes)
			}
			if m.Large {
				t.Errorf("Large = true, want false")
			}
			if m.ScheduledMs != 0 {
				t.Errorf("ScheduledMs = %d, want 0", m.ScheduledMs)
			}
			if len(m.QueueIDs) != 1 || m.QueueIDs[0] != 7 {
				t.Errorf("QueueIDs = %v, want [7]", m.QueueIDs)
			}
			if diag.UnknownPersister != 0 || diag.UndecodableBody != 0 || len(diag.CoreSkipped) != 0 {
				t.Errorf("diag = %+v, want zero", diag)
			}
		})
	}
}

func TestDecodeMessagesLargePersister(t *testing.T) {
	sv := Survivor{
		ID:       11,
		UserType: AddMessageProtocol,
		Body:     []byte{persisterAMQPLargeMessage}, // decodeAddMessageProtocol reads nothing past the persister id for the large case
		Updates:  []RawRecord{buildRefUpdate(11, 3, AddRef)},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(messages))
	}
	m := messages[0]
	if !m.Large {
		t.Errorf("Large = false, want true")
	}
	if len(m.AMQP) != 0 {
		t.Errorf("AMQP = %v, want empty (joined later by Task 8)", m.AMQP)
	}
	if len(m.QueueIDs) != 1 || m.QueueIDs[0] != 3 {
		t.Errorf("QueueIDs = %v, want [3]", m.QueueIDs)
	}
	if diag.UnknownPersister != 0 || diag.UndecodableBody != 0 || len(diag.CoreSkipped) != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}
}

func TestDecodeMessagesUnknownPersisterCounted(t *testing.T) {
	sv := Survivor{
		ID:       12,
		UserType: AddMessageProtocol,
		Body:     []byte{99}, // not a recognized persister id
		Updates:  []RawRecord{buildRefUpdate(12, 1, AddRef)},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(messages), messages)
	}
	if diag.UnknownPersister != 1 {
		t.Errorf("diag.UnknownPersister = %d, want 1", diag.UnknownPersister)
	}
	if len(diag.CoreSkipped) != 0 {
		t.Errorf("diag.CoreSkipped = %v, want empty", diag.CoreSkipped)
	}
}

// loadCoreRecord reads the golden Core persister payload harvested from a real
// Artemis 2.42.0 broker (an ADD_MESSAGE_PROTOCOL record's data: a BYTES
// message, messageID 28, address "salvage.core", 120-byte body). It is the
// ground truth for the Core decoder (format_notes.md §5c).
func loadCoreRecord(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "core-record-2.42.bin"))
	if err != nil {
		t.Fatalf("read core golden record: %v", err)
	}
	return b
}

func TestDecodeMessagesCoreStandardExported(t *testing.T) {
	sv := Survivor{
		ID:       13,
		UserType: AddMessageProtocol,
		Body:     loadCoreRecord(t),
		Updates:  []RawRecord{buildRefUpdate(13, 4, AddRef), buildRefUpdate(13, 5, AddRef)},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(diag.CoreSkipped) != 0 || diag.UndecodableBody != 0 || diag.UnknownPersister != 0 {
		t.Fatalf("diag = %+v, want zero (core is now decoded, not skipped)", diag)
	}
	if len(messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(messages))
	}
	m := messages[0]
	if m.Core == nil {
		t.Fatalf("Core = nil, want a decoded payload")
	}
	if m.Core.MessageID != 28 {
		t.Errorf("Core.MessageID = %d, want 28", m.Core.MessageID)
	}
	if m.Core.Address != "salvage.core" {
		t.Errorf("Core.Address = %q, want salvage.core", m.Core.Address)
	}
	if m.Core.Type != CoreTypeBytes {
		t.Errorf("Core.Type = %d, want %d (BYTES)", m.Core.Type, CoreTypeBytes)
	}
	if !m.Core.Durable {
		t.Errorf("Core.Durable = false, want true")
	}
	if len(m.Core.Body) != 120 {
		t.Errorf("len(Core.Body) = %d, want 120", len(m.Core.Body))
	}
	for i, b := range m.Core.Body {
		if b != '.' {
			t.Fatalf("Core.Body[%d] = %#x, want '.'", i, b)
		}
	}
	if _, ok := m.Core.Properties["__AMQ_CID"]; !ok {
		t.Errorf("Core.Properties missing __AMQ_CID; got keys %v", keysOf(m.Core.Properties))
	}
	if len(m.QueueIDs) != 2 || m.QueueIDs[0] != 4 || m.QueueIDs[1] != 5 {
		t.Errorf("QueueIDs = %v, want [4 5]", m.QueueIDs)
	}
}

func keysOf(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// TestDecodeCorePayloadRoundTrip covers the store (de)serialization of a
// decoded Core payload used by the Core store record.
func TestDecodeCorePayloadRoundTrip(t *testing.T) {
	sv := Survivor{ID: 1, UserType: AddMessageProtocol, Body: loadCoreRecord(t), Updates: []RawRecord{buildRefUpdate(1, 1, AddRef)}}
	messages, _, err := DecodeMessages([]Survivor{sv})
	if err != nil || len(messages) != 1 || messages[0].Core == nil {
		t.Fatalf("decode: err=%v messages=%d", err, len(messages))
	}
	orig := messages[0].Core
	got, err := DecodeCorePayload(orig.Encode())
	if err != nil {
		t.Fatalf("DecodeCorePayload: %v", err)
	}
	if got.MessageID != orig.MessageID || got.Address != orig.Address || got.Type != orig.Type ||
		got.Durable != orig.Durable || len(got.Body) != len(orig.Body) || len(got.Properties) != len(orig.Properties) {
		t.Errorf("round-trip mismatch:\n got=%+v\norig=%+v", got, orig)
	}
}

func TestDecodeMessagesAddMessageSkipped(t *testing.T) {
	// Legacy core ADD_MESSAGE (userType 31): out of scope for body decode,
	// but still reported per queue via its refs.
	sv := Survivor{
		ID:       14,
		UserType: AddMessage,
		Body:     []byte{0xDE, 0xAD, 0xBE, 0xEF}, // opaque core encoding, never parsed
		Updates:  []RawRecord{buildRefUpdate(14, 9, AddRef)},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(messages), messages)
	}
	got := diag.CoreSkipped[14]
	if len(got) != 1 || got[0] != 9 {
		t.Errorf("diag.CoreSkipped[14] = %v, want [9]", got)
	}
}

func TestDecodeMessagesAddLargeMessageSkipped(t *testing.T) {
	// Core large message (userType 30): out of scope, same treatment as
	// ADD_MESSAGE.
	sv := Survivor{
		ID:       15,
		UserType: AddLargeMessage,
		Body:     []byte{0x01, 0x02},
		Updates:  []RawRecord{buildRefUpdate(15, 6, AddRef)},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(messages), messages)
	}
	got := diag.CoreSkipped[15]
	if len(got) != 1 || got[0] != 6 {
		t.Errorf("diag.CoreSkipped[15] = %v, want [6]", got)
	}
}

func TestDecodeMessagesUndecodableBodyTruncated(t *testing.T) {
	// Persister id 5 is recognized, but nothing follows it: reading the
	// fixed prefix runs off the end of the body.
	sv := Survivor{
		ID:       16,
		UserType: AddMessageProtocol,
		Body:     []byte{persisterAMQPMessageV3},
		Updates:  []RawRecord{buildRefUpdate(16, 1, AddRef)},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(messages), messages)
	}
	if diag.UndecodableBody != 1 {
		t.Errorf("diag.UndecodableBody = %d, want 1", diag.UndecodableBody)
	}
	if diag.UnknownPersister != 0 || len(diag.CoreSkipped) != 0 {
		t.Errorf("diag = %+v, want only UndecodableBody set", diag)
	}
}

func TestDecodeMessagesUndecodableEmptyBody(t *testing.T) {
	sv := Survivor{ID: 17, UserType: AddMessageProtocol, Body: nil}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(messages), messages)
	}
	if diag.UndecodableBody != 1 {
		t.Errorf("diag.UndecodableBody = %d, want 1", diag.UndecodableBody)
	}
}

func TestDecodeMessagesZeroSurvivingRefsDropped(t *testing.T) {
	sv := Survivor{
		ID:       18,
		UserType: AddMessageProtocol,
		Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
		Updates: []RawRecord{
			buildRefUpdate(18, 20, AddRef),
			buildRefUpdate(18, 20, AcknowledgeRef),
		},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages (fully acked, nothing to salvage), got %d: %+v", len(messages), messages)
	}
	if diag.UnknownPersister != 0 || diag.UndecodableBody != 0 || len(diag.CoreSkipped) != 0 {
		t.Errorf("diag = %+v, want zero (a fully-acked message isn't a decode failure)", diag)
	}
}

func TestDecodeMessagesPartialAckKeepsOtherQueue(t *testing.T) {
	sv := Survivor{
		ID:       19,
		UserType: AddMessageProtocol,
		Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
		Updates: []RawRecord{
			buildRefUpdate(19, 20, AddRef),
			buildRefUpdate(19, 21, AddRef),
			buildRefUpdate(19, 20, AcknowledgeRef),
		},
	}

	messages, _, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(messages))
	}
	if len(messages[0].QueueIDs) != 1 || messages[0].QueueIDs[0] != 21 {
		t.Errorf("QueueIDs = %v, want [21]", messages[0].QueueIDs)
	}
}

func TestDecodeMessagesScheduledUpdateSetsScheduledMs(t *testing.T) {
	sv := Survivor{
		ID:       20,
		UserType: AddMessageProtocol,
		Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
		Updates: []RawRecord{
			buildRefUpdate(20, 30, AddRef),
			buildScheduledUpdate(20, 30, 4102444800000),
		},
	}

	messages, _, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(messages))
	}
	if messages[0].ScheduledMs != 4102444800000 {
		t.Errorf("ScheduledMs = %d, want 4102444800000", messages[0].ScheduledMs)
	}
}

func TestDecodeMessagesIgnoresUnrelatedUpdateTypes(t *testing.T) {
	sv := Survivor{
		ID:       21,
		UserType: AddMessageProtocol,
		Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
		Updates: []RawRecord{
			buildRefUpdate(21, 40, AddRef),
			{Type: UpdateRecord, ID: 21, UserType: UpdateDeliveryCount, Body: []byte{0, 0, 0, 3}},
			{Type: UpdateRecord, ID: 21, UserType: DuplicateID, Body: []byte("whatever")},
		},
	}

	messages, _, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("want 1 message, got %d", len(messages))
	}
	if len(messages[0].QueueIDs) != 1 || messages[0].QueueIDs[0] != 40 {
		t.Errorf("QueueIDs = %v, want [40]", messages[0].QueueIDs)
	}
	if messages[0].ScheduledMs != 0 {
		t.Errorf("ScheduledMs = %d, want 0", messages[0].ScheduledMs)
	}
}

func TestDecodeMessagesMalformedRefBodySkipped(t *testing.T) {
	sv := Survivor{
		ID:       22,
		UserType: AddMessageProtocol,
		Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
		Updates: []RawRecord{
			{Type: UpdateRecord, ID: 22, UserType: AddRef, Body: []byte{1, 2}}, // too short for an i64 queueID
		},
	}

	messages, diag, err := DecodeMessages([]Survivor{sv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 0 {
		t.Fatalf("want 0 messages (malformed ref => no surviving queue), got %d: %+v", len(messages), messages)
	}
	if diag.UnknownPersister != 0 || diag.UndecodableBody != 0 || len(diag.CoreSkipped) != 0 {
		t.Errorf("diag = %+v, want zero (a malformed ref is skipped, not a body decode failure)", diag)
	}
}

func TestDecodeMessagesIgnoresPageCursorSurvivor(t *testing.T) {
	messageSv := Survivor{
		ID:       23,
		UserType: AddMessageProtocol,
		Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
		Updates:  []RawRecord{buildRefUpdate(23, 1, AddRef)},
	}
	cursorSv := Survivor{ID: 24, UserType: PageCursorComplete, Body: []byte{1, 2, 3}}
	txSv := Survivor{ID: 25, UserType: PageTransaction, Body: []byte{4, 5, 6}}

	messages, diag, err := DecodeMessages([]Survivor{messageSv, cursorSv, txSv})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != 23 {
		t.Fatalf("want exactly message 23, got %+v", messages)
	}
	if diag.UnknownPersister != 0 || diag.UndecodableBody != 0 || len(diag.CoreSkipped) != 0 {
		t.Errorf("diag = %+v, want zero (page-cursor records are silently out of scope)", diag)
	}
}

func TestDecodeMessagesOutputSortedByAscendingID(t *testing.T) {
	mk := func(id int64) Survivor {
		return Survivor{
			ID:       id,
			UserType: AddMessageProtocol,
			Body:     buildAMQPStandardBody(persisterAMQPMessageV3, []byte("body")),
			Updates:  []RawRecord{buildRefUpdate(id, 1, AddRef)},
		}
	}

	messages, _, err := DecodeMessages([]Survivor{mk(5), mk(1), mk(3)})
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	if len(messages) != 3 {
		t.Fatalf("want 3 messages, got %d", len(messages))
	}
	gotIDs := []int64{messages[0].ID, messages[1].ID, messages[2].ID}
	want := []int64{1, 3, 5}
	for i, id := range gotIDs {
		if id != want[i] {
			t.Errorf("messages[%d].ID = %d, want %d (order = %v)", i, id, want[i], gotIDs)
		}
	}
}

// --- fixture tests: full pipe (ReadJournalDir -> Replayer -> DecodeMessages)
// against the harvested 2.42 broker data dir, cross-checked against
// manifest.json. ---

// manifestEntry / manifestFile mirror testdata/manifest.json's schema
// (harvest_integration_test.go's manifestEntry, in the journal_test build
// tag reserved for the fixture generator). Duplicated here rather than
// shared since that file is gated behind ARTEMISCTL_HARVEST and lives in
// package journal_test, not journal.
type manifestEntry struct {
	BodySha256    string         `json:"bodySha256"`
	BodyLen       int            `json:"bodyLen"`
	Props         map[string]any `json:"props,omitempty"`
	ScheduledAtMs int64          `json:"scheduledAtMs,omitempty"`
}

type manifestFile struct {
	Queues map[string][]manifestEntry `json:"queues"`
}

func loadManifest(t *testing.T) manifestFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m manifestFile
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	return m
}

// decodeFixtureMessages runs the full pipe over the harvested fixture and
// returns the decoded messages, diag, and the queueID -> name map from the
// bindings journal (Task 6).
func decodeFixtureMessages(t *testing.T) ([]Message, MessageDiag, map[int64]string) {
	t.Helper()
	dir := fixtureDir(t)

	names, bdiags, err := ReadQueueBindings(filepath.Join(dir, "bindings"))
	if err != nil {
		t.Fatalf("ReadQueueBindings: %v", err)
	}
	if len(bdiags) != 0 {
		t.Fatalf("bindings diags: %+v", bdiags)
	}

	p := NewReplayer()
	jdiags, err := ReadJournalDir(filepath.Join(dir, "journal"), "activemq-data", "amq", p.Feed)
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(jdiags) != 0 {
		t.Fatalf("journal diags: %+v", jdiags)
	}
	survivors, _ := p.Resolve()

	messages, diag, err := DecodeMessages(survivors)
	if err != nil {
		t.Fatalf("DecodeMessages: %v", err)
	}
	return messages, diag, names
}

// byQueue groups decoded messages by their single queue name. The fixture
// sends every message point-to-point to exactly one queue, so every decoded
// message is expected to carry exactly one surviving queueID.
func byQueue(t *testing.T, messages []Message, names map[int64]string) map[string][]Message {
	t.Helper()
	out := make(map[string][]Message)
	for _, m := range messages {
		if len(m.QueueIDs) != 1 {
			t.Fatalf("message %d has %d queueIDs, want 1: %v", m.ID, len(m.QueueIDs), m.QueueIDs)
		}
		name, ok := names[m.QueueIDs[0]]
		if !ok {
			t.Fatalf("message %d: queueID %d has no binding", m.ID, m.QueueIDs[0])
		}
		out[name] = append(out[name], m)
	}
	return out
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// verifyQueueBodies asserts got and want are the same set of messages
// (by body sha256/len), regardless of order. Set membership (not index
// alignment) because the manifest's per-message order has no relationship to
// the journal record IDs assigned at send time.
func verifyQueueBodies(t *testing.T, got []Message, want []manifestEntry, requireExactCount bool) {
	t.Helper()
	if requireExactCount && len(got) != len(want) {
		t.Fatalf("got %d messages, want %d", len(got), len(want))
	}
	wantLenByHash := make(map[string]int, len(want))
	for _, e := range want {
		wantLenByHash[e.BodySha256] = e.BodyLen
	}
	seen := make(map[string]bool, len(got))
	for _, m := range got {
		var am amqp.Message
		if err := am.UnmarshalBinary(m.AMQP); err != nil {
			t.Fatalf("unmarshal AMQP for message %d: %v", m.ID, err)
		}
		body := am.GetData()
		hash := sha256Hex(body)
		wantLen, ok := wantLenByHash[hash]
		if !ok {
			t.Fatalf("message %d body sha256 %s not present in manifest", m.ID, hash)
		}
		if wantLen != len(body) {
			t.Errorf("message %d body len = %d, want %d", m.ID, len(body), wantLen)
		}
		if seen[hash] {
			t.Errorf("message %d body sha256 %s seen more than once", m.ID, hash)
		}
		seen[hash] = true
	}
}

func TestDecodeMessagesFixturePlain(t *testing.T) {
	messages, _, names := decodeFixtureMessages(t)
	man := loadManifest(t)
	byQ := byQueue(t, messages, names)

	verifyQueueBodies(t, byQ["salvage.plain"], man.Queues["salvage.plain"], true)
}

func TestDecodeMessagesFixtureProps(t *testing.T) {
	messages, _, names := decodeFixtureMessages(t)
	man := loadManifest(t)
	byQ := byQueue(t, messages, names)

	got := byQ["salvage.props"]
	want := man.Queues["salvage.props"]
	verifyQueueBodies(t, got, want, true)

	for _, m := range got {
		var am amqp.Message
		if err := am.UnmarshalBinary(m.AMQP); err != nil {
			t.Fatalf("unmarshal AMQP for message %d: %v", m.ID, err)
		}
		region, ok := am.ApplicationProperties["region"]
		if !ok || fmt.Sprint(region) != "eu" {
			t.Errorf("message %d ApplicationProperties[region] = %v, want eu", m.ID, region)
		}
		attempt, ok := am.ApplicationProperties["attempt"]
		if !ok || fmt.Sprint(attempt) != "1" {
			t.Errorf("message %d ApplicationProperties[attempt] = %v, want 1", m.ID, attempt)
		}
	}
}

func TestDecodeMessagesFixtureScheduled(t *testing.T) {
	messages, _, names := decodeFixtureMessages(t)
	man := loadManifest(t)
	byQ := byQueue(t, messages, names)

	got := byQ["salvage.scheduled"]
	want := man.Queues["salvage.scheduled"]
	verifyQueueBodies(t, got, want, true)

	if len(got) != 1 {
		t.Fatalf("want 1 scheduled message, got %d", len(got))
	}
	if len(want) != 1 {
		t.Fatalf("manifest: want 1 scheduled entry, got %d", len(want))
	}
	if got[0].ScheduledMs != want[0].ScheduledAtMs {
		t.Errorf("ScheduledMs = %d, want %d", got[0].ScheduledMs, want[0].ScheduledAtMs)
	}
}

func TestDecodeMessagesFixtureLarge(t *testing.T) {
	messages, _, names := decodeFixtureMessages(t)
	byQ := byQueue(t, messages, names)

	got := byQ["salvage.large"]
	if len(got) != 1 {
		t.Fatalf("want 1 large message, got %d: %+v", len(got), got)
	}
	if !got[0].Large {
		t.Errorf("Large = false, want true")
	}
}

func TestDecodeMessagesFixtureAcked(t *testing.T) {
	messages, _, names := decodeFixtureMessages(t)
	byQ := byQueue(t, messages, names)

	if got := byQ["salvage.acked"]; len(got) != 0 {
		t.Fatalf("want 0 acked messages, got %d: %+v", len(got), got)
	}
}

// TestDecodeMessagesFixturePagedSpillover covers format_notes.md section 8's
// "paging is a spillover, not a mirror" finding: 44 of the 500
// salvage.paged messages were sent before the address crossed
// maxSizeBytes and landed in the journal like normal messages (the other
// 456 live in page files, out of scope for this reader -- Task 9). The
// brief's original "assert salvage.paged yields 0 here" is wrong for this
// fixture; the corrected count comes from format_notes.md's fixture census.
func TestDecodeMessagesFixturePagedSpillover(t *testing.T) {
	messages, _, names := decodeFixtureMessages(t)
	man := loadManifest(t)
	byQ := byQueue(t, messages, names)

	got := byQ["salvage.paged"]
	if len(got) != 44 {
		t.Fatalf("want 44 journal-resident salvage.paged messages, got %d", len(got))
	}
	// Subset check only (requireExactCount=false): got is 44 of the
	// manifest's full 500-entry set, not all of it.
	verifyQueueBodies(t, got, man.Queues["salvage.paged"], false)
}

// TestDecodeMessagesFixtureDiagClean asserts the fixture -- entirely
// AMQP-produced, no Core-protocol messages -- decodes with zero skips.
func TestDecodeMessagesFixtureDiagClean(t *testing.T) {
	_, diag, _ := decodeFixtureMessages(t)

	if diag.UnknownPersister != 0 {
		t.Errorf("diag.UnknownPersister = %d, want 0", diag.UnknownPersister)
	}
	if diag.UndecodableBody != 0 {
		t.Errorf("diag.UndecodableBody = %d, want 0", diag.UndecodableBody)
	}
	if len(diag.CoreSkipped) != 0 {
		t.Errorf("diag.CoreSkipped = %v, want empty", diag.CoreSkipped)
	}
}

// TestDecodeMessagesFixtureTotalCount cross-checks against
// TestReplayFixtureMessageJournal's independently-derived "56
// AddMessageProtocol survivors" figure (5 plain + 5 props + 1 scheduled + 1
// large + 44 paged = 56).
func TestDecodeMessagesFixtureTotalCount(t *testing.T) {
	messages, _, _ := decodeFixtureMessages(t)
	if len(messages) != 56 {
		t.Fatalf("want 56 decoded messages, got %d", len(messages))
	}
}
