package journal

import "sort"

// Persister ids (PersisterIDs), format_notes.md section 5. Verified against
// apache/activemq-artemis tag 2.42.0 PersisterIDs.java: MAX_PERSISTERS = 5,
// MessagePersister.getPersister(id) = persisters[id-1] (id 0 or > 5 is
// invalid). The persister id is the first byte of an ADD_MESSAGE_PROTOCOL
// (userType 45) record's body. NOTE this corrects the plan's guessed
// numbering (V3 is 5, not 4 -- AMQPLargeMessagePersister is 4).
const (
	// persisterCoreLargeMessage (id 0) documents the full PersisterIDs table
	// for reference; it never appears on an ADD_MESSAGE_PROTOCOL record --
	// core large messages use userType 30 instead (see DecodeMessages).
	persisterCoreLargeMessage byte = 0
	persisterCoreMessage      byte = 1
	persisterAMQPMessage      byte = 2
	persisterAMQPMessageV2    byte = 3
	persisterAMQPLargeMessage byte = 4
	persisterAMQPMessageV3    byte = 5
)

// Core message body type bytes (org.apache.activemq.artemis.api.core.Message
// TEXT_TYPE/BYTES_TYPE/... — verified against a real 2.42.0 record, which is a
// BYTES message, type 4). DEFAULT is the untyped core message.
const (
	CoreTypeDefault byte = 0
	CoreTypeObject  byte = 2
	CoreTypeText    byte = 3
	CoreTypeBytes   byte = 4
	CoreTypeMap     byte = 5
	CoreTypeStream  byte = 6
)

// Core buffer framing constants, verified byte-for-byte against a real 2.42.0
// broker record (testdata/core-record-2.42.bin); see format_notes.md §5c.
// coreBufferHeaderSpace = PacketImpl.PACKET_HEADERS_SIZE (SIZE_INT + SIZE_BYTE
// + SIZE_LONG); coreBodyOffset = DataConstants.SIZE_INT.
const (
	coreBufferHeaderSpace = 13
	coreBodyOffset        = 4
)

// CorePayload is a decoded Artemis Core-protocol message, either a standard
// message (journal userType 45 / persister id 1, or a persister-1 page entry)
// or a Core large message (userType 30, whose Body is joined from
// data/large-messages/<id>.msg). It is what a Core store record carries so it
// can be dumped and later converted to AMQP for redelivery (broker/coreconvert).
type CorePayload struct {
	MessageID  int64
	Address    string
	UserID     []byte // 16-byte UUID, or nil when absent
	Type       byte   // CoreType* — 0 DEFAULT, 3 TEXT, 4 BYTES, 5 MAP, 2 OBJECT, 6 STREAM
	Durable    bool
	Expiration int64
	Timestamp  int64
	Priority   byte
	Properties map[string]any // decoded TypedProperties (may be nil)
	Body       []byte         // message body bytes; empty for Large until joined
	Large      bool
}

// decodeCoreHeaders decodes a CoreMessage encodeHeadersAndProperties block
// (format_notes.md §5c): long messageID, nullableSimpleString address, a
// userID null-flag byte (+16 bytes when NOT_NULL), byte type, boolean durable,
// long expiration, long timestamp, byte priority, then TypedProperties. It
// returns nil if the block runs past the available bytes. Shared by the
// standard-message decoder (after its endOfBodyPosition/body prefix) and the
// Core large decoder (which is this block verbatim, body-in-.msg).
func decodeCoreHeaders(r *reader) *CorePayload {
	p := &CorePayload{}
	p.MessageID = r.i64()
	addr, _ := r.nullableSimpleString()
	p.Address = addr
	flag := r.u8()
	if r.err() != nil {
		return nil
	}
	if flag == dcNotNull {
		p.UserID = append([]byte(nil), r.bytes(16)...)
	}
	p.Type = r.u8()
	p.Durable = r.bool()
	p.Expiration = r.i64()
	p.Timestamp = r.i64()
	p.Priority = r.u8()
	p.Properties = r.typedProperties()
	if r.err() != nil {
		return nil
	}
	return p
}

// decodeCoreStandardBody decodes a CoreMessagePersister (id 1) payload, with r
// positioned just past the persister-id byte (format_notes.md §5c): a long
// messageID and nullableSimpleString address prefix (both redundant with the
// authoritative copies inside the headers block, so read-and-discarded), an
// int bufferSize, then the CoreMessage buffer whose first int is
// endOfBodyPosition. The body is buffer[coreBodyOffset : endOfBodyPosition -
// coreBufferHeaderSpace + coreBodyOffset]; the headers block follows. Returns
// nil on any truncation.
func decodeCoreStandardBody(r *reader) *CorePayload {
	r.i64()                  // messageID prefix — authoritative copy is in the headers
	r.nullableSimpleString() // address prefix — ditto
	r.i32()                  // bufferSize (message.persist length prefix); body/headers self-delimit
	if r.err() != nil {
		return nil
	}
	endOfBody := int(r.i32()) // CoreMessage buffer[0..4): endOfBodyPosition
	if r.err() != nil {
		return nil
	}
	bodyLen := endOfBody - coreBufferHeaderSpace
	if bodyLen < 0 {
		return nil
	}
	body := append([]byte(nil), r.bytes(bodyLen)...)
	if r.err() != nil {
		return nil
	}
	p := decodeCoreHeaders(r)
	if p == nil {
		return nil
	}
	p.Body = body
	return p
}

// Message is one decoded, surviving journal message, ready to be written to
// the .artx store (Large messages still need their body joined from
// data/large-messages/<ID>.msg by Task 8).
type Message struct {
	ID          int64
	AMQP        []byte       // raw AMQP wire bytes, verbatim from the persister payload; empty for Large or Core messages
	Core        *CorePayload // non-nil for a decoded Core-protocol message (AMQP is empty); Core.Large marks a body joined from large-messages dir
	Large       bool         // AMQP large message: body joined from large-messages dir (Task 8); AMQP is empty here, not partial
	ScheduledMs int64        // 0 = none; from a SET_SCHEDULED_DELIVERY_TIME update
	QueueIDs    []int64      // surviving refs (ADD_REF minus ACKNOWLEDGE_REF), first-seen order
}

// largeBodyTarget reports whether m needs its body joined from the
// large-messages dir, and whether it is a Core (vs AMQP) large message.
func (m Message) largeBodyTarget() (isLarge, isCore bool) {
	if m.Core != nil && m.Core.Large {
		return true, true
	}
	return m.Large, false
}

// MessageDiag itemizes skips per spec §1/§5.
type MessageDiag struct {
	CoreSkipped      map[int64][]int64 // messageID → surviving queueIDs, for Core-protocol messages (userType 31/30, or persister id 1 under userType 45) this AMQP-only reader cannot decode
	UnknownPersister int               // ADD_MESSAGE_PROTOCOL records whose first byte isn't a recognized persister id
	UndecodableBody  int               // records whose body was too short/malformed to parse past the point a persister id was identified
}

// DecodeMessages walks message-journal survivors and joins messages with
// their refs. Messages (and Core-skipped entries) with zero surviving refs
// are dropped entirely -- format_notes.md section 9's removeAcked: a message
// with no remaining queue refs has already been fully consumed, so there is
// nothing left to salvage or usefully report as skipped.
//
// Design note (page-cursor / non-message records, and the brief's
// CursorState question): the message journal interleaves message survivors
// (ADD_MESSAGE_PROTOCOL/ADD_MESSAGE/ADD_LARGE_MESSAGE, userTypes 45/31/30)
// with unrelated record families that replay through the same Replayer:
// PAGE_TRANSACTION (35) and the PAGE_CURSOR_*/ACKNOWLEDGE_CURSOR family
// (39-43), each journaled as its own top-level survivor with its own record
// ID (not as an update riding on a message ID). DecodeMessages' contract is
// message decode only: it recognizes exactly the three message-family
// userTypes above and silently ignores every other survivor -- no
// CursorRecords helper, no filtering of the input slice. Task 9 re-scans the
// same []Survivor returned by Replayer.Resolve for its own record families.
// This is the simpler of the brief's two options: it costs nothing here (a
// no-op default case) and avoids threading a second typed accessor through
// this package for state DecodeMessages never touches.
//
// error is reserved for future use (e.g. a structural invariant violation);
// per-record decode failures are diagnostic counters, not errors, so every
// call currently returns a nil error.
func DecodeMessages(survivors []Survivor) ([]Message, MessageDiag, error) {
	diag := MessageDiag{CoreSkipped: make(map[int64][]int64)}
	var out []Message

	for _, sv := range survivors {
		switch sv.UserType {
		case AddMessageProtocol:
			msg, outcome := decodeAddMessageProtocol(sv)
			switch outcome {
			case outcomeUndecodable:
				diag.UndecodableBody++
			case outcomeUnknownPersister:
				diag.UnknownPersister++
			case outcomeCore, outcomeMessage:
				// Both AMQP and decoded-Core messages are exported; the Message
				// carries either AMQP bytes or a *CorePayload.
				if len(msg.QueueIDs) > 0 {
					out = append(out, msg)
				}
			}

		case AddLargeMessage:
			// Core large message (userType 30, format_notes.md §5c): its body is
			// the encodeHeadersAndProperties block directly (no persister-id /
			// endOfBodyPosition prefix), and the message body lives in
			// data/large-messages/<id>.msg (joined by AttachLargeBodies).
			queueIDs, scheduledMs := decodeRefs(sv.Updates)
			core := decodeCoreHeaders(newReader(sv.Body))
			if core == nil {
				// Undecodable core-large header: still report the skip per queue.
				if len(queueIDs) > 0 {
					diag.CoreSkipped[sv.ID] = queueIDs
				}
				continue
			}
			core.Large = true
			if len(queueIDs) > 0 {
				out = append(out, Message{ID: sv.ID, Core: core, ScheduledMs: scheduledMs, QueueIDs: queueIDs})
			}

		case AddMessage:
			// Legacy pre-persister-id core add (userType 31): not produced by
			// Artemis 2.42 (which writes core messages as userType 45 / persister
			// 1), unverified against real bytes, so still reported as a skip
			// rather than best-effort decoded. Walk the refs to attribute it.
			queueIDs, _ := decodeRefs(sv.Updates)
			if len(queueIDs) > 0 {
				diag.CoreSkipped[sv.ID] = queueIDs
			}

		default:
			// Not a message record -- see the design note above.
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, diag, nil
}

// decodeOutcome classifies how decodeAddMessageProtocol resolved one
// ADD_MESSAGE_PROTOCOL survivor.
type decodeOutcome int

const (
	outcomeMessage decodeOutcome = iota
	outcomeCore
	outcomeUnknownPersister
	outcomeUndecodable
)

// decodeAddMessageProtocol decodes one ADD_MESSAGE_PROTOCOL (userType 45)
// survivor via the shared decodePersisterPayload (format_notes.md section 5:
// the body's first byte is the persister id, dispatching to Core (skip),
// AMQP standard (2/3/5), or AMQP large (4)).
func decodeAddMessageProtocol(sv Survivor) (Message, decodeOutcome) {
	r := newReader(sv.Body)
	amqpBytes, core, _, outcome := decodePersisterPayload(r)
	queueIDs, scheduledMs := decodeRefs(sv.Updates)

	switch outcome {
	case persisterDecodeCore:
		return Message{ID: sv.ID, Core: core, ScheduledMs: scheduledMs, QueueIDs: queueIDs}, outcomeCore

	case persisterDecodeLarge:
		// format_notes.md section 5b: the large body itself is not in the
		// journal (it lives in data/large-messages/<messageID>.msg, joined by
		// Task 8); nothing else in this record (durable/format/address/extra
		// props/saved-encoding block) is needed for Task 7's contract.
		return Message{ID: sv.ID, Large: true, ScheduledMs: scheduledMs, QueueIDs: queueIDs}, outcomeMessage

	case persisterDecodeStandard:
		return Message{ID: sv.ID, AMQP: amqpBytes, ScheduledMs: scheduledMs, QueueIDs: queueIDs}, outcomeMessage

	case persisterDecodeUnknown:
		return Message{}, outcomeUnknownPersister

	default: // persisterDecodeMalformed
		return Message{}, outcomeUndecodable
	}
}

// persisterDecodeOutcome classifies how decodePersisterPayload resolved a
// persister-tagged payload -- shared by message.go's ADD_MESSAGE_PROTOCOL
// bodies (format_notes.md section 5) and paging.go's PagedMessage entries
// (format_notes.md section 8), which both lead with a persister-id byte
// dispatching to the same cases.
type persisterDecodeOutcome int

const (
	// persisterDecodeStandard: AMQP standard (2/3/5). amqpBytes is
	// populated; r is left positioned just past the raw AMQP bytes -- the
	// V2/V3 tail (extra properties, V3's expiration) is NOT consumed, see
	// decodePersisterPayload's doc comment.
	persisterDecodeStandard persisterDecodeOutcome = iota
	// persisterDecodeLarge: AMQP large (4). The body lives outside this
	// payload entirely (data/large-messages/<id>.msg for journal-resident
	// messages; out of scope for paged entries, format_notes.md section 8).
	persisterDecodeLarge
	// persisterDecodeCore: Core message (persister 1), decoded into the
	// returned *CorePayload (format_notes.md §5c).
	persisterDecodeCore
	// persisterDecodeUnknown: an unrecognized persister-id byte.
	persisterDecodeUnknown
	// persisterDecodeMalformed: a recognized id, but the fixed prefix ran
	// past the available bytes.
	persisterDecodeMalformed
)

// decodePersisterPayload decodes a MessagePersister-encoded payload's
// leading dispatch (format_notes.md section 5's PersisterIDs table): a
// persister-id byte, then -- for the AMQP standard cases (2/3/5) only -- the
// fixed prefix through the raw AMQP bytes (format_notes.md section 5a:
// messageID, messageFormat, nullableSimpleString address, int amqpSize,
// amqpSize raw AMQP bytes). It reads from r in place (rather than taking a
// []byte) so a caller that needs to keep parsing past the payload --
// paging.go's PagedMessage, which has queueIDs following it -- can continue
// from the same cursor; a caller with nothing left to read (this file's
// ADD_MESSAGE_PROTOCOL body) simply stops.
//
// It deliberately does NOT consume the V2/V3 tail (int extraPropsSize +
// that many bytes of TypedProperties, V3's long expiration) even though
// that tail is formally part of "the persister payload" in the Artemis
// source: neither this file's caller needs its content, and the existing
// synthetic tests (buildAMQPStandardBody in message_test.go) don't encode
// it. paging.go, which DOES need the exact end offset to find the
// queueIDsCount field that follows, skips that tail itself using the
// returned persisterID (see paging.go's skipPersisterTail).
//
// Deviation from the task-9 brief: the brief specified
// `decodePersisterPayload(b []byte) (amqpBytes []byte, scheduledMs int64,
// core bool, err error)`. scheduledMs is dropped entirely -- it was never
// part of either caller's persister payload (this file's comes from a
// separate SET_SCHEDULED_DELIVERY_TIME update, decodeRefs below; a
// PagedMessage has no scheduled-delivery field on disk at all, per
// format_notes.md section 8's field table). `core bool, err error` are
// widened to `persisterID byte, persisterDecodeOutcome` so both callers keep
// Task 7's existing unknown-persister/malformed-body diagnostic granularity
// (and paging.go gets the persister id it needs for the tail skip) instead
// of collapsing everything to one generic error. The []byte parameter
// became a *reader for the in-place-continuation reason above.
func decodePersisterPayload(r *reader) (amqpBytes []byte, core *CorePayload, persisterID byte, outcome persisterDecodeOutcome) {
	persisterID = r.u8()
	if r.err() != nil {
		return nil, nil, 0, persisterDecodeMalformed
	}

	switch persisterID {
	case persisterCoreMessage:
		core = decodeCoreStandardBody(r)
		if core == nil {
			return nil, nil, persisterID, persisterDecodeMalformed
		}
		return nil, core, persisterID, persisterDecodeCore

	case persisterAMQPLargeMessage:
		return nil, nil, persisterID, persisterDecodeLarge

	case persisterAMQPMessage, persisterAMQPMessageV2, persisterAMQPMessageV3:
		amqp, ok := decodeAMQPStandardBody(r)
		if !ok {
			return nil, nil, persisterID, persisterDecodeMalformed
		}
		return amqp, nil, persisterID, persisterDecodeStandard

	default:
		return nil, nil, persisterID, persisterDecodeUnknown
	}
}

// decodeAMQPStandardBody decodes the remainder of an AMQPMessagePersister /
// V2 / V3 record after the persister id byte (format_notes.md section 5a):
// long messageID, long messageFormat, nullableSimpleString address, int
// amqpSize, then amqpSize bytes of raw AMQP-encoded message. The AMQP body
// is length-prefixed, so the V2/V3 tail (extra properties, V3's expiration
// long) is unambiguous and simply left unread -- Task 7 only needs the raw
// AMQP bytes, and the message ID used to join refs is the journal record ID
// (Survivor.ID), not this redundant persister-internal copy.
func decodeAMQPStandardBody(r *reader) ([]byte, bool) {
	r.i64()                  // messageID: redundant with Survivor.ID, not surfaced
	r.i64()                  // messageFormat: not surfaced on Message
	r.nullableSimpleString() // address: not surfaced (queue mapping comes from bindings + refs)
	amqpSize := r.i32()
	raw := r.bytes(int(amqpSize))
	if r.err() != nil {
		return nil, false
	}
	amqpBytes := make([]byte, len(raw))
	copy(amqpBytes, raw)
	return amqpBytes, true
}

// decodeRefs nets a message's surviving queue refs and scheduled delivery
// time out of its Updates (format_notes.md section 9): ADD_REF (32)
// increments a queueID's count, ACKNOWLEDGE_REF (33) decrements it, and only
// queueIDs with a positive count survive -- mirroring removeAcked's "no
// remaining refs" elimination without relying on the broker having also
// deleted the message record outright. SET_SCHEDULED_DELIVERY_TIME (36)
// updates set scheduledMs (ScheduledDeliveryEncoding: long queueID, long
// scheduledDeliveryTime -- the queueID is not surfaced on Message, which
// carries a single scheduled time regardless of queue count). Every other
// update userType (UPDATE_DELIVERY_COUNT, DUPLICATE_ID, ACK_RETRY, ...) is
// ignored here per the task brief. A malformed ref/scheduled body (should
// not happen against real journal bytes) is skipped rather than treated as
// fatal, consistent with this reader's general resync-not-abort posture.
func decodeRefs(updates []RawRecord) (queueIDs []int64, scheduledMs int64) {
	counts := make(map[int64]int)
	var order []int64 // first-seen order, for a deterministic QueueIDs result

	for _, u := range updates {
		switch u.UserType {
		case AddRef:
			qid, ok := decodeQueueID(u.Body)
			if !ok {
				continue
			}
			if _, seen := counts[qid]; !seen {
				order = append(order, qid)
			}
			counts[qid]++

		case AcknowledgeRef:
			qid, ok := decodeQueueID(u.Body)
			if !ok {
				continue
			}
			counts[qid]--

		case SetScheduledDeliveryTime:
			_, ms, ok := decodeScheduledDelivery(u.Body)
			if ok {
				scheduledMs = ms
			}
		}
	}

	for _, qid := range order {
		if counts[qid] > 0 {
			queueIDs = append(queueIDs, qid)
		}
	}
	return queueIDs, scheduledMs
}

// decodeQueueID decodes a RefEncoding body (format_notes.md section 9's
// "Supporting codec field orders": QueueEncoding.decode = long queueID).
func decodeQueueID(body []byte) (int64, bool) {
	r := newReader(body)
	qid := r.i64()
	if r.err() != nil {
		return 0, false
	}
	return qid, true
}

// decodeScheduledDelivery decodes a ScheduledDeliveryEncoding body
// (format_notes.md section 9: long queueID, long scheduledDeliveryTime).
func decodeScheduledDelivery(body []byte) (queueID, scheduledDeliveryTime int64, ok bool) {
	r := newReader(body)
	queueID = r.i64()
	scheduledDeliveryTime = r.i64()
	if r.err() != nil {
		return 0, 0, false
	}
	return queueID, scheduledDeliveryTime, true
}
