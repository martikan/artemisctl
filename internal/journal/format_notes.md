# Artemis 2.42 on-disk format notes (verified against tag 2.42.0)

Ground truth for the `salvage` offline reader. Every constant below was read from
`apache/activemq-artemis` at tag **2.42.0** (raw.githubusercontent.com). Byte
offsets cross-checked against the harvested fixture (`testdata/artemis-2.42-data.tar.gz`);
see "Fixture cross-check" at the bottom. **The fixture bytes are the final authority.**

## Endianness (read this first)

- All journal, bindings, and paging multi-byte **int/long/short** are **big-endian**
  (Java `ByteBuffer` / Netty `ByteBuf` default order). This differs from the tool's
  own `.artx` store (little-endian).
- `SimpleString` character data is stored as **little-endian UTF-16 pairs** (low byte,
  then high byte) — see SimpleString section. The `int` length prefix in front of it
  is still big-endian.
- `float` = big-endian int of `Float.floatToIntBits`; `double` = big-endian long of
  `Double.doubleToLongBits`.
- **boolean true = 0xFF** (ActiveMQBuffer's `writeBoolean` writes -1, not 1; fixture:
  the large-message durable byte is `ff`). Decode as nonzero = true.

---

## 1. Journal file header (`SIZE_HEADER` = 16 bytes)

Source: `JournalImpl.readFileHeader` / `writeHeader` / `SIZE_HEADER`
(`artemis-journal/.../core/journal/impl/JournalImpl.java`, lines 158, 3062-3130).

`SIZE_HEADER = SIZE_LONG + SIZE_INT + SIZE_INT = 16`. Note the constant's *summands*
are ordered fileID+version+version, but the actual **write/read order** is:

| off | size | field | value |
| --- | ---- | ----- | ----- |
| 0 | 4 | int formatVersion | **2** (`FORMAT_VERSION`; compatible: {1}) |
| 4 | 4 | int userVersion | broker user version (echoed, must match) |
| 8 | 8 | long fileID | file id (ordering id) |

The stale comment at line 613 ("First long is the ordering timestamp") is wrong; the
loop just skips `SIZE_HEADER` bytes. There is **no** timestamp field.

Files below `SIZE_HEADER` bytes are treated as damaged/empty (return -1, skip).
`MIN_FILE_SIZE = 1024`. Default journal files are named `activemq-data-N.amq`;
bindings journal files are `activemq-bindings-N.bindings` with the same header/framing.

---

## 2. Record type bytes (the outer journal framing type)

Source: `JournalImpl.java` lines 169-207. **`EVENT_RECORD=10` exists in 2.42.**

| const | value | body? | tx? | complete-tx? |
| ----- | ----- | ----- | --- | ------------ |
| EVENT_RECORD | 10 | yes | no | no |
| ADD_RECORD | 11 | yes | no | no |
| UPDATE_RECORD | 12 | yes | no | no |
| ADD_RECORD_TX | 13 | yes | yes | no |
| UPDATE_RECORD_TX | 14 | yes | yes | no |
| DELETE_RECORD_TX | 15 | yes | yes | no |
| DELETE_RECORD | 16 | no | no | no |
| PREPARE_RECORD | 17 | no | no | yes |
| COMMIT_RECORD | 18 | no | no | yes |
| ROLLBACK_RECORD | 19 | no | no | yes |

- **Valid record-type range = [10, 19]** (`recordType < EVENT_RECORD || recordType > ROLLBACK_RECORD` ⇒ skip).
- `isContainsBody(t)` = `t >= 10 && t <= 15` (EVENT/ADD/UPDATE/ADD_TX/UPDATE_TX/DELETE_TX).
- `isTransaction(t)` = ADD_TX/UPDATE_TX/DELETE_TX (13,14,15).
- `isCompleteTransaction(t)` = COMMIT/PREPARE/ROLLBACK (18,17,19).
- `FILL_CHARACTER = 'J'` (0x4A) — padding fill; 0x4A > 19 so it fails the range test and
  is skipped like any other non-record byte.

---

## 3. Per-record framing (`readJournalFile`, journalVersion >= 2)

Source: `JournalImpl.readJournalFile` (lines 595-900) and `getRecordSize` (3040-3060).
Sizes: `BASIC_SIZE = SIZE_BYTE + SIZE_INT + SIZE_INT = 9` (type byte + fileID int + trailing
check-size int). In v2, `getRecordSize` adds +1 for the compactCount byte.

### ADD_RECORD / UPDATE_RECORD / EVENT_RECORD (11/12/10) — carry user records

| off | size | field |
| --- | ---- | ----- |
| 0 | 1 | recordType byte (10/11/12) |
| 1 | 4 | int fileID echo (see note) |
| 5 | 1 | byte compactCount (v>=2 only) |
| 6 | 8 | long recordID |
| 14 | 4 | int variableSize (= length of `record`) |
| 18 | 1 | byte userRecordType (JournalRecordIds, e.g. 45) |
| 19 | variableSize | record data (persister/codec encoded) |
| 19+variableSize | 4 | int checkSize = variableSize + recordSize |

`recordSize` for ADD/UPDATE/EVENT (v2) = `SIZE_ADD_RECORD(22) + 1 = 23`. So
`checkSize = variableSize + 23`, and total on-disk record length = `23 + variableSize`.

### *_RECORD_TX (13/14) — same but with transactionID before recordID

Insert `[+8 long transactionID]` immediately after compactCount (off 6), shifting the
rest down 8. `recordSize` = `SIZE_ADD_RECORD_TX(30)+1 = 31`.

### DELETE_RECORD_TX (15)

type, fileID, compactCount, transactionID(8), recordID(8), int variableSize, record
(NO userRecordType byte — `recordType != DELETE_RECORD_TX` guards the userRecordType
read), checkSize. `recordSize = SIZE_DELETE_RECORD_TX(29)+1 = 30`.

**Correction (Task 4, re-verified against JournalImpl.java tag 2.42.0 source
directly):** the value `21` above was a transcription error. The real constant is
`SIZE_DELETE_RECORD_TX = BASIC_SIZE(9) + SIZE_LONG(8) + SIZE_LONG(8) + SIZE_INT(4) = 29`,
so the v2 (compactCount-adjusted) recordSize is `29+1 = 30`, not `22`. This type does not
appear in the harvested fixture, so it is unverified against real bytes, but `29` is
read straight from source and is internally consistent with every other SIZE_* constant
in this section (each one is BASIC_SIZE plus the sum of its type-specific fixed fields).

### DELETE_RECORD (16)

type, fileID, compactCount, recordID(8), checkSize. No body. `recordSize = SIZE_DELETE_RECORD(17)+1 = 18`.

### COMMIT_RECORD (18) / PREPARE_RECORD (17) / ROLLBACK_RECORD (19)

No recordID (complete-transaction). transactionID(8) present. COMMIT/PREPARE also read
`int transactionCheckNumberOfRecords`; PREPARE additionally reads `int
preparedTransactionExtraDataSize` + that many extra bytes. ROLLBACK = type+fileID+
compactCount+transactionID+checkSize.

### fileID echo note

The per-record fileID is read with `getInt()` (4 bytes) but the header fileID is a
`long`. The per-record echo is the **low 32 bits** of the file's long fileID
(`JournalFileImpl.getRecordID()` returns an int).

### Resync / corruption rules (CRITICAL — corrects the plan's "stop file" wording)

`readJournalFile` **never aborts the whole file** on a bad record. In every failure
case it repositions to `pos+1` (or `pos + SIZE_BYTE`) and keeps scanning to end-of-file:

- recordType outside [10,19] ⇒ `continue` (position already at pos+1). Padding/holes/`'J'` fill land here.
- `isInvalidSize(...)` (a field read would run past the file) ⇒ `position = pos+1`, continue.
- **fileID mismatch** (`readFileId != file.getRecordID() && !reclaimed`) ⇒ leftover from a
  reused file ⇒ `position = pos+1`, continue (skip this record, keep scanning). *Not* a stop.
- **checkSize mismatch** (trailing int != variableSize+recordSize+extra) ⇒ corruption ⇒
  `markAsDataFile`, `position = pos+1`, continue. *Not* a stop.

So the offline reader should mirror this: resync byte-by-byte, never truncate the file
on a single bad record. (The harvested fixture is clean, so every record validates on
the first try; the resync path is exercised synthetically in later tasks.)

---

## 4. User record type ids (`JournalRecordIds`)

Source: `artemis-server/.../persistence/impl/journal/JournalRecordIds.java`. All values
match the plan's expectations. **No `EVENT_RECORD` here** (that lives in the framing type
table, section 2). Full table:

| id | const | journal | used by salvage |
| -- | ----- | ------- | --------------- |
| 20 | GROUP_RECORD | msg | no |
| 21 | QUEUE_BINDING_RECORD | bindings | yes (queue id ↔ name/address) |
| 22 | QUEUE_STATUS_RECORD | bindings | no |
| 24 | ID_COUNTER_RECORD | bindings | no |
| 25 | ADDRESS_SETTING_RECORD | bindings | no |
| 26 | SECURITY_SETTING_RECORD | bindings | no |
| 27 | DIVERT_RECORD | bindings | no |
| 28 | BRIDGE_RECORD | bindings | no |
| 29 | ADD_LARGE_MESSAGE_PENDING | msg | no (deprecated) |
| 30 | ADD_LARGE_MESSAGE | msg | core large msgs only — **AMQP large msgs arrive as 45, see §5b** |
| 31 | ADD_MESSAGE | msg | yes (legacy core) |
| 32 | ADD_REF | msg | yes (msg↔queue placement) |
| 33 | ACKNOWLEDGE_REF | msg | yes (survivor elimination) |
| 34 | UPDATE_DELIVERY_COUNT | msg | optional |
| 35 | PAGE_TRANSACTION | msg | no (v1) |
| 36 | SET_SCHEDULED_DELIVERY_TIME | msg | yes (scheduled time via UPDATE) |
| 37 | DUPLICATE_ID | msg | no |
| 38 | HEURISTIC_COMPLETION | msg | no |
| 39 | ACKNOWLEDGE_CURSOR | msg | no (v1) |
| 40 | PAGE_CURSOR_COUNTER_VALUE | msg | no |
| 41 | PAGE_CURSOR_COUNTER_INC | msg | no |
| 42 | PAGE_CURSOR_COMPLETE | msg | no |
| 43 | PAGE_CURSOR_PENDING_COUNTER | msg | no |
| 44 | ADDRESS_BINDING_RECORD | bindings | no |
| 45 | ADD_MESSAGE_PROTOCOL | msg | **yes (the AMQP message add)** |
| 46 | ADDRESS_STATUS_RECORD | bindings | no |
| 47 | USER_RECORD | bindings | no |
| 48 | ROLE_RECORD | bindings | no |
| 49 | ADD_MESSAGE_BODY | msg | no (history) |
| 50 | KEY_VALUE_PAIR_RECORD | bindings | no |
| 51 | CONNECTOR_RECORD | bindings | no |
| 52 | ADDRESS_SETTING_RECORD_JSON | bindings | no |
| 53 | ACK_RETRY | msg | no |

---

## 5. Persister ids (`PersisterIDs`) — CORRECTION vs plan

Source: `artemis-server/.../core/persistence/PersisterIDs.java`. `MAX_PERSISTERS = 5`.
**The plan guessed V3=4; it is actually V3=5, and AMQPLargeMessage=4.**

| id | persister |
| -- | --------- |
| 0 | CoreLargeMessagePersister |
| 1 | CoreMessagePersister |
| 2 | AMQPMessagePersister |
| 3 | AMQPMessagePersisterV2 |
| **4** | **AMQPLargeMessagePersister** |
| **5** | **AMQPMessagePersisterV3** |

`MessagePersister.getPersister(id)` maps `persisters[id-1]`; `id==0 || id>5` ⇒ null.
The persister id is the **first byte** of the record data (`MessagePersister.encode`
writes `buffer.writeByte(getID())`). Artemis 2.42 writes AMQP standard messages with the
**V3** persister (id 5) and AMQP large messages with id 4.

### 5a. AMQP standard message record data (ADD_MESSAGE_PROTOCOL, userType 45)

Encode chain: `MessagePersister.encode` → `AMQPMessagePersister.encode` →
(V2) extra props → (V3) expiration. Source:
`AMQPMessagePersister.encode` (lines 57-63), `AMQPMessagePersisterV2.encode` (65-73),
`AMQPMessagePersisterV3.encode` (54-59), `AMQPStandardMessage.persist` (210-218).

| off | size | field | present in |
| --- | ---- | ----- | ---------- |
| 0 | 1 | byte persisterID (2 / 3 / 5) | all |
| 1 | 8 | long messageID | all |
| 9 | 8 | long messageFormat (AMQP format) | all |
| 17 | var | nullableSimpleString address | all |
| . | 4 | int amqpSize (= internalPersistSize) | all |
| . | amqpSize | **raw AMQP-encoded message bytes** (Header…Footer) | all |
| . | 4 | int extraPropsSize | V2, V3 |
| . | extraPropsSize | TypedProperties extra props (if size != 0) | V2, V3 |
| . | 8 | long expiration | V3 only |

`msg.AMQP` for Task 7 = the `amqpSize` bytes verbatim. The AMQP body is
**length-prefixed** by `int amqpSize`, so the extra-props/expiration tail is
unambiguous (do not treat "all remaining bytes" as the AMQP message).

### 5b. AMQP large message record data (persisterID 4) — CORRECTION vs plan

**FIXTURE FINDING: AMQP large messages are journaled under userType 45
(ADD_MESSAGE_PROTOCOL) with persisterID 4 — NOT under userType 30 ADD_LARGE_MESSAGE.**
The fixture's 300 KiB message is an ADD_RECORD, userType 45, first data byte 4.
userType 30 is used for *core* large messages only. Dispatch on the persister id
byte, not the userType, to tell standard vs large AMQP messages apart.

Source: `AMQPLargeMessagePersister.encode/decode` (lines 74-137),
`AMQPLargeMessage.saveEncoding/readSavedEncoding` (210-281).

| off | size | field |
| --- | ---- | ----- |
| 0 | 1 | byte persisterID = 4 |
| 1 | 8 | long messageID |
| 9 | 1 | boolean durable |
| 10 | 8 | long messageFormat |
| 18 | var | nullableSimpleString address |
| . | 4 | int extraPropsSize (0 ⇒ none) |
| . | extraPropsSize | TypedProperties extra props |
| . | var | **saved encoding** (section positions/sizes interleaved with AMQP-encoded Header/MessageAnnotations/Properties/ApplicationProperties objects) |
| . | 8 | long expiration |
| . | 1 | boolean reencoded |

The **large body itself is NOT in the journal**; it lives in
`data/large-messages/<messageID>.msg`. The "saved encoding" block is a self-delimiting
sequence of `int` position/size fields interleaved with AMQP object encodings (9 ints:
headerPosition, encodedHeaderSize, [Header], deliveryAnnotationsPosition,
encodedDeliveryAnnotationsSize, messageAnnotationsPosition, [MessageAnnotations],
propertiesPosition, [Properties], applicationPropertiesPosition, remainingBodyPosition,
[ApplicationProperties]). Positions may be -1 (section absent); an absent AMQP object
encodes as the single AMQP null byte `0x40`.

**Fixture-verified join rule for Task 8**: `data/large-messages/<messageID>.msg` holds
the **complete AMQP-encoded message** (Header + Properties + … + Data section with the
full body) — fixture `64.msg` is 307255 bytes = 55 bytes of sections + 307200 body, and
it starts with `00 53 70` (Header descriptor), not raw body bytes. So
`msg.AMQP = the .msg file bytes verbatim`; the journal record's saved-encoding block is
only a section index / header cross-check. Fixture extra props observed:
`_AMQ_AD` (STRING) = the original address name.

### 5c. CoreMessage (persister id 1, userType 45) — DECODED

**Verified byte-for-byte against a real 2.42.0 record** harvested with `artemis
producer --protocol CORE` (committed as `testdata/core-record-2.42.bin`, a BYTES
message: messageID 28, address `salvage.core`, 120-byte body). Modern 2.42 writes core
messages as **ADD_MESSAGE_PROTOCOL (userType 45) with persister id 1**, not userType 31.

`CoreMessagePersister.encode` writes a `messageID` + `address` prefix, then delegates to
`CoreMessage.persist` (`writeInt(buffer.writerIndex())` + the buffer). The buffer holds
`endOfBodyPosition`, the body, then `encodeHeadersAndProperties` at the end:

| off | size | field |
| --- | ---- | ----- |
| 0 | 1 | byte persisterID = 1 |
| 1 | 8 | long messageID (prefix; redundant with headers) |
| . | var | nullableSimpleString address (prefix; redundant with headers) |
| . | 4 | int bufferSize (`message.persist` length prefix) |
| . | 4 | int endOfBodyPosition (CoreMessage buffer[0..4)) |
| . | endOfBodyPosition − 13 | **message body bytes** |
| . | var | encodeHeadersAndProperties (below) |

`encodeHeadersAndProperties`: `long messageID`, `nullableSimpleString address`, `byte
userID-null-flag` (+16-byte UUID when NOT_NULL), `byte type`, `boolean durable`, `long
expiration`, `long timestamp`, `byte priority`, `TypedProperties`.

Constants (verified): **`BUFFER_HEADER_SPACE = PacketImpl.PACKET_HEADERS_SIZE = 13`**
(SIZE_INT + SIZE_BYTE + SIZE_LONG), `BODY_OFFSET = 4`. Body = `buffer[BODY_OFFSET :
endOfBodyPosition − BUFFER_HEADER_SPACE + BODY_OFFSET]` = `buffer[4 :
endOfBodyPosition − 9]`; headers follow. Core `type` byte: 0 DEFAULT, 2 OBJECT, 3 TEXT,
4 BYTES, 5 MAP, 6 STREAM. Decoded by `decodeCoreStandardBody` (message.go).

**Core large message (userType 30):** the record body is `encodeHeadersAndProperties`
directly (no persister-id / endOfBodyPosition / bufferSize prefix); the message body
lives in `data/large-messages/<id>.msg` as raw bytes (verified: `48.msg` = 307200 bytes
for a 300 KiB BYTES message). Decoded by `decodeCoreHeaders`, body joined by
`AttachLargeBodies`.

**Paged core:** a page entry with `largeMessageType = NONE` and persister id 1 is the
same `CoreMessagePersister` payload, decoded by the same path; `largeMessageType =
CORE/OLD_CORE` is a core-large header whose body is outside the page file (still skipped).

**Legacy ADD_MESSAGE (userType 31):** pre-persister-id core add, not produced by 2.42
and unverified — still reported as a skip rather than best-effort decoded.

---

## 6. SimpleString byte layout

Source: `SimpleString` `getData`/`writeSimpleString`/`writeNullableSimpleString`
(lines 143-151, 263-276) and `readSimpleString` (242-261).

**nullableSimpleString**:

| off | size | field |
| --- | ---- | ----- |
| 0 | 1 | byte flag: 0 = NULL (stop), 1 = NOT_NULL |
| 1 | 4 | int byteLength (= 2 × charCount) |
| 5 | byteLength | UTF-16 chars, **little-endian pairs** (low byte, high byte) |

**simpleString** (non-nullable, e.g. TypedProperties keys / STRING values): the same
minus the leading flag byte (int byteLength + LE char pairs).

Char decode: `char = (data[j] & 0xFF) | ((data[j+1] << 8) & 0xFF00)` — index j is the low
byte. ASCII strings therefore appear as `<ascii> 0x00 <ascii> 0x00 …` in a hexdump.

---

## 7. TypedProperties layout + DataConstants type ids

Source: `TypedProperties.encode` (661-692), value writers (865-1193),
`DataConstants` (all values).

**DataConstants type ids** (all verified):

| id | const |
| -- | ----- |
| 0 | NULL |
| 1 | NOT_NULL |
| 2 | BOOLEAN |
| 3 | BYTE |
| 4 | BYTES |
| 5 | SHORT |
| 6 | INT |
| 7 | LONG |
| 8 | FLOAT |
| 9 | DOUBLE |
| 10 | STRING |
| 11 | CHAR |

**TypedProperties.encode**:

| off | size | field |
| --- | ---- | ----- |
| 0 | 1 | byte: NULL(0) ⇒ empty, stop; NOT_NULL(1) ⇒ continue |
| 1 | 4 | int propertyCount |
| . | .. | repeated `propertyCount` times: [int keyByteLen][key bytes = SimpleString data, LE pairs, NO flag][value] |

**value** = 1 type byte + payload:

| type | byte | payload |
| ---- | ---- | ------- |
| NULL | 0 | (none) |
| BOOLEAN | 2 | 1 byte |
| BYTE | 3 | 1 byte |
| BYTES | 4 | int length + bytes |
| SHORT | 5 | 2 bytes (BE) |
| INT | 6 | 4 bytes (BE) |
| LONG | 7 | 8 bytes (BE) |
| FLOAT | 8 | 4 bytes (BE int of floatToIntBits) |
| DOUBLE | 9 | 8 bytes (BE long of doubleToLongBits) |
| STRING | 10 | writeSimpleString (int byteLen + LE pairs, NO flag) |
| CHAR | 11 | 2 bytes (BE short) |

Note: keys are raw `SimpleString` (int length + data), **not** nullableSimpleString — no
leading flag byte.

---

## 8. Page file + PagedMessage layout

Sources: `PageReadWriter` (`START_BYTE`/`END_BYTE`/`SIZE_RECORD`, lines 45-97),
`PagedMessageImpl.decode/encode` (200-265). Page files: `data/paging/<addrHash>/<n>.page`.

**Per page entry** (`SIZE_RECORD = 1+4+1 = 6` overhead):

| off | size | field |
| --- | ---- | ----- |
| 0 | 1 | START_BYTE = `'{'` = 0x7B |
| 1 | 4 | int messageEncodedSize (= PagedMessage encode size) |
| 5 | messageEncodedSize | PagedMessage bytes (below) |
| 5+size | 1 | END_BYTE = `'}'` = 0x7D |

Reader validates START_BYTE, reads size, checks `pos+6+size <= fileSize`, and verifies the
trailing byte is END_BYTE. A bad start/end byte marks the file suspect (partial trailing
write tolerated).

**PagedMessage** (`PagedMessageImpl`):

| off | size | field |
| --- | ---- | ----- |
| 0 | 8 | long transactionID (**fixture: -1 = non-transactional**; treat <= 0 as no tx) |
| 8 | 1 | byte largeMessageType: 0=NONE, 1=CORE, -1=OLD_CORE, 2=NOT_CORE |
| . | .. | if type ∈ {CORE, OLD_CORE}: int coreLargeHeaderSize + that many core-large-persister header bytes. Otherwise: `MessagePersister.decode` = [byte persisterID][persister payload from §5] (persisterID 2/3/5 for normal AMQP, 4 for AMQP large) |
| . | 4 | int queueIDsCount |
| . | 8×count | long queueIDs[] |

For our fixture (`salvage.paged`, plain 1 KiB AMQP messages): transactionID=-1,
largeMessageType=0 (NONE), then `[persisterID=5][V3 AMQP standard message]`, then queueIDs.

**Paging dir layout (fixture-verified)**: `data/paging/<UUID>/` — one UUID-named dir per
paged address, containing `000000001.page`, `000000002.page`, … and **`address.txt`**
whose single line is the address name (fixture: `salvage.paged`). Use address.txt to map
the dir to its address.

**Paging is a spillover, not a mirror (fixture-verified)**: messages sent *before* the
address crossed maxSizeBytes live in the journal like normal messages (fixture: 44 of
the 500 salvage.paged messages are journal ADD_MESSAGE_PROTOCOL records; the other 456
are page entries). Replay must union journal survivors + page entries per queue.

---

## 9. Reference read path (`XmlDataExporter` / `RecoverMessages`) — replay decisions to mirror

Source: `XmlDataExporter.processMessageJournal` (185-296), `removeAcked` (288-...).

1. `messageJournal.load(records, preparedTransactions, failureCb, false)` — the Journal's
   own load applies **transaction semantics**: DELETE_RECORD removes the add; COMMIT
   applies the tx's records; ROLLBACK and **unterminated** transactions are discarded;
   PREPARE-only tx go into `preparedTransactions` (in-doubt). XmlDataExporter **discards
   `preparedTransactions`** (in-doubt messages are NOT exported). Mirror this: only
   committed / non-transactional adds survive.
2. Build `messages[messageID]` from ADD_MESSAGE (31), ADD_MESSAGE_PROTOCOL (45),
   ADD_LARGE_MESSAGE (30).
3. Build `messageRefs[messageID][queueID]` from ADD_REF (32) records (RefEncoding =
   long queueID). This is how a message maps to its queue(s). Fixture: ADD_REF arrives
   as **UPDATE_RECORD** frames (type 12) with recordID = the messageID; treat adds and
   updates uniformly when collecting user records (as `RecordInfo` does).
4. Collect ACKNOWLEDGE_REF (33) records; in `removeAcked`, for each ack remove
   `messageRefs[id][queueID]`; if a message has **no remaining refs**, drop the message
   entirely. ⇒ **acked messages are eliminated** (our `salvage.acked` set).
5. SET_SCHEDULED_DELIVERY_TIME (36) arrives as an UPDATE record carrying
   ScheduledDeliveryEncoding = `long queueID` + `long scheduledDeliveryTime`.

### Supporting codec field orders (verified)

- **RefEncoding / QueueEncoding** (`QueueEncoding.decode`): `long queueID` only.
- **ScheduledDeliveryEncoding.decode**: `long queueID` then `long scheduledDeliveryTime`.
- **PersistentQueueBindingEncoding.decode** (queue binding, userType 21): `SimpleString
  queueName`, `SimpleString address`, `nullableSimpleString filterString`,
  `nullableSimpleString metadata (user)`, `boolean autoCreated`, then a versioned tail
  of flags (maxConsumers int, purgeOnNoConsumers bool, routingType byte, …). Task 6
  finalizes the full tail; the queueName/address/filter/autoCreated prefix is stable and
  is all salvage needs to map queueID → name/address.

---

## 10. Page-cursor journal records (Task 9 addition -- not exercised by the fixture)

The harvested fixture's message journal contains **zero** ACKNOWLEDGE_CURSOR (39) or
PAGE_CURSOR_COMPLETE (42) records (section 3's census: the only page-cursor-family
record present is one `ADD_TX/40` PAGE_CURSOR_COUNTER_VALUE, which is page-count
bookkeeping, not a per-message/per-page ack -- consistent with "nothing consumed" in
the fixture). Task 7 explicitly deferred these record families (see the design note in
`message.go`'s `DecodeMessages` doc comment). The layout below was **not** available in
this document and was pulled from `apache/activemq-artemis` tag **2.42.0** source
(not fixture-verified; recorded here per the "derive from Java sources, cite, then
code" rule) rather than guessed.

**Both record types share one encoding class, `CursorAckRecordEncoding`** (source:
`artemis-server/.../persistence/impl/journal/codec/CursorAckRecordEncoding.java`,
`getEncodeSize`/`encode`/`decode`):

| off | size | field |
| --- | ---- | ----- |
| 0 | 8 | long queueID |
| 8 | 8 | long pageNr (from `PagePosition.getPageNr()`) |
| 16 | 4 | int messageNr (from `PagePosition.getMessageNr()`) |

Total 20 bytes, no variable-length parts.

**Write path** (source: `AbstractJournalStorageManager.java`, tag 2.42.0):

- `storeCursorAcknowledge(queueID, position)`: `messageJournal.appendAddRecord(freshID,
  ACKNOWLEDGE_CURSOR, new CursorAckRecordEncoding(queueID, position), ...)` -- **ADD**
  frame (type 11 non-tx), record ID is a **freshly generated id-generator value**, not
  the paged message's own id and not the queueID. `storeCursorAcknowledgeTransactional`
  is the same but via `appendAddRecordTransactional` (ADD_RECORD_TX, type 13).
- `storePageCompleteTransactional(txID, queueID, position)`: same encoding class, same
  fresh-ID ADD_RECORD_TX pattern, userRecordType PAGE_CURSOR_COMPLETE instead.
- Practical consequence for replay: these are ordinary top-level Survivors (own record
  ID, no relation to the message/page they describe) that ride through
  `Replayer`/`ReadJournalDir` exactly like message records; `DecodeMessages` already
  ignores them via its default case (design note above). `BuildCursorState`
  (`paging.go`) re-scans the same `[]Survivor` for `UserType` 39/42 and decodes each
  Body with the 20-byte layout above.

**`messageNr` is not stored in the page file** (cross-check against section 8's
`PagedMessage` field table: no such field exists on disk). Source: `Page.java`'s
`addMessage(PagedMessage message)`, called once per entry as `PageReadWriter` decodes a
`.page` file in order: `message.setMessageNumber(messages.size())` -- i.e. **messageNr
is the entry's 0-based ordinal position within its page file, assigned at read time**,
matching the first entry decoded from a page to messageNr 0, the second to 1, etc. This
is the same value later wrapped in a `PagePositionImpl(pageNr, messageNr)` when the
broker acks/completes a cursor position, so `ReadPaging` reconstructing the same
ordinal while walking a `.page` file's entries in on-disk order reproduces the position
cursor acks reference.

---

## Fixture cross-check results

Fixture: `testdata/artemis-2.42-data.tar.gz` from `make fixtures` (2.42.0-alpine,
clean SIGTERM stop). A byte-walker implementing exactly the framing above parsed
**every record in both journals with 0 check-size failures** (one expected resync in
the bindings journal on a compaction leftover, recovered per §3's rules).

- Header `activemq-data-1.amq`: `00 00 00 02 | 00 00 00 00 | 00 00 00 00 00 00 00 01`
  = formatVersion 2, userVersion 0, fileID 1. ✓ §1. (`activemq-data-2.amq`: fileID 2,
  all-zero body = preallocated, parses to 0 records. ✓ padding rule.)
- First record at 0x10: `0b | 00 00 00 01 | 00 | …recordID 38… | 00 00 00 ea | 2d | …`
  = ADD_RECORD, fileID echo 1, compactCount 0, variableSize 234, userType 45,
  persister byte 5 (V3), messageID echo 38; trailing checkSize 234+23. ✓ §3, §5a.
- Message journal census: 59× ADD/45 (58× persister 5, 1× persister 4 = the large msg),
  59× UPDATE/32 (ADD_REF arrives as UPDATE_RECORD frames), 1× UPDATE/36 (scheduled),
  3× UPDATE_TX/33 + COMMITs (acks are transactional), 3× DELETE_RECORD (the fully-acked
  messages), 1× ADD_TX/40. ✓ §4, §9 (adds+deletes+acks all present for replay tests).
- Large message: journal record userType 45 / persister 4, durable byte `ff`,
  address `salvage.large`, expiration 0, reencoded 0; `64.msg` = 307255 B complete AMQP
  message (starts `00 53 70`, Data section `00 53 75 b0` + int 307200). ✓ §5b.
- Paging: 36 page files under `paging/<uuid>/` + `address.txt` = `salvage.paged`;
  456 entries total, every entry `7b | int size | … | 7d`, transactionID -1,
  largeMessageType 0, persister 5; 456 + 44 journal-resident = 500 = manifest count;
  a paged entry's AMQP Data payload sha256 matches its manifest entry. ✓ §8.
- Manifest: plain 5, props 5, scheduled 1, large 1, paged 500, acked 0. ✓ brief schema.
