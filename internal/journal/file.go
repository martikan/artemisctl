package journal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// --- Journal record framing types (JournalImpl.java's outer per-record type
// byte, as opposed to the JournalRecordIds "user record type" table below).
// Verified against apache/activemq-artemis tag 2.42.0, JournalImpl.java
// lines 169-207; see format_notes.md section 2. Valid range is
// [EventRecord, RollbackRecord]; any other byte is padding/fill (e.g. the
// 'J' FILL_CHARACTER = 0x4A) and is skipped without comment.
const (
	EventRecord    byte = 10
	AddRecord      byte = 11
	UpdateRecord   byte = 12
	AddRecordTx    byte = 13
	UpdateRecordTx byte = 14
	DeleteRecordTx byte = 15
	DeleteRecord   byte = 16
	PrepareRecord  byte = 17
	CommitRecord   byte = 18
	RollbackRecord byte = 19
)

// --- User record type ids (JournalRecordIds), read from the userRecordType
// byte carried by ADD/UPDATE-family journal records (see UserType on
// RawRecord). Verified against
// artemis-server/.../persistence/impl/journal/JournalRecordIds.java tag
// 2.42.0; see format_notes.md section 4. The full table is defined here
// (rather than per-consumer) so later decode stages (bindings.go, message.go,
// large.go) can share one source of truth instead of redeclaring literals.
const (
	GroupRecord              byte = 20
	QueueBindingRecord       byte = 21
	QueueStatusRecord        byte = 22
	IDCounterRecord          byte = 24
	AddressSettingRecord     byte = 25
	SecuritySettingRecord    byte = 26
	DivertRecord             byte = 27
	BridgeRecord             byte = 28
	AddLargeMessagePending   byte = 29
	AddLargeMessage          byte = 30
	AddMessage               byte = 31
	AddRef                   byte = 32
	AcknowledgeRef           byte = 33
	UpdateDeliveryCount      byte = 34
	PageTransaction          byte = 35
	SetScheduledDeliveryTime byte = 36
	DuplicateID              byte = 37
	HeuristicCompletion      byte = 38
	AcknowledgeCursor        byte = 39
	PageCursorCounterValue   byte = 40
	PageCursorCounterInc     byte = 41
	PageCursorComplete       byte = 42
	PageCursorPendingCounter byte = 43
	AddressBindingRecord     byte = 44
	AddMessageProtocol       byte = 45
	AddressStatusRecord      byte = 46
	UserRecord               byte = 47
	RoleRecord               byte = 48
	AddMessageBody           byte = 49
	KeyValuePairRecord       byte = 50
	ConnectorRecord          byte = 51
	AddressSettingRecordJSON byte = 52
	AckRetry                 byte = 53
)

// formatVersion is the only journal header format version this offline
// reader accepts. Real Artemis also tolerates the legacy value 1
// (COMPATIBLE_VERSIONS), but the brief for this reader treats any mismatch
// as fatal: salvage targets 2.42 data directories, which are always written
// with format 2. format_notes.md section 1 / JournalImpl.FORMAT_VERSION.
const formatVersion = 2

// sizeHeader is the fixed journal file header length: int formatVersion +
// int userVersion + long fileID, in that write/read order (NOT the order
// implied by the misleading SIZE_HEADER summand comment in JournalImpl.java).
// format_notes.md section 1.
const sizeHeader = 16

// recordSizeOverhead is the fixed (non-variable) per-record byte count for
// journalVersion 2, i.e. JournalImpl.getRecordSize(recordType, 2): the
// leading type byte, 4-byte fileID echo, compactCount byte, any fixed-width
// fields (transactionID/recordID/userRecordType/numberOfRecords), and the
// trailing 4-byte checkSize -- everything except the variable-length body
// (and, for PREPARE_RECORD, the prepared-transaction extra data).
//
// Derived directly from JournalImpl.java's SIZE_* constants (tag 2.42.0)
// plus getRecordSize's "+1 for journalVersion>=2" adjustment. NOTE this
// corrects a transcription error in format_notes.md's DELETE_RECORD_TX entry
// (it states 21+1=22; the real Java constant SIZE_DELETE_RECORD_TX =
// BASIC_SIZE(9)+SIZE_LONG(8)+SIZE_LONG(8)+SIZE_INT(4) = 29, so the v2
// overhead is 29+1=30). DELETE_RECORD_TX does not appear in the harvested
// fixture, so this correction is unverified against real bytes, but it is
// read straight from JournalImpl.java source and is internally consistent
// with every other entry in this table.
var recordSizeOverhead = map[byte]int{
	EventRecord:    23,
	AddRecord:      23,
	UpdateRecord:   23,
	AddRecordTx:    31,
	UpdateRecordTx: 31,
	DeleteRecordTx: 30,
	DeleteRecord:   18,
	PrepareRecord:  26,
	CommitRecord:   22,
	RollbackRecord: 18,
}

func isTransactionType(t byte) bool {
	switch t {
	case AddRecordTx, UpdateRecordTx, DeleteRecordTx, PrepareRecord, CommitRecord, RollbackRecord:
		return true
	default:
		return false
	}
}

func isCompleteTransactionType(t byte) bool {
	switch t {
	case PrepareRecord, CommitRecord, RollbackRecord:
		return true
	default:
		return false
	}
}

func isContainsBodyType(t byte) bool {
	return t >= EventRecord && t <= DeleteRecordTx
}

// RawRecord is one framed journal record, decoded to fields but with the
// user-record body left opaque.
type RawRecord struct {
	Type            byte   // ADD_RECORD … ROLLBACK_RECORD (verified constants)
	TxID            int64  // tx-scoped records only
	ID              int64  // record id (message id / binding id); 0 for commit/rollback/prepare
	UserType        byte   // user record type (JournalRecordIds); add/update family only
	Body            []byte // user body; add/update/delete-tx body; prepare extraData
	NumberOfRecords int32  // commit/prepare only
}

// FileDiag reports where and why scanning a file hit a corruption incident
// that required resyncing.
type FileDiag struct {
	Path   string
	Offset int64
	Reason string
	// Corrupt distinguishes structural journal/page/bindings damage (check-size
	// mismatch, truncated record, broken page-entry framing, an undecodable
	// bindings-record body) from benign diagnostic notes such as a
	// fileID-mismatch "reuse leftover" from a normally-reused journal file
	// (see parseRecord's "fileID mismatch" case below). salvage.go gates the
	// CLI's exit code on Corrupt diags exactly like Skips (spec §1/§5); the
	// zero value (false) is the safe default for a caller that has no reason
	// to mark an entry corrupt. Set explicitly at every FileDiag construction
	// site (this file, paging.go, bindings.go) rather than inferred later by
	// matching against the assembled Reason prose.
	Corrupt bool
}

// ReadJournalDir orders <prefix>*.<ext> files by fileID (from each header),
// streams every well-framed record to emit in replay order, and returns
// diagnostics for corruption incidents encountered along the way. A journal
// header format version other than the verified constant is an immediate
// error naming the file and both versions.
func ReadJournalDir(dir, prefix, ext string, emit func(RawRecord) error) ([]FileDiag, error) {
	pattern := filepath.Join(dir, prefix+"*."+ext)
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("journal: glob %s: %w", pattern, err)
	}

	type fileEntry struct {
		path   string
		fileID int64
		data   []byte
	}
	entries := make([]fileEntry, 0, len(paths))
	for _, p := range paths {
		data, err := os.ReadFile(p) //nolint:gosec // salvage reads operator-supplied broker data dirs by design
		if err != nil {
			return nil, fmt.Errorf("journal: read %s: %w", p, err)
		}
		if len(data) < sizeHeader {
			// Below SIZE_HEADER: damaged/empty, per format_notes.md section 1
			// (mirrors JournalImpl.readJournalFile's early -1 return). Not
			// worth a diagnostic; a preallocated-but-never-written file looks
			// exactly like this too.
			continue
		}

		hdr := newReader(data[:sizeHeader])
		gotVersion := hdr.i32()
		_ = hdr.i32() // userVersion: echoed by the broker, not needed for framing
		fileID := hdr.i64()
		if hdr.err() != nil {
			return nil, fmt.Errorf("journal: %s: reading header: %w", p, hdr.err())
		}
		if gotVersion != formatVersion {
			return nil, fmt.Errorf("journal: %s: unsupported journal format version %d (want %d)", p, gotVersion, formatVersion)
		}

		entries = append(entries, fileEntry{path: p, fileID: fileID, data: data})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].fileID < entries[j].fileID })

	var diags []FileDiag
	for _, e := range entries {
		fileDiags, err := readJournalFile(e.path, e.data, e.fileID, emit)
		diags = append(diags, fileDiags...)
		if err != nil {
			return diags, err
		}
	}
	return diags, nil
}

// readJournalFile scans one already-loaded journal file for well-framed
// records, streaming each to emit in on-disk order.
//
// It never aborts the file on a bad record; it mirrors
// JournalImpl.readJournalFile's "never stop the file" behavior
// (format_notes.md section 3): a record-type byte outside [EventRecord,
// RollbackRecord] is normal padding/fill and is skipped silently one byte at
// a time. A genuine corruption signal -- a fileID-echo mismatch, a field
// that would read past EOF, or a trailing check-size mismatch -- resyncs the
// same way, one byte at a time from the failed record's start, until a
// record parses cleanly again or the file ends.
//
// Diagnostics are coalesced per corruption incident rather than per resync
// byte: once a diagnostic has been recorded for the incident currently being
// recovered from, further failed attempts during that same recovery scan are
// silent, and the next successfully parsed record clears the state. Without
// this, a single corrupted record can spray one diagnostic per byte of its
// own body (any byte whose value happens to fall in the valid record-type
// range looks like a new candidate record start), which would drown out the
// single real incident it represents.
func readJournalFile(path string, data []byte, fileID int64, emit func(RawRecord) error) ([]FileDiag, error) {
	var diags []FileDiag
	fileIDEcho := int32(fileID) // low 32 bits; JournalFileImpl.getRecordID() truncates the long fileID to int

	pos := sizeHeader
	resyncing := false
	for pos < len(data) {
		typeByte := data[pos]
		if typeByte < EventRecord || typeByte > RollbackRecord {
			pos++
			continue
		}

		rec, consumed, reason, corrupt := parseRecord(data, pos, fileIDEcho, typeByte)
		if reason != "" {
			if !resyncing {
				diags = append(diags, FileDiag{Path: path, Offset: int64(pos), Reason: reason, Corrupt: corrupt})
				resyncing = true
			}
			pos++
			continue
		}

		resyncing = false
		if err := emit(rec); err != nil {
			return diags, fmt.Errorf("journal: %s: emit at offset %d: %w", path, pos, err)
		}
		pos += consumed
	}
	return diags, nil
}

// parseRecord attempts to decode one record starting at pos. On success it
// returns the decoded record and the total number of bytes it occupies
// on-disk (including its trailing check-size int); the caller advances pos
// by that amount. On failure it returns a non-empty reason, a corrupt flag
// classifying that reason (true = structural damage, gates salvage's exit
// code; false = benign, report-only), and the caller resyncs by one byte.
// typeByte is data[pos], already verified to be in [EventRecord,
// RollbackRecord] by the caller.
func parseRecord(data []byte, pos int, fileIDEcho int32, typeByte byte) (RawRecord, int, string, bool) {
	r := newReader(data[pos:])
	r.u8() // type byte, already known

	echo := r.i32()
	if r.err() != nil {
		return RawRecord{}, 0, "truncated record", true
	}
	if echo != fileIDEcho {
		// Leftover bytes from a reused file, not corruption in the current
		// generation -- but still worth a diagnostic for operator visibility
		// (format_notes.md section 3's "CRITICAL" resync note; real Artemis
		// stays silent here, our offline tool does not). Benign: never gates
		// salvage's exit code.
		return RawRecord{}, 0, "fileID mismatch", false
	}

	r.u8() // compactCount (v2 journals only; format version is pinned to 2 above)

	var txID int64
	if isTransactionType(typeByte) {
		txID = r.i64()
	}

	var recordID int64
	if !isCompleteTransactionType(typeByte) {
		recordID = r.i64()
	}

	var variableSize int32
	var userType byte
	var body []byte
	if isContainsBodyType(typeByte) {
		variableSize = r.i32()
		if typeByte != DeleteRecordTx {
			userType = r.u8()
		}
		body = r.bytes(int(variableSize))
	}

	var numberOfRecords int32
	var extraDataSize int32
	if typeByte == PrepareRecord || typeByte == CommitRecord {
		numberOfRecords = r.i32()
		if typeByte == PrepareRecord {
			extraDataSize = r.i32()
			body = r.bytes(int(extraDataSize))
		}
	}

	checkSize := r.i32()
	if r.err() != nil {
		return RawRecord{}, 0, "truncated record", true
	}

	overhead, ok := recordSizeOverhead[typeByte]
	if !ok {
		// Unreachable: typeByte is already verified to be in
		// [EventRecord, RollbackRecord], and every value in that range has
		// an entry in recordSizeOverhead.
		return RawRecord{}, 0, "truncated record", true
	}
	total := overhead + int(variableSize) + int(extraDataSize)
	if int(checkSize) != total {
		return RawRecord{}, 0, "check-size mismatch", true
	}

	return RawRecord{
		Type:            typeByte,
		TxID:            txID,
		ID:              recordID,
		UserType:        userType,
		Body:            body,
		NumberOfRecords: numberOfRecords,
	}, total, "", false
}
