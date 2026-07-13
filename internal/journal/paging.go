package journal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// pageStartByte / pageEndByte: page-file entry framing (format_notes.md
// section 8, PageReadWriter's START_BYTE/END_BYTE).
const (
	pageStartByte byte = '{'
	pageEndByte   byte = '}'
)

// pageLargeMessageType constants (format_notes.md section 8's "byte
// largeMessageType" field). Only NONE/NOT_CORE take the
// MessagePersister.decode path this AMQP-only reader understands;
// CORE/OLD_CORE carry a core-large-persister header it cannot decode.
const (
	pageLargeTypeOldCore int8 = -1
	pageLargeTypeNone    int8 = 0
	pageLargeTypeCore    int8 = 1
	pageLargeTypeNotCore int8 = 2
)

// pageFilePattern matches Artemis page file names (format_notes.md section
// 8, fixture-verified: "000000001.page", "000000002.page", ...).
var pageFilePattern = regexp.MustCompile(`^(\d+)\.page$`)

// CursorState is the best-effort page-cursor knowledge extracted from the
// message journal's ACKNOWLEDGE_CURSOR (39) / PAGE_CURSOR_COMPLETE (42)
// survivors (format_notes.md section 10, added by this task -- deferred by
// Task 7, see message.go's DecodeMessages design note). Both record
// families share one on-disk encoding keyed by queueID + page position.
type CursorState struct {
	// CompletePages[queueID][pageNr] marks a page fully consumed by queueID
	// (PAGE_CURSOR_COMPLETE): every entry in that page can be skipped for
	// that queue without inspecting individual acks.
	CompletePages map[int64]map[int64]bool
	// AckedEntries[queueID][{pageNr, messageNr}] marks one page entry --
	// identified by its 0-based ordinal position within its page file, see
	// format_notes.md section 10's Page.java citation -- individually acked
	// by queueID (ACKNOWLEDGE_CURSOR).
	AckedEntries map[int64]map[[2]int64]bool
}

// BuildCursorState scans message-journal survivors (the same []Survivor
// Replayer.Resolve returns, also consumed by DecodeMessages) for the
// page-cursor record family DecodeMessages's design note explicitly leaves
// for this task, and folds them into a CursorState for ReadPaging's
// best-effort filtering. Survivors of any other UserType are ignored.
//
// The returned CursorState's maps are always non-nil, even for empty input,
// so callers can index them directly without a nil check.
func BuildCursorState(survivors []Survivor) CursorState {
	cs := CursorState{
		CompletePages: make(map[int64]map[int64]bool),
		AckedEntries:  make(map[int64]map[[2]int64]bool),
	}

	for _, sv := range survivors {
		switch sv.UserType {
		case AcknowledgeCursor:
			queueID, pageNr, messageNr, ok := decodeCursorAck(sv.Body)
			if !ok {
				continue
			}
			if cs.AckedEntries[queueID] == nil {
				cs.AckedEntries[queueID] = make(map[[2]int64]bool)
			}
			cs.AckedEntries[queueID][[2]int64{pageNr, int64(messageNr)}] = true

		case PageCursorComplete:
			queueID, pageNr, _, ok := decodeCursorAck(sv.Body)
			if !ok {
				continue
			}
			if cs.CompletePages[queueID] == nil {
				cs.CompletePages[queueID] = make(map[int64]bool)
			}
			cs.CompletePages[queueID][pageNr] = true

		default:
			// Every other survivor family (messages, PAGE_TRANSACTION,
			// PAGE_CURSOR_COUNTER_VALUE/INC, ...) is out of scope here, same
			// as DecodeMessages's default case.
		}
	}
	return cs
}

// decodeCursorAck decodes a CursorAckRecordEncoding body (format_notes.md
// section 10): long queueID, long pageNr, int messageNr. Shared by both
// ACKNOWLEDGE_CURSOR and PAGE_CURSOR_COMPLETE, which use the identical
// on-disk layout.
func decodeCursorAck(body []byte) (queueID, pageNr int64, messageNr int32, ok bool) {
	r := newReader(body)
	queueID = r.i64()
	pageNr = r.i64()
	messageNr = r.i32()
	if r.err() != nil {
		return 0, 0, 0, false
	}
	return queueID, pageNr, messageNr, true
}

// PagedMessage is one message recovered from a page file.
type PagedMessage struct {
	AMQP     []byte
	Core     *CorePayload // non-nil for a decoded Core-protocol paged entry (AMQP empty)
	QueueIDs []int64
	// ScheduledMs is always 0: format_notes.md section 8's PagedMessage
	// field table has no scheduled-delivery field on disk (unlike
	// message.go's Message, which gets it from a separate
	// SET_SCHEDULED_DELIVERY_TIME journal update). Kept for field-shape
	// symmetry with Message per the task-9 brief's struct definition.
	ScheduledMs int64
}

// PagingDiag: skips + best-effort notes.
//
// Fields beyond the task-9 brief's three (LargeSkipped, UnknownPersister,
// UndecodableEntries) are an intentional deviation, documented in the task
// report: they mirror message.go's MessageDiag granularity (which itself
// went beyond its own brief) rather than folding every non-exportable entry
// into CoreSkipped, which would misreport why an entry was skipped.
type PagingDiag struct {
	CoreSkipped          int        // persister-id Core (1), or largeMessageType CORE/OLD_CORE entries this reader cannot decode
	CorruptPages         []FileDiag // page files where framing broke down (bad START/END byte, or a size that overruns EOF); entries decoded before the break are still kept
	PagesSkippedComplete int        // whole pages skipped via PAGE_CURSOR_COMPLETE (every queue the page's entries target has it marked complete)

	LargeSkipped       int // paged AMQP-large entries (persister id 4): body lives outside the page file, out of scope for this reader (format_notes.md sections 5b/8; correction #4 -- none observed in the fixture, handled defensively)
	UnknownPersister   int // entries whose persister-id byte isn't recognized
	UndecodableEntries int // well-framed entries whose PagedMessage payload couldn't be parsed past a recognized point

	DirMissing bool // pagingDir did not exist; distinguish from an existing-but-empty dir
}

// ReadPaging walks <pagingDir>/<address-dir>/*.page in page-number order,
// decodes size-prefixed PagedMessage entries ('{' size bytes '}'), and
// filters best-effort against cursor state (format_notes.md sections 8/10):
// a page marked complete for every queue any of its exportable entries
// target is skipped as a whole (PagesSkippedComplete); within other pages,
// an entry individually acked (ACKNOWLEDGE_CURSOR) for every queue it
// targets is skipped; otherwise it is exported (at-least-once, spec §4).
//
// A missing pagingDir is not an error: it means nothing was ever paged.
func ReadPaging(pagingDir string, cursors CursorState) ([]PagedMessage, PagingDiag, error) {
	var diag PagingDiag
	var out []PagedMessage

	addrEntries, err := os.ReadDir(pagingDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			diag.DirMissing = true
			return nil, diag, nil
		}
		return nil, diag, fmt.Errorf("journal: read paging dir %s: %w", pagingDir, err)
	}

	var addrDirs []string
	for _, e := range addrEntries {
		if e.IsDir() {
			addrDirs = append(addrDirs, e.Name())
		}
	}
	// Deterministic order across address dirs. address.txt (the address-name
	// marker file, format_notes.md section 8) is intentionally never read:
	// neither PagedMessage nor PagingDiag surfaces an address, since
	// queueIDs -- which do ride in each entry -- are all this reader needs.
	sort.Strings(addrDirs)

	for _, addrName := range addrDirs {
		addrDir := filepath.Join(pagingDir, addrName)
		pageFiles, err := listPageFiles(addrDir)
		if err != nil {
			return nil, diag, err
		}

		for _, pf := range pageFiles {
			path := filepath.Join(addrDir, pf.name)
			data, err := os.ReadFile(path) //nolint:gosec // salvage reads operator-supplied broker data dirs by design
			if err != nil {
				return nil, diag, fmt.Errorf("journal: read page file %s: %w", path, err)
			}
			readOnePageFile(path, data, pf.pageNr, cursors, &out, &diag)
		}
	}

	return out, diag, nil
}

// PagingReferenced reports whether any message-journal survivor is evidence of
// paging activity: PAGE_TRANSACTION (35), ACKNOWLEDGE_CURSOR (39),
// PAGE_CURSOR_COUNTER_VALUE (40), PAGE_CURSOR_COUNTER_INC (41),
// PAGE_CURSOR_COMPLETE (42), PAGE_PENDING_COUNTER (43). The orchestrator uses
// it to warn when the journal references paging but the paging dir is missing.
func PagingReferenced(survivors []Survivor) bool {
	for _, sv := range survivors {
		switch sv.UserType {
		case PageTransaction, AcknowledgeCursor, PageCursorCounterValue, PageCursorCounterInc, PageCursorComplete, PageCursorPendingCounter:
			return true
		}
	}
	return false
}

// pageFileInfo is one page file's name plus its parsed page number (the
// numeric prefix before ".page", which doubles as the pageNr half of a
// cursor position, format_notes.md section 10).
type pageFileInfo struct {
	name   string
	pageNr int64
}

// listPageFiles returns dir's *.page entries in ascending page-number order.
// Non-.page files (notably address.txt) are silently skipped.
func listPageFiles(dir string) ([]pageFileInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("journal: read paging address dir %s: %w", dir, err)
	}

	var out []pageFileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := pageFilePattern.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, err := strconv.ParseInt(m[1], 10, 64)
		if err != nil {
			// Unreachable: the regex already guarantees an all-digit group.
			continue
		}
		out = append(out, pageFileInfo{name: e.Name(), pageNr: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].pageNr < out[j].pageNr })
	return out, nil
}

// pagedCandidate is one exportable (persisterDecodeStandard) entry decoded
// from a page file, pending cursor filtering.
type pagedCandidate struct {
	amqp      []byte
	core      *CorePayload
	queueIDs  []int64
	messageNr int64
}

// readOnePageFile decodes one page file's entries and applies best-effort
// cursor filtering, appending survivors to *out and updating *diag in place.
func readOnePageFile(path string, data []byte, pageNr int64, cursors CursorState, out *[]PagedMessage, diag *PagingDiag) {
	frames, badOffset, corrupt := readPageFileFrames(data)
	if corrupt {
		diag.CorruptPages = append(diag.CorruptPages, FileDiag{
			Path:    path,
			Offset:  badOffset,
			Reason:  "bad page-entry framing",
			Corrupt: true, // structural page-file damage: gates salvage's exit code (spec §1/§5)
		})
	}

	var candidates []pagedCandidate
	queueSet := make(map[int64]bool)

	for i, frame := range frames {
		amqpBytes, core, queueIDs, outcome := decodePagedMessage(frame)
		switch outcome {
		case persisterDecodeCore:
			if core == nil {
				// Core large paged entry (body outside the page file): cannot export.
				diag.CoreSkipped++
				break
			}
			for _, q := range queueIDs {
				queueSet[q] = true
			}
			candidates = append(candidates, pagedCandidate{core: core, queueIDs: queueIDs, messageNr: int64(i)})
		case persisterDecodeLarge:
			diag.LargeSkipped++
		case persisterDecodeUnknown:
			diag.UnknownPersister++
		case persisterDecodeMalformed:
			diag.UndecodableEntries++
		case persisterDecodeStandard:
			for _, q := range queueIDs {
				queueSet[q] = true
			}
			candidates = append(candidates, pagedCandidate{amqp: amqpBytes, queueIDs: queueIDs, messageNr: int64(i)})
		}
	}

	if len(candidates) == 0 {
		return
	}

	if pageComplete(queueSet, pageNr, cursors) {
		diag.PagesSkippedComplete++
		return
	}

	for _, c := range candidates {
		if entryAcked(c.queueIDs, pageNr, c.messageNr, cursors) {
			continue
		}
		*out = append(*out, PagedMessage{AMQP: c.amqp, Core: c.core, QueueIDs: c.queueIDs})
	}
}

// pageComplete reports whether every queue any candidate entry in this page
// targets has that page marked complete (CursorState.CompletePages,
// format_notes.md section 10's PAGE_CURSOR_COMPLETE). An empty queueSet
// (defensive; every real entry carries at least one queueID) is never
// "complete" -- there is nothing to have completed.
func pageComplete(queueSet map[int64]bool, pageNr int64, cursors CursorState) bool {
	if len(queueSet) == 0 {
		return false
	}
	for q := range queueSet {
		pages := cursors.CompletePages[q]
		if pages == nil || !pages[pageNr] {
			return false
		}
	}
	return true
}

// entryAcked reports whether every queue this entry targets has
// individually acked this exact position (CursorState.AckedEntries,
// format_notes.md section 10's ACKNOWLEDGE_CURSOR).
func entryAcked(queueIDs []int64, pageNr, messageNr int64, cursors CursorState) bool {
	if len(queueIDs) == 0 {
		return false
	}
	pos := [2]int64{pageNr, messageNr}
	for _, q := range queueIDs {
		acked := cursors.AckedEntries[q]
		if acked == nil || !acked[pos] {
			return false
		}
	}
	return true
}

// readPageFileFrames scans one page file's outer '{' size '}' framing
// (format_notes.md section 8), returning every well-framed entry body in
// on-disk order. It stops at the first framing violation (bad start/end
// byte, or a size that would run past EOF) rather than resyncing
// byte-by-byte like the message/bindings journal reader (file.go):
// format_notes.md section 8 notes a bad start/end byte "marks the file
// suspect (partial trailing write tolerated)" -- i.e. real Artemis's own
// page reader treats this as the normal end of valid data (a page file
// actively being appended to when the broker died), not corruption to
// recover past. Whatever decoded cleanly before the violation is kept, and
// the caller records one diagnostic for the incident.
func readPageFileFrames(data []byte) (frames [][]byte, badOffset int64, corrupt bool) {
	pos := 0
	for pos < len(data) {
		if data[pos] != pageStartByte {
			return frames, int64(pos), true
		}
		if pos+5 > len(data) {
			return frames, int64(pos), true
		}
		r := newReader(data[pos+1 : pos+5])
		size := int(r.i32())
		start := pos + 5
		end := start + size
		if size < 0 || end < start || end+1 > len(data) {
			return frames, int64(pos), true
		}
		if data[end] != pageEndByte {
			return frames, int64(pos), true
		}
		frames = append(frames, data[start:end])
		pos = end + 1
	}
	return frames, 0, false
}

// skipPersisterTail consumes the V2/V3 tail decodePersisterPayload leaves
// unread (format_notes.md section 5a): int extraPropsSize + that many
// bytes (V2 id=3, V3 id=5 only -- V1 id=2 has neither field), then a
// V3-only 8-byte expiration long. paging.go needs this to reach the
// queueIDsCount field that follows a PagedMessage's persister payload
// (format_notes.md section 8); message.go never calls this since nothing
// follows an ADD_MESSAGE_PROTOCOL record's persister payload.
func skipPersisterTail(r *reader, persisterID byte) {
	if persisterID != persisterAMQPMessageV2 && persisterID != persisterAMQPMessageV3 {
		return
	}
	extraPropsSize := r.i32()
	if r.err() != nil {
		return
	}
	if extraPropsSize > 0 {
		r.bytes(int(extraPropsSize))
	}
	if persisterID == persisterAMQPMessageV3 {
		r.i64()
	}
}

// decodePagedMessage decodes one page-file entry's PagedMessage payload
// (format_notes.md section 8) far enough to export it: transactionID
// (read and discarded, see below), largeMessageType, the AMQP-standard
// persister payload (shared with message.go via decodePersisterPayload),
// its V2/V3 tail (needed to reach queueIDsCount), then queueIDsCount +
// queueIDs.
//
// Entries this v1 reader cannot or does not export (CORE/OLD_CORE large
// types, or a Core/AMQP-large/unknown/malformed persister) return early
// with just the outcome the caller uses to pick a diagnostic counter --
// their queueIDs are never needed, since a skipped entry is never
// individually cursor-filtered and the entry's byte range is already known
// from the outer '{' size '}' framing (readPageFileFrames), so skipping it
// does not require parsing all the way through.
//
// The entry's own transactionID (format_notes.md section 8: the fixture's
// only observed value is -1, non-transactional) is read and discarded:
// ReadPaging has no paging-transaction log to cross-reference it against
// (PAGE_TRANSACTION, userType 35, is out of this reader's scope per
// message.go's DecodeMessages design note), so every entry is treated as
// committed and exported at-least-once (spec §4) regardless.
func decodePagedMessage(body []byte) (amqpBytes []byte, core *CorePayload, queueIDs []int64, outcome persisterDecodeOutcome) {
	r := newReader(body)
	r.i64() // transactionID
	largeType := int8(r.u8())
	if r.err() != nil {
		return nil, nil, nil, persisterDecodeMalformed
	}

	if largeType != pageLargeTypeNone && largeType != pageLargeTypeNotCore {
		// CORE or OLD_CORE large: the body lives outside the page file
		// (large-messages dir), so a paged core-large entry cannot be exported
		// from the page file alone -- reported as CoreSkipped (core == nil).
		return nil, nil, nil, persisterDecodeCore
	}

	amqp, corePayload, persisterID, pOutcome := decodePersisterPayload(r)
	switch pOutcome {
	case persisterDecodeStandard:
		skipPersisterTail(r, persisterID)
	case persisterDecodeCore:
		// Core payload is fully self-delimiting (decodeCoreStandardBody
		// consumed body+headers+properties); no V2/V3 tail follows.
	default:
		return nil, nil, nil, pOutcome
	}

	qCount := r.i32()
	if r.err() != nil || qCount < 0 || qCount > int32(r.remaining()/8) {
		return nil, nil, nil, persisterDecodeMalformed
	}
	ids := make([]int64, 0, qCount)
	for i := int32(0); i < qCount; i++ {
		ids = append(ids, r.i64())
	}
	if r.err() != nil {
		return nil, nil, nil, persisterDecodeMalformed
	}

	return amqp, corePayload, ids, pOutcome
}
