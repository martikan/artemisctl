package journal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Azure/go-amqp"
)

// amqpDataSection unmarshals raw AMQP-encoded message bytes and returns the
// Data section's body, mirroring message_test.go's verifyQueueBodies.
func amqpDataSection(raw []byte) ([]byte, error) {
	var am amqp.Message
	if err := am.UnmarshalBinary(raw); err != nil {
		return nil, err
	}
	return am.GetData(), nil
}

// --- synthetic byte-builders (mirror format_notes.md sections 8/10's
// write-side layouts; the beI32/beI64/encodeNullableSimpleString helpers
// live in message_test.go, same package) ---

// buildPagedMessagePayload builds one PagedMessage payload (format_notes.md
// section 8): long transactionID (-1, non-transactional -- the fixture's
// only observed value), byte largeMessageType (0 = NONE), then
// MessagePersister.decode = [persisterID][persister payload from section 5a],
// then int queueIDsCount + queueIDs. For V2/V3 persisters it also encodes an
// empty extraProps block (and, for V3, a zero expiration) so the entry's
// true on-disk length matches what a real broker would have written --
// ReadPaging needs to walk past that tail to reach queueIDsCount.
func buildPagedMessagePayload(persisterID byte, amqpBytes []byte, queueIDs []int64) []byte {
	body := beI64(-1)                            // transactionID
	body = append(body, byte(pageLargeTypeNone)) // largeMessageType
	body = append(body, persisterID)
	body = append(body, beI64(77)...) // messageID (persister-internal, unused by the reader)
	body = append(body, beI64(0)...)  // messageFormat
	body = append(body, encodeNullableSimpleString("test.paged", true)...)
	body = append(body, beI32(int32(len(amqpBytes)))...)
	body = append(body, amqpBytes...)
	if persisterID == persisterAMQPMessageV2 || persisterID == persisterAMQPMessageV3 {
		body = append(body, beI32(0)...) // extraPropsSize = 0 (none)
		if persisterID == persisterAMQPMessageV3 {
			body = append(body, beI64(0)...) // expiration
		}
	}
	body = append(body, beI32(int32(len(queueIDs)))...)
	for _, q := range queueIDs {
		body = append(body, beI64(q)...)
	}
	return body
}

// encodePageEntry wraps a PagedMessage payload in the page-file entry
// framing (format_notes.md section 8): START_BYTE '{', int size, payload,
// END_BYTE '}'.
func encodePageEntry(payload []byte) []byte {
	out := []byte{pageStartByte}
	out = append(out, beI32(int32(len(payload)))...)
	out = append(out, payload...)
	out = append(out, pageEndByte)
	return out
}

// encodeCursorAckBody builds a CursorAckRecordEncoding body (format_notes.md
// section 10, shared by ACKNOWLEDGE_CURSOR and PAGE_CURSOR_COMPLETE): long
// queueID, long pageNr, int messageNr.
func encodeCursorAckBody(queueID, pageNr int64, messageNr int32) []byte {
	b := beI64(queueID)
	b = append(b, beI64(pageNr)...)
	b = append(b, beI32(messageNr)...)
	return b
}

// writePageFile writes data as <dir>/<addrDirName>/<pageFileName>, creating
// directories as needed, and returns the paging root dir (the ReadPaging
// argument).
func writePageFile(t *testing.T, addrDirName, pageFileName string, data []byte) string {
	t.Helper()
	root := t.TempDir()
	addrDir := filepath.Join(root, addrDirName)
	if err := os.MkdirAll(addrDir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", addrDir, err)
	}
	if err := os.WriteFile(filepath.Join(addrDir, pageFileName), data, 0o644); err != nil {
		t.Fatalf("write %s: %v", pageFileName, err)
	}
	return root
}

// --- BuildCursorState ---

func TestBuildCursorStateDecodesAckAndComplete(t *testing.T) {
	survivors := []Survivor{
		{ID: 100, UserType: AcknowledgeCursor, Body: encodeCursorAckBody(5, 2, 7)},
		{ID: 101, UserType: PageCursorComplete, Body: encodeCursorAckBody(5, 3, 0)},
		// Unrelated survivor families must not be touched.
		{ID: 102, UserType: AddMessageProtocol, Body: []byte{persisterAMQPMessageV3}},
		{ID: 103, UserType: PageCursorCounterValue, Body: []byte{1, 2, 3, 4}},
	}

	cs := BuildCursorState(survivors)

	if !cs.AckedEntries[5][[2]int64{2, 7}] {
		t.Errorf("AckedEntries[5][{2,7}] = false, want true")
	}
	if len(cs.AckedEntries[5]) != 1 {
		t.Errorf("AckedEntries[5] has %d entries, want 1: %v", len(cs.AckedEntries[5]), cs.AckedEntries[5])
	}
	if !cs.CompletePages[5][3] {
		t.Errorf("CompletePages[5][3] = false, want true")
	}
	if len(cs.CompletePages[5]) != 1 {
		t.Errorf("CompletePages[5] has %d entries, want 1", len(cs.CompletePages[5]))
	}
}

func TestBuildCursorStateMalformedBodyIgnored(t *testing.T) {
	survivors := []Survivor{
		{ID: 100, UserType: AcknowledgeCursor, Body: []byte{1, 2}}, // too short for the 20-byte encoding
	}
	cs := BuildCursorState(survivors)
	if len(cs.AckedEntries) != 0 {
		t.Errorf("AckedEntries = %v, want empty", cs.AckedEntries)
	}
}

func TestBuildCursorStateEmptyInput(t *testing.T) {
	cs := BuildCursorState(nil)
	if cs.CompletePages == nil || cs.AckedEntries == nil {
		t.Fatalf("maps must be initialized (non-nil) even for empty input: %+v", cs)
	}
	if len(cs.CompletePages) != 0 || len(cs.AckedEntries) != 0 {
		t.Errorf("want empty maps, got %+v", cs)
	}
}

// --- ReadPaging: synthetic cursor filtering (brief step 1b) ---

func TestReadPagingCursorFilteringAckedEntrySkipped(t *testing.T) {
	const queueID = int64(5)
	const pageNr = int64(1)

	entries := [][]byte{
		buildPagedMessagePayload(persisterAMQPMessageV3, []byte("entry-zero-body"), []int64{queueID}),
		buildPagedMessagePayload(persisterAMQPMessageV3, []byte("entry-one-body"), []int64{queueID}),
		buildPagedMessagePayload(persisterAMQPMessageV3, []byte("entry-two-body"), []int64{queueID}),
	}
	var page []byte
	for _, e := range entries {
		page = append(page, encodePageEntry(e)...)
	}
	root := writePageFile(t, "addr1", "000000001.page", page)

	// Entry index 1 (0-based, per format_notes.md section 10's messageNr =
	// ordinal position within the page) acked for its only queue.
	cursors := CursorState{
		AckedEntries: map[int64]map[[2]int64]bool{
			queueID: {{pageNr, 1}: true},
		},
	}

	got, diag, err := ReadPaging(root, cursors)
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 surviving entries, got %d: %+v", len(got), got)
	}
	if diag.PagesSkippedComplete != 0 {
		t.Errorf("PagesSkippedComplete = %d, want 0", diag.PagesSkippedComplete)
	}
	bodies := map[string]bool{}
	for _, m := range got {
		bodies[string(m.AMQP)] = true
	}
	if !bodies["entry-zero-body"] || !bodies["entry-two-body"] {
		t.Errorf("got bodies %v, want entry-zero-body and entry-two-body (entry-one-body acked)", bodies)
	}
	if bodies["entry-one-body"] {
		t.Errorf("entry-one-body should have been filtered as acked")
	}
}

func TestReadPagingCursorFilteringPageCompleteSkipsAllEntries(t *testing.T) {
	const queueID = int64(5)
	const pageNr = int64(1)

	entries := [][]byte{
		buildPagedMessagePayload(persisterAMQPMessageV3, []byte("entry-a"), []int64{queueID}),
		buildPagedMessagePayload(persisterAMQPMessageV3, []byte("entry-b"), []int64{queueID}),
		buildPagedMessagePayload(persisterAMQPMessageV3, []byte("entry-c"), []int64{queueID}),
	}
	var page []byte
	for _, e := range entries {
		page = append(page, encodePageEntry(e)...)
	}
	root := writePageFile(t, "addr1", "000000001.page", page)

	cursors := CursorState{
		CompletePages: map[int64]map[int64]bool{
			queueID: {pageNr: true},
		},
	}

	got, diag, err := ReadPaging(root, cursors)
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 surviving entries (page complete), got %d: %+v", len(got), got)
	}
	if diag.PagesSkippedComplete != 1 {
		t.Errorf("PagesSkippedComplete = %d, want 1", diag.PagesSkippedComplete)
	}
}

func TestReadPagingPageCompletePartialQueuesNotSkipped(t *testing.T) {
	// Two entries in the same page target different queues; only one
	// queue's cursor marks the page complete. format_notes.md section 10:
	// completion is per (queueID, pageNr) -- partial completion across the
	// page's queues must not wholesale-skip the page (at-least-once, spec
	// §4).
	const pageNr = int64(1)
	payloadA := buildPagedMessagePayload(persisterAMQPMessageV3, []byte("queueA-entry"), []int64{1})
	payloadB := buildPagedMessagePayload(persisterAMQPMessageV3, []byte("queueB-entry"), []int64{2})
	page := append(encodePageEntry(payloadA), encodePageEntry(payloadB)...)
	root := writePageFile(t, "addr1", "000000001.page", page)

	cursors := CursorState{
		CompletePages: map[int64]map[int64]bool{
			1: {pageNr: true}, // only queue 1's page marked complete
		},
	}

	got, diag, err := ReadPaging(root, cursors)
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if diag.PagesSkippedComplete != 0 {
		t.Errorf("PagesSkippedComplete = %d, want 0 (only 1 of 2 queues complete)", diag.PagesSkippedComplete)
	}
	if len(got) != 2 {
		t.Fatalf("want both entries exported, got %d: %+v", len(got), got)
	}
}

// --- ReadPaging: corrupt entry (brief step 1c) ---

func TestReadPagingCorruptEntryKeepsEarlierEntries(t *testing.T) {
	good := encodePageEntry(buildPagedMessagePayload(persisterAMQPMessageV3, []byte("good-entry-body"), []int64{1}))
	bad := encodePageEntry(buildPagedMessagePayload(persisterAMQPMessageV3, []byte("second-entry-body"), []int64{1}))
	bad[len(bad)-1] = 0x00 // corrupt the trailing END_BYTE

	page := append(append([]byte{}, good...), bad...)
	root := writePageFile(t, "addr1", "000000001.page", page)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 surviving entry (earlier one kept), got %d: %+v", len(got), got)
	}
	if string(got[0].AMQP) != "good-entry-body" {
		t.Errorf("AMQP = %q, want good-entry-body", got[0].AMQP)
	}
	if len(diag.CorruptPages) != 1 {
		t.Fatalf("want 1 CorruptPages diag, got %d: %+v", len(diag.CorruptPages), diag.CorruptPages)
	}
}

func TestReadPagingCorruptStartByteStopsFile(t *testing.T) {
	good := encodePageEntry(buildPagedMessagePayload(persisterAMQPMessageV3, []byte("only-good-body"), []int64{1}))
	page := append(append([]byte{}, good...), 0xFF) // trailing junk byte: not a valid START_BYTE
	root := writePageFile(t, "addr1", "000000001.page", page)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 1 || string(got[0].AMQP) != "only-good-body" {
		t.Fatalf("want 1 entry only-good-body, got %+v", got)
	}
	if len(diag.CorruptPages) != 1 {
		t.Errorf("want 1 CorruptPages diag, got %d", len(diag.CorruptPages))
	}
}

// --- ReadPaging: Core-persisted entry (brief step 1d) ---

func TestReadPagingCorePersisterExported(t *testing.T) {
	// A well-formed persister-1 Core entry (the golden 2.42.0 payload) followed
	// by a queueIDs list is now decoded and exported, not skipped.
	golden, rerr := os.ReadFile(filepath.Join("testdata", "core-record-2.42.bin"))
	if rerr != nil {
		t.Fatalf("read golden: %v", rerr)
	}
	body := beI64(-1)                            // transactionID
	body = append(body, byte(pageLargeTypeNone)) // largeMessageType
	body = append(body, golden...)               // [persisterID=1][core persister payload]
	body = append(body, beI32(1)...)             // queueIDsCount
	body = append(body, beI64(7)...)             // queueID 7
	entry := encodePageEntry(body)
	root := writePageFile(t, "addr1", "000000001.page", entry)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if diag.CoreSkipped != 0 || diag.UndecodableEntries != 0 {
		t.Fatalf("diag = %+v, want zero", diag)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 exported paged core message, got %d", len(got))
	}
	if got[0].Core == nil || got[0].Core.Address != "salvage.core" {
		t.Errorf("Core payload not decoded: %+v", got[0].Core)
	}
	if len(got[0].QueueIDs) != 1 || got[0].QueueIDs[0] != 7 {
		t.Errorf("QueueIDs = %v, want [7]", got[0].QueueIDs)
	}
}

func TestReadPagingCoreMalformedUndecodable(t *testing.T) {
	body := beI64(-1)                                      // transactionID
	body = append(body, byte(pageLargeTypeNone))           // largeMessageType
	body = append(body, persisterCoreMessage)              // persister id
	body = append(body, []byte{0xDE, 0xAD, 0xBE, 0xEF}...) // truncated core payload
	entry := encodePageEntry(body)
	root := writePageFile(t, "addr1", "000000001.page", entry)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(got), got)
	}
	if diag.UndecodableEntries != 1 {
		t.Errorf("UndecodableEntries = %d, want 1", diag.UndecodableEntries)
	}
}

func TestReadPagingCoreLargeTypeSkipped(t *testing.T) {
	// largeMessageType = CORE (1): a Core-protocol large-message header this
	// AMQP-only reader cannot decode, per format_notes.md section 8's "if
	// type in {CORE, OLD_CORE}" branch. Grouped with persister-id Core under
	// the same CoreSkipped counter (both mean "cannot decode this entry").
	body := beI64(-1)
	body = append(body, byte(pageLargeTypeCore))
	body = append(body, 0x01, 0x02, 0x03) // opaque core-large header, never parsed
	entry := encodePageEntry(body)
	root := writePageFile(t, "addr1", "000000001.page", entry)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(got), got)
	}
	if diag.CoreSkipped != 1 {
		t.Errorf("CoreSkipped = %d, want 1", diag.CoreSkipped)
	}
}

// --- ReadPaging: defensive persister cases (correction #4: no paged-large
// entries in the fixture, but handle defensively) ---

func TestReadPagingLargePersisterSkipped(t *testing.T) {
	body := beI64(-1)
	body = append(body, byte(pageLargeTypeNotCore))
	body = append(body, persisterAMQPLargeMessage)
	entry := encodePageEntry(body)
	root := writePageFile(t, "addr1", "000000001.page", entry)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(got), got)
	}
	if diag.LargeSkipped != 1 {
		t.Errorf("LargeSkipped = %d, want 1", diag.LargeSkipped)
	}
}

func TestReadPagingUnknownPersisterCounted(t *testing.T) {
	body := beI64(-1)
	body = append(body, byte(pageLargeTypeNotCore))
	body = append(body, 99) // not a recognized persister id
	entry := encodePageEntry(body)
	root := writePageFile(t, "addr1", "000000001.page", entry)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want 0 messages, got %d: %+v", len(got), got)
	}
	if diag.UnknownPersister != 1 {
		t.Errorf("UnknownPersister = %d, want 1", diag.UnknownPersister)
	}
}

// --- ReadPaging: misc ---

func TestReadPagingNonexistentDir(t *testing.T) {
	got, diag, err := ReadPaging(filepath.Join(t.TempDir(), "nonexistent"), CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got = %v, want empty", got)
	}
	if diag.CoreSkipped != 0 || diag.LargeSkipped != 0 || diag.UnknownPersister != 0 ||
		diag.UndecodableEntries != 0 || len(diag.CorruptPages) != 0 || diag.PagesSkippedComplete != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}
}

func TestReadPagingEmptyAddressDir(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "addr1"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	got, _, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got = %v, want empty", got)
	}
}

// --- ReadPaging: fixture (brief step 1a) ---

// fixtureCursorState replays the fixture's message journal through
// BuildCursorState. The fixture has zero ACKNOWLEDGE_CURSOR/
// PAGE_CURSOR_COMPLETE survivors (format_notes.md section 3's census: only
// a PAGE_CURSOR_COUNTER_VALUE bookkeeping record is present), so this is
// expected to yield an effectively-empty CursorState -- exercised here so
// the fixture test runs the real pipe end to end rather than hand-building
// an empty CursorState{}.
func fixtureCursorState(t *testing.T, dataDir string) CursorState {
	t.Helper()
	p := NewReplayer()
	jdiags, err := ReadJournalDir(filepath.Join(dataDir, "journal"), "activemq-data", "amq", p.Feed)
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(jdiags) != 0 {
		t.Fatalf("journal diags: %+v", jdiags)
	}
	survivors, _ := p.Resolve()
	return BuildCursorState(survivors)
}

// TestReadPagingFixture covers format_notes.md section 8's fixture census:
// 456 of the 500 salvage.paged manifest entries live in page files (the
// other 44 are journal-resident, covered by
// TestDecodeMessagesFixturePagedSpillover in message_test.go; 456+44=500).
// Nothing was consumed against salvage.paged in the fixture, so every page
// entry round-trips and no cursor filtering should trigger.
func TestReadPagingFixture(t *testing.T) {
	dir := fixtureDir(t)
	cursors := fixtureCursorState(t, dir)

	names, bdiags, err := ReadQueueBindings(filepath.Join(dir, "bindings"))
	if err != nil {
		t.Fatalf("ReadQueueBindings: %v", err)
	}
	if len(bdiags) != 0 {
		t.Fatalf("bindings diags: %+v", bdiags)
	}
	var pagedQueueID int64 = -1
	for id, name := range names {
		if name == "salvage.paged" {
			pagedQueueID = id
		}
	}
	if pagedQueueID == -1 {
		t.Fatalf("no salvage.paged binding in %v", names)
	}

	messages, diag, err := ReadPaging(filepath.Join(dir, "paging"), cursors)
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if len(messages) != 456 {
		t.Fatalf("want 456 paged messages, got %d", len(messages))
	}
	if diag.CoreSkipped != 0 || diag.LargeSkipped != 0 || diag.UnknownPersister != 0 ||
		diag.UndecodableEntries != 0 || len(diag.CorruptPages) != 0 || diag.PagesSkippedComplete != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}

	man := loadManifest(t)
	want := man.Queues["salvage.paged"]
	wantLenByHash := make(map[string]int, len(want))
	for _, e := range want {
		wantLenByHash[e.BodySha256] = e.BodyLen
	}
	seen := make(map[string]bool, len(messages))

	for _, m := range messages {
		if len(m.QueueIDs) != 1 || m.QueueIDs[0] != pagedQueueID {
			t.Fatalf("QueueIDs = %v, want [%d]", m.QueueIDs, pagedQueueID)
		}
		// PagedMessage.AMQP is the raw AMQP-encoded message; extract the
		// Data section the same way message_test.go's verifyQueueBodies
		// does, via go-amqp, so this cross-checks against the manifest's
		// body-only sha256/len.
		body, err := amqpDataSection(m.AMQP)
		if err != nil {
			t.Fatalf("unmarshal AMQP: %v", err)
		}
		hash := sha256Hex(body)
		wantLen, ok := wantLenByHash[hash]
		if !ok {
			t.Fatalf("body sha256 %s not present in manifest", hash)
		}
		if wantLen != len(body) {
			t.Errorf("body len = %d, want %d", len(body), wantLen)
		}
		if seen[hash] {
			t.Errorf("body sha256 %s seen more than once", hash)
		}
		seen[hash] = true
	}
}

// --- OOM guard regression test ---

func TestReadPagingHugeQueueCountRegression(t *testing.T) {
	// A corrupt queueIDsCount (e.g., 0x7FFFFFFF) should not allocate ~17GB.
	// Build an entry with a huge queueIDsCount that would overflow if not guarded.
	payload := beI64(-1)                               // transactionID
	payload = append(payload, byte(pageLargeTypeNone)) // largeMessageType
	payload = append(payload, persisterAMQPMessageV3)
	payload = append(payload, beI64(77)...) // messageID
	payload = append(payload, beI64(0)...)  // messageFormat
	payload = append(payload, encodeNullableSimpleString("test.addr", true)...)
	payload = append(payload, beI32(5)...)        // amqp size
	payload = append(payload, []byte("hello")...) // 5 bytes of AMQP
	payload = append(payload, beI32(0)...)        // extraPropsSize
	payload = append(payload, beI64(0)...)        // expiration (V3)
	// Corrupt queueIDsCount: 0x7FFFFFFF (huge value that would preallocate ~17GB)
	payload = append(payload, beI32(0x7FFFFFFF)...)

	// Place it in a page with a good entry before it.
	goodPayload := buildPagedMessagePayload(persisterAMQPMessageV3, []byte("good-body"), []int64{1})
	page := append(encodePageEntry(goodPayload), encodePageEntry(payload)...)
	root := writePageFile(t, "addr1", "000000001.page", page)

	got, diag, err := ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}

	// The good entry should be kept; the corrupt one should be skipped.
	if len(got) != 1 {
		t.Fatalf("want 1 surviving entry (first one good, second corrupt), got %d: %+v", len(got), got)
	}
	if string(got[0].AMQP) != "good-body" {
		t.Errorf("AMQP = %q, want good-body", got[0].AMQP)
	}

	// The corrupt entry should increment UndecodableEntries, not cause a panic or massive allocation.
	if diag.UndecodableEntries != 1 {
		t.Errorf("UndecodableEntries = %d, want 1", diag.UndecodableEntries)
	}
}

// --- PagingReferenced tests ---

func TestPagingReferencedDetectsPagingUserTypes(t *testing.T) {
	// Table-driven: each of the six paging userTypes should return true.
	tests := []struct {
		name     string
		userType byte
		want     bool
	}{
		{"PAGE_TRANSACTION", PageTransaction, true},
		{"ACKNOWLEDGE_CURSOR", AcknowledgeCursor, true},
		{"PAGE_CURSOR_COUNTER_VALUE", PageCursorCounterValue, true},
		{"PAGE_CURSOR_COUNTER_INC", PageCursorCounterInc, true},
		{"PAGE_CURSOR_COMPLETE", PageCursorComplete, true},
		{"PAGE_PENDING_COUNTER", PageCursorPendingCounter, true},
		{"ADD_MESSAGE_PROTOCOL", AddMessageProtocol, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			survivors := []Survivor{
				{ID: 1, UserType: tt.userType, Body: []byte{1, 2, 3}},
			}
			if got := PagingReferenced(survivors); got != tt.want {
				t.Errorf("PagingReferenced = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPagingReferencedEmptyInput(t *testing.T) {
	if got := PagingReferenced(nil); got {
		t.Errorf("PagingReferenced(nil) = %v, want false", got)
	}
	if got := PagingReferenced([]Survivor{}); got {
		t.Errorf("PagingReferenced(empty) = %v, want false", got)
	}
}

func TestReadPagingDirMissingFlag(t *testing.T) {
	// Nonexistent dir should set DirMissing=true
	_, diag, err := ReadPaging(filepath.Join(t.TempDir(), "nonexistent"), CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if !diag.DirMissing {
		t.Errorf("DirMissing = %v, want true (dir does not exist)", diag.DirMissing)
	}

	// Existing but empty dir should leave DirMissing=false
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "addr1"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	_, diag, err = ReadPaging(root, CursorState{})
	if err != nil {
		t.Fatalf("ReadPaging: %v", err)
	}
	if diag.DirMissing {
		t.Errorf("DirMissing = %v, want false (dir exists but is empty)", diag.DirMissing)
	}
}
