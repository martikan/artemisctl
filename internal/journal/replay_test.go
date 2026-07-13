package journal

import (
	"path/filepath"
	"testing"
)

// findSurvivor returns the survivor with the given id, or nil.
func findSurvivor(survivors []Survivor, id int64) *Survivor {
	for i := range survivors {
		if survivors[i].ID == id {
			return &survivors[i]
		}
	}
	return nil
}

func TestReplayAddSurvives(t *testing.T) {
	p := NewReplayer()
	if err := p.Feed(RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")}); err != nil {
		t.Fatalf("Feed: %v", err)
	}
	survivors, diag := p.Resolve()
	if len(survivors) != 1 {
		t.Fatalf("want 1 survivor, got %d", len(survivors))
	}
	sv := survivors[0]
	if sv.ID != 1 || string(sv.Body) != "body" || sv.UserType != AddMessageProtocol {
		t.Errorf("survivor = %+v, want ID=1 Body=body UserType=AddMessageProtocol", sv)
	}
	if len(sv.Updates) != 0 {
		t.Errorf("want 0 updates, got %d", len(sv.Updates))
	}
	if diag != (ReplayDiag{}) {
		t.Errorf("diag = %+v, want zero", diag)
	}
}

func TestReplayAddThenDeleteGone(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: DeleteRecord, ID: 1})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag.Deleted != 1 {
		t.Errorf("diag.Deleted = %d, want 1", diag.Deleted)
	}
}

func TestReplayAddThenUpdateSurvivesWithUpdate(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	upd := RawRecord{Type: UpdateRecord, ID: 1, UserType: AddRef, Body: []byte("ref")}
	mustFeed(t, p, upd)

	survivors, _ := p.Resolve()
	sv := findSurvivor(survivors, 1)
	if sv == nil {
		t.Fatalf("survivor 1 not found")
	}
	if len(sv.Updates) != 1 || string(sv.Updates[0].Body) != "ref" {
		t.Errorf("sv.Updates = %+v, want 1 update with body 'ref'", sv.Updates)
	}
}

func TestReplayTxAddCommitSurvives(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecordTx, TxID: 100, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: CommitRecord, TxID: 100})

	survivors, diag := p.Resolve()
	if len(survivors) != 1 {
		t.Fatalf("want 1 survivor, got %d", len(survivors))
	}
	if diag.RolledBack != 0 || diag.InDoubt != 0 {
		t.Errorf("diag = %+v, want zero RolledBack/InDoubt", diag)
	}
}

func TestReplayTxAddRollbackGone(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecordTx, TxID: 100, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: RollbackRecord, TxID: 100})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag.RolledBack != 1 {
		t.Errorf("diag.RolledBack = %d, want 1", diag.RolledBack)
	}
}

// TestReplayTxNoTerminalRecordDiscarded covers a transaction with buffered
// records but no COMMIT/ROLLBACK/PREPARE at all by end of stream (e.g. the
// broker crashed mid-transaction, so the journal simply has no terminal
// record for it). format_notes.md section 9 (citing
// XmlDataExporter.processMessageJournal / Journal.load): "ROLLBACK and
// **unterminated** transactions are discarded" -- an unterminated tx is
// treated exactly like an explicit rollback, so this counts toward
// RolledBack, not InDoubt (InDoubt is reserved for transactions that reached
// PREPARE but never got a COMMIT/ROLLBACK).
func TestReplayTxNoTerminalRecordDiscarded(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecordTx, TxID: 100, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag.RolledBack != 1 {
		t.Errorf("diag.RolledBack = %d, want 1", diag.RolledBack)
	}
	if diag.InDoubt != 0 {
		t.Errorf("diag.InDoubt = %d, want 0", diag.InDoubt)
	}
}

// TestReplayTxPrepareOnlyInDoubt: format_notes.md section 9 -- "PREPARE-only
// tx go into preparedTransactions (in-doubt)... XmlDataExporter discards
// preparedTransactions (in-doubt messages are NOT exported)."
func TestReplayTxPrepareOnlyInDoubt(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecordTx, TxID: 100, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: PrepareRecord, TxID: 100, NumberOfRecords: 1})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag.InDoubt != 1 {
		t.Errorf("diag.InDoubt = %d, want 1", diag.InDoubt)
	}
	if diag.RolledBack != 0 {
		t.Errorf("diag.RolledBack = %d, want 0", diag.RolledBack)
	}
}

func TestReplayTxPrepareThenCommitSurvives(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecordTx, TxID: 100, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: PrepareRecord, TxID: 100, NumberOfRecords: 1})
	mustFeed(t, p, RawRecord{Type: CommitRecord, TxID: 100})

	survivors, diag := p.Resolve()
	if len(survivors) != 1 {
		t.Fatalf("want 1 survivor, got %d", len(survivors))
	}
	if diag.InDoubt != 0 || diag.RolledBack != 0 {
		t.Errorf("diag = %+v, want zero", diag)
	}
}

// TestReplayTxPrepareThenRollbackDiscarded covers a heuristic rollback of a
// prepared (2PC) transaction: it must count as RolledBack, not InDoubt --
// InDoubt is only for prepared transactions that never got a terminal
// COMMIT/ROLLBACK at all.
func TestReplayTxPrepareThenRollbackDiscarded(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecordTx, TxID: 100, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: PrepareRecord, TxID: 100, NumberOfRecords: 1})
	mustFeed(t, p, RawRecord{Type: RollbackRecord, TxID: 100})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag.RolledBack != 1 {
		t.Errorf("diag.RolledBack = %d, want 1", diag.RolledBack)
	}
	if diag.InDoubt != 0 {
		t.Errorf("diag.InDoubt = %d, want 0", diag.InDoubt)
	}
}

func TestReplayDeleteInTxCommittedGone(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: DeleteRecordTx, TxID: 100, ID: 1})
	mustFeed(t, p, RawRecord{Type: CommitRecord, TxID: 100})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag.Deleted != 1 {
		t.Errorf("diag.Deleted = %d, want 1", diag.Deleted)
	}
}

func TestReplayDeleteInTxRolledBackSurvives(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: DeleteRecordTx, TxID: 100, ID: 1})
	mustFeed(t, p, RawRecord{Type: RollbackRecord, TxID: 100})

	survivors, diag := p.Resolve()
	if len(survivors) != 1 {
		t.Fatalf("want 1 survivor, got %d: %+v", len(survivors), survivors)
	}
	if survivors[0].ID != 1 {
		t.Errorf("survivor ID = %d, want 1", survivors[0].ID)
	}
	if diag.RolledBack != 1 {
		t.Errorf("diag.RolledBack = %d, want 1", diag.RolledBack)
	}
	if diag.Deleted != 0 {
		t.Errorf("diag.Deleted = %d, want 0", diag.Deleted)
	}
}

// TestReplayUpdateBeforeAddAppliedWhenAddSurvives covers a compaction
// artifact (format_notes.md section 9 bullet 3 / section on paging
// cross-check): compaction can rewrite a message's UPDATE (e.g. its ADD_REF)
// ahead of its ADD_RECORD in the resulting file. The update must still land
// on the survivor once the add arrives.
func TestReplayUpdateBeforeAddAppliedWhenAddSurvives(t *testing.T) {
	p := NewReplayer()
	upd := RawRecord{Type: UpdateRecord, ID: 1, UserType: AddRef, Body: []byte("ref")}
	mustFeed(t, p, upd)
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})

	survivors, _ := p.Resolve()
	sv := findSurvivor(survivors, 1)
	if sv == nil {
		t.Fatalf("survivor 1 not found")
	}
	if len(sv.Updates) != 1 || string(sv.Updates[0].Body) != "ref" {
		t.Errorf("sv.Updates = %+v, want 1 update with body 'ref'", sv.Updates)
	}
}

// TestReplayUpdateBeforeAddDroppedWhenAddNeverArrives covers the other half
// of the same compaction scenario: the update's target ADD lived in an
// earlier, since-compacted file and never shows up in this stream at all
// (normal at file boundaries). The orphan update must be silently dropped,
// not surfaced as a survivor or a diagnostic.
func TestReplayUpdateBeforeAddDroppedWhenAddNeverArrives(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: UpdateRecord, ID: 1, UserType: AddRef, Body: []byte("ref")})

	survivors, diag := p.Resolve()
	if len(survivors) != 0 {
		t.Fatalf("want 0 survivors, got %d: %+v", len(survivors), survivors)
	}
	if diag != (ReplayDiag{}) {
		t.Errorf("diag = %+v, want zero (orphan update is dropped silently)", diag)
	}
}

func TestReplayUpdatesPreserveJournalOrder(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("body")})
	mustFeed(t, p, RawRecord{Type: UpdateRecord, ID: 1, UserType: AddRef, Body: []byte("first")})
	mustFeed(t, p, RawRecord{Type: UpdateRecord, ID: 1, UserType: SetScheduledDeliveryTime, Body: []byte("second")})

	survivors, _ := p.Resolve()
	sv := findSurvivor(survivors, 1)
	if sv == nil {
		t.Fatalf("survivor 1 not found")
	}
	if len(sv.Updates) != 2 || string(sv.Updates[0].Body) != "first" || string(sv.Updates[1].Body) != "second" {
		t.Errorf("sv.Updates = %+v, want [first, second] in order", sv.Updates)
	}
}

func TestReplayResultOrderedByAscendingID(t *testing.T) {
	p := NewReplayer()
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 5, UserType: AddMessageProtocol, Body: []byte("five")})
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 1, UserType: AddMessageProtocol, Body: []byte("one")})
	mustFeed(t, p, RawRecord{Type: AddRecord, ID: 3, UserType: AddMessageProtocol, Body: []byte("three")})

	survivors, _ := p.Resolve()
	if len(survivors) != 3 {
		t.Fatalf("want 3 survivors, got %d", len(survivors))
	}
	var ids []int64
	for _, sv := range survivors {
		ids = append(ids, sv.ID)
	}
	want := []int64{1, 3, 5}
	for i, id := range ids {
		if id != want[i] {
			t.Errorf("survivors[%d].ID = %d, want %d (order = %v)", i, id, want[i], ids)
		}
	}
}

func mustFeed(t *testing.T, p *Replayer, r RawRecord) {
	t.Helper()
	if err := p.Feed(r); err != nil {
		t.Fatalf("Feed(%+v): %v", r, err)
	}
}

// TestReplayFixtureMessageJournal replays the harvested 2.42 fixture's
// message journal end to end (ReadJournalDir -> Feed -> Resolve). The broker
// shut down clean, so there are no ROLLBACK or PREPARE records here -- the
// transaction paths above are synthetic by necessity; this covers the happy
// path with real bytes.
//
// Expected numbers (manifest.json + format_notes.md fixture census):
//   - 59 ADD_MESSAGE_PROTOCOL (45) adds reach the journal: 5 plain + 5 props
//   - 1 scheduled + 1 large + 3 acked + 44 of the 500 paged messages
//     (paging is a spillover, not a mirror; the other 456 live in page
//     files, out of scope for journal replay).
//   - The 3 salvage.acked messages were received+accepted, leaving 3
//     DELETE_RECORDs => Deleted = 3, and 59-3 = 56 message survivors.
//   - Every surviving message carries its ADD_REF (userType 32), which
//     arrives framed as an UPDATE_RECORD.
//   - Exactly 1 survivor (salvage.scheduled) carries a
//     SET_SCHEDULED_DELIVERY_TIME (36) update.
func TestReplayFixtureMessageJournal(t *testing.T) {
	dir := fixtureDir(t)

	p := NewReplayer()
	diags, err := ReadJournalDir(filepath.Join(dir, "journal"), "activemq-data", "amq", p.Feed)
	if err != nil {
		t.Fatalf("ReadJournalDir: %v", err)
	}
	if len(diags) != 0 {
		t.Fatalf("want 0 diags over the clean fixture, got %d: %+v", len(diags), diags)
	}

	survivors, diag := p.Resolve()

	if diag.Deleted != 3 {
		t.Errorf("diag.Deleted = %d, want 3 (the fully-acked salvage.acked messages)", diag.Deleted)
	}
	if diag.RolledBack != 0 {
		t.Errorf("diag.RolledBack = %d, want 0 (clean shutdown, no rolled-back txs)", diag.RolledBack)
	}
	if diag.InDoubt != 0 {
		t.Errorf("diag.InDoubt = %d, want 0 (clean shutdown, no prepared txs)", diag.InDoubt)
	}

	var messages, withRef, withScheduled int
	for _, sv := range survivors {
		if sv.UserType != AddMessageProtocol {
			continue
		}
		messages++
		var hasRef, hasScheduled bool
		for _, u := range sv.Updates {
			switch u.UserType {
			case AddRef:
				hasRef = true
			case SetScheduledDeliveryTime:
				hasScheduled = true
			}
		}
		if hasRef {
			withRef++
		}
		if hasScheduled {
			withScheduled++
		}
	}
	if messages != 56 {
		t.Errorf("AddMessageProtocol survivors = %d, want 56 (59 journal adds - 3 acked)", messages)
	}
	if withRef != messages {
		t.Errorf("survivors with an AddRef update = %d, want all %d", withRef, messages)
	}
	if withScheduled != 1 {
		t.Errorf("survivors with a SetScheduledDeliveryTime update = %d, want 1 (salvage.scheduled)", withScheduled)
	}
}
