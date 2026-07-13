package journal

import (
	"fmt"
	"sort"
)

// Survivor is a record that outlived replay: its ADD plus every committed
// UPDATE applied to it, in order.
type Survivor struct {
	ID       int64
	UserType byte
	Body     []byte
	Updates  []RawRecord // committed UPDATE records, journal order
}

// ReplayDiag counts what replay discarded.
type ReplayDiag struct {
	RolledBack int // records in rolled-back txs
	InDoubt    int // records in prepared-but-uncommitted txs (spec: skipped, reported)
	Deleted    int
}

// txBuffer holds the not-yet-resolved records of one open transaction, plus
// whether it has seen a PREPARE_RECORD. A tx that never sees a terminal
// record (COMMIT/ROLLBACK) by the time Resolve is called is treated as
// rolled back unless it was prepared, in which case it is in-doubt -- see
// Resolve.
type txBuffer struct {
	records  []RawRecord
	prepared bool
}

// Replayer folds a journal record stream into surviving records, honoring
// transactions: tx-scoped adds/updates/deletes count only under COMMIT;
// PREPARE without COMMIT is in-doubt; ROLLBACK (or a tx with no terminal
// record) discards. Feed the records in on-disk/journal order (as produced
// by ReadJournalDir), then call Resolve once the stream is exhausted.
//
// Pure logic -- no bytes, no I/O.
type Replayer struct {
	// records holds the currently-live survivors, keyed by record ID.
	records map[int64]*Survivor

	// orphanUpdates holds UPDATE (and EVENT) records that arrived for an ID
	// with no live ADD yet -- normal at file boundaries (the ADD lived in an
	// earlier, since-compacted file) or as a compaction artifact where the
	// compactor wrote an update ahead of its add in the rewritten file. They
	// are applied to the survivor's Updates, in arrival order, the moment a
	// matching ADD is seen; if no matching ADD ever arrives they are
	// silently dropped in Resolve.
	orphanUpdates map[int64][]RawRecord

	// txs holds buffered records for transactions not yet resolved by a
	// terminal record (PREPARE does not resolve a tx; only COMMIT/ROLLBACK
	// do).
	txs map[int64]*txBuffer

	diag ReplayDiag
}

// NewReplayer returns an empty Replayer ready for Feed.
func NewReplayer() *Replayer {
	return &Replayer{
		records:       make(map[int64]*Survivor),
		orphanUpdates: make(map[int64][]RawRecord),
		txs:           make(map[int64]*txBuffer),
	}
}

// Feed is the emit callback for ReadJournalDir: it folds one raw journal
// record into replay state. Non-transactional ADD/UPDATE/DELETE apply
// immediately; transactional records are buffered under their TxID until a
// terminal record (COMMIT applies, ROLLBACK discards) resolves the
// transaction; PREPARE marks the transaction as prepared without resolving
// it (see Resolve for how unresolved transactions are counted).
func (p *Replayer) Feed(r RawRecord) error {
	switch r.Type {
	case AddRecord:
		p.applyAdd(r)
	case UpdateRecord, EventRecord:
		// EVENT_RECORD shares ADD/UPDATE_RECORD's wire shape (record ID +
		// user-typed body, format_notes.md section 3) and never appears in
		// the harvested fixture; it is folded in alongside plain updates so
		// an unanticipated use doesn't silently vanish from replay output.
		p.applyUpdate(r)
	case DeleteRecord:
		p.applyDelete(r)
	case AddRecordTx, UpdateRecordTx, DeleteRecordTx:
		tx := p.txFor(r.TxID)
		tx.records = append(tx.records, r)
	case PrepareRecord:
		p.txFor(r.TxID).prepared = true
	case CommitRecord:
		p.commitTx(r.TxID)
	case RollbackRecord:
		p.rollbackTx(r.TxID)
	default:
		// Unreachable in practice: every byte ReadJournalDir hands to emit
		// has already been validated into [EventRecord, RollbackRecord] by
		// readJournalFile/parseRecord, and every value in that range is
		// handled above.
		return fmt.Errorf("journal: replay: unexpected record type %d", r.Type)
	}
	return nil
}

// Resolve finalizes replay: any transaction still open (no COMMIT/ROLLBACK
// ever arrived for it) is resolved now. Per format_notes.md section 9
// (XmlDataExporter.processMessageJournal / Journal.load semantics): an
// unterminated transaction is discarded exactly like an explicit rollback,
// so it counts toward RolledBack; a transaction that reached PREPARE but
// never got a terminal record is in-doubt (XmlDataExporter discards
// preparedTransactions -- in-doubt messages are not exported, only counted).
//
// The returned survivors are ordered by ascending ID.
func (p *Replayer) Resolve() ([]Survivor, ReplayDiag) {
	for _, tx := range p.txs {
		if tx.prepared {
			p.diag.InDoubt += len(tx.records)
		} else {
			p.diag.RolledBack += len(tx.records)
		}
	}
	p.txs = make(map[int64]*txBuffer)

	out := make([]Survivor, 0, len(p.records))
	for _, sv := range p.records {
		out = append(out, *sv)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, p.diag
}

// txFor returns the buffer for txID, creating it if this is the first
// record seen for that transaction.
func (p *Replayer) txFor(txID int64) *txBuffer {
	tx, ok := p.txs[txID]
	if !ok {
		tx = &txBuffer{}
		p.txs[txID] = tx
	}
	return tx
}

// commitTx applies every buffered record of txID, in the order it was fed,
// then drops the transaction. A COMMIT for a TxID with no buffered records
// (e.g. an empty or already-resolved transaction) is a no-op.
func (p *Replayer) commitTx(txID int64) {
	tx, ok := p.txs[txID]
	if !ok {
		return
	}
	for _, r := range tx.records {
		switch r.Type {
		case AddRecordTx:
			p.applyAdd(r)
		case UpdateRecordTx:
			p.applyUpdate(r)
		case DeleteRecordTx:
			p.applyDelete(r)
		}
	}
	delete(p.txs, txID)
}

// rollbackTx discards every buffered record of txID, counting them into
// RolledBack, then drops the transaction. A ROLLBACK for a TxID with no
// buffered records is a no-op.
func (p *Replayer) rollbackTx(txID int64) {
	tx, ok := p.txs[txID]
	if !ok {
		return
	}
	p.diag.RolledBack += len(tx.records)
	delete(p.txs, txID)
}

// applyAdd installs a new survivor for r.ID, folding in any updates that
// arrived for this ID before its add (orphaned at the time, now resolved).
// A later ADD for an ID that already has a live survivor replaces it
// outright (this reader has not observed that case in practice, but it
// mirrors non-transactional semantics: the newest add for an ID wins).
func (p *Replayer) applyAdd(r RawRecord) {
	sv := &Survivor{ID: r.ID, UserType: r.UserType, Body: r.Body}
	if pending, ok := p.orphanUpdates[r.ID]; ok {
		sv.Updates = append(sv.Updates, pending...)
		delete(p.orphanUpdates, r.ID)
	}
	p.records[r.ID] = sv
}

// applyUpdate appends r to its target survivor's Updates if the survivor is
// already live, or buffers it as an orphan (see orphanUpdates) if the add
// has not been seen yet.
func (p *Replayer) applyUpdate(r RawRecord) {
	if sv, ok := p.records[r.ID]; ok {
		sv.Updates = append(sv.Updates, r)
		return
	}
	p.orphanUpdates[r.ID] = append(p.orphanUpdates[r.ID], r)
}

// applyDelete removes r.ID's survivor, if any, counting it into Deleted. A
// delete for an ID with no live survivor (e.g. its add lived in an earlier,
// since-compacted file) is a no-op: there is nothing to discard, and any
// orphan updates queued for that ID are dropped too since no add can ever
// resurrect them.
func (p *Replayer) applyDelete(r RawRecord) {
	if _, ok := p.records[r.ID]; ok {
		delete(p.records, r.ID)
		p.diag.Deleted++
	}
	delete(p.orphanUpdates, r.ID)
}
