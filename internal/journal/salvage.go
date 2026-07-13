package journal

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/store"
)

// Options locates the four data sub-dirs (already resolved by the CLI).
type Options struct{ Bindings, Journal, LargeMessages, Paging string }

// Summary is everything the CLI prints and gates exit codes on.
type Summary struct {
	PerQueue     map[string]int // exported records per queue name
	Large, Paged int            // Large = records from large-message-sourced messages; Paged = records read from page files (not journal-spilled paged messages)
	LargestBytes int64
	Core         int      // exported Core-protocol records (subset of the per-queue totals)
	Skips        []string // human-readable, one per skip class w/ counts+ids
	Diags        []string // corruption/diagnostic notes (non-skip)

	// corrupt counts how many of the entries in Diags represent
	// corruption-class incidents (structural journal/page/bindings damage --
	// check-size mismatch, truncated record, broken page-entry framing, an
	// undecodable bindings-record body) as opposed to benign notes (missing
	// large-messages/paging dirs, unknown-queue fallback, orphaned
	// large-message files, or a fileID-mismatch "reuse leftover" from a
	// normally-reused journal file). Populated exclusively by
	// appendFileDiags, which reads the classification straight off each
	// source FileDiag's Corrupt field -- never by matching against the
	// assembled Diags prose. Unexported: only this package ever needs to set
	// it, and Salvage's caller reads it via HasCorruption.
	corrupt int
}

// Total returns the total number of exported records across all queues.
func (s Summary) Total() int {
	total := 0
	for _, n := range s.PerQueue {
		total += n
	}
	return total
}

// HasSkips reports whether anything was skipped (lost) during the run — the
// CLI gates its non-zero exit code on this (spec §1: "a recovery tool must
// not silently lose messages").
func (s Summary) HasSkips() bool {
	return len(s.Skips) > 0
}

// HasCorruption reports whether any Diags entry represents corruption-class
// structural damage (see the corrupt field's doc comment). The CLI gates its
// non-zero exit code on this exactly like HasSkips, and with the same
// --allow-skips override: corrupted journal/page/bindings data is data loss
// just as surely as an unsupported record type, and a recovery tool must not
// let that be missable in a script (spec §1/§5).
func (s Summary) HasCorruption() bool {
	return s.corrupt > 0
}

// appendFileDiags formats diags (from ReadJournalDir/ReadPaging/
// ReadQueueBindings) into s.Diags, prefixed with source (e.g. "bindings
// journal", "message journal", "paging") to disambiguate which reader hit
// the incident, and tallies how many are corruption-class into s.corrupt so
// HasCorruption sees them. Classification comes straight from each source
// FileDiag's own Corrupt field, set at the point the diag was created --
// not from matching against the prose assembled here.
func (s *Summary) appendFileDiags(source string, diags []FileDiag) {
	for _, d := range diags {
		s.Diags = append(s.Diags, fmt.Sprintf("%s: %s (offset %d): %s", source, d.Path, d.Offset, d.Reason))
		if d.Corrupt {
			s.corrupt++
		}
	}
}

// Salvage runs the full offline pipeline -- ReadQueueBindings ->
// ReadJournalDir(journal) -> Replayer -> DecodeMessages -> AttachLargeBodies
// -> ReadPaging -- and emits one store.Record per surviving (message, queue)
// pair, fanning a message referenced by N queues out into N records (spec
// §3). Records are emitted via emit as they are produced; an error from emit
// aborts the run immediately, returning the Summary accumulated so far
// alongside the error.
//
// Missing/unreadable Bindings or Journal dirs are fatal (spec §5). Missing
// LargeMessages or Paging dirs are not fatal -- AttachLargeBodies/ReadPaging
// already treat them as "nothing to attach/nothing was paged" -- but are
// worth a Diags warning when the journal actually references large messages
// or paging state, since an operator seeing a suspiciously low large/paged
// count needs to know the dir was simply absent rather than everything
// having failed to decode.
func Salvage(opts Options, emit func(store.Record) error) (Summary, error) {
	summary := Summary{PerQueue: make(map[string]int)}

	if err := requireDir(opts.Bindings); err != nil {
		return summary, fmt.Errorf("salvage: bindings dir: %w", err)
	}
	if err := requireDir(opts.Journal); err != nil {
		return summary, fmt.Errorf("salvage: journal dir: %w", err)
	}

	names, bindingDiags, err := ReadQueueBindings(opts.Bindings)
	if err != nil {
		return summary, fmt.Errorf("salvage: read bindings: %w", err)
	}
	summary.appendFileDiags("bindings journal", bindingDiags)

	replayer := NewReplayer()
	journalDiags, err := ReadJournalDir(opts.Journal, "activemq-data", "amq", replayer.Feed)
	if err != nil {
		return summary, fmt.Errorf("salvage: read journal: %w", err)
	}
	summary.appendFileDiags("message journal", journalDiags)

	survivors, replayDiag := replayer.Resolve()
	if replayDiag.InDoubt > 0 {
		summary.Skips = append(summary.Skips, fmt.Sprintf(
			"in-doubt transaction records (prepared but never committed or rolled back): %d", replayDiag.InDoubt))
	}

	messages, msgDiag, err := DecodeMessages(survivors)
	if err != nil {
		return summary, fmt.Errorf("salvage: decode messages: %w", err)
	}
	summary.Skips = append(summary.Skips, messageDiagSkips(msgDiag)...)

	// spec §5: warn (Diags only, not a Skip) if the journal references large
	// messages but the large-messages dir is missing. Checked against the
	// pre-attach messages, since a missing dir makes AttachLargeBodies drop
	// every Large message into MissingFile -- checking post-attach could
	// wrongly conclude "not referenced" once none are left.
	if anyLarge(messages) && dirMissing(opts.LargeMessages) {
		summary.Diags = append(summary.Diags, fmt.Sprintf(
			"large-messages dir %s is missing but the journal references large messages", opts.LargeMessages))
	}

	messages, largeDiag, err := AttachLargeBodies(messages, opts.LargeMessages)
	if err != nil {
		return summary, fmt.Errorf("salvage: attach large bodies: %w", err)
	}
	summary.LargestBytes = largeDiag.LargestBytes
	if len(largeDiag.MissingFile) > 0 {
		summary.Skips = append(summary.Skips, fmt.Sprintf(
			"large-message body files missing: %d (ids: %s)", len(largeDiag.MissingFile), joinInt64s(largeDiag.MissingFile)))
	}
	if len(largeDiag.Orphans) > 0 {
		sortedOrphans := append([]string(nil), largeDiag.Orphans...)
		sort.Strings(sortedOrphans)
		summary.Diags = append(summary.Diags, fmt.Sprintf(
			"orphaned large-message files with no surviving journal record (normal): %d (%s)",
			len(sortedOrphans), strings.Join(sortedOrphans, ", ")))
	}

	cursors := BuildCursorState(survivors)
	pagedMessages, pagingDiag, err := ReadPaging(opts.Paging, cursors)
	if err != nil {
		return summary, fmt.Errorf("salvage: read paging: %w", err)
	}
	if PagingReferenced(survivors) && pagingDiag.DirMissing {
		summary.Diags = append(summary.Diags, fmt.Sprintf(
			"paging dir %s is missing but the journal references paging state", opts.Paging))
	}
	summary.appendFileDiags("paging", pagingDiag.CorruptPages)
	if pagingDiag.PagesSkippedComplete > 0 {
		summary.Diags = append(summary.Diags, fmt.Sprintf(
			"pages skipped as fully consumed per cursor state (not a loss): %d", pagingDiag.PagesSkippedComplete))
	}
	summary.Skips = append(summary.Skips, pagingDiagSkips(pagingDiag)...)

	// unknownQueues counts, per queueID, how many records were exported
	// under the synthetic "unknown-queue-<id>" fallback name (spec §5: the
	// message is still saved, so this is a Diags warning, not a Skip).
	unknownQueues := make(map[int64]int)

	for _, m := range messages {
		if m.Core != nil {
			if err := emitCoreFanout(m.Core, m.ScheduledMs, m.QueueIDs, false, names, unknownQueues, &summary, emit); err != nil {
				return summary, fmt.Errorf("salvage: core message %d: %w", m.ID, err)
			}
			continue
		}
		if err := emitFanout(m.AMQP, m.ScheduledMs, m.QueueIDs, m.Large, false, names, unknownQueues, &summary, emit); err != nil {
			return summary, fmt.Errorf("salvage: message %d: %w", m.ID, err)
		}
	}
	for _, pm := range pagedMessages {
		if pm.Core != nil {
			if err := emitCoreFanout(pm.Core, pm.ScheduledMs, pm.QueueIDs, true, names, unknownQueues, &summary, emit); err != nil {
				return summary, fmt.Errorf("salvage: paged core message: %w", err)
			}
			continue
		}
		if err := emitFanout(pm.AMQP, pm.ScheduledMs, pm.QueueIDs, false, true, names, unknownQueues, &summary, emit); err != nil {
			return summary, fmt.Errorf("salvage: paged message: %w", err)
		}
	}

	if len(unknownQueues) > 0 {
		ids := make([]int64, 0, len(unknownQueues))
		for id := range unknownQueues {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		for _, id := range ids {
			summary.Diags = append(summary.Diags, fmt.Sprintf(
				"queueID %d has no surviving binding; %d record(s) exported under synthetic queue name unknown-queue-%d",
				id, unknownQueues[id], id))
		}
	}

	return summary, nil
}

// emitFanout unmarshals amqpBytes once (applying the scheduled-delivery
// annotation if scheduledMs != 0), then emits one store.Record per queueID
// in queueIDs, updating summary's per-queue/Large/Paged counters as it goes.
// It returns the first error from emit, if any, without emitting further
// records for this message.
func emitFanout(amqpBytes []byte, scheduledMs int64, queueIDs []int64, large, paged bool, names map[int64]string, unknownQueues map[int64]int, summary *Summary, emit func(store.Record) error) error {
	preparedAMQP, msg, err := prepareMessage(amqpBytes, scheduledMs)
	if err != nil {
		return err
	}

	for _, qid := range queueIDs {
		queue := queueName(names, qid, unknownQueues)
		rec := store.Record{
			UUID:      store.DedupID(msg, queue),
			Queue:     queue,
			DrainedAt: time.Now().UnixNano(),
			AMQP:      preparedAMQP,
		}
		if err := emit(rec); err != nil {
			return err
		}
		summary.PerQueue[queue]++
		if large {
			summary.Large++
		}
		if paged {
			summary.Paged++
		}
	}
	return nil
}

// emitCoreFanout serializes a decoded Core message once and emits one
// KindCore store.Record per surviving queueID, updating summary's
// per-queue/Core/Paged counters. A non-zero scheduledMs (from a
// SET_SCHEDULED_DELIVERY_TIME journal update) is threaded into the payload as
// the synthetic property "_ARTX_SCHEDULED_MS" so broker/coreconvert can set
// the AMQP x-opt-delivery-time annotation on redelivery. Dedup ids come from
// DedupIDCore (the Core message has no amqp.Message form).
func emitCoreFanout(core *CorePayload, scheduledMs int64, queueIDs []int64, paged bool, names map[int64]string, unknownQueues map[int64]int, summary *Summary, emit func(store.Record) error) error {
	if scheduledMs != 0 {
		if core.Properties == nil {
			core.Properties = map[string]any{}
		}
		core.Properties["_ARTX_SCHEDULED_MS"] = scheduledMs
	}
	payload := core.Encode()

	for _, qid := range queueIDs {
		queue := queueName(names, qid, unknownQueues)
		rec := store.Record{
			UUID:        store.DedupIDCore(payload, queue),
			Queue:       queue,
			DrainedAt:   time.Now().UnixNano(),
			Kind:        store.KindCore,
			CorePayload: payload,
		}
		if err := emit(rec); err != nil {
			return err
		}
		summary.PerQueue[queue]++
		summary.Core++
		if paged {
			summary.Paged++
		}
	}
	return nil
}

// prepareMessage unmarshals amqpBytes once. For a non-scheduled message
// (scheduledMs == 0) it returns amqpBytes verbatim, unmodified, alongside the
// decoded message (used only for hashing). For a scheduled message it sets
// the "x-opt-delivery-time" message annotation to scheduledMs and
// re-marshals, returning the new bytes -- this is the only case that
// re-marshals; every other message's stored AMQP bytes are the original,
// untouched journal/large/paged bytes.
func prepareMessage(amqpBytes []byte, scheduledMs int64) ([]byte, *amqp.Message, error) {
	msg := new(amqp.Message)
	if err := msg.UnmarshalBinary(amqpBytes); err != nil {
		return nil, nil, fmt.Errorf("unmarshal AMQP message: %w", err)
	}
	if scheduledMs == 0 {
		return amqpBytes, msg, nil
	}

	if msg.Annotations == nil {
		msg.Annotations = amqp.Annotations{}
	}
	msg.Annotations["x-opt-delivery-time"] = scheduledMs
	remarshaled, err := msg.MarshalBinary()
	if err != nil {
		return nil, nil, fmt.Errorf("marshal scheduled AMQP message: %w", err)
	}
	return remarshaled, msg, nil
}

// queueName resolves queueID via names, falling back to the synthetic
// "unknown-queue-<id>" name (spec §5) when the bindings journal has no
// surviving binding for it -- e.g. the queue was deleted after the message
// was enqueued but before the broker died. unknownQueues counts fallback
// uses per queueID for the caller's summary Diags.
func queueName(names map[int64]string, queueID int64, unknownQueues map[int64]int) string {
	if name, ok := names[queueID]; ok {
		return name
	}
	unknownQueues[queueID]++
	return fmt.Sprintf("unknown-queue-%d", queueID)
}

// anyLarge reports whether any message in msgs is a Large message (body
// joined from data/large-messages/<id>.msg) -- used to decide whether a
// missing LargeMessages dir is worth a Diags warning.
func anyLarge(msgs []Message) bool {
	for _, m := range msgs {
		if m.Large {
			return true
		}
	}
	return false
}

// requireDir returns an error if dir does not exist, is unreadable, or is
// not a directory.
func requireDir(dir string) error {
	fi, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s: not a directory", dir)
	}
	return nil
}

// dirMissing reports whether dir does not exist. Any other stat error (e.g.
// a permission problem) is deliberately NOT treated as "missing" here --
// AttachLargeBodies/ReadPaging already surface those as hard errors from
// Salvage, so this helper only needs to detect the specific "absent
// directory" case the spec calls non-fatal.
func dirMissing(dir string) bool {
	_, err := os.Stat(dir)
	if err == nil {
		return false
	}
	return errors.Is(err, fs.ErrNotExist)
}

// messageDiagSkips formats MessageDiag's skip classes (Core-protocol
// messages this AMQP-only reader cannot decode, unrecognized persister ids,
// undecodable bodies) into Summary.Skips entries.
func messageDiagSkips(diag MessageDiag) []string {
	var out []string
	if len(diag.CoreSkipped) > 0 {
		ids := make([]int64, 0, len(diag.CoreSkipped))
		refs := 0
		for id, qids := range diag.CoreSkipped {
			ids = append(ids, id)
			refs += len(qids)
		}
		out = append(out, fmt.Sprintf(
			"Core-protocol messages skipped in message journal (unsupported by this AMQP-only reader): %d messages, %d surviving queue refs (ids: %s)",
			len(ids), refs, joinInt64s(ids)))
	}
	if diag.UnknownPersister > 0 {
		out = append(out, fmt.Sprintf("unrecognized persister id in message journal: %d", diag.UnknownPersister))
	}
	if diag.UndecodableBody > 0 {
		out = append(out, fmt.Sprintf("undecodable message bodies in message journal: %d", diag.UndecodableBody))
	}
	return out
}

// pagingDiagSkips formats PagingDiag's skip classes (Core-protocol/AMQP
// large entries embedded in page files, unrecognized persister ids,
// undecodable entries) into Summary.Skips entries. PagesSkippedComplete and
// CorruptPages are handled separately by the caller -- the former is not a
// loss (already-consumed pages are correctly excluded) and the latter is
// file-diag material, not a skip class.
func pagingDiagSkips(diag PagingDiag) []string {
	var out []string
	if diag.CoreSkipped > 0 {
		out = append(out, fmt.Sprintf("Core-protocol entries skipped in page files (unsupported by this AMQP-only reader): %d", diag.CoreSkipped))
	}
	if diag.LargeSkipped > 0 {
		out = append(out, fmt.Sprintf("AMQP large-message entries embedded in page files skipped (unsupported): %d", diag.LargeSkipped))
	}
	if diag.UnknownPersister > 0 {
		out = append(out, fmt.Sprintf("unrecognized persister id in page files: %d", diag.UnknownPersister))
	}
	if diag.UndecodableEntries > 0 {
		out = append(out, fmt.Sprintf("undecodable entries in page files: %d", diag.UndecodableEntries))
	}
	return out
}

// joinInt64s formats ids in ascending order as a comma-separated list, for
// Skips/Diags entries.
func joinInt64s(ids []int64) string {
	sorted := append([]int64(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	parts := make([]string, len(sorted))
	for i, id := range sorted {
		parts[i] = strconv.FormatInt(id, 10)
	}
	return strings.Join(parts, ", ")
}
