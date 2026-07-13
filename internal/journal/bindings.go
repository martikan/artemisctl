package journal

import "fmt"

// ReadQueueBindings replays the bindings journal and returns queueID → queue
// name for every surviving QUEUE_BINDING_RECORD. Other bindings journal
// record types (address bindings, queue status, address settings, security
// settings, diverts, bridges, …, see format_notes.md section 4) share the
// same record-ID namespace and file set but are ignored here: this reader
// only needs the queueID → name/address mapping to label salvaged messages.
//
// Records are replayed through Replayer so transaction semantics and
// DELETE_RECORD apply exactly as they do for the message journal (Task 5):
// a queue binding that was deleted (queue removed) before the broker died
// does not survive into the returned map.
//
// A single binding record whose body fails to decode (e.g. a garbled
// length-prefix byte) does NOT abort the run: that one binding is skipped
// and reported as a corruption-class FileDiag (id + reason), and every other
// binding still decodes normally. The messages that would have resolved
// through the skipped binding still export fine -- they simply fall back to
// the synthetic unknown-queue-<id> name (salvage.go's queueName), exactly as
// they would for a queue whose binding legitimately never existed. Aborting
// the entire salvage over one damaged bindings-journal record would be a far
// worse outcome than that fallback (spec §1: "a recovery tool must not
// silently lose messages" -- losing everything over one bad record is its
// own kind of silent loss).
func ReadQueueBindings(bindingsDir string) (map[int64]string, []FileDiag, error) {
	replayer := NewReplayer()
	diags, err := ReadJournalDir(bindingsDir, "activemq-bindings", "bindings", func(r RawRecord) error {
		return replayer.Feed(r)
	})
	if err != nil {
		return nil, diags, err
	}

	survivors, _ := replayer.Resolve()

	out := make(map[int64]string, len(survivors))
	for _, sv := range survivors {
		if sv.UserType != QueueBindingRecord {
			continue
		}

		name, address, err := decodeQueueBinding(sv.Body)
		if err != nil {
			diags = append(diags, FileDiag{
				Path:    bindingsDir,
				Reason:  fmt.Sprintf("decode queue binding id %d: %v", sv.ID, err),
				Corrupt: true,
			})
			continue
		}
		_ = address // read for framing correctness only; not surfaced by this API (brief: name/address for a debug log)

		out[sv.ID] = name
	}
	return out, diags, nil
}

// decodeQueueBinding decodes the stable prefix of
// PersistentQueueBindingEncoding.decode (format_notes.md section 9's
// "Supporting codec field orders"): SimpleString queueName, SimpleString
// address. The remaining fields (nullable filter string, nullable user
// metadata, autoCreated boolean, and a versioned tail of routing-type/
// max-consumers/purge flags) are intentionally left unread -- the tail's
// shape is version-dependent and salvage does not need it.
func decodeQueueBinding(body []byte) (name, address string, err error) {
	r := newReader(body)
	name = r.simpleString()
	address = r.simpleString()
	if r.err() != nil {
		return "", "", r.err()
	}
	return name, address, nil
}
