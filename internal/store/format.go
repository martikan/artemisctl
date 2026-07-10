// Package store is the on-disk format for drained messages: an append-only
// write-ahead log (WAL) chosen so an evacuation that crashes mid-write is still
// readable up to the last intact record. Writer appends and fsyncs records
// (writer.go), Reader streams them back for redelivery (reader.go), and a
// sidecar checkpoint tracks replay progress so an interrupted redelivery
// resumes (checkpoint.go). This file defines the shared constants and record
// shape; the exact byte layout lives alongside Writer/Reader.
package store

const (
	Magic          = "ARTX" // 4-byte file signature at offset 0
	Version   byte = 1      // format version, one byte after the magic
	headerLen      = 5      // len(Magic) + 1 version byte
)

// Record is one drained message as held in the WAL. UUID is the deterministic
// content id reused as _AMQ_DUPL_ID on redelivery to defeat duplicates; Queue
// is the originating queue; DrainedAt is the drain time in unix nanoseconds;
// AMQP is the raw amqp.Message wire encoding, replayed verbatim for
// perfect-fidelity redelivery.
type Record struct {
	UUID      [16]byte
	Queue     string
	DrainedAt int64
	AMQP      []byte
}
