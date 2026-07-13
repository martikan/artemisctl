package store

import (
	"crypto/sha256"

	"github.com/Azure/go-amqp"
)

// DedupID returns a stable 16-byte content id for a message on a given
// queue, excluding the transport fields Artemis mutates on redelivery —
// Header.DeliveryCount and Header.FirstAcquirer, plus the
// delivery-annotations section — so a re-drained redelivered message hashes
// identically to its first drain. Everything that is part of the message's
// identity (durable/priority/ttl, message annotations, properties,
// application-properties, body, footer) is kept, so two messages that differ
// only in, say, priority still get different ids.
//
// It hashes a shallow clone with a fresh Header copy (the volatile fields
// zeroed) and the delivery annotations cleared, so the original msg is never
// mutated and stays full-fidelity for replay.
//
// The normalized bytes are then salted with queue: salvage fans one journal
// message out to N queues, and an unsalted content hash would collide across
// those queues, causing the broker to drop N-1 copies as duplicates on
// redeliver. Salting with the queue name gives each fanned-out copy its own
// id while a same-queue crash re-drain (queue identical) still collides onto
// one id, preserving the invariant the WAL depends on.
func DedupID(msg *amqp.Message, queue string) [16]byte {
	clone := *msg
	if msg.Header != nil {
		h := *msg.Header
		h.DeliveryCount = 0
		h.FirstAcquirer = false
		clone.Header = &h
	}
	clone.DeliveryAnnotations = nil
	var id [16]byte
	raw, err := clone.MarshalBinary()
	if err != nil {
		// A message that already marshaled successfully for rec.AMQP cannot
		// fail to marshal here after only clearing fields; fall back to hashing
		// the original bytes so we still produce a deterministic id.
		raw, _ = msg.MarshalBinary()
	}
	sum := sha256.New()
	sum.Write(raw)       // normalized marshaled message
	sum.Write([]byte{0}) // domain separator
	sum.Write([]byte(queue))
	digest := sum.Sum(nil)
	copy(id[:], digest[:16])
	return id
}

// DedupIDCore returns a stable 16-byte content id for a Core message (which
// has no amqp.Message form) from its serialized Core payload salted with the
// queue name, mirroring DedupID's domain separation so a same-queue re-drain
// collides onto one id while a fan-out to N queues gets N distinct ids.
func DedupIDCore(payload []byte, queue string) [16]byte {
	var id [16]byte
	sum := sha256.New()
	sum.Write(payload)
	sum.Write([]byte{1}) // domain separator (distinct from DedupID's 0)
	sum.Write([]byte(queue))
	digest := sum.Sum(nil)
	copy(id[:], digest[:16])
	return id
}
