package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/store"
)

// teeSink persists each record to the underlying store AND captures it in
// memory so a test can assert on the exact records that were drained.
type teeSink struct {
	w    *store.Writer
	recs []store.Record
}

func (t *teeSink) Append(r store.Record) error {
	t.recs = append(t.recs, r)
	return t.w.Append(r)
}
func (t *teeSink) Sync() error { return t.w.Sync() }

// TestDrainUUIDIsDeterministicContentHash is the C1 regression: the record
// UUID (replayed as _AMQ_DUPL_ID on redeliver) must be a deterministic hash of
// the AMQP bytes, not a random uuid. It asserts each drained record's UUID
// equals sha256(AMQP)[:16], and that re-draining the SAME content yields the
// SAME UUID across two independent DrainQueue runs.
func TestDrainUUIDIsDeterministicContentHash(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	const queue = "dedup-det"
	const body = "identical-body"

	// First drain of one message with body.
	if err := sendTestMessages(ctx, c, queue, []string{body}); err != nil {
		t.Fatalf("seed 1: %v", err)
	}
	s1 := &sliceSink{}
	if _, err := c.DrainQueue(ctx, queue, s1, 3*time.Second, 10); err != nil {
		t.Fatalf("drain 1: %v", err)
	}
	if len(s1.recs) != 1 {
		t.Fatalf("want 1 record on first drain, got %d", len(s1.recs))
	}

	// UUID must equal the queue-salted content hash, not be random.
	sum := sha256.New()
	sum.Write(s1.recs[0].AMQP)
	sum.Write([]byte{0})
	sum.Write([]byte(queue))
	want := sum.Sum(nil)
	var wantUUID [16]byte
	copy(wantUUID[:], want[:16])
	if s1.recs[0].UUID != wantUUID {
		t.Fatalf("UUID is not sha256(AMQP||0x00||queue)[:16]: got %x want %x", s1.recs[0].UUID, wantUUID)
	}

	// Second, independent drain of the SAME content -> SAME UUID.
	if err := sendTestMessages(ctx, c, queue, []string{body}); err != nil {
		t.Fatalf("seed 2: %v", err)
	}
	s2 := &sliceSink{}
	if _, err := c.DrainQueue(ctx, queue, s2, 3*time.Second, 10); err != nil {
		t.Fatalf("drain 2: %v", err)
	}
	if len(s2.recs) != 1 {
		t.Fatalf("want 1 record on second drain, got %d", len(s2.recs))
	}
	if s1.recs[0].UUID != s2.recs[0].UUID {
		t.Fatalf("re-drained identical content produced different UUIDs: %x vs %x",
			s1.recs[0].UUID, s2.recs[0].UUID)
	}
}

// TestCrossDrainSingleDelivery is the guarantee C1 restores: a message that was
// fsync'd to the store but crashed BEFORE its ack is redelivered by the broker
// and re-drained on the next run; the two drained copies must still collapse to
// exactly one delivery on redeliver.
//
// Unlike a naive "send the same body twice" test, this forces a REAL broker
// redelivery: it receives the message, records it un-acked, then settles it as
// modified/delivery-failed so Artemis redelivers it with a bumped
// Header.DeliveryCount (0 -> 1). That bump changes the marshaled AMQP bytes, so
// the two records have DIFFERENT rec.AMQP. Hashing rec.AMQP directly (the
// pre-fix behavior) would therefore give the two records DIFFERENT _AMQ_DUPL_IDs
// and the broker would NOT dedup — the exact double-delivery this WAL exists to
// prevent. store.DedupID normalizes out the volatile delivery-count, so both records
// share one id and Artemis drops the repeat.
func TestCrossDrainSingleDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)

	const queue = "orders"
	const body = "crash-and-redrain"

	path := filepath.Join(t.TempDir(), "dump.artx")
	w, err := store.NewWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	sink := &teeSink{w: w}

	// Seed one DURABLE message so its header section (where delivery-count
	// lives) survives the round-trip and a redelivery bump is observable.
	sender, err := c.sess.NewSender(ctx, queue, &amqp.SenderOptions{TargetCapabilities: []string{"queue"}})
	if err != nil {
		t.Fatalf("sender: %v", err)
	}
	seed := amqp.NewMessage([]byte(body))
	seed.Header = &amqp.MessageHeader{Durable: true}
	if err := sender.Send(ctx, seed, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	_ = sender.Close(context.Background())

	// Drain #1 (crash before ack): receive the message and persist a record
	// exactly as DrainQueue would (rec.AMQP = full bytes, rec.UUID = store.DedupID),
	// fsync it, but do NOT ack. Then settle it modified/delivery-failed so the
	// broker redelivers it with a bumped delivery-count — standing in for the
	// crash-then-restart the WAL must survive.
	recv, err := c.sess.NewReceiver(ctx, queue, &amqp.ReceiverOptions{Credit: 1})
	if err != nil {
		t.Fatalf("receiver: %v", err)
	}
	rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
	msg1, err := recv.Receive(rctx, nil)
	rcancel()
	if err != nil {
		t.Fatalf("receive 1: %v", err)
	}
	raw1, err := msg1.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal 1: %v", err)
	}
	rec1 := store.Record{Queue: queue, DrainedAt: time.Now().UnixNano(), AMQP: raw1, UUID: store.DedupID(msg1, queue)}
	if err := sink.Append(rec1); err != nil {
		t.Fatalf("append 1: %v", err)
	}
	if err := sink.Sync(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := recv.ModifyMessage(ctx, msg1, &amqp.ModifyMessageOptions{DeliveryFailed: true}); err != nil {
		t.Fatalf("modify (force redelivery): %v", err)
	}
	_ = recv.Close(context.Background())

	// Drain #2 (recovery run): the real DrainQueue re-drains the REDELIVERED
	// message (delivery-count now 1) into the same store and acks it.
	if n, err := c.DrainQueue(ctx, queue, sink, 3*time.Second, 10); err != nil || n != 1 {
		t.Fatalf("drain 2: n=%d err=%v", n, err)
	}
	if len(sink.recs) != 2 {
		t.Fatalf("want 2 drained records, got %d", len(sink.recs))
	}
	rec2 := sink.recs[1]

	// The redelivery really did mutate the wire bytes (delivery-count bump);
	// this is what makes a raw sha256(rec.AMQP) fail to dedup.
	if bytes.Equal(rec1.AMQP, rec2.AMQP) {
		t.Fatalf("expected a real redelivery to change the AMQP bytes, but they were identical")
	}
	// Despite the different bytes, the dedup ids must match.
	if rec1.UUID != rec2.UUID {
		t.Fatalf("redelivered copy got a different dedup id: %x vs %x (raw sha256(AMQP) would give %x vs %x)",
			rec1.UUID, rec2.UUID, sha256.Sum256(rec1.AMQP), sha256.Sum256(rec2.AMQP))
	}

	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	// Redeliver the two-record store; dedup must collapse to a single message.
	if _, _, err := c.Redeliver(ctx, path, RedeliverOpts{}, nil); err != nil {
		t.Fatalf("redeliver: %v", err)
	}

	stats, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var count int64 = -1
	for _, s := range stats {
		if s.Name == queue {
			count = s.MessageCount
		}
	}
	if count != 1 {
		t.Fatalf("cross-drain dedup failed: %s has %d messages, want 1", queue, count)
	}
}
