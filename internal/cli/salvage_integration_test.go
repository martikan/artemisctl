// internal/cli/salvage_integration_test.go
package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/broker"
	"github.com/martikan/artemisctl/internal/store"
)

// manifestEntry / manifestFile mirror internal/journal/testdata/manifest.json's
// schema. internal/journal/message_test.go carries the canonical copy of this
// same shape (manifestEntry/manifestFile); it is unexported to that package
// (package journal, not journal_test), so this is a small local duplicate per
// the task brief.
type manifestEntry struct {
	BodySha256    string         `json:"bodySha256"`
	BodyLen       int            `json:"bodyLen"`
	Props         map[string]any `json:"props,omitempty"`
	ScheduledAtMs int64          `json:"scheduledAtMs,omitempty"`
}

type manifestFile struct {
	Queues map[string][]manifestEntry `json:"queues"`
}

func loadSalvageManifest(t *testing.T) manifestFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "journal", "testdata", "manifest.json"))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var m manifestFile
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	return m
}

// captureSink is a broker.RecordSink that keeps every drained record in
// memory in delivery order, for DrainQueue-based verification.
type captureSink struct {
	records []store.Record
}

func (s *captureSink) Append(r store.Record) error {
	s.records = append(s.records, r)
	return nil
}
func (s *captureSink) Sync() error { return nil }

// destroyQueue removes a queue entirely -- both any remaining messages and
// the queue definition itself -- via a raw broker.destroyQueue management
// call, replicating internal/broker's unexported callManagement locally
// (package cli cannot reach it). This exists purely for final cleanup: this
// test's redirect queue always ends up holding one message DrainQueue can
// never remove (the fixture's scheduled record, see the CAUTION below, is
// not deliverable until 2100-01-01), so without an explicit destroy the
// queue -- and that stuck message -- would live forever on the shared,
// never-terminated broker container (see brokertest.Shared's docstring:
// "state accumulates"). That is not hypothetical: earlier iterations of this
// test left exactly such orphaned salvage.e2e.* queues behind, and their
// accumulation broke internal/broker's TestDrainAllEnumeratesAndDrainsEveryQueue
// by exceeding its bounded drain-everything deadline. Confirmed working
// against the shared broker before wiring it in here.
func destroyQueue(ctx context.Context, sess *amqp.Session, name string) error {
	recv, err := sess.NewReceiver(ctx, "", &amqp.ReceiverOptions{DynamicAddress: true})
	if err != nil {
		return fmt.Errorf("create reply receiver: %w", err)
	}
	defer recv.Close(context.Background())
	replyTo := recv.Address()

	sender, err := sess.NewSender(ctx, "activemq.management", nil)
	if err != nil {
		return fmt.Errorf("create management sender: %w", err)
	}
	defer sender.Close(context.Background())

	msg := &amqp.Message{
		Value:      fmt.Sprintf(`[%q, true, true]`, name), // [queueName, removeConsumers, autoDeleteAddress]
		Properties: &amqp.MessageProperties{ReplyTo: &replyTo},
		ApplicationProperties: map[string]interface{}{
			"_AMQ_ResourceName":  "broker",
			"_AMQ_OperationName": "destroyQueue",
		},
	}
	if err := sender.Send(ctx, msg, nil); err != nil {
		return fmt.Errorf("send destroyQueue request: %w", err)
	}
	reply, err := recv.Receive(ctx, nil)
	if err != nil {
		return fmt.Errorf("receive destroyQueue reply: %w", err)
	}
	_ = recv.AcceptMessage(ctx, reply)
	if ok, present := reply.ApplicationProperties["_AMQ_OperationSucceeded"].(bool); present && !ok {
		return fmt.Errorf("broker rejected destroyQueue(%s): %v", name, reply.Value)
	}
	return nil
}

// TestSalvageE2E proves the full offline-recovery workflow against a real
// broker: salvage a broker data directory into a store file, redeliver it
// into a single unique redirect queue, and verify every recoverable record
// made it across with its body (and, for the props sample, its application
// properties) intact.
func TestSalvageE2E(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}

	dir := salvageFixtureDir(t)
	storePath := filepath.Join(t.TempDir(), "rescue.artx")

	// --force is deliberately NOT used here: the fixture's
	// data/journal/server.lock exists but is unheld (nothing holds the
	// flock), so checkLiveBroker's guard already passes on its own -- see
	// TestSalvageFixtureSuccess, which salvages this same fixture without
	// --force. Exercising the no-force path matches the real recovery
	// workflow: --force is only needed when the live-broker guard actually
	// fires, which it does not for a harvested/copied data directory.
	stdout, err := runSalvage(t, "--data", dir, "--out", storePath)
	if err != nil {
		t.Fatalf("salvage: %v\noutput:\n%s", err, stdout)
	}
	const wantTotal = 512 // 5 plain + 5 props + 1 scheduled + 1 large + 500 paged
	wantHeadline := fmt.Sprintf("salvaged %d messages to %s", wantTotal, storePath)
	if !strings.Contains(stdout, wantHeadline) {
		t.Fatalf("salvage stdout missing %q; got:\n%s", wantHeadline, stdout)
	}

	man := loadSalvageManifest(t)
	// CAUTION (dedup): Redeliver dedups on the broker via _AMQ_DUPL_ID, a
	// content hash salted with the record's OWN originating queue at salvage
	// time (not the redirect queue -- see store.DedupID and internal/journal's
	// salvage path), so collapsing every record onto one redirect queue only
	// risks losing a message if two records shared a hash to begin with.
	// Confirm the premise holds for this fixture: every body sha256 in the
	// manifest is unique, so there is no real duplicate content for the
	// broker's duplicate-detection to legitimately collapse.
	bodyOrigin := map[string]string{} // body sha256 -> manifest queue name
	for q, entries := range man.Queues {
		for _, e := range entries {
			if prev, ok := bodyOrigin[e.BodySha256]; ok {
				t.Fatalf("manifest has duplicate body sha256 %s on both %s and %s; dedup-safety assumption violated", e.BodySha256, prev, q)
			}
			bodyOrigin[e.BodySha256] = q
		}
	}

	props := startArtemisForCLI(t)
	redirectQueue := fmt.Sprintf("salvage.e2e.%d", time.Now().UnixNano())

	runConn := func(t *testing.T, args ...string) (string, error) {
		t.Helper()
		conn := []string{"--url", props.URL, "-u", props.Username, "-p", props.Password}
		return runCmdStdin(t, "", append(args, conn...)...)
	}

	redelivOut, err := runConn(t, "redeliver", "--in", storePath, "--queue", redirectQueue)
	if err != nil {
		t.Fatalf("redeliver: %v\noutput:\n%s", err, redelivOut)
	}
	wantRedelivHeadline := fmt.Sprintf("redelivered %d messages from %s", wantTotal, storePath)
	if !strings.Contains(redelivOut, wantRedelivHeadline) {
		t.Fatalf("redeliver stdout missing %q; got:\n%s", wantRedelivHeadline, redelivOut)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	c, err := broker.Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(context.Background())

	// Guaranteed cleanup: destroy the redirect queue at test end, whatever
	// the outcome. Registered right after Connect so it runs even if any
	// assertion below Fatals, and (defers being LIFO) BEFORE the deferred
	// c.Close, so the client's session is still open when it fires.
	// Best-effort with t.Logf, not t.Errorf: a cleanup hiccup must not turn
	// a passing verification of the recovery pipeline into a failure -- but
	// it is loud in the log so an operator knows the shared broker needs a
	// manual sweep of this one queue.
	defer func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		if err := destroyQueue(cctx, c.Session(), redirectQueue); err != nil {
			t.Logf("cleanup: destroyQueue(%s) failed (shared broker may need a manual sweep): %v", redirectQueue, err)
		}
	}()

	// The broker-reported queue depth is a true aggregate stat (Artemis's
	// listQueues management op), unaffected by browse-page-size limits or
	// consumer credit -- it is the deterministic proof that every one of the
	// 512 salvaged records actually reached the broker under the redirect
	// queue, matching the salvage summary total exactly.
	qstats, err := c.ListQueues(ctx)
	if err != nil {
		t.Fatalf("ListQueues: %v", err)
	}
	var brokerCount int64 = -1
	for _, q := range qstats {
		if q.Name == redirectQueue {
			brokerCount = q.MessageCount
		}
	}
	if brokerCount != wantTotal {
		t.Fatalf("broker-reported message count for %s = %d, want %d", redirectQueue, brokerCount, wantTotal)
	}

	// CAUTION (scheduled message) + why verification uses DrainQueue rather
	// than BrowseQueue:
	//
	// The fixture's salvage.scheduled record carries an x-opt-delivery-time
	// annotation of 4102444800000ms, i.e. 2100-01-01T00:00:00Z (see
	// manifest.json) -- far in the future relative to any real test run.
	// Redeliver preserves the record byte-for-byte (it round-trips the raw
	// AMQP message and only adds the _AMQ_DUPL_ID property), so Artemis
	// honors the annotation and holds that one message as scheduled: it is
	// enqueued (counted in brokerCount above) but not delivery-eligible to
	// any consumer, ever, until then. So exactly wantTotal-1 = 511 records
	// are actually retrievable right now.
	//
	// A first attempt used BrowseQueue (a non-destructive receive-then-
	// release peek, see internal/broker/browse.go) to both count and sample
	// those 511. It does not work cleanly at this queue depth against the
	// shared broker's settings: brokertest.PermissiveWildcardSettings sets
	// managementBrowsePageSize=200, so BrowseQueue's internal queueCount()
	// (via listMessagesAsJSON) caps out at 200 regardless of the requested
	// limit -- and that same settings blob sets redeliveryDelay=0 (needed
	// elsewhere so back-to-back browses in other tests don't hit the
	// "recently-released invisibility window" also documented in
	// browse.go). With no redelivery delay, a message released mid-peek can
	// be redispatched to the SAME manual-credit receiver before every
	// distinct message has been seen once, producing real, observed
	// duplicate deliveries within a single peek (confirmed empirically:
	// requesting 200 yielded only ~189-190 distinct message-ids across
	// repeated runs). That makes an exact count/bucket assertion via
	// BrowseQueue flaky at this scale -- not a bug in this test's logic, a
	// real interaction between two address-settings this shared broker needs
	// for other tests.
	//
	// DrainQueue sidesteps both problems: it has no page-size cap (it is a
	// plain AMQP receive loop, not listMessagesAsJSON-backed) and it ACCEPTs
	// each message instead of releasing it, so a message is durably removed
	// on first delivery -- no redelivery, no duplicates, exactly-once by
	// construction. It is also the same call the per-test reset contract
	// uses to leave the shared broker clean, so this one exhaustive drain
	// serves as BOTH the deterministic, full (not sampled) content
	// verification AND the message-level cleanup; the still-scheduled
	// record it cannot touch is removed by the deferred destroyQueue.
	const wantDrainable = wantTotal - 1

	sink := &captureSink{}
	dctx, dcancel := context.WithTimeout(context.Background(), 60*time.Second)
	n, err := c.DrainQueue(dctx, redirectQueue, sink, 1*time.Second, 200)
	dcancel()
	// The scheduled record stays on the queue, so this drain is by definition
	// incomplete and DrainQueue must say so rather than return nil: a drain
	// that leaves messages behind while reporting success is what let an export
	// silently ship a partial store. The rest of the queue must still have been
	// drained in full.
	var pde *broker.PartialDrainError
	if !errors.As(err, &pde) {
		t.Fatalf("DrainQueue err = %v, want *PartialDrainError for the undrainable scheduled record", err)
	}
	if pde.Stat.MessageCount != 1 || pde.Stat.ScheduledCount != 1 {
		t.Fatalf("DrainQueue reported %+v, want exactly 1 remaining, scheduled", pde.Stat)
	}
	if n != wantDrainable {
		t.Fatalf("DrainQueue drained %d messages, want %d (%d total minus the not-yet-due scheduled record, which cannot be drained until 2100-01-01)",
			n, wantDrainable, wantTotal)
	}
	if len(sink.records) != wantDrainable {
		t.Fatalf("captured %d records, want %d", len(sink.records), wantDrainable)
	}

	// After the drain exactly one message must remain: the scheduled record,
	// which no consumer can remove until its far-future delivery time. (The
	// deferred destroyQueue above takes the whole queue -- stuck record
	// included -- with it at test end.)
	//
	// Artemis's listQueues message-count is settled asynchronously after a
	// batch of acks (empirically observed: immediately after DrainQueue
	// returns it can still reflect a partially-decremented count, converging
	// to the true value within ~1s), so poll briefly instead of asserting on
	// the first read.
	var afterCount int64 = -1
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		afterQstats, err := c.ListQueues(ctx)
		if err != nil {
			t.Fatalf("ListQueues after drain: %v", err)
		}
		for _, q := range afterQstats {
			if q.Name == redirectQueue {
				afterCount = q.MessageCount
			}
		}
		if afterCount == 1 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if afterCount != 1 {
		t.Errorf("post-drain message count for %s = %d, want 1 (the stuck scheduled record)", redirectQueue, afterCount)
	}

	// Verify every drained record's body sha256 against the manifest (full
	// coverage of all 511 retrievable records, not just a sample), and
	// bucket by manifest origin queue to confirm the expected composition:
	// 5 plain + 5 props + 500 paged + 1 large = 511.
	var plainCount, propsCount, pagedCount, largeCount int
	var propsSample *amqp.Message
	for i, rec := range sink.records {
		var am amqp.Message
		if err := am.UnmarshalBinary(rec.AMQP); err != nil {
			t.Fatalf("record %d: unmarshal AMQP: %v", i, err)
		}
		body := am.GetData()
		sum := sha256.Sum256(body)
		hash := hex.EncodeToString(sum[:])
		origin, ok := bodyOrigin[hash]
		if !ok {
			t.Errorf("record %d body sha256 %s (len %d) not found in manifest", i, hash, len(body))
			continue
		}
		switch origin {
		case "salvage.plain":
			plainCount++
		case "salvage.props":
			propsCount++
			if propsSample == nil {
				propsSample = &am
			}
		case "salvage.paged":
			pagedCount++
		case "salvage.large":
			largeCount++
		default:
			t.Errorf("record %d body sha256 %s belongs to unexpected manifest queue %s", i, hash, origin)
		}
	}
	if plainCount != 5 {
		t.Errorf("plain-origin records = %d, want 5", plainCount)
	}
	if propsCount != 5 {
		t.Errorf("props-origin records = %d, want 5", propsCount)
	}
	if pagedCount != 500 {
		t.Errorf("paged-origin records = %d, want 500", pagedCount)
	}
	if largeCount != 1 {
		t.Errorf("large-origin records = %d, want 1", largeCount)
	}

	// Property round-trip: confirm a props-origin record carries its
	// original application properties (attempt=1, region=eu per
	// manifest.json's salvage.props entries) byte-for-byte through
	// salvage -> store -> redeliver -> broker.
	if propsSample == nil {
		t.Fatal("no props-origin record found to verify properties on")
	}
	if got := fmt.Sprint(propsSample.ApplicationProperties["region"]); got != "eu" {
		t.Errorf("props record region = %q, want %q", got, "eu")
	}
	if got := fmt.Sprint(propsSample.ApplicationProperties["attempt"]); got != "1" {
		t.Errorf("props record attempt = %q, want %q", got, "1")
	}
}
