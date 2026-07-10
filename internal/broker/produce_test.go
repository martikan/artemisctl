package broker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestGenerateMessagesBodySizeAndProps(t *testing.T) {
	props := map[string]interface{}{"region": "eu"}
	msgs := GenerateMessages(3, 64, props)
	if len(msgs) != 3 {
		t.Fatalf("want 3 messages, got %d", len(msgs))
	}
	for i, m := range msgs {
		if got := len(m.GetData()); got != 64 {
			t.Fatalf("msg %d body size = %d, want 64", i, got)
		}
		if m.Header == nil || !m.Header.Durable {
			t.Fatalf("msg %d not durable", i)
		}
		if m.ApplicationProperties["region"] != "eu" {
			t.Fatalf("msg %d missing property, got %v", i, m.ApplicationProperties)
		}
	}
	// Distinct property maps: mutating one must not touch another.
	msgs[0].ApplicationProperties["region"] = "us"
	if msgs[1].ApplicationProperties["region"] != "eu" {
		t.Fatalf("property maps are shared across generated messages")
	}
}

func TestGenerateMessagesTinySize(t *testing.T) {
	// size smaller than the "msg-<n>-" prefix must still yield exactly size bytes.
	msgs := GenerateMessages(1, 2, nil)
	if got := len(msgs[0].GetData()); got != 2 {
		t.Fatalf("body size = %d, want 2", got)
	}
}

func TestGenerateMessagesClampsNegative(t *testing.T) {
	// Negative count and size must clamp to 0 rather than panic.
	if msgs := GenerateMessages(-5, -1, nil); len(msgs) != 0 {
		t.Fatalf("negative count: got %d messages, want 0", len(msgs))
	}
	msgs := GenerateMessages(1, -1, nil)
	if len(msgs) != 1 || len(msgs[0].GetData()) != 0 {
		t.Fatalf("negative size: got %d msgs, body %d, want 1 msg / 0 bytes", len(msgs), len(msgs[0].GetData()))
	}
}

func TestParseArtemisMessagesInvalidJSON(t *testing.T) {
	if _, err := ParseArtemisMessages([]byte("{not an array"), nil); err == nil {
		t.Fatal("want parse error for malformed JSON")
	}
}

// TestParseArtemisMessagesRemainingBuckets covers the Byte/Short/Float/Double
// typed-property buckets the primary test doesn't touch.
func TestParseArtemisMessagesRemainingBuckets(t *testing.T) {
	data := []byte(`[{
		"text": "x",
		"ByteProperties":   { "b": 7 },
		"ShortProperties":  { "s": 300 },
		"FloatProperties":  { "f": 1.5 },
		"DoubleProperties": { "d": 2.5 }
	}]`)
	msgs, err := ParseArtemisMessages(data, nil)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ap := msgs[0].ApplicationProperties
	if v, ok := ap["b"].(int8); !ok || v != 7 {
		t.Fatalf("ByteProperties b = %#v, want int8(7)", ap["b"])
	}
	if v, ok := ap["s"].(int16); !ok || v != 300 {
		t.Fatalf("ShortProperties s = %#v, want int16(300)", ap["s"])
	}
	if v, ok := ap["f"].(float32); !ok || v != 1.5 {
		t.Fatalf("FloatProperties f = %#v, want float32(1.5)", ap["f"])
	}
	if v, ok := ap["d"].(float64); !ok || v != 2.5 {
		t.Fatalf("DoubleProperties d = %#v, want float64(2.5)", ap["d"])
	}
}

func TestParseArtemisMessagesTypedProps(t *testing.T) {
	data := []byte(`[
		{
			"address": "orders",
			"durable": true,
			"priority": 7,
			"expiration": 1700000000000,
			"type": 3,
			"text": "hello",
			"StringProperties": { "region": "eu" },
			"IntProperties": { "n": 42 },
			"BooleanProperties": { "flag": true },
			"LongProperties": { "big": 9000000000 }
		},
		{ "text": "second" }
	]`)
	msgs, err := ParseArtemisMessages(data, map[string]interface{}{"injected": "x"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}

	m := msgs[0]
	if string(m.GetData()) != "hello" {
		t.Fatalf("body = %q, want hello", string(m.GetData()))
	}
	if m.Header == nil || !m.Header.Durable || m.Header.Priority != 7 {
		t.Fatalf("header = %+v, want durable priority 7", m.Header)
	}
	if m.Properties == nil || m.Properties.AbsoluteExpiryTime == nil {
		t.Fatalf("expiration not mapped to absolute expiry time")
	}
	if !m.Properties.AbsoluteExpiryTime.Equal(time.UnixMilli(1700000000000)) {
		t.Fatalf("expiry = %v, want %v", m.Properties.AbsoluteExpiryTime, time.UnixMilli(1700000000000))
	}
	// Typed property buckets must land as their Go types, not float64.
	if v, ok := m.ApplicationProperties["n"].(int32); !ok || v != 42 {
		t.Fatalf("IntProperties n = %#v, want int32(42)", m.ApplicationProperties["n"])
	}
	if v, ok := m.ApplicationProperties["big"].(int64); !ok || v != 9000000000 {
		t.Fatalf("LongProperties big = %#v, want int64(9000000000)", m.ApplicationProperties["big"])
	}
	if m.ApplicationProperties["flag"] != true {
		t.Fatalf("BooleanProperties flag = %#v, want true", m.ApplicationProperties["flag"])
	}
	if m.ApplicationProperties["region"] != "eu" {
		t.Fatalf("StringProperties region = %#v, want eu", m.ApplicationProperties["region"])
	}
	if m.ApplicationProperties["injected"] != "x" {
		t.Fatalf("extra property not merged: %v", m.ApplicationProperties)
	}

	// Defaults: absent durable -> true, absent priority -> 4.
	m2 := msgs[1]
	if m2.Header == nil || !m2.Header.Durable || m2.Header.Priority != defaultPriority {
		t.Fatalf("second msg header = %+v, want durable priority %d", m2.Header, defaultPriority)
	}
}

func TestParseArtemisMessagesExtraOverridesFile(t *testing.T) {
	data := []byte(`[{ "text": "x", "StringProperties": { "region": "eu" } }]`)
	msgs, err := ParseArtemisMessages(data, map[string]interface{}{"region": "override"})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if msgs[0].ApplicationProperties["region"] != "override" {
		t.Fatalf("extra property did not override file property: %v", msgs[0].ApplicationProperties)
	}
}

func TestProduceThenBrowseIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	msgs := GenerateMessages(5, 128, map[string]interface{}{"batch": "test"})
	sent, err := c.Produce(ctx, "produce-test", msgs, 0, 1, nil)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if sent != 5 {
		t.Fatalf("sent = %d, want 5", sent)
	}

	// Non-destructive browse must see all 5 with the expected body size.
	browsed := browseWithRetry(ctx, t, c, "produce-test", 20, 0)
	if len(browsed) != 5 {
		t.Fatalf("browsed %d messages, want 5", len(browsed))
	}
	for _, b := range browsed {
		if b.Size != 128 {
			t.Fatalf("browsed message size = %d, want 128", b.Size)
		}
	}
}

// TestProduceRateLimited drives the throttled paths in both produceSequential
// (interval>0) and produceParallel (shared ticker), which the other produce
// tests skip by sending at rate 0.
func TestProduceRateLimited(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	// A progress counter exercises the onProgress branch on both the sequential
	// and parallel paths (the other produce tests pass nil).
	var seqProg atomic.Int64
	// rate 500/s keeps the test near-instant while still exercising the
	// throttle branch (interval = 2ms between sends).
	seqSent, err := c.Produce(ctx, "rate-seq", GenerateMessages(6, 32, nil), 500, 1,
		func(int) { seqProg.Add(1) })
	if err != nil {
		t.Fatalf("sequential rate produce: %v", err)
	}
	if seqSent != 6 || seqProg.Load() != 6 {
		t.Fatalf("sequential sent = %d, progress = %d, want 6/6", seqSent, seqProg.Load())
	}

	var parProg atomic.Int64
	parSent, err := c.Produce(ctx, "rate-par", GenerateMessages(8, 32, nil), 500, 3,
		func(int) { parProg.Add(1) })
	if err != nil {
		t.Fatalf("parallel rate produce: %v", err)
	}
	if parSent != 8 || parProg.Load() != 8 {
		t.Fatalf("parallel sent = %d, progress = %d, want 8/8", parSent, parProg.Load())
	}

	// workers > message count must clamp to len(msgs) rather than spin up idle
	// senders (or divide by zero).
	clampSent, err := c.Produce(ctx, "rate-clamp", GenerateMessages(2, 32, nil), 0, 8, nil)
	if err != nil {
		t.Fatalf("clamped parallel produce: %v", err)
	}
	if clampSent != 2 {
		t.Fatalf("clamped sent = %d, want 2", clampSent)
	}
}

// TestProduceCancellation covers the mid-flight cancellation paths of both
// produce strategies: a rate limit keeps the send loop running long enough that
// canceling the context lands inside it, so the throttle-cancel and ctx.Err
// returns fire and the call reports the partial count with context.Canceled.
func TestProduceCancellation(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	for _, workers := range []int{1, 4} {
		// rate 100/s (10ms/msg) over 200 msgs => ~2s of work; cancel after 100ms.
		cctx, ccancel := context.WithCancel(ctx)
		time.AfterFunc(100*time.Millisecond, ccancel)
		sent, err := c.Produce(cctx, "cancel-test", GenerateMessages(200, 32, nil), 100, workers, nil)
		ccancel()
		if err == nil {
			t.Fatalf("workers=%d: want cancellation error, got nil (sent %d)", workers, sent)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("workers=%d: want context.Canceled, got %v", workers, err)
		}
		if sent >= 200 {
			t.Fatalf("workers=%d: expected partial send before cancel, got %d", workers, sent)
		}
	}
}

func TestProduceParallelIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	props := startArtemis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	c, err := Connect(ctx, props)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer c.Close(ctx)

	msgs := GenerateMessages(50, 128, nil)
	sent, err := c.Produce(ctx, "produce-parallel-test", msgs, 0, 4, nil)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if sent != 50 {
		t.Fatalf("sent = %d, want 50", sent)
	}

	// Exactly 50 on the queue — parallel workers must not lose or duplicate.
	browsed := browseWithRetry(ctx, t, c, "produce-parallel-test", 100, 0)
	if len(browsed) != 50 {
		t.Fatalf("browsed %d messages, want 50", len(browsed))
	}
	for _, b := range browsed {
		if b.Size != 128 {
			t.Fatalf("browsed message size = %d, want 128", b.Size)
		}
	}
}
