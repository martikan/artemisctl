package journal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/go-amqp"
)

func TestQueueName(t *testing.T) {
	names := map[int64]string{7: "orders"}
	unknown := map[int64]int{}

	if got := queueName(names, 7, unknown); got != "orders" {
		t.Errorf("queueName(known) = %q, want orders", got)
	}
	if len(unknown) != 0 {
		t.Errorf("known queue must not touch unknown map, got %v", unknown)
	}

	if got := queueName(names, 99, unknown); got != "unknown-queue-99" {
		t.Errorf("queueName(missing) = %q, want unknown-queue-99", got)
	}
	if got := queueName(names, 99, unknown); got != "unknown-queue-99" {
		t.Errorf("second miss = %q, want unknown-queue-99", got)
	}
	if unknown[99] != 2 {
		t.Errorf("unknown count for 99 = %d, want 2", unknown[99])
	}
}

func TestMessageDiagSkips(t *testing.T) {
	if got := messageDiagSkips(MessageDiag{}); got != nil {
		t.Errorf("empty diag = %v, want nil", got)
	}

	diag := MessageDiag{
		CoreSkipped:      map[int64][]int64{5: {1, 2}, 6: {3}},
		UnknownPersister: 2,
		UndecodableBody:  4,
	}
	got := messageDiagSkips(diag)
	if len(got) != 3 {
		t.Fatalf("skips = %d entries, want 3: %v", len(got), got)
	}
	if !contains(got, "2 messages, 3 surviving queue refs") {
		t.Errorf("core-skip line missing count: %v", got)
	}
	if !contains(got, "unrecognized persister id in message journal: 2") {
		t.Errorf("persister line missing: %v", got)
	}
	if !contains(got, "undecodable message bodies in message journal: 4") {
		t.Errorf("undecodable line missing: %v", got)
	}
}

func contains(ss []string, sub string) bool {
	for _, s := range ss {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

func TestPagingDiagSkips(t *testing.T) {
	if got := pagingDiagSkips(PagingDiag{}); got != nil {
		t.Errorf("empty diag = %v, want nil", got)
	}

	diag := PagingDiag{
		CoreSkipped:        1,
		LargeSkipped:       2,
		UnknownPersister:   3,
		UndecodableEntries: 4,
	}
	got := pagingDiagSkips(diag)
	if len(got) != 4 {
		t.Fatalf("skips = %d entries, want 4: %v", len(got), got)
	}
}

func TestJoinInt64s(t *testing.T) {
	if got := joinInt64s([]int64{3, 1, 2}); got != "1, 2, 3" {
		t.Errorf("joinInt64s = %q, want \"1, 2, 3\"", got)
	}
	if got := joinInt64s(nil); got != "" {
		t.Errorf("joinInt64s(nil) = %q, want empty", got)
	}
}

func TestRequireDir(t *testing.T) {
	dir := t.TempDir()
	if err := requireDir(dir); err != nil {
		t.Errorf("requireDir(dir) = %v, want nil", err)
	}

	file := filepath.Join(dir, "f")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := requireDir(file); err == nil {
		t.Error("requireDir(file) = nil, want not-a-directory error")
	}

	if err := requireDir(filepath.Join(dir, "nope")); err == nil {
		t.Error("requireDir(missing) = nil, want stat error")
	}
}

func TestPrepareMessage(t *testing.T) {
	raw, err := (&amqp.Message{Value: "hi"}).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	// scheduledMs == 0: bytes returned unchanged, no delivery-time annotation.
	out, msg, err := prepareMessage(raw, 0)
	if err != nil {
		t.Fatalf("prepareMessage(0) = %v", err)
	}
	if string(out) != string(raw) {
		t.Error("scheduledMs=0 must return the original bytes unchanged")
	}
	if _, ok := msg.Annotations["x-opt-delivery-time"]; ok {
		t.Error("scheduledMs=0 must not stamp x-opt-delivery-time")
	}

	// scheduledMs != 0: annotation stamped and bytes remarshaled.
	out, msg, err = prepareMessage(raw, 123456)
	if err != nil {
		t.Fatalf("prepareMessage(123456) = %v", err)
	}
	if got := msg.Annotations["x-opt-delivery-time"]; got != int64(123456) {
		t.Errorf("delivery-time = %v, want 123456", got)
	}
	if string(out) == string(raw) {
		t.Error("scheduled message must be remarshaled, not the original bytes")
	}

	if _, _, err := prepareMessage([]byte{0xFF, 0x00}, 0); err == nil {
		t.Error("prepareMessage(bad bytes) = nil, want unmarshal error")
	}
}
