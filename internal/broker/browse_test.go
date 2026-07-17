package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
)

func TestAmqpMessageID(t *testing.T) {
	ts := time.UnixMilli(0)
	tests := []struct {
		name string
		msg  *amqp.Message
		want string
	}{
		{
			name: "nil properties",
			msg:  &amqp.Message{},
			want: "",
		},
		{
			name: "nil message-id",
			msg:  &amqp.Message{Properties: &amqp.MessageProperties{MessageID: nil}},
			want: "",
		},
		{
			name: "string message-id",
			msg:  &amqp.Message{Properties: &amqp.MessageProperties{MessageID: "orders-3"}},
			want: "orders-3",
		},
		{
			name: "non-string message-id is stringified",
			msg:  &amqp.Message{Properties: &amqp.MessageProperties{MessageID: uint64(42)}},
			want: "42",
		},
		{
			// A property block with other fields set but no message-id must
			// still report "" rather than reaching for another field.
			name: "properties present without message-id",
			msg:  &amqp.Message{Properties: &amqp.MessageProperties{CreationTime: &ts}},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := amqpMessageID(tc.msg); got != tc.want {
				t.Fatalf("amqpMessageID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseListMessagesCount(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    int
		wantErr bool
	}{
		{
			// The real broker shape: an outer JSON array holding one string,
			// which itself is a JSON array of message metadata objects.
			name: "double-encoded three messages",
			in:   `["[{\"messageID\":602},{\"messageID\":605},{\"messageID\":609}]"]`,
			want: 3,
		},
		{
			name: "empty inner array counts zero",
			in:   `["[]"]`,
			want: 0,
		},
		{
			name: "empty outer array counts zero",
			in:   `[]`,
			want: 0,
		},
		{
			name:    "malformed outer array errors",
			in:      `not json`,
			wantErr: true,
		},
		{
			name:    "malformed inner metadata errors",
			in:      `["not json"]`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseListMessagesCount(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseListMessagesCount(%q) = %d, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseListMessagesCount(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("parseListMessagesCount(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}

func TestBrowsedFromMessage(t *testing.T) {
	t.Run("short body keeps full preview and reports size", func(t *testing.T) {
		m := amqp.NewMessage([]byte("hello"))
		m.Properties = &amqp.MessageProperties{MessageID: "id-1"}
		got := browsedFromMessage(m)
		if got.ID != "id-1" {
			t.Errorf("ID = %q, want id-1", got.ID)
		}
		if got.Size != 5 {
			t.Errorf("Size = %d, want 5", got.Size)
		}
		if got.Preview != "hello" {
			t.Errorf("Preview = %q, want hello", got.Preview)
		}
	})

	t.Run("body over previewMaxLen is truncated but Size is the full length", func(t *testing.T) {
		body := strings.Repeat("a", previewMaxLen+50)
		m := amqp.NewMessage([]byte(body))
		got := browsedFromMessage(m)
		if got.Size != len(body) {
			t.Errorf("Size = %d, want %d", got.Size, len(body))
		}
		if len(got.Preview) != previewMaxLen {
			t.Errorf("Preview len = %d, want %d", len(got.Preview), previewMaxLen)
		}
		if got.Preview != body[:previewMaxLen] {
			t.Errorf("Preview = %q, want first %d bytes", got.Preview, previewMaxLen)
		}
	})

	t.Run("no creation time yields zero timestamp", func(t *testing.T) {
		m := amqp.NewMessage([]byte("x"))
		if got := browsedFromMessage(m); got.Timestamp != 0 {
			t.Errorf("Timestamp = %d, want 0", got.Timestamp)
		}
	})

	t.Run("creation time is reported as unix millis", func(t *testing.T) {
		when := time.UnixMilli(1_700_000_000_123)
		m := amqp.NewMessage([]byte("x"))
		m.Properties = &amqp.MessageProperties{CreationTime: &when}
		if got := browsedFromMessage(m); got.Timestamp != 1_700_000_000_123 {
			t.Errorf("Timestamp = %d, want 1700000000123", got.Timestamp)
		}
	})
}
