package store

import (
	"testing"

	"github.com/Azure/go-amqp"
)

func TestDedupID(t *testing.T) {
	newMsg := func(body string) *amqp.Message {
		return amqp.NewMessage([]byte(body))
	}

	t.Run("same message and queue produce equal ids", func(t *testing.T) {
		m1 := newMsg("hello")
		m2 := newMsg("hello")
		if DedupID(m1, "orders") != DedupID(m2, "orders") {
			t.Fatalf("expected equal ids for identical message+queue")
		}
	})

	t.Run("redelivery bump does not change the id (crash re-drain invariant)", func(t *testing.T) {
		m1 := newMsg("hello")
		m1.Header = &amqp.MessageHeader{Durable: true}

		m2 := newMsg("hello")
		m2.Header = &amqp.MessageHeader{Durable: true, DeliveryCount: 1, FirstAcquirer: true}
		m2.DeliveryAnnotations = amqp.Annotations{"x-opt-delivery-count": 1}

		if DedupID(m1, "orders") != DedupID(m2, "orders") {
			t.Fatalf("expected equal ids across a redelivery bump (DeliveryCount/FirstAcquirer/DeliveryAnnotations)")
		}
	})

	t.Run("same message, different queue produce different ids", func(t *testing.T) {
		m1 := newMsg("hello")
		m2 := newMsg("hello")
		if DedupID(m1, "orders") == DedupID(m2, "shipping") {
			t.Fatalf("expected different ids for the same message fanned out to different queues")
		}
	})

	t.Run("different body, same queue produce different ids", func(t *testing.T) {
		m1 := newMsg("hello")
		m2 := newMsg("goodbye")
		if DedupID(m1, "orders") == DedupID(m2, "orders") {
			t.Fatalf("expected different ids for different message bodies")
		}
	})
}
