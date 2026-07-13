package broker

import (
	"fmt"
	"strings"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/martikan/artemisctl/internal/journal"
)

// x-opt-jms-msg-type annotation values (Artemis AMQPMessageSupport JMS_*_TYPE),
// so a JMS/Core consumer sees the right message class after redelivery.
const (
	jmsMessageType       = 0
	jmsObjectMessageType = 1
	jmsMapMessageType    = 2
	jmsBytesMessageType  = 3
	jmsStreamMessageType = 4
	jmsTextMessageType   = 5
)

// coreToAMQP converts a serialized Core payload (store.Record.CorePayload,
// Kind=Core) into an amqp.Message for redelivery. It mirrors Artemis's own
// CoreAmqpConverter.fromCore / CoreMessageWrapper.createAMQPSection mapping:
// the Core body becomes the AMQP body section that matches its type, and the
// core headers/properties map onto AMQP Header/Properties/ApplicationProperties.
// go-amqp performs the actual wire marshalling; the broker re-converts the
// message back to Core for any Core/JMS consumer.
//
// It returns an error only when the payload itself cannot be parsed, so the
// caller (Redeliver) can skip-not-lose a single bad record without aborting.
func coreToAMQP(payload []byte) (*amqp.Message, error) {
	p, err := journal.DecodeCorePayload(payload)
	if err != nil {
		return nil, fmt.Errorf("decode core payload: %w", err)
	}

	msg := &amqp.Message{
		Header:     &amqp.MessageHeader{Durable: p.Durable, Priority: p.Priority},
		Properties: &amqp.MessageProperties{},
	}

	setCoreBody(msg, p)

	if p.Address != "" {
		to := p.Address
		msg.Properties.To = &to
	}
	if p.Timestamp > 0 {
		t := time.UnixMilli(p.Timestamp)
		msg.Properties.CreationTime = &t
	}
	if p.Expiration > 0 {
		t := time.UnixMilli(p.Expiration)
		msg.Properties.AbsoluteExpiryTime = &t
	}
	if len(p.UserID) > 0 {
		msg.Properties.UserID = append([]byte(nil), p.UserID...)
	}

	msg.Annotations = amqp.Annotations{"x-opt-jms-msg-type": int8(jmsTypeFor(p.Type))}

	// Application properties: carry the user-visible core properties, dropping
	// Artemis-internal keys (_AMQ*/__AMQ*) and our synthetic scheduling key.
	// A few internal keys carry standard AMQP message-properties (group id and
	// group sequence) rather than being pure Artemis bookkeeping: promote those
	// onto Properties before the strip so message grouping survives redelivery.
	for k, v := range p.Properties {
		if k == scheduledMsProperty {
			if ms, ok := asInt64(v); ok && ms != 0 {
				msg.Annotations["x-opt-delivery-time"] = ms
			}
			continue
		}
		if k == coreGroupIDProperty {
			if s, ok := v.(string); ok && s != "" {
				gid := s
				msg.Properties.GroupID = &gid
			}
			continue
		}
		if k == coreGroupSeqProperty {
			if n, ok := asInt64(v); ok && n >= 0 {
				msg.Properties.GroupSequence = uint32PtrFromInt64(n)
			}
			continue
		}
		if isInternalCoreProperty(k) {
			continue
		}
		if msg.ApplicationProperties == nil {
			msg.ApplicationProperties = map[string]any{}
		}
		msg.ApplicationProperties[k] = v
	}

	return msg, nil
}

// scheduledMsProperty is the synthetic property emitCoreFanout uses to carry a
// journal SET_SCHEDULED_DELIVERY_TIME through the store to redelivery.
const scheduledMsProperty = "_ARTX_SCHEDULED_MS"

// Core internal property keys that map onto standard AMQP message-properties
// rather than application-properties (Artemis Message.HDR_GROUP_ID /
// HDR_GROUP_SEQUENCE). coreToAMQP promotes these onto Properties so message
// grouping is preserved through the Core->AMQP redelivery conversion.
const (
	coreGroupIDProperty  = "_AMQ_GROUP_ID"
	coreGroupSeqProperty = "_AMQ_GROUP_SEQUENCE"
)

func uint32PtrFromInt64(n int64) *uint32 {
	u := uint32(n)
	return &u
}

// setCoreBody maps the Core body to the AMQP body section matching its type.
func setCoreBody(msg *amqp.Message, p *journal.CorePayload) {
	switch p.Type {
	case journal.CoreTypeText:
		if s, ok := journal.CoreTextBody(p.Body); ok {
			msg.Value = s
			return
		}
		msg.Data = [][]byte{p.Body} // malformed text: preserve raw bytes
	case journal.CoreTypeMap:
		if m, ok := journal.CoreMapBody(p.Body); ok {
			msg.Value = m
			return
		}
		msg.Data = [][]byte{p.Body}
	case journal.CoreTypeObject:
		msg.Data = [][]byte{p.Body}
		ct := "application/x-java-serialized-object"
		msg.Properties.ContentType = &ct
	default:
		// BYTES, DEFAULT, STREAM and anything else: raw body as a Data section.
		msg.Data = [][]byte{p.Body}
	}
}

func jmsTypeFor(coreType byte) int {
	switch coreType {
	case journal.CoreTypeText:
		return jmsTextMessageType
	case journal.CoreTypeBytes:
		return jmsBytesMessageType
	case journal.CoreTypeMap:
		return jmsMapMessageType
	case journal.CoreTypeObject:
		return jmsObjectMessageType
	case journal.CoreTypeStream:
		return jmsStreamMessageType
	default:
		return jmsMessageType
	}
}

// isInternalCoreProperty reports whether key is an Artemis-internal core
// property that should not be re-exposed as an AMQP application-property.
func isInternalCoreProperty(key string) bool {
	return strings.HasPrefix(key, "_AMQ") || strings.HasPrefix(key, "__AMQ")
}

func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int16:
		return int64(n), true
	case int:
		return int64(n), true
	default:
		return 0, false
	}
}
