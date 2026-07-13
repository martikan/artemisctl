package broker

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestAsBrokerTooOld(t *testing.T) {
	if got := asBrokerTooOld(nil); got != nil {
		t.Errorf("nil err mapped to %v, want nil", got)
	}
	tooOld := errors.New(`broker rejected broker.addAddressSettings: there is no operation addAddressSettings/2`)
	if got := asBrokerTooOld(tooOld); !errors.Is(got, ErrBrokerTooOld) {
		t.Errorf("too-old rejection mapped to %v, want ErrBrokerTooOld", got)
	}
	other := errors.New("connection reset")
	if got := asBrokerTooOld(other); got != other {
		t.Errorf("unrelated error mapped to %v, want passthrough", got)
	}
}

func TestWithCordonPolicyOverridesAndPreserves(t *testing.T) {
	in := `{"DLA":"DLQ","expiryAddress":"ExpiryQueue","addressFullMessagePolicy":"PAGE","maxSizeBytes":-1,"pageSizeBytes":10485760,"maxSizeBytesRejectThreshold":-1,"maxDeliveryAttempts":10}`
	out, err := withCordonPolicy(in)
	if err != nil {
		t.Fatalf("withCordonPolicy: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	// overridden
	if m["addressFullMessagePolicy"] != "FAIL" {
		t.Errorf("addressFullMessagePolicy = %v, want FAIL", m["addressFullMessagePolicy"])
	}
	if m["maxSizeBytes"].(float64) != cordonMaxSizeBytes {
		t.Errorf("maxSizeBytes = %v, want %d", m["maxSizeBytes"], cordonMaxSizeBytes)
	}
	if m["pageSizeBytes"].(float64) != cordonPageSizeBytes {
		t.Errorf("pageSizeBytes = %v, want %d", m["pageSizeBytes"], cordonPageSizeBytes)
	}
	if m["maxSizeBytesRejectThreshold"].(float64) != cordonRejectThreshold {
		t.Errorf("maxSizeBytesRejectThreshold = %v, want %d", m["maxSizeBytesRejectThreshold"], cordonRejectThreshold)
	}
	// preserved
	if m["DLA"] != "DLQ" {
		t.Errorf("DLA not preserved: %v", m["DLA"])
	}
	if m["expiryAddress"] != "ExpiryQueue" {
		t.Errorf("expiryAddress not preserved: %v", m["expiryAddress"])
	}
	if m["maxDeliveryAttempts"].(float64) != 10 {
		t.Errorf("maxDeliveryAttempts not preserved: %v", m["maxDeliveryAttempts"])
	}
}

func TestWithCordonPolicyRejectsBadJSON(t *testing.T) {
	if _, err := withCordonPolicy(`not json`); err == nil {
		t.Fatal("expected error for invalid settings JSON")
	}
}

func TestWithUncordonPolicyReversesCordonAndPreserves(t *testing.T) {
	// Start from a cordoned settings object; uncordon must undo exactly the
	// cordon fields and preserve the rest.
	cordoned, err := withCordonPolicy(`{"DLA":"DLQ","expiryAddress":"ExpiryQueue","addressFullMessagePolicy":"PAGE","maxSizeBytes":-1,"pageSizeBytes":10485760,"maxSizeBytesRejectThreshold":-1,"maxDeliveryAttempts":10}`)
	if err != nil {
		t.Fatalf("withCordonPolicy: %v", err)
	}
	out, err := withUncordonPolicy(cordoned)
	if err != nil {
		t.Fatalf("withUncordonPolicy: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	// reversed to permissive
	if m["addressFullMessagePolicy"] != "PAGE" {
		t.Errorf("addressFullMessagePolicy = %v, want PAGE", m["addressFullMessagePolicy"])
	}
	if m["maxSizeBytes"].(float64) != -1 {
		t.Errorf("maxSizeBytes = %v, want -1", m["maxSizeBytes"])
	}
	if m["pageSizeBytes"].(float64) != uncordonPageSizeBytes {
		t.Errorf("pageSizeBytes = %v, want %d", m["pageSizeBytes"], uncordonPageSizeBytes)
	}
	if m["maxSizeBytesRejectThreshold"].(float64) != -1 {
		t.Errorf("maxSizeBytesRejectThreshold = %v, want -1", m["maxSizeBytesRejectThreshold"])
	}
	// preserved
	if m["DLA"] != "DLQ" {
		t.Errorf("DLA not preserved: %v", m["DLA"])
	}
	if m["expiryAddress"] != "ExpiryQueue" {
		t.Errorf("expiryAddress not preserved: %v", m["expiryAddress"])
	}
	if m["maxDeliveryAttempts"].(float64) != 10 {
		t.Errorf("maxDeliveryAttempts not preserved: %v", m["maxDeliveryAttempts"])
	}
}

func TestWithUncordonPolicyRejectsBadJSON(t *testing.T) {
	if _, err := withUncordonPolicy(`not json`); err == nil {
		t.Fatal("expected error for invalid settings JSON")
	}
}
