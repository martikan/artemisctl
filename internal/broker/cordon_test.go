package broker

import (
	"encoding/json"
	"testing"
)

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
