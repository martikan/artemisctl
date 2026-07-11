package brokertest

import "testing"

// TestSharedBoots smoke-tests the shared broker helper: it returns a usable
// endpoint and credentials (and, via Reuse, attaches to the one shared container
// the rest of the integration suite uses).
func TestSharedBoots(t *testing.T) {
	if testing.Short() {
		t.Skip("skip integration in -short")
	}
	c := Shared(t)
	if c.URL == "" {
		t.Fatal("Shared returned empty URL")
	}
	if c.Username == "" || c.Password == "" {
		t.Fatalf("Shared returned empty credentials: %+v", c)
	}
}
