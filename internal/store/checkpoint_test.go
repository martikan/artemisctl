// internal/store/checkpoint_test.go
package store

import (
	"path/filepath"
	"testing"
)

func TestCheckpointRoundTrip(t *testing.T) {
	sp := filepath.Join(t.TempDir(), "s.artx")
	if off, err := LoadCheckpoint(sp); err != nil || off != 0 {
		t.Fatalf("empty checkpoint want 0, got %d err %v", off, err)
	}
	if err := SaveCheckpoint(sp, 12345); err != nil {
		t.Fatal(err)
	}
	off, err := LoadCheckpoint(sp)
	if err != nil || off != 12345 {
		t.Fatalf("want 12345 got %d err %v", off, err)
	}
}
