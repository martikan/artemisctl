// internal/cli/produce_test.go
package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestProduceRejectsInvalidWorkers(t *testing.T) {
	root := NewRootCmd()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"produce", "--queue", "q", "--workers", "0"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "--workers") {
		t.Fatalf("want --workers validation error, got %v", err)
	}
}
