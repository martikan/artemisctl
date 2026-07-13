package journal

import (
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/martikan/artemisctl/internal/store"
)

// coreFixtureDir extracts testdata/artemis-2.42-core-data.tar.gz — a data dir
// harvested from a real Artemis 2.42.0 broker that was fed Core-protocol
// messages (3 standard BYTES messages on salvage.core + 1 300 KiB large
// message on salvage.corelarge, via `artemis producer --protocol CORE`).
func coreFixtureDir(t *testing.T) string {
	t.Helper()
	tarball := filepath.Join("testdata", "artemis-2.42-core-data.tar.gz")
	f, err := os.Open(tarball)
	if err != nil {
		if os.IsNotExist(err) {
			t.Skipf("core fixture %s missing", tarball)
		}
		t.Fatalf("open core fixture: %v", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	defer gz.Close()
	dst := t.TempDir()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read tar: %v", err)
		}
		name := filepath.Clean(hdr.Name)
		if strings.HasPrefix(name, "..") || filepath.IsAbs(name) {
			t.Fatalf("unsafe path %q", hdr.Name)
		}
		path := filepath.Join(dst, name)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(path, 0o755); err != nil {
				t.Fatalf("mkdir %s: %v", name, err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatalf("mkdir parent %s: %v", name, err)
			}
			out, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				t.Fatalf("create %s: %v", name, err)
			}
			if _, err := io.Copy(out, tr); err != nil { //nolint:gosec // trusted committed fixture
				out.Close()
				t.Fatalf("copy %s: %v", name, err)
			}
			out.Close()
		}
	}
	return filepath.Join(dst, "data")
}

// TestSalvageCoreFixtureEndToEnd runs the whole offline pipeline over a real
// Core-message data dir and asserts every Core message is decoded, exported as
// a KindCore store record, its large body joined, and no Core skips remain.
func TestSalvageCoreFixtureEndToEnd(t *testing.T) {
	dir := coreFixtureDir(t)
	opts := Options{
		Bindings:      filepath.Join(dir, "bindings"),
		Journal:       filepath.Join(dir, "journal"),
		LargeMessages: filepath.Join(dir, "large-messages"),
		Paging:        filepath.Join(dir, "paging"),
	}

	var recs []store.Record
	summary, err := Salvage(opts, func(r store.Record) error {
		recs = append(recs, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Salvage: %v", err)
	}

	// 3 standard salvage.core + 1 salvage.corelarge = 4 Core messages, each on
	// exactly one queue.
	if summary.Core != 4 {
		t.Errorf("summary.Core = %d, want 4", summary.Core)
	}
	if summary.HasSkips() {
		t.Errorf("unexpected skips: %v", summary.Skips)
	}

	kindCore, large := 0, 0
	byQueue := map[string]int{}
	for _, r := range recs {
		if r.Kind != store.KindCore {
			t.Errorf("record on %s has kind %d, want KindCore", r.Queue, r.Kind)
			continue
		}
		kindCore++
		byQueue[r.Queue]++
		p, derr := DecodeCorePayload(r.CorePayload)
		if derr != nil {
			t.Fatalf("DecodeCorePayload for %s: %v", r.Queue, derr)
		}
		if p.Type != CoreTypeBytes {
			t.Errorf("%s: Core.Type = %d, want BYTES", r.Queue, p.Type)
		}
		if p.Large {
			large++
			if len(p.Body) != 307200 {
				t.Errorf("large body len = %d, want 307200", len(p.Body))
			}
		} else if len(p.Body) != 120 {
			t.Errorf("%s: standard body len = %d, want 120", r.Queue, len(p.Body))
		}
	}
	if kindCore != 4 {
		t.Errorf("KindCore records = %d, want 4", kindCore)
	}
	if large != 1 {
		t.Errorf("large core records = %d, want 1", large)
	}
	if byQueue["salvage.core"] != 3 {
		t.Errorf("salvage.core count = %d, want 3", byQueue["salvage.core"])
	}
	if byQueue["salvage.corelarge"] != 1 {
		t.Errorf("salvage.corelarge count = %d, want 1", byQueue["salvage.corelarge"])
	}
}
